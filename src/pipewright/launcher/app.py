"""Launcher web app (Cloud Run). Platform-owned; one per environment, shared by every tenant.

Environment:
  MANIFEST_URI       gs://<artifacts>/pipewright/environment.json   (required)
  POINTER_TTL        seconds to cache contract pointers (default 60)

Routes:
  POST /pubsub   GCS landing events (Pub/Sub push)  -> 2xx ack, 5xx retry
  POST /run      {"usecase", "schedule"?, "pipeline"?, "source_uri"?, "run_key"?, "release"?}   schedules and API runs
  POST /pubsub/run   the same body as /run, in a Pub/Sub push envelope (manifest launcher.run_requests_topic):
                     how pipelines inside a private perimeter request runs  -> 2xx ack, 5xx retry
  POST /retraining/propose   {"usecase", "run_id", "observation", "champion"}: drift or performance from an
                     inference run opens or updates a retrain decision case (also as kind retrain.proposal on
                     /pubsub/run)
  POST /retraining/decide    a person's decision: a Pub/Sub push from an approver group's subscription (the message
                     carries the person's ID token), or a direct call by the deciding identity (also as kind
                     retrain.decision on /pubsub/run, with the person's "id_token": a private launcher accepts no
                     direct calls, so a decision no approver group has to make arrives there)
  POST /retraining/tick      {"digest"?}: snooze and decline expiry, escalation, the weekly digest (Cloud Scheduler);
                     also removes Ray clusters whose run ended or whose deadline passed, and records run.failed
                     for each run that ended failed or cancelled (Pipewright 1.2.0)
  POST /ray/clusters {"action": "create" | "delete", "usecase", "run_id", "step"}: a Ray step's cluster, from the use
                     case's runner identity (also as kind ray.cluster on /pubsub/run, with the runner's "id_token")
  POST /online/lookup-service {"usecase", "release"?}: create or update the use case's Cloud SQL lookup service from
                     its live release (FR-104; deploy-model calls it). A schedule with pipeline online_sync runs the
                     tenant's sync job instead of a pipeline
  POST /models/register {"usecase", "run_id", "display_name", "artifact_uri", "serving_image", "evaluation", ...}:
                     a training run's model version, registered by the launcher after its checks (Pipewright 1.4.3,
                     FR-100, with policies.registration: launcher); also as kind model.registration on /pubsub/run,
                     with the runner's "id_token". The answer is also written to runs/registration/<tenant>/
  POST /incidents   a Cloud Monitoring incident of a health policy (Pub/Sub push from the incident channel's topic):
                     serving.degraded when it opens, serving.recovered when it closes (Pipewright 1.4.3, FR-98)
  GET  /health   live use cases and their releases (Cloud Run reserves paths ending in "z", e.g. /healthz)
"""

from __future__ import annotations

import json
import logging
import os

from flask import Flask, jsonify, request

from pipewright import adapters
from pipewright.adapters.vertex import AlreadySubmitted
from pipewright.gcp import Gcp
from pipewright.launcher.registry import Registry
from pipewright.launcher.routing import (
    BadRequest,
    Ignored,
    job_id,
    labels_for,
    landing_pipeline,
    parameters_for,
    parse_pubsub,
    parse_run_request,
    resolve_run,
    route_file,
    run_id,
    target_for,
)
from pipewright.models.registration import RegistrationRefused, Registrations, kfp_job_finder
from pipewright.observability import events
from pipewright.online import launcher as online_launcher
from pipewright.ray_clusters.service import RayClusters, RayRequestError
from pipewright.release.contract import ReleaseContract
from pipewright.retraining import identity
from pipewright.retraining.service import Decisions
from pipewright.retraining.store import CaseStore

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("pipewright.launcher")


def create_app(registry: Registry | None = None) -> Flask:
    app = Flask(__name__)
    state = {"registry": registry}

    def reg() -> Registry:
        if state["registry"] is None:
            state["registry"] = Registry(Gcp(), os.environ["MANIFEST_URI"], int(os.environ.get("POINTER_TTL", "60")))
        return state["registry"]

    def _event(event_type: str, contract: ReleaseContract, rid: str | None, attributes: dict | None = None,
               **data) -> None:
        topic = (reg().manifest.get("observability") or {}).get("events_topic")
        events.emit(events.event(event_type, contract.usecase, contract.environment, contract.tenant,
                                 contract.release_id, rid, **data), topic, getattr(reg(), "gcp", None),
                    region=contract.region, attributes=attributes)

    def _policy_env(contract: ReleaseContract):
        from types import SimpleNamespace

        return SimpleNamespace(name=contract.environment, policies=reg().manifest.get("policies") or {})

    def start(contract: ReleaseContract, pipeline: str, source_uri: str, rid: str, trigger: str,
              hint: str = "", extra: dict | None = None, loop: str | None = None, ml: dict | None = None,
              recorded: dict | None = None) -> dict:
        target = target_for(contract, pipeline)
        jid = job_id(contract.usecase, rid, hint if pipeline == "training" else f"{pipeline}-{hint}")
        params_extra: dict = {}
        if pipeline == "monitoring" and not (extra or {}).get("model_version"):
            # GA QA: online monitoring judges the version that serves, not the registry's default, which can move
            # without a deploy (promote_model auto): judging the default refused the run (that version was never
            # approved) and would have read traffic it never received.
            try:
                from pipewright.config.environment import environment_from_dict
                from pipewright.models import watch as served_watch

                ml_cfg = (contract.static_parameters or {}).get("ml_config") or {}
                served = served_watch.served_version(getattr(reg(), "gcp", None) or Gcp(),
                                                     environment_from_dict(reg().manifest), contract.usecase,
                                                     ml_cfg.get("model_name") or "")
            except Exception:  # noqa: BLE001 - not readable: the default, as before
                log.exception("served version of %s not readable", contract.usecase)
                served = None
            if served:
                extra = {**(extra or {}), "model_version": served}
        if pipeline in ("inference", "monitoring"):         # 1.4.3 (FR-98): online monitoring loads it too
            # Phase 6 (policies.integrity.inference, off by default): score only with an approved version.
            from pipewright.models import integrity

            # 1.4.1 (FR-101 item 1): the full check of the version the run will use, and with pin on (the default)
            # the approved files passed to the run, which loads exactly those generations and checks each SHA-256.
            gate = integrity.inference_gate(getattr(reg(), "gcp", None), reg().manifest, contract,
                                            (extra or {}).get("model_version") or None)
            mode, problem = gate["mode"], gate["problem"]
            if problem:
                _event("run.integrity", contract, rid, pipeline=pipeline, trigger=trigger, mode=mode,
                       problem=problem, **({"version": gate["version"]} if gate["version"] else {}))
                if mode == "refuse":
                    log.warning("refused %s %s: %s", contract.usecase, pipeline, problem)
                    return {"usecase": contract.usecase, "pipeline": pipeline, "status": "refused",
                            "reason": problem}
                log.warning("integrity (warn) %s %s: %s", contract.usecase, pipeline, problem)
            if gate["approved_files"] and "approved_files" in target.parameters:
                params_extra = {"approved_files": json.dumps(gate["approved_files"], sort_keys=True)}
                recorded = {**(recorded or {}), "pinned": {"version": gate["version"],
                                                           "files": len(gate["approved_files"]["files"])}}
            elif gate["approved_files"]:
                log.warning("integrity: %s %s does not declare approved_files, so it loads the live model folder: "
                            "rebuild the release with Pipewright 1.4.1 to pin the approved files",
                            contract.usecase, pipeline)
            # FR-57: a running A/B test scores each unit with its arm's model (pipelines that declare ab_test).
            if "ab_test" in target.parameters and not (extra or {}).get("model_version"):
                from pipewright.abtest import scoring as ab_scoring

                try:
                    ab_param, ab_record = ab_scoring.param(getattr(reg(), "gcp", None), reg().manifest, contract)
                except Exception as e:  # noqa: BLE001 - a test that cannot be read scores everyone with the control
                    ab_param, ab_record = None, {"problem": f"the A/B test cannot be read: {str(e)[:200]}"}
                if ab_param:
                    params_extra["ab_test"] = ab_param
                if ab_record:
                    recorded = {**(recorded or {}), "ab_test": ab_record}
                    if ab_record.get("problem"):
                        _event("run.integrity", contract, rid, pipeline=pipeline, trigger=trigger,
                               mode="refuse" if not ab_param else "warn", problem=ab_record["problem"], check="ab_test")
        # 1.4.1 (FR-103): with a feature contract, a training run's definitions are checked first (features check
        # on), and run.submitted records the contract and, for scoring with record_reads, what the run reads.
        from pipewright.models import integrity as _integrity

        try:
            fg = _integrity.feature_gate(getattr(reg(), "gcp", None), reg().manifest, contract, pipeline, ml, rid)
        except Exception as e:  # noqa: BLE001 - a check that cannot run is a problem, not a pass
            pol = _integrity.policy(_policy_env(contract))
            fg = {"mode": "off", "problem": None, "record": {}}
            if pol["features"]["check"] and pipeline == "training":
                fg = {"mode": pol["mode"], "record": {},
                      "problem": f"the feature definitions cannot be checked: {str(e).splitlines()[0][:200]}"}
        if fg["problem"]:
            _event("run.integrity", contract, rid, pipeline=pipeline, trigger=trigger, mode=fg["mode"],
                   problem=fg["problem"], check="features")
            if fg["mode"] == "refuse":
                log.warning("refused %s %s: %s", contract.usecase, pipeline, fg["problem"])
                return {"usecase": contract.usecase, "pipeline": pipeline, "status": "refused",
                        "reason": fg["problem"]}
            log.warning("integrity (warn) %s %s: %s", contract.usecase, pipeline, fg["problem"])
        if fg["record"]:
            recorded = {**(recorded or {}), **fg["record"]}
        try:
            params = parameters_for(contract, target, source_uri, rid, {**(extra or {}), **params_extra,
                                                                        **({"ml_config": ml} if ml else {})})
            name = adapters.submit(contract, target, params, jid,
                                   labels_for(contract, trigger, rid, pipeline, loop), reg().manifest.get("security"),
                                   manifest=reg().manifest, gcp=getattr(reg(), "gcp", None))
            log.info("submitted %s (%s %s, release %s) for %s", name, contract.usecase, pipeline,
                     contract.release_id, source_uri)
            _event("run.submitted", contract, rid, pipeline=pipeline, trigger=trigger, job_id=jid,
                   source_uri=source_uri, job=name, **(extra or {}), **({"loop": loop} if loop else {}),
                   **(recorded or {}))
            return {"usecase": contract.usecase, "pipeline": pipeline, "status": "submitted", "job_id": jid,
                    "run_id": rid, "release": contract.release_id}
        except AlreadySubmitted:
            log.info("duplicate %s for %s", jid, source_uri)
            _event("run.duplicate", contract, rid, pipeline=pipeline, trigger=trigger, job_id=jid)
            return {"usecase": contract.usecase, "pipeline": pipeline, "status": "duplicate", "job_id": jid}

    # ---------------------------------------------------------------------------------- retrain decisions --
    def launcher_url() -> str:
        return ((reg().manifest.get("launcher") or {}).get("url") or request.url_root).rstrip("/")

    def audiences() -> list[str]:
        """Every name the launcher answers to: its URL and launcher.audiences (its Cloud Run URL and its name in the
        cluster), so steps of releases built under either runtime keep working after the platform switches."""
        extra = (reg().manifest.get("launcher") or {}).get("audiences") or []
        return list(dict.fromkeys([launcher_url(), *(a.rstrip("/") for a in extra)]))

    def decisions() -> Decisions:
        if "decisions" not in state:
            r = reg()

            def submit(contract, pipeline, source_uri, run_key, trigger):
                return start(contract, pipeline, source_uri, run_id(contract.usecase, trigger, run_key), trigger,
                             trigger)

            def emit(event_type, contract, data, attributes):
                _event(event_type, contract, None, attributes=attributes, **data)

            def verify(token):
                return identity.verify(token, [*audiences(), identity.GCLOUD_CLIENT_ID])

            def champion_info(contract, champion):
                name, version = champion.get("resource_name"), champion.get("version")
                if not name or not version:
                    return {}
                resp = r.gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/{name}@{version}")
                return {"created": resp.body.get("versionCreateTime")} if resp.ok else {}

            def job_state(contract, job_id):
                return adapters.job_state(contract, job_id, reg().manifest, getattr(r, "gcp", None))

            def version_for_run(contract, run):
                """The registry version a training run registered (container and BigQuery ML labels)."""
                name = (contract.metadata or {}).get("model")
                if not name:
                    return None
                api = f"https://{contract.region}-aiplatform.googleapis.com/v1"
                found = r.gcp.get(f"{api}/projects/{contract.project}/locations/{contract.region}/models",
                                  filter=f'display_name="{name}"')
                part = run.lower().replace("-", "_")
                for m in (found.body or {}).get("models", []) if found.ok else []:
                    listed = r.gcp.get(f"{api}/{m['name']}:listVersions", pageSize=100)
                    for v in (listed.body or {}).get("models", []) if listed.ok else []:
                        labels = v.get("labels") or {}
                        if labels.get("pipewright-run") in (run, part) or labels.get("run_id") == f"run-{run}":
                            return str(v["versionId"])
                return None

            def reasons_for_job(contract, job_id):
                return adapters.job_reasons(contract, job_id, getattr(r, "gcp", None))

            state["decisions"] = Decisions(CaseStore(r.gcp, r.bucket), lambda: reg().manifest, submit, emit,
                                           verify=verify, champion_info=champion_info, job_state=job_state,
                                           version_for_run=version_for_run, reasons_for_job=reasons_for_job)
        return state["decisions"]

    def loop_on(contract: ReleaseContract) -> bool:
        """retraining.loop.enabled for the release here (read from its ml_config; no case store needed)."""
        from pipewright.retraining import cases as rc
        from pipewright.retraining.service import ml_config

        policy = ((reg().manifest.get("policies") or {}).get("retraining")) or {}
        return rc.Settings.from_config(ml_config(contract).get("retraining") or {}, policy).loop

    def via_loop(contract: ReleaseContract, rid: str, trigger: str, source_uri: str, **detail) -> dict:
        """The closed retrain loop (D-104): this environment never trains. A scheduled or landing training run
        becomes a retrain case (or joins the open one), which the loop trains where releases come from."""
        result = decisions().propose(contract, {"run_id": rid, "observation": {
            "triggers": [trigger], "source_uri": source_uri, **detail}})
        log.info("%s training of %s through the closed retrain loop: %s", trigger, contract.usecase, result)
        return {**result, "usecase": contract.usecase, "loop": True}

    def _propose(body: dict):
        contract = reg().get(body.get("usecase") or "")
        if contract is None:
            return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 404
        try:
            result = decisions().propose(contract, body)
        except Exception as e:  # noqa: BLE001 - retried (Pub/Sub) or reported to the component
            log.exception("proposal for %s failed", contract.usecase)
            return jsonify(status="error", reason=str(e)[:300]), 500
        log.info("retrain proposal for %s: %s", contract.usecase, result)
        return jsonify(result), 200

    @app.post("/retraining/propose")
    def retraining_propose():
        return _propose(request.get_json(silent=True) or {})

    @app.post("/retraining/decide")
    def retraining_decide():
        body = request.get_json(silent=True) or {}
        if request.environ.get("pipewright.pulled"):
            # Pulled from the subscription by the launcher itself (launcher.runtime gke): the same trust as a push.
            caller = (reg().manifest.get("launcher") or {}).get("service_account", "").lower()
        else:
            caller = identity.cloud_run_caller(request.headers.get("Authorization"))
        if "message" in body:
            # Pushed from an approver group's subscription: only Pub/Sub (as the launcher identity) may push.
            if caller != (reg().manifest.get("launcher") or {}).get("service_account", "").lower():
                log.warning("decision push from %r refused: not the launcher identity", caller)
                return jsonify(status="refused", reason="pushes must come from Pub/Sub"), 403
            groups = {v["subscription"]: k for k, v in (reg().manifest.get("approver_groups") or {}).items()}
            group = groups.get(body.get("subscription"))
            try:
                message = parse_run_request(body)
            except BadRequest as e:
                return jsonify(status="dropped", reason=str(e)), 200
            if group is None:
                log.warning("decision from unregistered subscription %s dropped", body.get("subscription"))
                return jsonify(status="dropped", reason="not an approver group subscription"), 200
            via = f"topic:{group}"
        else:
            if not caller:
                return jsonify(status="refused", reason="no caller identity"), 401
            message, group, via = body, None, "direct"
        contract = reg().get(message.get("usecase") or "")
        if contract is None:
            return jsonify(status="error", reason=f"{message.get('usecase')}: no live release"), (
                503 if via != "direct" else 404)
        if message.get("kind") == "ab.decision":          # FR-57: an approver group's A/B decision
            return _ab_decision(contract, message, group, via, caller)
        try:
            result = decisions().decide(contract, message, group, via, by=caller if via == "direct" else None)
        except Exception as e:  # noqa: BLE001
            log.exception("decision on %s failed", message.get("case_id"))
            return jsonify(status="error", reason=str(e)[:300]), 500
        log.info("retrain decision for %s: %s", contract.usecase, {k: v for k, v in result.items() if k != "training"})
        return jsonify(result), (409 if via == "direct" and result["status"] == "refused" else 200)

    def _ab_decision(contract, message: dict, group: str | None, via: str, caller: str | None):
        """FR-57: the group's decision on the use case's running A/B test, recorded on the test (the launcher is the
        only writer of decisions). The person is the ID token's identity, verified as for retrain decisions."""
        from datetime import datetime, timezone

        from pipewright.abtest import decide as ab_decide
        from pipewright.abtest.store import TestStore

        if group is None:
            return jsonify(status="refused", reason="A/B decisions come from an approver group's topic"), 409
        try:
            # people sign with gcloud (its client is the audience), service accounts for the launcher, as retrain
            # decisions (found in the A/B battle test: a person's decision was dropped)
            by = identity.verify(message.get("id_token") or "", [*audiences(), identity.GCLOUD_CLIENT_ID])
        except identity.NotVerified as e:
            log.warning("A/B decision dropped: %s", e)
            return jsonify(status="dropped", reason=str(e)), 200
        at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        held = {}

        def change(t):
            try:
                return ab_decide.record(t, group, by, message.get("winner", ""), message.get("reason", ""), at,
                                        message.get("approval") if isinstance(message.get("approval"), dict) else None)
            except ab_decide.Refused as e:
                held["refused"] = str(e)
                return None

        store = TestStore(getattr(reg(), "gcp", None), reg().manifest["artifacts_bucket"])
        t = store.update(contract.usecase, message.get("ab_test") or "", change)
        if t is None or "refused" in held:
            why = held.get("refused") or f"no test {message.get('ab_test')} for {contract.usecase}"
            log.warning("A/B decision on %s by %s refused: %s", message.get("ab_test"), by, why)
            return jsonify(status="refused", reason=why), 200
        data = {"ab_test": t["name"], "group": group, "by": by, "winner": message.get("winner"),
                "reason": message.get("reason"), "decided": (t.get("decision") or {}).get("winner")}
        _event("ab_test.decision", contract, None, **data)
        try:                          # the team's feed too (found in the QA edge-case review: it never got it)
            from pipewright.abtest import service as ab_service
            from pipewright.abtest import watch as ab_watch
            from pipewright.config.environment import environment_from_dict

            env = environment_from_dict(reg().manifest)
            ab_service.to_feed(env, ab_watch.released_usecase(contract), getattr(reg(), "gcp", None),
                               "ab_test.decision", t,
                               events.event("ab_test.decision", contract.usecase, contract.environment,
                                            contract.tenant, contract.release_id, None, **data))
        except Exception:  # noqa: BLE001 - the decision is recorded; the feed is a copy
            log.exception("A/B decision not copied to the feed")
        return jsonify(status="recorded", ab_test=t["name"], group=group, by=by,
                       winner=(t.get("decision") or {}).get("winner")), 200

    @app.post("/retraining/tick")
    def retraining_tick():
        body = request.get_json(silent=True) or {}
        digest = bool(body.get("digest")) or request.args.get("digest") in ("1", "true")
        results = decisions().tick(reg().live(), digest=digest)
        m = reg().manifest
        ray = rays().reap(m["project"], m["region"]) if m.get("project") and m.get("region") else []
        try:
            watched = integrity_watch(force=False)
        except Exception as e:  # noqa: BLE001 - the watch must not stop the tick; its heartbeat goes missing instead
            log.exception("integrity watch failed")
            watched = {"status": "error", "reason": str(e)[:300]}
        try:                                  # FR-104: stale syncs and billed BigQuery lookups
            from pipewright.online import watch as online_watch

            online = online_watch.tick(Gcp(), m, reg().live(),
                                       lambda t, c, data: _event(t, c, None, None, **data))
        except Exception as e:  # noqa: BLE001
            log.exception("online store health not checked")
            online = {"status": "error", "reason": str(e)[:300]}
        try:                                  # FR-57: results in, the serving guardrails, the scheduled analysis
            from pipewright.abtest import watch as ab_watch
            from pipewright.online import launcher as online_launcher

            ab = ab_watch.tick(Gcp(), m, reg().live(), refresh=lambda c: online_launcher.ensure_lookup_service(
                c, reg().manifest, Gcp(), wait=240))
        except Exception as e:  # noqa: BLE001
            log.exception("A/B tests not checked")
            ab = {"status": "error", "reason": str(e)[:300]}
        try:                                  # 1.4.3 (FR-98): capture coverage
            covered = capture_coverage()
        except Exception as e:  # noqa: BLE001
            log.exception("capture coverage not checked")
            covered = {"status": "error", "reason": str(e)[:300]}
        return jsonify(results=results, ray=ray, failed=failed_runs(), integrity=watched, online=online,
                       ab=ab, coverage=covered), 200

    def capture_coverage() -> list:
        """serving.capture_gap and serving.capture_silent for live use cases with capture on (FR-98)."""
        from pipewright.serving import capture, coverage

        m, out, gcp = reg().manifest, [], Gcp()
        for c in reg().live():
            ml = (c.static_parameters or {}).get("ml_config") or {}
            cap = ml.get("serving_capture") or {}
            if not cap.get("enabled"):
                continue
            rate = float(cap.get("sample_rate", 0.05))
            dataset = (ml.get("platform") or {}).get("bigquery_dataset")
            if c.orchestrator == "kfp":
                ns = ((m.get("kserve") or {}).get("namespaces") or {}).get(c.tenant)
                isvc = c.usecase.lower().replace("_", "-").replace(".", "-")[:45].rstrip("-")

                def prom(q, project=c.project):
                    r = gcp.post(f"https://monitoring.googleapis.com/v1/projects/{project}/location/global/"
                                 "prometheus/api/v1/query", {"query": q})
                    res = ((r.body or {}).get("data") or {}).get("result") or [] if r.ok else []
                    return float(res[0]["value"][1]) if res else None

                found = coverage.kserve(prom, ns, isvc, rate) if ns else None
            else:
                api = f"https://{c.region}-aiplatform.googleapis.com/v1/projects/{c.project}/locations/{c.region}"
                eps = gcp.get(f"{api}/endpoints", filter=f'display_name="{c.usecase}-endpoint"')
                ep = ((eps.body or {}).get("endpoints") or [None])[0] if eps.ok else None
                found = (coverage.agent_platform(gcp, c.project, c.region, ep["name"].rsplit("/", 1)[-1],
                                                f"{c.project}.{dataset}.{capture.table_name(c.usecase)}",
                                                rate, kms_key=(m.get("security") or {}).get("bigquery_kms_key"))
                         if ep and dataset else None)
            if found:
                _event(found.pop("event"), c, None, **found)
                out.append({"usecase": c.usecase, **found})
        return out

    @app.post("/integrity/tick")
    def integrity_tick():
        """The watch now, whatever its interval (Pipewright 1.4.1): for operators and tests."""
        return jsonify(integrity_watch(force=True)), 200

    def feature_groups(live) -> dict:
        """FR-103: {feature group: use case} for live releases with a feature contract (the watch checks the feature
        views serving them)."""
        from pipewright.retraining.service import ml_config

        out = {}
        for c in live:
            try:
                cfg = ml_config(c)
            except Exception:  # noqa: BLE001 - a release without ml_config has no contract
                continue
            if not cfg.get("feature_contract"):
                continue
            for src in ((cfg.get("data") or {}).get("features_from") or {}).get("sources") or []:
                if src.get("feature_group"):
                    out[str(src["feature_group"])] = c.usecase
        return out

    def integrity_watch(force: bool) -> dict:
        """Pipewright 1.4.1 (FR-101 items 4 and 5): with policies.integrity.watch on, every default and serving
        version re-checked against its approval; model.integrity when a result changes, and a model.integrity.check
        heartbeat every run (its absence alerts: a broken watch must not look like "unchanged")."""
        from datetime import datetime, timezone

        from pipewright.config.environment import environment_from_dict
        from pipewright.gcp import PreconditionFailed, gcs_read, gcs_write
        from pipewright.models import approvals, integrity
        from pipewright.models import watch as iwatch
        from pipewright.observability import events

        live = reg().live()                   # refreshes the manifest too (found live: a policy change was read late)
        m = reg().manifest
        if not m.get("artifacts_bucket") or not m.get("environment"):
            return {"status": "skipped", "reason": "no environment in the manifest"}
        env = environment_from_dict(m)
        pol = integrity.policy(env)
        if not pol["watch"] and not force:
            return {"status": "off"}
        gcp = getattr(reg(), "gcp", None) or Gcp()
        bucket, name = m["artifacts_bucket"], f"runs/cache/integrity-{env.name}.json"
        approvals.use_store(bucket, f"runs/cache/approvals-{env.name}.json")
        text, gen = gcs_read(gcp, bucket, name)
        state = json.loads(text) if text else {}
        now = datetime.now(timezone.utc)
        last = state.get("last_run")
        due_in = pol["watch_every_hours"] * 3600 - 300
        if not force and last and (now - datetime.fromisoformat(last)).total_seconds() < due_in:
            return {"status": "not due", "last_run": last}
        # Claim the run before checking (found live in 1.4.1: a tick cut off by Cloud Run's request timeout kept going
        # while the scheduler's retry ran, and both emitted the same transition): the first to write last_run checks.
        state["last_run"] = now.isoformat()
        try:
            gen = gcs_write(gcp, bucket, name, json.dumps(state), if_generation_match=gen)
        except PreconditionFailed:
            log.info("integrity watch: another run is under way")
            return {"status": "skipped", "reason": "another watch run is under way"}
        models = {(c.metadata or {}).get("model"): c.usecase for c in live if (c.metadata or {}).get("model")}
        groups = feature_groups(live) if pol["features"]["check"] else {}
        result = iwatch.run(gcp, env, state, models, groups)
        topic = (m.get("observability") or {}).get("events_topic")
        if pol["lock"]["days"] > 0:
            # The cross-identity check: this read-only identity watches what the renewer (or anyone) did to the
            # protected files' holds and metadata (1.4.1, section 8.18).
            from pipewright.models import protection

            try:
                result["changes"] += protection.observe(gcp, env, state, models)
            except Exception as e:  # noqa: BLE001 - said in the heartbeat, never silent
                result["heartbeat"]["notes"].append(f"protection not observed: {str(e)[:160]}")
        if pol["mode"] == "warn" or pol["expired_warn"]:
            # 1.4.1 (FR-102): a time-boxed warn is recorded while it lasts (alerted), and when it has expired.
            events.emit(events.event("policy.override", None, env.name, None, None, None,
                                     status="expired" if pol["expired_warn"] else "active", until=pol["until"],
                                     reason=pol["reason"], open_ended=not pol["until"]), topic, gcp,
                        region=env.region)
        for change in result["changes"]:
            events.emit(events.event("model.integrity", change.get("usecase"), env.name, None, None, None,
                                     **{k: v for k, v in change.items() if k != "usecase"}),
                        topic, gcp, region=env.region)
        events.emit(events.event("model.integrity.check", None, env.name, None, None, None, **result["heartbeat"],
                                 every_hours=pol["watch_every_hours"]), topic, gcp, region=env.region)
        state["last_run"] = now.isoformat()
        try:
            gcs_write(gcp, bucket, name, json.dumps(state), if_generation_match=gen)
        except PreconditionFailed:
            log.info("integrity watch: another run wrote the state first")
        log.info("integrity watch: %s", result["heartbeat"])
        return {"status": "checked", **result["heartbeat"], "changes": result["changes"]}

    def failed_runs() -> list:
        """run.failed for each failed run since the last look (Pipewright 1.2.0, FR-67)."""
        from pipewright.launcher import watch

        m, r = reg().manifest, reg()
        if not m.get("project") or not m.get("region"):
            return []
        out = []
        live = r.live()
        # Agent Platform runs, then Kubeflow runs (orchestrator kfp, private-gke), each where it runs.
        for contracts, manifest in (([c for c in live if c.orchestrator != "kfp"], None),
                                    ([c for c in live if c.orchestrator == "kfp"], m)):
            if not contracts:
                continue
            try:
                out += watch.report(r.gcp, r.bucket, m["project"], m["region"], contracts,
                                    lambda contract, rid, detail: _event("run.failed", contract, rid, **detail),
                                    manifest=manifest)
            except Exception as e:  # noqa: BLE001 - a failed look must not stop the tick
                log.exception("failed runs not checked")
                out.append({"error": str(e)[:300]})
            try:                      # FR-124: approved runs that ended succeeded without registering the model
                from pipewright.launcher import unregistered

                out += unregistered.report(
                    r.gcp, r.bucket, m["project"], m["region"], contracts,
                    lambda contract, rid, detail: _event("run.unregistered", contract, rid, **detail),
                    manifest=manifest, log_bucket=(m.get("observability") or {}).get("log_bucket"))
                out += unregistered.duplicates(
                    r.gcp, r.bucket, m["project"], m["region"], contracts,
                    lambda contract, rid, detail: _event("model.duplicated", contract, rid, **detail))
            except Exception as e:  # noqa: BLE001
                log.exception("approved runs not checked for a registration")
                out.append({"error": f"registrations not checked: {str(e)[:280]}"})
        return out

    # ---------------------------------------------------------------------------------- Ray clusters --
    def rays() -> RayClusters:
        if "rays" not in state:
            state["rays"] = RayClusters(reg().gcp, lambda: reg().manifest)
        return state["rays"]

    def _ray(body: dict, caller: str | None):
        contract = reg().get(body.get("usecase") or "")
        if contract is None:
            return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 404
        action = body.get("action")
        if action not in ("create", "delete"):
            return jsonify(status="error", reason="action must be create or delete"), 400
        try:
            result = getattr(rays(), action)(contract, body.get("step") or "", body.get("run_id") or "", caller)
            if "record" in result:
                _event("ray.cluster.created", contract, body.get("run_id"), **result.pop("record"))
        except RayRequestError as e:
            log.warning("ray %s for %s refused: %s", action, contract.usecase, e)
            return jsonify(status="refused", reason=str(e)), e.status
        except Exception as e:  # noqa: BLE001 - retried by the step (or Pub/Sub)
            log.exception("ray %s for %s failed", action, contract.usecase)
            return jsonify(status="error", reason=str(e)[:300]), 500
        return jsonify(result), 200

    # ---------------------------------------------------------------- registration (1.4.3, FR-100) --
    def registrations() -> Registrations:
        if "registrations" not in state:
            r = reg()
            finder = kfp_job_finder(lambda: reg().manifest, r.gcp) if r.manifest.get("orchestrator") == "kfp" \
                else None
            state["registrations"] = Registrations(r.gcp, r.bucket, lambda: reg().manifest, finder)
        return state["registrations"]

    def _register(body: dict, caller: str | None):
        contract = reg().get(body.get("usecase") or "")
        if contract is None:
            return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 404
        rid = body.get("run_id")
        try:
            result = registrations().register(contract, body, caller)
        except RegistrationRefused as e:
            log.warning("registration for %s run %s refused: %s", contract.usecase, rid, e.reason)
            _event("model.registration_refused", contract, rid, model=body.get("display_name"), reason=e.reason,
                   caller=caller, artifact_uri=body.get("artifact_uri"))
            return jsonify(status="refused", reason=e.reason), e.status
        except Exception as e:  # noqa: BLE001 - retried by the step (or Pub/Sub)
            log.exception("registration for %s run %s failed", contract.usecase, rid)
            return jsonify(status="error", reason=str(e)[:300]), 500
        if not result.get("repeated"):
            ml = (contract.static_parameters or {}).get("ml_config") or {}
            primary = (ml.get("evaluation") or {}).get("primary_metric")
            _event("model.registered", contract, rid, model=body.get("display_name"), version=result.get("version"),
                   default=result.get("default"), primary_metric=primary,
                   primary_value=(body.get("metrics") or {}).get(primary) if primary else None,
                   files=body.get("files"), registered_by="launcher")
        return jsonify(result), 200

    # ---------------------------------------------------------------- serving health (1.4.3, FR-98) --
    def handle_incident(envelope: dict | None):
        from pipewright.serving import incidents, rollback

        try:
            incident = incidents.parse(envelope)
        except incidents.NotOurs as e:
            return jsonify(status="ignored", reason=str(e)), 200
        contract = next((c for c in reg().live() if _slug_label(c.usecase) == incident["usecase_label"]), None)
        if contract is None:
            log.warning("incident for %s: no live release", incident["usecase_label"])
            return jsonify(status="ignored", reason="no live release"), 200
        _event(incidents.event_type(incident), contract, None, **incidents.event_data(incident))
        result = {"status": "recorded", "event": incidents.event_type(incident)}
        try:
            proposal = rollback.maybe_propose(contract, incident, reg().gcp, reg().bucket)
        except Exception as e:  # noqa: BLE001 - the event is recorded; a proposal is a convenience on top
            log.exception("rollback proposal for %s", contract.usecase)
            proposal = {"error": str(e)[:200]}
        if proposal and proposal.get("case_id"):
            _event("rollback.proposed", contract, None, **proposal)
            result["rollback"] = proposal["case_id"]
        return jsonify(result), 200

    @app.post("/incidents")
    def incidents_push():
        return handle_incident(request.get_json(silent=True))

    @app.post("/models/register")
    def models_register():
        caller = identity.cloud_run_caller(request.headers.get("Authorization"))
        if not caller:
            return jsonify(status="refused", reason="no caller identity"), 401
        return _register(request.get_json(silent=True) or {}, caller)

    @app.post("/ray/clusters")
    def ray_clusters():
        caller = identity.cloud_run_caller(request.headers.get("Authorization"))
        if not caller:
            return jsonify(status="refused", reason="no caller identity"), 401
        return _ray(request.get_json(silent=True) or {}, caller)

    @app.post("/pubsub")
    def pubsub():
        try:
            event = parse_pubsub(request.get_json(silent=True))
            targets = route_file(reg().live(), event)
        except BadRequest as e:
            log.warning("dropping malformed message: %s", e)
            return jsonify(status="dropped", reason=str(e)), 200
        except Ignored as e:
            return jsonify(status="ignored", reason=str(e)), 200

        results, failed = [], False
        for contract in targets:
            rid = run_id(contract.usecase, event.bucket, event.name, event.generation)
            stem = os.path.splitext(os.path.basename(event.name))[0]
            try:
                if landing_pipeline(contract) == "training" and loop_on(contract):
                    results.append(via_loop(contract, rid, "landing", event.uri))
                    continue
                results.append(start(contract, landing_pipeline(contract), event.uri, rid, "landing", stem))
            except Exception:  # noqa: BLE001 - retried by Pub/Sub; duplicates are rejected by job ID
                failed = True
                log.exception("failed to submit %s for %s", contract.usecase, event.uri)
                results.append({"usecase": contract.usecase, "status": "error"})
        return jsonify(results=results), (500 if failed else 200)

    def _run(body: dict):
        usecase = body.get("usecase")
        if not usecase:
            return jsonify(status="error", reason="usecase is required"), 400
        contract = reg().get(usecase, release=body.get("release"))
        if contract is None:
            reason = reg().errors.get(usecase, "no live release")
            return jsonify(status="error", reason=f"{usecase}: {reason}"), 404
        sched = (contract.triggers.get("schedules") or {}).get(body.get("schedule") or "") or {}
        sync = online_launcher.SYNC_PIPELINE
        if sync in (sched.get("pipeline"), body.get("pipeline")):
            try:                                # FR-104: the tenant's sync job, not a pipeline run
                return jsonify(online_launcher.start_sync(contract, reg().manifest,
                                                          os.environ.get("MANIFEST_URI", ""), Gcp(),
                                                          body.get("schedule") or "")), 200
            except Exception as e:  # noqa: BLE001
                log.exception("online sync of %s not started", usecase)
                return jsonify(status="error", reason=str(e)[:300]), 500
        try:
            r = resolve_run(contract, body, manifest=reg().manifest)
        except BadRequest as e:
            return jsonify(status="error", reason=str(e)), 400
        if r.pipeline == "training" and r.trigger == "schedule" and loop_on(contract):
            try:
                return jsonify(via_loop(contract, r.run_id, "schedule", r.source_uri,
                                        schedule=body.get("schedule"))), 200
            except Exception as e:  # noqa: BLE001
                log.exception("scheduled retrain of %s not recorded", usecase)
                return jsonify(status="error", reason=str(e)[:300]), 500
        try:
            result = start(contract, r.pipeline, r.source_uri, r.run_id, r.trigger, body.get("schedule") or "",
                           dict(r.overrides), r.loop, r.ml_config, dict(r.recorded))
        except Exception as e:  # noqa: BLE001
            log.exception("failed to submit %s", usecase)
            return jsonify(status="error", reason=str(e)), 500
        return jsonify(result), 200

    @app.post("/run")
    def run():
        return _run(request.get_json(silent=True) or {})

    @app.post("/pubsub/run")
    def pubsub_run():
        try:
            body = parse_run_request(request.get_json(silent=True))
        except BadRequest as e:
            log.warning("dropping malformed run request: %s", e)
            return jsonify(status="dropped", reason=str(e)), 200
        if body.get("kind") == "retrain.proposal":
            response, code = _propose(body)
            return response, (503 if code == 404 else code)
        if body.get("kind") == "retrain.decision":
            # A decision no approver group has to make, from outside a private perimeter. Pub/Sub does not say who
            # published: the message carries the person's ID token, verified as for a direct call.
            contract = reg().get(body.get("usecase") or "")
            if contract is None:
                return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 503
            try:
                result = decisions().decide(contract, body, None, "run-requests")
            except Exception as e:  # noqa: BLE001 - Pub/Sub redelivers
                log.exception("decision on %s failed", body.get("case_id"))
                return jsonify(status="error", reason=str(e)[:300]), 500
            log.info("retrain decision for %s: %s", contract.usecase,
                     {k: v for k, v in result.items() if k != "training"})
            return jsonify(result), 200            # a refusal is recorded on the case: acknowledged
        if body.get("kind") == "online.lookup_service":      # FR-104, from deploy-model in a private environment
            contract = reg().get(body.get("usecase") or "", release=body.get("release"))
            if contract is None:
                return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 503
            try:
                return jsonify(online_launcher.ensure_lookup_service(contract, reg().manifest, Gcp(), wait=240)), 200
            except Exception as e:  # noqa: BLE001 - a refusal never gets better: acknowledge it
                log.exception("lookup service of %s", contract.usecase)
                return jsonify(status="dropped", reason=str(e)[:400]), 200
        if body.get("kind") == "online.lookup_service.delete":     # 1.4.2, from deploy-model --undeploy
            contract = reg().get(body.get("usecase") or "")
            if contract is None:
                return jsonify(status="dropped", reason=f"{body.get('usecase')}: no live release"), 200
            try:
                return jsonify(online_launcher.remove_lookup_service(contract, reg().manifest, Gcp())), 200
            except Exception as e:  # noqa: BLE001 - a refusal never gets better: acknowledge it
                log.exception("removing the lookup service of %s", contract.usecase)
                return jsonify(status="dropped", reason=str(e)[:400]), 200
        if body.get("kind") == "model.registration":          # 1.4.3 (FR-100), from a private register step
            try:
                caller = identity.verify(body.get("id_token") or "", audiences())
            except identity.NotVerified as e:
                log.warning("registration request for %s dropped: %s", body.get("usecase"), e)
                return jsonify(status="dropped", reason=str(e)), 200
            response, code = _register(body, caller)
            # A refusal is answered in runs/registration/ and never gets better: acknowledge. No live release or a
            # failed upload: let Pub/Sub retry (a repeated request finds the version it registered).
            return response, (200 if code in (400, 403) else 503 if code == 404 else code)
        if body.get("kind") == "ray.cluster":
            # Pub/Sub does not say who published: the message carries the runner's ID token.
            try:
                caller = identity.verify(body.get("id_token") or "", audiences())
            except identity.NotVerified as e:
                log.warning("ray request for %s dropped: %s", body.get("usecase"), e)
                return jsonify(status="dropped", reason=str(e)), 200
            response, code = _ray(body, caller)
            # Refusals never get better: acknowledge. No live release or a failed call: let Pub/Sub retry.
            return response, (200 if code in (400, 403) else 503 if code == 404 else code)
        response, code = _run(body)
        # A bad request never gets better: acknowledge it. No live release (404) or a submit error (5xx) may:
        # Pub/Sub redelivers with backoff, then dead-letters. Duplicates are rejected by job ID.
        if code == 400:
            reason = response.get_json().get("reason")
            log.warning("dropping run request for %s: %s", body.get("usecase"), reason)
            return jsonify(status="dropped", reason=reason), 200
        return response, (503 if code == 404 else code)

    @app.post("/online/lookup-service")
    def lookup_service():
        """FR-104: create or update a use case's Cloud SQL lookup service from its live release (deploy-model)."""
        body = request.get_json(silent=True) or {}
        contract = reg().get(body.get("usecase") or "", release=body.get("release"))
        if contract is None:
            return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 404
        try:
            return jsonify(online_launcher.ensure_lookup_service(contract, reg().manifest, Gcp(), wait=240)), 200
        except Exception as e:  # noqa: BLE001
            log.exception("lookup service of %s", contract.usecase)
            return jsonify(status="error", reason=str(e)[:400]), 500

    @app.post("/online/lookup-service/delete")
    def lookup_service_delete():
        """1.4.2: remove a use case's lookup service when its models are undeployed (deploy-model --undeploy)."""
        body = request.get_json(silent=True) or {}
        contract = reg().get(body.get("usecase") or "")
        if contract is None:
            return jsonify(status="error", reason=f"{body.get('usecase')}: no live release"), 404
        try:
            return jsonify(online_launcher.remove_lookup_service(contract, reg().manifest, Gcp())), 200
        except Exception as e:  # noqa: BLE001
            log.exception("removing the lookup service of %s", contract.usecase)
            return jsonify(status="error", reason=str(e)[:400]), 500

    @app.get("/health")
    def health():
        usecases = {c.usecase: {"release": c.release_id, "pipelines": c.pipeline_names} for c in reg().live()}
        return jsonify(status="ok", environment=reg().manifest["environment"], usecases=usecases,
                       errors=reg().errors)

    return app


def _slug_label(value: str) -> str:
    import re

    return re.sub(r"[^a-z0-9_-]", "-", value.lower())[:63]


def _startup_app() -> Flask | None:
    """Loads the manifest and live contracts at import time: an unreadable manifest crashes the revision, so it
    never takes traffic. A broken contract only disables its own use case (see /health)."""
    uri = os.environ.get("MANIFEST_URI")
    if not uri:
        return None
    registry = Registry(Gcp(), uri, int(os.environ.get("POINTER_TTL", "60")))
    live = registry.live(force=True)
    log.info("launcher for %s: %d live use case(s) %s, errors %s", registry.manifest["environment"], len(live),
             [c.usecase for c in live], registry.errors)
    return create_app(registry)


app = _startup_app()
