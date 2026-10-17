"""Launcher web app (Cloud Run). Platform-owned; one per environment, shared by every tenant.

Environment:
  MANIFEST_URI       gs://<artifacts>/pipewright/environment.json   (required)
  POINTER_TTL        seconds to cache contract pointers (default 60)

Routes:
  POST /pubsub   GCS landing events (Pub/Sub push)  -> 2xx ack, 5xx retry
  POST /run      {"usecase", "schedule"?, "pipeline"?, "source_uri"?, "run_key"?}   schedules and API runs
  POST /pubsub/run   the same body as /run, in a Pub/Sub push envelope (manifest launcher.run_requests_topic):
                     how pipelines inside a private perimeter request runs  -> 2xx ack, 5xx retry
  POST /retraining/propose   {"usecase", "run_id", "observation", "champion"}: drift or performance from an
                     inference run opens or updates a retrain decision case (also as kind retrain.proposal on
                     /pubsub/run)
  POST /retraining/decide    a person's decision: a Pub/Sub push from an approver group's subscription (the message
                     carries the person's ID token), or a direct call by the deciding identity
  POST /retraining/tick      {"digest"?}: snooze and decline expiry, escalation, the weekly digest (Cloud Scheduler);
                     also removes Ray clusters whose run ended or whose deadline passed
  POST /ray/clusters {"action": "create" | "delete", "usecase", "run_id", "step"}: a Ray step's cluster, from the use
                     case's runner identity (also as kind ray.cluster on /pubsub/run, with the runner's "id_token")
  GET  /health   live use cases and their releases (Cloud Run reserves paths ending in "z", e.g. /healthz)
"""

from __future__ import annotations

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
from pipewright.observability import events
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

    def start(contract: ReleaseContract, pipeline: str, source_uri: str, rid: str, trigger: str,
              hint: str = "") -> dict:
        target = target_for(contract, pipeline)
        jid = job_id(contract.usecase, rid, hint if pipeline == "training" else f"{pipeline}-{hint}")
        try:
            name = adapters.submit(contract, target, parameters_for(contract, target, source_uri, rid), jid,
                                   labels_for(contract, trigger, rid, pipeline), reg().manifest.get("security"),
                                   manifest=reg().manifest, gcp=getattr(reg(), "gcp", None))
            log.info("submitted %s (%s %s, release %s) for %s", name, contract.usecase, pipeline,
                     contract.release_id, source_uri)
            _event("run.submitted", contract, rid, pipeline=pipeline, trigger=trigger, job_id=jid,
                   source_uri=source_uri, job=name)
            return {"usecase": contract.usecase, "pipeline": pipeline, "status": "submitted", "job_id": jid,
                    "release": contract.release_id}
        except AlreadySubmitted:
            log.info("duplicate %s for %s", jid, source_uri)
            _event("run.duplicate", contract, rid, pipeline=pipeline, trigger=trigger, job_id=jid)
            return {"usecase": contract.usecase, "pipeline": pipeline, "status": "duplicate", "job_id": jid}

    # ---------------------------------------------------------------------------------- retrain decisions --
    def launcher_url() -> str:
        return ((reg().manifest.get("launcher") or {}).get("url") or request.url_root).rstrip("/")

    def decisions() -> Decisions:
        if "decisions" not in state:
            r = reg()

            def submit(contract, pipeline, source_uri, run_key, trigger):
                return start(contract, pipeline, source_uri, run_id(contract.usecase, trigger, run_key), trigger,
                             trigger)

            def emit(event_type, contract, data, attributes):
                _event(event_type, contract, None, attributes=attributes, **data)

            def verify(token):
                return identity.verify(token, [launcher_url(), identity.GCLOUD_CLIENT_ID])

            def champion_info(contract, champion):
                name, version = champion.get("resource_name"), champion.get("version")
                if not name or not version:
                    return {}
                resp = r.gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/{name}@{version}")
                return {"created": resp.body.get("versionCreateTime")} if resp.ok else {}

            state["decisions"] = Decisions(CaseStore(r.gcp, r.bucket), lambda: reg().manifest, submit, emit,
                                           verify=verify, champion_info=champion_info)
        return state["decisions"]

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
        try:
            result = decisions().decide(contract, message, group, via, by=caller if via == "direct" else None)
        except Exception as e:  # noqa: BLE001
            log.exception("decision on %s failed", message.get("case_id"))
            return jsonify(status="error", reason=str(e)[:300]), 500
        log.info("retrain decision for %s: %s", contract.usecase, {k: v for k, v in result.items() if k != "training"})
        return jsonify(result), (409 if via == "direct" and result["status"] == "refused" else 200)

    @app.post("/retraining/tick")
    def retraining_tick():
        body = request.get_json(silent=True) or {}
        digest = bool(body.get("digest")) or request.args.get("digest") in ("1", "true")
        results = decisions().tick(reg().live(), digest=digest)
        m = reg().manifest
        ray = rays().reap(m["project"], m["region"]) if m.get("project") and m.get("region") else []
        return jsonify(results=results, ray=ray), 200

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
        contract = reg().get(usecase)
        if contract is None:
            reason = reg().errors.get(usecase, "no live release")
            return jsonify(status="error", reason=f"{usecase}: {reason}"), 404
        try:
            r = resolve_run(contract, body)
        except BadRequest as e:
            return jsonify(status="error", reason=str(e)), 400
        try:
            result = start(contract, r.pipeline, r.source_uri, r.run_id, r.trigger, body.get("schedule") or "")
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
        if body.get("kind") == "ray.cluster":
            # Pub/Sub does not say who published: the message carries the runner's ID token.
            try:
                caller = identity.verify(body.get("id_token") or "", [launcher_url()])
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

    @app.get("/health")
    def health():
        usecases = {c.usecase: {"release": c.release_id, "pipelines": c.pipeline_names} for c in reg().live()}
        return jsonify(status="ok", environment=reg().manifest["environment"], usecases=usecases,
                       errors=reg().errors)

    return app


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
