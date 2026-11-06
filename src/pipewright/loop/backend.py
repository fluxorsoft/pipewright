"""The closed retrain loop's Backend on Google Cloud: run requests through each environment's launcher (over its run
requests topic when private), pipeline job states (Agent Platform or Kubeflow), Model Registry versions, the use
case's monitoring table, `pipewright promote` and `model approve`, and the production case store."""

from __future__ import annotations

from pathlib import Path

from pipewright.config import Environment, resolve_usecase
from pipewright.errors import ConfigError


class GcpBackend:
    def __init__(self, usecase_path: Path, case_id: str, test: Environment, production: Environment, gcp=None):
        from pipewright.gcp import Gcp
        from pipewright.retraining import client

        self.path, self.case_id = Path(usecase_path), case_id
        self.gcp = gcp or Gcp()
        self.envs = {test.name: test, production.name: production}
        self.test, self.production = test, production
        self.ucs = {name: resolve_usecase(self.path, env) for name, env in self.envs.items()}
        self.usecase = self.ucs[production.name].name
        self.store = client.store(production, self.gcp)

    # ------------------------------------------------------------------ runs --

    def run_ids(self, pipeline: str, source_uri: str, run_key: str) -> tuple[str, str]:
        """The run and job IDs the launcher gives this request (deterministic: launcher.routing)."""
        from pipewright.launcher.routing import job_id, run_id

        parts = ["api", source_uri, run_key] if pipeline == "training" else ["api", pipeline, source_uri, run_key]
        rid = run_id(self.usecase, *parts)
        return rid, job_id(self.usecase, rid, "" if pipeline == "training" else pipeline)

    def submit(self, env: str, body: dict) -> dict:
        from pipewright.runs import launch

        return launch.run(self.envs[env], launch.body(
            body["usecase"], body["pipeline"], body["source_uri"], None, body["run_key"],
            champion_version=body.get("champion_version"), model_version=body.get("model_version"),
            loop=body.get("loop"), loop_data=body.get("loop_data")), gcp=self.gcp)

    def production_data(self) -> dict | None:
        """FR-48 phase 6 (retraining.loop.data: production in production's configuration): production's feature
        tables, resolved from its Feature Registry as the runtime would, and its label source, for the loop's
        training run in test. None: test's own data configuration, as before."""
        from pipewright.build.templates import pipewright_runtime as rt

        uc, e = self.ucs[self.production.name], self.production
        if ((uc.ml.get("retraining") or {}).get("loop") or {}).get("data") != "production":
            return None
        data, out = uc.ml.get("data") or {}, {}
        if data.get("features_from"):
            sources = []
            for src in data["features_from"]["sources"]:
                if "feature_group" not in src:
                    sources.append({**src, "table": "bq://" + rt.bq_table(src["table"], e.project)})
                    continue
                url = rt._REGISTRY.format(region=e.region, project=e.project, group=src["feature_group"])
                group = self.gcp.get(url)
                if not group.ok:
                    raise ConfigError(f"feature group {src['feature_group']} in {e.name}: HTTP {group.status}")
                features, token = [], None
                while True:
                    page = self.gcp.get(f"{url}/features", pageSize=100, **({"pageToken": token} if token else {}))
                    if not page.ok:
                        raise ConfigError(f"features of {src['feature_group']} in {e.name}: HTTP {page.status}")
                    features += (page.body or {}).get("features", [])
                    token = (page.body or {}).get("nextPageToken")
                    if not token:
                        break
                resolved = rt.group_source(src, group.body or {}, features)
                sources.append({**resolved, "table": "bq://" + rt.bq_table(resolved["table"], e.project)})
            out["features_from"] = {"sources": sources}
        if data.get("label_source"):
            out["label_source"] = {**data["label_source"],
                                   "table": "bq://" + rt.bq_table(data["label_source"]["table"], e.project)}
        return out or None

    def job_state(self, env: str, job_id: str) -> str | None:
        e = self.envs[env]
        if e.orchestrator == "kfp":
            from pipewright.adapters.kfp import find
            from pipewright.kube import KubeClient

            kube = KubeClient.from_manifest({"kubernetes": e.kubernetes}, self.gcp, dns=True)  # from outside the VPC
            run = find(kube, (e.kubernetes or {}).get("namespace") or "kubeflow", job_id)
            return run.get("state") if run else None
        url = (f"https://{e.region}-aiplatform.googleapis.com/v1/projects/{e.project}/locations/{e.region}/"
               f"pipelineJobs/{job_id}")
        r = self.gcp.get(url)
        if r.status == 404:
            return None
        if not r.ok:
            raise ConfigError(f"pipeline job {job_id} in {env}: HTTP {r.status}")
        return (r.body or {}).get("state")

    # --------------------------------------------------------------- registry --

    def versions(self, env: str) -> list[dict]:
        e, uc = self.envs[env], self.ucs[env]
        api = f"https://{e.region}-aiplatform.googleapis.com/v1/projects/{e.project}/locations/{e.region}/models"
        found = self.gcp.get(api, filter=f'display_name="{uc.model["name"]}"')
        out = []
        for m in (found.body or {}).get("models", []) if found.ok else []:
            listed = self.gcp.get(f"https://{e.region}-aiplatform.googleapis.com/v1/{m['name']}:listVersions",
                                  pageSize=100)
            for v in (listed.body or {}).get("models", []) if listed.ok else []:
                out.append({"version": str(v["versionId"]), "default": "default" in (v.get("versionAliases") or []),
                            "labels": v.get("labels") or {}})
        return out

    def promote(self, version: str) -> dict:
        from pipewright.release import promote

        result = promote.promote(self.path, self.test, self.production, gcp=self.gcp, model_version=version,
                                 approve=True, loop_case=self.case_id)
        promoted = result.model_version
        if promoted is None:          # promoted already (the step is repeated): the version that came from it
            promoted = next((v["version"] for v in self.versions(self.production.name)
                             if v["labels"].get("source-version") == str(version)
                             and v["labels"].get("promoted-from") == self.test.name), None)
        if promoted is None:
            raise ConfigError(f"promotion of v{version} to {self.production.name} left no model version")
        return {"version": str(promoted), "release_id": result.contract.release_id}

    # ----------------------------------------------------- the canary (phase 6) --

    def canary_settings(self) -> dict | None:
        from pipewright.loop import canary

        uc = self.ucs[self.production.name]
        return canary.settings(((uc.ml.get("retraining") or {}).get("loop") or {}).get("canary"))

    def _prod(self):
        return self.ucs[self.production.name], self.production, self.gcp.session.credentials

    def canary_ready(self) -> None:
        from pipewright.models import deploy

        uc, env, creds = self._prod()
        ai = deploy._aiplatform(env, creds)
        endpoints = ai.Endpoint.list(filter=f'display_name="{deploy.endpoint_name(uc)}"')
        if not endpoints or not endpoints[0].list_models():
            raise ConfigError(f"retraining.loop.canary: {deploy.endpoint_name(uc)} serves no version in "
                              f"{env.name}, so there is nothing to compare a canary with: deploy the current version "
                              "first (`pipewright deploy-model`); nothing was promoted")

    def deploy_canary(self, version: str, cfg: dict) -> dict:
        """The default (the candidate, just approved) as a canary at cfg percent; a container model is probed."""
        import dataclasses
        import json

        from pipewright.loop import canary
        from pipewright.models import deploy

        uc, env, creds = self._prod()
        model = {**uc.model, "serving": {**uc.model["serving"], "canary_percent": cfg["percent"]}}
        deploy.deploy(dataclasses.replace(uc, model=model), env, creds, canary=True, log=lambda *_: None)
        endpoint, deployed, newest, _ = deploy._canary_state(uc, env, creds)
        out = {"endpoint": endpoint.resource_name, "deployed_model_id": str(newest.id),
               "current_deployed_model_ids": [str(d.id) for d in deployed if d.id != newest.id]}
        bqml = bool((uc.ml or {}).get("bqml"))
        if cfg.get("probe", True) and not bqml:      # a BigQuery ML deploy has run its parity check already
            from pipewright.gcp.storage import gcs_read

            found = self.gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{newest.model}"
                                 f"@{newest.model_version_id}")
            uri = (found.body or {}).get("artifactUri") if found.ok else None
            bucket, _, prefix = str(uri or "").removeprefix("gs://").partition("/")
            text, _ = gcs_read(self.gcp, bucket, prefix.rstrip("/") + "/training_stats.json") if bucket else (None, 0)
            stats = json.loads(text) if text else {}
            out["probe"] = canary.probe(endpoint, out["deployed_model_id"], canary.probe_instances(stats))
        elif bqml:
            out["probe"] = {"ok": True, "note": "BigQuery ML parity with ML.PREDICT checked by the deploy"}
        return out

    def canary_health(self, c: dict) -> tuple[bool, str]:
        from datetime import datetime, timezone

        from pipewright.loop import canary

        uc, env, _ = self._prod()
        cfg = self.canary_settings()
        start, end = canary.window(c["started_at"].replace("+00:00", "Z"), datetime.now(timezone.utc))
        metrics = canary.endpoint_metrics(self.gcp, env.project, c["endpoint"].rsplit("/", 1)[-1], start, end)
        current: dict = {}
        for d in c.get("current_deployed_model_ids") or []:
            for k, v in (metrics.get(d) or {}).items():
                current[k] = current.get(k, 0) + v if k != "latency_ms" else v
        return canary.judge(metrics.get(c["deployed_model_id"]) or {}, current, cfg, c.get("probe"))

    def promote_canary(self) -> None:
        from pipewright.models import deploy

        uc, env, creds = self._prod()
        deploy.promote(uc, env, creds, log=lambda *_: None)

    def rollback_canary(self, previous: str | None) -> None:
        from pipewright.models import deploy

        uc, env, creds = self._prod()
        deploy.undeploy_canary(uc, env, creds, log=lambda *_: None)
        if previous:
            self.approve(self.production.name, previous)

    def approve(self, env: str, version: str) -> None:
        from pipewright.models import registry

        registry.approve(self.ucs[env], self.envs[env], self.gcp.session.credentials, version, self.gcp.identity(),
                         True, log=lambda *_: None)

    # ------------------------------------------------------------- monitoring --

    def monitoring(self, env: str, run_id: str) -> dict | None:
        """The run's row in the use case's monitoring table (monitoring.output_table), or None without one."""
        from pipewright.gcp.bigquery import query, rows

        e, uc = self.envs[env], self.ucs[env]
        table = uc.ml["monitoring"].get("output_table")
        if not table:
            return None
        sql = (f"SELECT alert, triggers, max_feature_psi, prediction_psi FROM "
               f"`{e.project}.{uc.resources['bigquery_dataset']}.{table}` WHERE run_id = @run ORDER BY monitored_at "
               "DESC LIMIT 1")
        found = rows(query(self.gcp, e.project, sql, [{"name": "run", "parameterType": {"type": "STRING"},
                                                       "parameterValue": {"value": run_id}}],
                           kms_key=e.security.get("bigquery_kms_key")))
        return found[0] if found else {"alert": True, "triggers": "no monitoring row for the run"}

    # ------------------------------------------------------------------ case --

    def case(self) -> dict:
        return self.store.get(self.usecase, self.case_id) or {}

    def other_cases(self) -> list[dict]:
        return self.store.list(self.usecase)

    def save(self, change) -> dict:
        case = self.store.update_case(self.usecase, self.case_id, change)
        if case is None:
            raise ConfigError(f"no case {self.case_id} for {self.usecase} in {self.production.name}")
        return case

    def emit(self, event_type: str, data: dict) -> None:
        import io

        from pipewright.observability import events

        uc = self.ucs[self.production.name]
        events.emit(events.event(event_type, self.usecase, self.production.name, uc.tenant, None, None, **data),
                    self.production.observability.get("events_topic"), self.gcp, stream=io.StringIO(),
                    region=self.production.region)
