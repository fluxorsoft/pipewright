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
            loop=body.get("loop")), gcp=self.gcp)

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
                                 approve=True)
        promoted = result.model_version
        if promoted is None:          # promoted already (the step is repeated): the version that came from it
            promoted = next((v["version"] for v in self.versions(self.production.name)
                             if v["labels"].get("source-version") == str(version)
                             and v["labels"].get("promoted-from") == self.test.name), None)
        if promoted is None:
            raise ConfigError(f"promotion of v{version} to {self.production.name} left no model version")
        return {"version": str(promoted), "release_id": result.contract.release_id}

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
