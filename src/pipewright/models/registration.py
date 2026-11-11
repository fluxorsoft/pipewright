"""Registration through the launcher (Pipewright 1.4.3, FR-100): a runner that cannot move a model's default.

With policies.registration: launcher, runners hold a role without aiplatform.models.upload, update and delete (no
role can stop an identity that uploads from moving any model's default: the FR-100 spike). The register step asks
the launcher instead (pipewright_runtime.register_version), and the launcher, before it uploads:

1. checks the request comes from the run it names: the use case's runner identity, and a pipeline job with the
   run's ID that belongs to the use case, runs as that runner and is still running;
2. checks the files are the tenant's: the model folder is under the release's pipeline root, and the evaluation
   record next to it has the digest the step sent;
3. recomputes the gates from that record against the released evaluation configuration (as `model approve` does);
4. uploads under the use case's fixed model ID (FR-124), as a version of the existing model. The default moves only
   where the release's policy moves it (promote_model auto); under manual it never does, except for a first version,
   which Agent Platform makes the default by itself (FR-83 records it as pending approval);
5. writes the answer to gs://<artifacts>/runs/registration/<tenant>/<run>.json, which the step waits for, and
   records model.registered or model.registration_refused.

A repeated request (a Pub/Sub redelivery, a step retry) finds the version it registered and answers the same.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time

from pipewright.gcp import gcs_read, gcs_write
from pipewright.models import gates
from pipewright.models.names import model_id, oldest

log = logging.getLogger("pipewright.registration")

RESULTS_PREFIX = "runs/registration"
RUNNING = ("PIPELINE_STATE_RUNNING", "RUNNING")
UPLOAD_WAIT_SECONDS = 900


class RegistrationRefused(Exception):
    def __init__(self, reason: str, status: int = 403):
        super().__init__(reason)
        self.reason, self.status = reason, status


def request_digest(body: dict) -> str:
    """The digest of a registration request as the step sent it (without the transport's kind and id_token), carried
    in the answer so a step takes only the answer to its own request: a run that sent a refused request and then a
    good one must not read the first answer (GA QA). pipewright_runtime keeps the same rule (a test keeps them
    equal)."""
    keep = {k: v for k, v in body.items() if k not in ("kind", "id_token")}
    return hashlib.sha256(json.dumps(keep, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def run_alias(run_id: str) -> str:
    """The version alias every registered version carries (the register step's rule)."""
    return re.sub(r"[^a-z0-9-]", "-", f"run-{run_id}".lower())[:127]


def result_name(tenant: str, run_id: str) -> str:
    return f"{RESULTS_PREFIX}/{tenant}/{run_alias(run_id)}.json"


def _api(region: str) -> str:
    return f"https://{region}-aiplatform.googleapis.com/v1"


class Registrations:
    """The launcher's side. gcp: its session; bucket: the artifacts bucket; manifest: a callable returning the
    environment manifest; job_finder(contract, run_id) -> {"state", "service_account", "usecase"} | None."""

    def __init__(self, gcp, bucket: str, manifest, job_finder=None):
        self.gcp, self.bucket, self.manifest = gcp, bucket, manifest
        self.job_finder = job_finder or self._agent_platform_job

    # ------------------------------------------------------------------------------------------- checks --

    def _agent_platform_job(self, contract, run_id: str) -> dict | None:
        label = re.sub(r"[^a-z0-9_-]", "-", run_id.lower())[:63]
        r = self.gcp.get(f"{_api(contract.region)}/projects/{contract.project}/locations/{contract.region}/"
                         "pipelineJobs", filter=f'labels.run-id="{label}"', pageSize=5)
        if not r.ok:
            raise RuntimeError(f"pipeline jobs not readable: HTTP {r.status}")
        jobs = (r.body or {}).get("pipelineJobs") or []
        if not jobs:
            return None
        job = jobs[0]
        labels = job.get("labels") or {}
        return {"state": job.get("state"), "service_account": job.get("serviceAccount"),
                "usecase": labels.get("usecase") or labels.get("pipewright-usecase"), "name": job.get("name")}

    def check_caller(self, contract, caller: str | None) -> None:
        if caller is not None and caller.lower() != contract.runner_service_account.lower():
            raise RegistrationRefused(f"{caller} may not register {contract.usecase}'s models: only its runner "
                                      "identity may ask")

    def check_run(self, contract, run_id: str) -> dict:
        if not run_id:
            raise RegistrationRefused("run_id is required", 400)
        job = self.job_finder(contract, run_id)
        if not job:
            raise RegistrationRefused(f"no pipeline run with run ID {run_id} for {contract.usecase}")
        if job.get("usecase") and _slug(job["usecase"]) != _slug(contract.usecase):
            raise RegistrationRefused(f"run {run_id} belongs to {job['usecase']}, not {contract.usecase}")
        if (job.get("service_account") or "").lower() != contract.runner_service_account.lower():
            raise RegistrationRefused(f"run {run_id} runs as {job.get('service_account') or 'an unknown identity'}, "
                                      f"not {contract.usecase}'s runner")
        if str(job.get("state") or "").upper() not in RUNNING:
            raise RegistrationRefused(f"run {run_id} is {job.get('state')}: only a running run may register")
        return job

    def check_files(self, contract, artifact_uri: str, evaluation: dict | None, ml: dict) -> dict | None:
        root = contract.pipeline_root.rstrip("/") + "/"
        if not artifact_uri or not artifact_uri.rstrip("/").startswith(root.rstrip("/") + "/"):
            raise RegistrationRefused(f"the model folder {artifact_uri or '(none)'} is not under {contract.usecase}'s "
                                      f"pipeline root {root}")
        record = None
        if evaluation and evaluation.get("uri"):
            if not evaluation["uri"].startswith(artifact_uri.rstrip("/") + "/"):
                raise RegistrationRefused("the evaluation record is not in the model folder")
            bucket, _, name = evaluation["uri"].removeprefix("gs://").partition("/")
            text, _gen = gcs_read(self.gcp, bucket, name)
            if text is None:
                raise RegistrationRefused(f"the evaluation record {evaluation['uri']} is not there")
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if evaluation.get("sha256") and digest != evaluation["sha256"]:
                raise RegistrationRefused(f"the evaluation record changed after the step wrote it (sha256 "
                                          f"{digest[:16]}, the step sent {evaluation['sha256'][:16]})")
            record = json.loads(text)
        elif gates.needs_record(ml.get("evaluation") or {}):
            raise RegistrationRefused("the release has evaluation gates and the request names no evaluation record")
        return record

    def check_gates(self, contract, record: dict | None, ml: dict, champion_expected: bool) -> None:
        evaluation = ml.get("evaluation") or {}
        if not gates.needs_record(evaluation):
            return
        problems = gates.check(record or {}, evaluation, champion_expected)
        if problems:
            raise RegistrationRefused(f"the evaluation record does not pass {contract.usecase}'s gates (release "
                                      f"{contract.release_id}): {'; '.join(problems[:3])}")

    # ------------------------------------------------------------------------------------------ registry --

    def _models(self, contract, display_name: str) -> list[dict]:
        r = self.gcp.get(f"{_api(contract.region)}/projects/{contract.project}/locations/{contract.region}/models",
                         filter=f'display_name="{display_name}"')
        if not r.ok:
            raise RuntimeError(f"registry not readable: HTTP {r.status}")
        return oldest((r.body or {}).get("models") or [])

    def _version_with_alias(self, model_name: str, region: str, alias: str) -> dict | None:
        r = self.gcp.get(f"{_api(region)}/{model_name}@{alias}")
        return r.body if r.ok and r.body else None

    def _wait(self, region: str, operation: str) -> dict:
        deadline = time.time() + UPLOAD_WAIT_SECONDS
        while time.time() < deadline:
            r = self.gcp.get(f"{_api(region)}/{operation}")
            if r.ok and (r.body or {}).get("done"):
                if (r.body or {}).get("error"):
                    raise RuntimeError(f"upload failed: {r.body['error'].get('message', r.body['error'])}"[:400])
                return (r.body or {}).get("response") or {}
            time.sleep(10)
        raise RuntimeError(f"upload {operation} did not finish in {UPLOAD_WAIT_SECONDS // 60} minutes")

    def upload(self, contract, body: dict, ml: dict) -> dict:
        display = body["display_name"]
        alias = run_alias(body["run_id"])
        existing = self._models(contract, display)
        if existing:
            found = self._version_with_alias(existing[0]["name"], contract.region, alias)
            if found:                                      # a repeated request: answer as before
                return {"resource": existing[0]["name"], "version": str(found.get("versionId")),
                        "default": "default" in (found.get("versionAliases") or []), "repeated": True}
        manual = ((ml.get("registry") or {}).get("set_default")) == "manual"
        first = not existing
        make_default = not manual or first
        labels = dict(body.get("labels") or {})
        labels.update({"run_id": alias[:63], "pipeline": "training", "usecase": contract.usecase[:63],
                       "status": "champion" if make_default else "candidate", "registered-by": "launcher"})
        if first and manual:
            labels["approval"] = "pending"
        else:
            labels.pop("approval", None)
        model = {"displayName": display, "artifactUri": body["artifact_uri"],
                 "containerSpec": {"imageUri": body["serving_image"],
                                   **({"env": [{"name": k, "value": v} for k, v in body["env"].items()]}
                                      if body.get("env") else {})},
                 "labels": labels, "versionAliases": [alias] + (["default"] if make_default and not first else []),
                 "versionDescription": body.get("version_description") or f"run {body['run_id']}"}
        kms = (ml.get("platform") or {}).get("kms_key")
        if kms:
            model["encryptionSpec"] = {"kmsKeyName": kms}
        parent = f"projects/{contract.project}/locations/{contract.region}"
        request = {"model": model}
        if existing:
            request["parentModel"] = existing[0]["name"]
        else:
            request["modelId"] = model_id(display)
        r = self.gcp.post(f"{_api(contract.region)}/{parent}/models:upload", request)
        if r.status == 409 and first:
            # Another run made the first version a moment ago (FR-124): register this one as a version of it.
            for _ in range(12):
                existing = self._models(contract, display)
                if existing:
                    break
                time.sleep(10)
            if not existing:
                raise RuntimeError("model ID taken but no model with the name is readable")
            request.pop("modelId", None)
            request["parentModel"] = existing[0]["name"]
            labels["status"] = "champion" if not manual else "candidate"
            labels.pop("approval", None)
            model["versionAliases"] = [alias] + (["default"] if not manual else [])
            make_default = not manual
            r = self.gcp.post(f"{_api(contract.region)}/{parent}/models:upload", request)
        if not r.ok:
            message = (r.body or {}).get("error", {}).get("message", "") if isinstance(r.body, dict) else ""
            raise RuntimeError(f"upload refused: HTTP {r.status} {message}"[:400])
        done = self._wait(contract.region, r.body["name"])
        resource = done.get("model") or (existing[0]["name"] if existing else f"{parent}/models/{model_id(display)}")
        return {"resource": resource, "version": str(done.get("modelVersionId") or ""), "default": make_default,
                "first": first}

    # -------------------------------------------------------------------------------------------- answer --

    def answer(self, contract, run_id: str, result: dict) -> None:
        gcs_write(self.gcp, self.bucket, result_name(contract.tenant, run_id), json.dumps(result, indent=2))

    def register(self, contract, body: dict, caller: str | None) -> dict:
        """Checks, uploads and answers. Raises RegistrationRefused (answered first) for a refusal."""
        ml = (contract.static_parameters or {}).get("ml_config") or {}
        run_id = body.get("run_id") or ""
        digest = request_digest(body)
        bqml = body.get("engine") in ("bqml", "ai_forecast")
        try:
            self.check_caller(contract, caller)
            self.check_run(contract, run_id)
            if bqml:
                record = self.check_bqml(contract, body, ml)
                self.check_gates(contract, record, ml, champion_expected=bool(
                    self._registry_model(contract, ml["bqml"]["registry_id"])))
            else:
                record = self.check_files(contract, body.get("artifact_uri") or "", body.get("evaluation"), ml)
                existing = self._models(contract, body.get("display_name") or "")
                self.check_gates(contract, record, ml, champion_expected=bool(existing))
        except RegistrationRefused as e:
            if run_id:
                self.answer(contract, run_id, {"status": "refused", "reason": e.reason, "run_id": run_id,
                                               "request": digest})
            raise
        result = self.register_bqml(contract, body, ml) if bqml else self.upload(contract, body, ml)
        answer = {"status": "registered", "run_id": run_id, "request": digest, **result}
        self.answer(contract, run_id, answer)
        return answer


# ------------------------------------------------------------------------------------- BigQuery ML --

BQ = "https://bigquery.googleapis.com/bigquery/v2"


def _bq_ref(ref: str) -> tuple[str, str, str]:
    parts = ref.replace("`", "").split(".")
    if len(parts) != 3:
        raise RegistrationRefused(f"{ref!r} is not a project.dataset.model reference", 400)
    return parts[0], parts[1], parts[2]


def _check_bqml(self, contract, body: dict, ml: dict) -> dict | None:
    """BigQuery ML: the model is in the release's dataset, the record in the release's records folder (FR-100)."""
    section = ml.get("bqml") or {}
    if body.get("registry_id") != section.get("registry_id"):
        raise RegistrationRefused(f"registry model {body.get('registry_id')!r} is not {contract.usecase}'s "
                                  f"({section.get('registry_id')})")
    if body.get("engine") == "bqml":
        project, dataset, _ = _bq_ref(body.get("bigquery_model") or "")
        want = (ml.get("platform") or {}).get("bigquery_dataset")
        if (project, dataset) != (contract.project, want):
            raise RegistrationRefused(f"BigQuery model {body.get('bigquery_model')} is not in {contract.usecase}'s "
                                      f"dataset {contract.project}.{want}")
    records = section.get("records") or ""
    uri = (body.get("evaluation") or {}).get("uri") or ""
    if not records or not uri.startswith(records):
        raise RegistrationRefused(f"the evaluation record {uri or '(none)'} is not under {records or 'the records'}")
    bucket, _, name = uri.removeprefix("gs://").partition("/")
    text, _gen = gcs_read(self.gcp, bucket, name)
    if text is None:
        raise RegistrationRefused(f"the evaluation record {uri} is not there")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if body["evaluation"].get("sha256") and digest != body["evaluation"]["sha256"]:
        raise RegistrationRefused(f"the evaluation record changed after the step wrote it (sha256 {digest[:16]})")
    return json.loads(text)


def _registry_model(self, contract, registry_id: str) -> dict | None:
    r = self.gcp.get(f"{_api(contract.region)}/projects/{contract.project}/locations/{contract.region}/models/"
                     f"{registry_id}")
    return r.body if r.ok else None


def _bq_query(self, contract, sql: str, labels: dict) -> None:
    r = self.gcp.post(f"{BQ}/projects/{contract.project}/queries",
                      {"query": sql, "useLegacySql": False, "location": contract.region, "timeoutMs": 60000,
                       **({"destinationEncryptionConfiguration": {"kmsKeyName": key}}
                          if (key := ((self.manifest() or {}).get("security") or {}).get("bigquery_kms_key")) else {}),
                       "labels": {k: re.sub(r"[^a-z0-9_-]", "-", str(v).lower())[:63] for k, v in labels.items()}})
    if not r.ok:
        raise RuntimeError(f"registration query refused: HTTP {r.status} "
                           f"{(r.body or {}).get('error', {}).get('message', '')}"[:400])
    job = (r.body or {}).get("jobReference") or {}
    deadline = time.time() + UPLOAD_WAIT_SECONDS
    done = (r.body or {}).get("jobComplete")
    while not done and time.time() < deadline:
        time.sleep(5)
        g = self.gcp.get(f"{BQ}/projects/{contract.project}/jobs/{job.get('jobId')}", location=contract.region)
        status = (g.body or {}).get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(f"registration query failed: {status['errorResult'].get('message')}"[:400])
            done = True
    if not done:
        raise RuntimeError("registration query did not finish")


def _registered_bq_version(self, contract, ref: str) -> str:
    project, dataset, model = _bq_ref(ref)
    deadline = time.time() + UPLOAD_WAIT_SECONDS
    while time.time() < deadline:
        r = self.gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}/models/{model}")
        runs = (r.body or {}).get("trainingRuns") or [] if r.ok else []
        version = next((x.get("vertexAiModelVersion") for x in reversed(runs) if x.get("vertexAiModelVersion")), None)
        if version:
            return str(version)
        time.sleep(10)
    raise RuntimeError(f"{ref} was registered, but no registry version appeared")


def _label(self, contract, registry_id: str, version: str, labels: dict, description: str | None = None) -> None:
    url = (f"{_api(contract.region)}/projects/{contract.project}/locations/{contract.region}/models/"
           f"{registry_id}@{version}")
    for _ in range(31):                           # BigQuery reports the version before the registry serves it
        got = self.gcp.get(url)
        if got.status != 404:
            break
        time.sleep(10)
    current = (got.body or {}).get("labels") or {} if got.ok else {}
    body = {"labels": {**current, **labels}, **({"versionDescription": description} if description else {})}
    r = self.gcp.patch(url, body, updateMask="labels,versionDescription" if description else "labels")
    if not r.ok:
        raise RuntimeError(f"labelling v{version}: HTTP {r.status}")


def _register_bqml(self, contract, body: dict, ml: dict) -> dict:
    """ALTER MODEL (or, for AI.FORECAST, an upload without files) as the launcher, then the labels and the default
    as the pipeline step set them before 1.4.3 (bqml_register)."""
    registry_id = ml["bqml"]["registry_id"]
    run_id = body["run_id"]
    tags = dict(body.get("labels") or {})
    existed = self._registry_model(contract, registry_id) is not None
    base = f"{_api(contract.region)}/projects/{contract.project}/locations/{contract.region}"
    if body["engine"] == "ai_forecast":
        model = {"displayName": registry_id, "description": body.get("version_description") or "",
                 "versionDescription": body.get("version_description") or "", "labels": tags,
                 **({"encryptionSpec": {"kmsKeyName": ml["platform"]["kms_key"]}}
                    if (ml.get("platform") or {}).get("kms_key") else {})}
        request = {"model": model, **({"parentModel": f"projects/{contract.project}/locations/{contract.region}/"
                                                      f"models/{registry_id}"} if existed
                                      else {"modelId": registry_id})}
        r = self.gcp.post(f"{base}/models:upload", request)
        if r.status == 409 and not existed:
            request.pop("modelId")
            request["parentModel"] = f"projects/{contract.project}/locations/{contract.region}/models/{registry_id}"
            r = self.gcp.post(f"{base}/models:upload", request)
        if not r.ok:
            raise RuntimeError(f"upload refused: HTTP {r.status}")
        version = str(self._wait(contract.region, r.body["name"]).get("modelVersionId"))
    else:
        ref = body["bigquery_model"]
        self._bq_query(contract, f"ALTER MODEL `{ref}` SET OPTIONS (vertex_ai_model_id = '{registry_id}')",
                       {"pipewright-usecase": contract.usecase, "run-id": run_id, "pipewright-step": "registration"})
        version = self._registered_bq_version(contract, ref)
        self._label(contract, registry_id, version, {**tags, "registered-by": "launcher"},
                    body.get("version_description"))
    default = (ml.get("registry") or {}).get("set_default") == "on_approval"
    if default:
        r = self.gcp.post(f"{base}/models/{registry_id}@{version}:mergeVersionAliases", {"versionAliases": ["default"]})
        if not r.ok:
            raise RuntimeError(f"making v{version} the default: HTTP {r.status}")
    first = not existed
    if default or first:
        self._label(contract, registry_id, version, {"status": "champion", **({} if default else
                                                                               {"approval": "pending"})})
        default = True
    return {"resource": f"projects/{contract.project}/locations/{contract.region}/models/{registry_id}",
            "version": version, "default": default, "first": first}


Registrations.check_bqml = _check_bqml
Registrations._registry_model = _registry_model
Registrations._bq_query = _bq_query
Registrations._registered_bq_version = _registered_bq_version
Registrations._label = _label
Registrations.register_bqml = _register_bqml


def kfp_job_finder(manifest_getter, gcp):
    """The Kubeflow run with this run ID (orchestrator kfp, private GKE): runs carry the launcher's labels in their
    description and run as the tenant's Kubernetes service account, which is the runner's identity there."""
    def find(contract, run_id: str) -> dict | None:
        from pipewright.adapters.kfp import api_path, run_labels
        from pipewright.kube import KubeClient

        manifest = manifest_getter()
        k8s = manifest.get("kubernetes") or {}
        namespace = k8s.get("namespace") or "kubeflow"
        kube = KubeClient.from_manifest(manifest, gcp)
        r = kube.get(f"{api_path(namespace)}/runs", page_size=100, sort_by="created_at desc")
        want = re.sub(r"[^a-z0-9_-]", "-", run_id.lower())[:63]
        tenant = (manifest.get("tenants") or {}).get(contract.tenant) or {}
        for item in ((r.body or {}).get("runs") or []) if r.ok else []:
            labels = run_labels(item.get("description"))
            if labels.get("run-id") != want:
                continue
            same = item.get("service_account") == tenant.get("kubernetes_service_account")
            return {"state": item.get("state"), "usecase": labels.get("pipewright-usecase"),
                    "service_account": contract.runner_service_account if same else item.get("service_account"),
                    "name": item.get("run_id")}
        return None
    return find


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())
