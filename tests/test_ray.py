"""Ray on Agent Platform: a cluster per Ray step of a run (pipewright.ray_clusters, the launcher's /ray/clusters, the
runtime helpers, release and the Ray example)."""

import base64
import importlib.util
import json
from dataclasses import replace
from pathlib import Path

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.config.schema import validate
from pipewright.errors import ConfigError
from pipewright.ray_clusters import settings
from pipewright.ray_clusters.service import RayClusters, RayRequestError, cluster_body
from pipewright.release.contract import ReleaseContract
from tests.test_release_parts import DIGEST, V3

RUNNER = "runner@p.iam.gserviceaccount.com"
STEP = {"head_machine": "n1-standard-4", "worker_machine": "n1-standard-4", "workers": 2, "accelerator": None,
        "max_minutes": 60}
RAY_CONTRACT = replace(V3, ray={"image": f"europe-west2-docker.pkg.dev/p/r/churn-ray@{DIGEST}", "ray_version": "2.47",
                                "steps": {"tune-on-ray": STEP}})


def ray_step(repo, step="train-model", sized=False, **cluster):
    repo.usecase["spec"]["resources_per_step"] = {step: {"ray": cluster, **({"cpu": 1} if sized else {})}}
    return repo


# ---------------------------------------------------------------- settings and limits

def test_defaults_need_no_manifest_block():
    s = settings.platform_settings(None, None)
    assert s["mode"] == "per_run" and s["ray_version"] == "2.47" and s["network_attachment"] is None
    assert settings.base_image(s, "us-central1") == "us-docker.pkg.dev/vertex-ai/training/ray-cpu.2-47.py311:latest"
    gpu = settings.base_image(s, "europe-west2", gpu=True)
    assert gpu.startswith("europe-docker.pkg.dev/vertex-ai/training/ray-gpu")
    tenant = settings.platform_settings({"limits": {"max_workers": 8}}, {"limits": {"max_workers": 1}})
    assert tenant["limits"]["max_workers"] == 1 and tenant["limits"]["max_minutes"] == 120   # tenant wins, per key


def test_steps_are_checked_against_the_limits():
    s = settings.platform_settings({"limits": {"machines": ["n1-standard-4", "n1-standard-8", "n1-standard-16"],
                                               "max_workers": 2, "max_minutes": 60}}, None)
    ok, problems = settings.resolve_steps({"tune": {"ray": {}}, "train": {"cpu": 2}}, s)
    # The head defaults to the first allowed machine big enough for a Ray head (16 vCPUs, more than 18 GB).
    assert problems == [] and ok == {"tune": {"head_machine": "n1-standard-16", "worker_machine": "n1-standard-16",
                                              "workers": 1, "accelerator": None, "max_minutes": 60}}
    _, problems = settings.resolve_steps({"tune": {"ray": {"head_machine": "n1-standard-8"}}}, s)
    assert problems == ["steps.tune.ray.head_machine n1-standard-8 has 8 vCPUs and 30 GB: a Ray head node needs at "
                        "least 16 vCPUs and more than 18 GB (for example n1-standard-16)"]
    assert settings.machine_cpus("n1-highmem-32") == 32 and settings.machine_cpus("custom-4-16384") is None
    assert settings.machine_memory_gb("e2-highmem-4") == 32 and settings.machine_memory_gb("custom-4-16384") is None
    _, problems = settings.resolve_steps({"tune": {"ray": {"head_machine": "n1-highmem-64", "workers": 9,
                                                           "accelerator": {"type": "NVIDIA_L4", "count": 1},
                                                           "max_minutes": 600}}}, s)
    assert len(problems) == 5
    assert "steps.tune.ray.head_machine n1-highmem-64 is not allowed" in problems[0]
    assert any("workers 9 is outside 0 to 2" in p for p in problems)
    assert any("allows no accelerators for Ray" in p for p in problems)
    assert any("max_minutes 600 is above the platform's limit of 60" in p for p in problems)


def test_cluster_ids_are_deterministic_and_match_the_runtime():
    a = settings.cluster_id("churn-nimbus-with-a-long-name", "r-1234567890abcdef", "tune-on-ray")
    assert a == settings.cluster_id("churn-nimbus-with-a-long-name", "r-1234567890abcdef", "tune-on-ray")
    assert a != settings.cluster_id("churn-nimbus-with-a-long-name", "r-other", "tune-on-ray")
    assert len(a) <= 63 and a[0].isalpha() and a == a.lower()
    spec = importlib.util.spec_from_file_location(
        "runtime", Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py")
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    for args in (("churn", "r1", "tune-on-ray"), ("A_Weird.Name", "x" * 80, "step_with_underscores")):
        assert runtime.ray_cluster_id(*args) == settings.cluster_id(*args)


def test_manifest_schema_takes_ray_per_run_and_standing():
    from tests.conftest import MANIFEST

    m = json.loads(json.dumps(MANIFEST))
    m["ray"] = {"mode": "per_run", "network_attachment": "projects/p/regions/us-central1/networkAttachments/ray",
                "limits": {"max_workers": 4, "machines": ["n1-standard-8"], "accelerators": [], "max_minutes": 90}}
    m["tenants"]["churn"]["ray"] = {"limits": {"max_workers": 1}}
    validate(m, "manifest", "m")
    m["tenants"]["churn"]["ray"] = {"mode": "standing", "standing": {
        "cluster": "ray-churn", "address": "http://ray-churn-head-svc.kubeflow.svc:8265"}}
    validate(m, "manifest", "m")                    # standing clusters (private-gke), kept by the platform
    m["tenants"]["churn"]["ray"] = {"mode": "shared"}
    with pytest.raises(ConfigError, match="shared"):
        validate(m, "manifest", "m")


# ---------------------------------------------------------------- resolving a use case

def test_ray_can_be_turned_off(repo):
    repo.manifests["dev"]["ray"] = {"enabled": False}
    ray_step(repo).write()
    with pytest.raises(ConfigError, match="Ray clusters are not enabled in this environment"):
        resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))
    repo.manifests["dev"]["tenants"]["churn"]["ray"] = {"enabled": True}            # one tenant may have it
    repo.write()
    assert resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev")).ray
    rays = RayClusters(FakeVertex(), lambda: {"ray": {"enabled": False}, "tenants": {}})
    with pytest.raises(RayRequestError, match="not enabled"):
        rays.create(RAY_CONTRACT, "tune-on-ray", "run-1", RUNNER)


def test_a_private_environment_without_a_ray_block_has_ray_off(repo):
    repo.manifests["dev"]["security"] = {"network": "projects/1/global/networks/vpc"}
    ray_step(repo).write()
    with pytest.raises(ConfigError, match="Ray clusters are not enabled in this environment"):
        resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))
    repo.manifests["dev"]["ray"] = {"mode": "per_run"}                # the platform turned Ray on
    repo.write()
    assert resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev")).ray
    assert settings.platform_settings(None, None, private=True)["enabled"] is False
    assert settings.platform_settings(None, None)["enabled"] is True              # public environments: on


def test_a_ray_step_reaches_ml_config(repo):
    ray_step(repo, workers=2).write()
    uc = resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))
    assert uc.ray["steps"]["train-model"]["workers"] == 2 and uc.ray["mode"] == "per_run"
    assert uc.ml_config()["platform"]["ray"]["steps"]["train-model"]["max_minutes"] == 120


def test_a_ray_step_cannot_size_itself(repo):
    ray_step(repo, sized=True)                          # resources_per_step: {train-model: {cpu: 1, ray: {}}}
    repo.write()
    with pytest.raises(ConfigError, match="a Ray step cannot also set cpu.*cannot authenticate to the launcher"):
        resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))


def test_a_step_over_the_platform_limit_fails_validation(repo):
    repo.manifests["dev"]["tenants"]["churn"]["ray"] = {"limits": {"max_workers": 1}}
    ray_step(repo, workers=2).write()
    with pytest.raises(ConfigError, match="workers 2 is outside 0 to 1"):
        resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))


def test_the_cluster_size_may_differ_per_environment(repo):
    ray_step(repo, workers=0)
    repo.usecase["spec"]["environments"]["prod"]["steps"] = {"train-model": {"ray": {"workers": 2}}}
    repo.add_env("prod")
    repo.write()
    dev = resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))
    prod = resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "prod"))
    assert dev.ray["steps"]["train-model"]["workers"] == 0 and prod.ray["steps"]["train-model"]["workers"] == 2


def test_the_ray_client_must_match_the_clusters(repo):
    from pipewright.checks import ray_version

    ray_step(repo).write()
    uc = resolve_usecase(repo.root / "usecases/churn/usecase.yaml", load_environment(repo.root, "dev"))
    check = ray_version.check(uc)
    assert not check.ok and "add `ray[default]==2.47.1`" in check.message and "ray` extra" in check.message
    reqs = repo.root / "usecases/churn/components/requirements.txt"
    reqs.write_text(reqs.read_text() + "ray[default]>=2.40\ngoogle-cloud-aiplatform[ray]==2.3.0\n")
    assert "must pin `ray[default]==2.47.1`" in ray_version.check(uc).message
    reqs.write_text(reqs.read_text().replace("ray[default]>=2.40", "ray[default]==2.47.1"))
    assert ray_version.check(uc).ok


# ---------------------------------------------------------------- the contract and the cluster

def test_contract_carries_the_ray_image_by_digest():
    again = ReleaseContract.from_json(RAY_CONTRACT.to_json())
    assert again == RAY_CONTRACT
    again.validate()
    with pytest.raises(ConfigError, match="ray.image must be pinned by digest"):
        replace(RAY_CONTRACT, ray={**RAY_CONTRACT.ray, "image": "x/churn-ray:latest"}).validate()
    old = json.loads(V3.to_json())
    old.pop("ray")
    assert ReleaseContract.from_dict(old).ray == {}                  # contracts from before Ray still read


def test_cluster_body():
    s = settings.platform_settings({"network_attachment": "projects/p/regions/r/networkAttachments/ray"}, None)
    body = cluster_body(RAY_CONTRACT, "tune-on-ray", "run-1", s, {"kms_key": "projects/p/locations/r/keyRings/k/"
                                                                            "cryptoKeys/aiplatform"}, now=1000)
    assert body["resourcePools"] == [{"id": "head-node", "replicaCount": "3",
                                      "machineSpec": {"machineType": "n1-standard-4"},
                                      "diskSpec": {"bootDiskType": "pd-ssd", "bootDiskSizeGb": 100}}]
    runtime = body["resourceRuntimeSpec"]
    assert runtime["raySpec"]["resourcePoolImages"] == {"head-node": RAY_CONTRACT.ray["image"]}
    assert runtime["serviceAccountSpec"] == {"enableCustomServiceAccount": True, "serviceAccount": RUNNER}
    assert body["pscInterfaceConfig"] == {"networkAttachment": "projects/p/regions/r/networkAttachments/ray"}
    assert body["encryptionSpec"]["kmsKeyName"].endswith("cryptoKeys/aiplatform")
    labels = body["labels"]
    assert labels["managed-by"] == "pipewright" and labels["run-id"] == "run-1"
    assert labels["pipewright-step"] == "tune-on-ray" and labels["pipewright-deadline"] == str(1000 + 60 * 90)

    gpu = replace(RAY_CONTRACT, ray={**RAY_CONTRACT.ray, "steps": {"tune-on-ray": {
        **STEP, "accelerator": {"type": "NVIDIA_TESLA_T4", "count": 1}}}})
    pools = cluster_body(gpu, "tune-on-ray", "run-1", settings.platform_settings(None, None), None)["resourcePools"]
    assert [p["id"] for p in pools] == ["head-node", "worker-pool1"]
    assert "acceleratorType" not in pools[0]["machineSpec"] and pools[1]["machineSpec"]["acceleratorCount"] == 1
    plain = cluster_body(RAY_CONTRACT, "tune-on-ray", "run-1", settings.platform_settings(None, None), None)
    assert "pscInterfaceConfig" not in plain and "encryptionSpec" not in plain


class FakeVertex:
    """persistentResources and pipelineJobs, as the REST API returns them."""

    def __init__(self):
        self.clusters, self.jobs, self.calls = {}, {}, []

    def post(self, url, body, **params):
        from pipewright.gcp.session import Response

        self.calls.append(("POST", url, params))
        name = f"{url.split('/v1/', 1)[1]}/{params['persistentResourceId']}"
        if name in self.clusters:
            return Response(409, {"error": {"message": "exists"}})
        self.clusters[name] = {"name": name, "labels": body["labels"], "state": "PROVISIONING",
                               "createTime": "2026-10-10T00:00:00Z"}
        return Response(200, {"name": f"{name}/operations/1"})

    def delete(self, url, **params):
        from pipewright.gcp.session import Response

        self.calls.append(("DELETE", url, params))
        name = url.split("/v1/", 1)[1]
        return Response(200, {}) if self.clusters.pop(name, None) else Response(404, {})

    def get(self, url, **params):
        from pipewright.gcp.session import Response

        if url.endswith("/persistentResources"):
            return Response(200, {"persistentResources": list(self.clusters.values())})
        run = params["filter"].split('"')[1]
        return Response(200, {"pipelineJobs": [{"state": self.jobs[run]}] if run in self.jobs else []})


def test_create_delete_and_who_may_ask():
    gcp = FakeVertex()
    rays = RayClusters(gcp, lambda: {"tenants": {}})
    first = rays.create(RAY_CONTRACT, "tune-on-ray", "run-1", RUNNER)
    assert first["status"] == "creating" and first["cluster"].endswith(settings.cluster_id("churn", "run-1",
                                                                                         "tune-on-ray"))
    assert first["record"]["service_account"] == RUNNER and first["record"]["pools"] == [{"machine": "n1-standard-4",
                                                                                          "nodes": 3}]
    assert first["record"]["kms_key"] is None and first["record"]["network_attachment"] is None   # recorded as is
    assert rays.create(RAY_CONTRACT, "tune-on-ray", "run-1", RUNNER)["status"] == "exists"       # a retry
    with pytest.raises(RayRequestError, match="only its runner identity may") as e:
        rays.create(RAY_CONTRACT, "tune-on-ray", "run-1", "someone@else.com")
    assert e.value.status == 403
    with pytest.raises(RayRequestError, match="has no Ray cluster"):
        rays.create(RAY_CONTRACT, "train-model", "run-1", RUNNER)
    with pytest.raises(RayRequestError, match="has no Ray cluster"):
        rays.create(V3, "tune-on-ray", "run-1", RUNNER)                    # a release without Ray
    assert rays.delete(RAY_CONTRACT, "tune-on-ray", "run-1", RUNNER)["status"] == "deleting"
    assert rays.delete(RAY_CONTRACT, "tune-on-ray", "run-1", RUNNER)["status"] == "gone"


def test_cleanup_removes_clusters_of_ended_runs_and_past_deadlines():
    gcp = FakeVertex()
    rays = RayClusters(gcp, lambda: {"tenants": {}})
    for run in ("running", "done", "late", "orphan"):
        rays.create(RAY_CONTRACT, "tune-on-ray", run, RUNNER)
    gcp.jobs = {"running": "PIPELINE_STATE_RUNNING", "done": "PIPELINE_STATE_FAILED",
                "late": "PIPELINE_STATE_RUNNING"}
    late = next(c for c in gcp.clusters.values() if c["labels"]["run-id"] == "late")
    late["labels"]["pipewright-deadline"] = "1"
    results = rays.reap("p", "europe-west2")
    assert {r["reason"] for r in results} == {"its run ended (failed)", "past its deadline",
                                              "no pipeline run has its run ID"}
    assert [c["labels"]["run-id"] for c in gcp.clusters.values()] == ["running"]


# ---------------------------------------------------------------- the launcher

def test_launcher_ray_routes(monkeypatch):
    pytest.importorskip("flask")
    from pipewright.launcher.app import create_app
    from pipewright.retraining import identity
    from tests.test_launcher import FakeRegistry
    from tests.test_retraining import _token

    reg = FakeRegistry([RAY_CONTRACT])
    reg.gcp, reg.bucket = FakeVertex(), "b"
    reg.manifest.update(project="p", region="europe-west2", ray={"mode": "per_run"},
                        launcher={"url": "https://l", "service_account": "l@p"},
                        tenants={})
    client = create_app(reg).test_client()
    body = {"action": "create", "usecase": "churn", "run_id": "run-1", "step": "tune-on-ray"}
    assert client.post("/ray/clusters", json=body).status_code == 401
    assert client.post("/ray/clusters", json=body, headers={"Authorization": _token("x@p")}).status_code == 403
    events = []
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: events.append(event))
    r = client.post("/ray/clusters", json=body, headers={"Authorization": _token(RUNNER)})
    assert r.status_code == 200 and r.json["status"] == "creating" and "record" not in r.json
    assert events[-1]["event_type"] == "ray.cluster.created" and events[-1]["data"]["step"] == "tune-on-ray"
    assert events[-1]["run_id"] == "run-1" and events[-1]["data"]["service_account"] == RUNNER
    assert client.post("/ray/clusters", json={**body, "step": "nope"},
                       headers={"Authorization": _token(RUNNER)}).status_code == 400

    # Over Pub/Sub the message carries the runner's ID token, verified by the launcher.
    monkeypatch.setattr(identity, "verify", lambda token, audiences: {"tok-runner": RUNNER}.get(token) or "")
    def push(message):
        return client.post("/pubsub/run", json={"message": {"data": base64.b64encode(
            json.dumps(message).encode()).decode()}})
    assert push({**body, "kind": "ray.cluster", "action": "delete", "id_token": "tok-runner"}).json["status"] == \
        "deleting"
    refused = push({**body, "kind": "ray.cluster", "id_token": "tok-other"})
    assert refused.status_code == 200 and refused.json["status"] == "refused"                  # acknowledged
    assert client.post("/retraining/tick", json={}).json["ray"] == []


# ---------------------------------------------------------------- the Ray example

def test_ray_example_resolves(repo):
    from pipewright.scaffold.new_usecase import list_examples, new_usecase

    assert "ray" in list_examples()
    repo.write()
    dest = new_usecase(repo.root, "churn-ray", "churn", "team", example="ray")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    assert uc.ray["steps"]["tune-on-ray"]["workers"] == 1 and uc.ray_requirements.name == "ray-requirements.txt"
    from pipewright.checks import ray_version

    assert ray_version.check(uc).ok
    assert (dest / "components" / "ray_tune_job.py").is_file() and (dest / "components" / "train.py").is_file()


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_ray_example_compiles(repo):
    from pipewright.build.compile import check_parameters, compile_all
    from pipewright.scaffold.new_usecase import new_usecase

    repo.write()
    dest = new_usecase(repo.root, "churn-ray", "churn", "team", example="ray")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    infos = compile_all(uc)
    assert check_parameters(uc, infos) == ([], []) and "tune-on-ray" in infos["training"].tasks


def test_ray_image_context(repo, tmp_path):
    from pipewright.build.images import ray_build_context
    from pipewright.scaffold.new_usecase import new_usecase

    repo.write()
    dest = new_usecase(repo.root, "churn-ray", "churn", "team", example="ray")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    ctx = ray_build_context(uc, tmp_path / "ctx")
    assert "torch==2.4.1+cpu" in (ctx / "requirements.txt").read_text()
    assert (ctx / "components" / "ray_tune_job.py").is_file()
    assert (ctx / "components" / "pipewright_runtime.py").is_file()
    assert not (ctx / "components" / "requirements.txt").exists()
    assert "ENV PYTHONPATH=/app/components${PYTHONPATH:+:${PYTHONPATH}}" in (ctx / "Dockerfile").read_text()


def _runtime(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py")
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    return runtime


class _Resp:
    def __init__(self, status, text="", body=None):
        self.status_code, self.text, self._body = status, text, body

    def json(self):
        return self._body


def test_id_token_from_the_metadata_server_by_account_name(monkeypatch):
    import requests

    runtime = _runtime("runtime_tok")
    asked = []

    def get(url, params=None, headers=None, timeout=None):
        assert headers == {"Metadata-Flavor": "Google"}
        asked.append(url.rsplit("/service-accounts", 1)[1])
        if url.endswith("/service-accounts/"):
            return _Resp(200, f"default/\n{RUNNER}/\n")
        if f"/{RUNNER}/identity" in url and params["audience"] == "https://launcher":
            return _Resp(200, "a.b.c")
        return _Resp(404, "please provide a user-specified service account")

    monkeypatch.setattr(requests, "get", get)
    assert runtime.id_token("https://launcher") == "a.b.c"
    assert asked == ["/", "/", f"/{RUNNER}/identity"]               # the named account first, never `default`


def test_id_token_falls_back_to_iam_when_the_metadata_server_refuses(monkeypatch):
    """Agent Platform workers' identity endpoint is unreliable ("please provide a user-specified service account"):
    the IAM Credentials API signs a token for the runner instead."""
    import requests

    runtime = _runtime("runtime_tok_iam")

    def get(url, params=None, headers=None, timeout=None):
        if url.endswith("/service-accounts/"):
            return _Resp(200, "default/\n")
        if url.endswith("/default/email"):
            return _Resp(200, RUNNER)
        return _Resp(404, "please provide a user-specified service account")

    posted = {}

    class Session:
        def post(self, url, json=None, timeout=None):
            posted.update(url=url, body=json)
            return _Resp(200, body={"token": "iam.signed.token"})

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(runtime, "_session", lambda: Session())
    assert runtime.id_token("https://launcher", wait=0) == "iam.signed.token"
    assert posted["url"].endswith(f"/serviceAccounts/{RUNNER}:generateIdToken")
    assert posted["body"] == {"audience": "https://launcher", "includeEmail": True}

    class Refused:
        def post(self, url, json=None, timeout=None):
            return _Resp(403, "Permission 'iam.serviceAccounts.getOpenIdToken' denied")

    monkeypatch.setattr(runtime, "_session", lambda: Refused())
    with pytest.raises(RuntimeError, match="after 2 tries .*serviceAccountOpenIdTokenCreator.*getOpenIdToken"):
        runtime.id_token("https://launcher", attempts=2, wait=0)


def test_a_dropped_cluster_request_is_sent_again(monkeypatch):
    """No cluster a few minutes after asking (a dead-lettered request): ask again; the ID is fixed, so it is safe."""
    import time as real_time

    spec = importlib.util.spec_from_file_location(
        "runtime_resend", Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py")
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    clock = {"t": 1000.0}
    monkeypatch.setattr(real_time, "time", lambda: clock["t"])
    monkeypatch.setattr(real_time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    requests_sent = []
    monkeypatch.setattr(runtime, "_ray_request", lambda cfg, action, step, run: requests_sent.append(action))
    # The cluster appears only after the second create request.
    monkeypatch.setattr(runtime, "_ray_state", lambda name, region: {"state": "RUNNING"}
                        if requests_sent.count("create") >= 2 else {})
    monkeypatch.setattr(runtime, "log", lambda *a, **k: None)
    ml = {"usecase": "churn", "platform": {"project": "p", "region": "r", "ray": {"steps": {
        "tune-on-ray": {"max_minutes": 60}}}}}
    with runtime.ray_cluster(ml, "tune-on-ray", "run-1") as cluster:
        assert cluster.endswith(runtime.ray_cluster_id("churn", "run-1", "tune-on-ray"))
    assert requests_sent == ["create", "create", "delete"]
