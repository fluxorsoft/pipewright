"""model.serving.target kserve (private-gke): the registry's default version as an InferenceService, with the
container and environment it was registered with, in the tenant's serving namespace."""

import copy
from types import SimpleNamespace

import pytest

from pipewright import kube
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.kube import Response
from pipewright.models import kserve
from tests.conftest import MANIFEST

MIRROR = "europe-west2-docker.pkg.dev/acme/images/mirror"
VERSION = {"name": "projects/1/locations/r/models/9@3", "versionId": "3",
           "artifactUri": "gs://art/pipeline-root/churn/churn/model",
           "containerSpec": {"imageUri": "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"}}


def serving_env(repo, **kserve_over):
    tenants = copy.deepcopy(MANIFEST["tenants"])
    for t in tenants.values():
        t["online_serving"] = True
    repo.add_env("gke", orchestrator="kfp", tenants=tenants,
                 kubernetes={"endpoint": "https://10.0.0.2", "dns_endpoint": "gke.example", "namespace": "kubeflow"},
                 kserve={"mirror": MIRROR, "service_account": "serving",
                         "namespaces": {t: f"serving-{t}" for t in tenants}, "exposure": "cluster", **kserve_over})
    repo.usecase["spec"].setdefault("model", {})["serving"] = {"enabled": True, "target": "kserve",
                                                               "machine": "e2-standard-4",
                                                               "replicas": {"min": 0, "max": 3}}
    root = repo.write()
    env = load_environment(repo.root, "gke")
    return resolve_usecase(root, env), env


def test_an_inference_service_runs_the_registered_container_as_agent_platform_does(repo):
    uc, env = serving_env(repo)
    s = kserve.settings(uc, env)
    body = kserve.inference_service(uc, env, VERSION, s)
    assert body["metadata"]["namespace"] == f"serving-{uc.tenant}"
    assert body["metadata"]["annotations"]["serving.kserve.io/deploymentMode"] == "Standard"
    predictor = body["spec"]["predictor"]
    assert predictor["serviceAccountName"] == "serving"
    assert predictor["minReplicas"] == 1 and predictor["maxReplicas"] == 3        # Standard mode: no scale to zero
    c = predictor["containers"][0]
    assert c["image"] == f"{MIRROR}/us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"
    envs = {e["name"]: e["value"] for e in c["env"]}
    assert envs["AIP_STORAGE_URI"] == VERSION["artifactUri"] and envs["AIP_PREDICT_ROUTE"] == "/predictions/model"
    assert envs["AIP_HTTP_PORT"] == "8080" and c["readinessProbe"]["httpGet"]["path"] == "/ping"
    assert c["resources"]["requests"] == {"cpu": "3400m", "memory": "12Gi"}       # e2-standard-4 as a pod
    assert body["metadata"]["labels"]["pipewright.dev/version"] == "3"


def test_images_in_the_environments_registry_are_not_mirrored(repo):
    uc, env = serving_env(repo)
    own = env.artifact_registry["docker"].split("/", 2)
    image = f"{own[0]}/{own[1]}/serving/churn@sha256:" + "a" * 64
    assert kserve.serving_image(image, env, MIRROR) == image
    assert kserve.serving_image("ghcr.io/x/y:1", env, MIRROR) == f"{MIRROR}/ghcr.io/x/y:1"


def test_kserve_needs_the_platforms_serving_namespace(repo):
    uc, env = serving_env(repo, namespaces={})
    with pytest.raises(ConfigError, match="no serving namespace"):
        kserve.settings(uc, env)


class FakeKube:
    def __init__(self):
        self.isvc, self.posted = None, []

    def get(self, path, **params):
        if "/inferenceservices/" in path:
            return Response(200, self.isvc) if self.isvc else Response(404, {})
        if "/deployments/" in path:
            return Response(200, {"spec": {"replicas": 1}, "status": {"updatedReplicas": 1, "availableReplicas": 1,
                                                                       "replicas": 1}})
        return Response(404, {})

    def post(self, path, body):
        self.posted.append((path, body))
        if path.endswith("/inferenceservices"):
            self.isvc = {**body, "metadata": {**body["metadata"], "resourceVersion": "1"},
                         "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
            return Response(201, body)
        return Response(200, {"predictions": [[0.1, 0.9]]})

    def request(self, method, path, body=None, **params):
        self.isvc = {**body, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        return Response(200, body)

    def delete(self, path):
        return Response(200, {})


@pytest.fixture
def fake(monkeypatch):
    k = FakeKube()
    monkeypatch.setattr(kube.KubeClient, "from_manifest", classmethod(lambda cls, m, gcp, dns=False: k))
    monkeypatch.setattr(kserve, "default_version", lambda uc, env, gcp: VERSION)
    monkeypatch.setattr(kserve, "_deployed", lambda *a, **k: None)
    from pipewright.models import gates

    monkeypatch.setattr(gates, "verify", lambda *a, **k: [])
    return k


def test_deploy_dry_run_then_deploy_then_already_live_then_predict(repo, fake):
    uc, env = serving_env(repo)
    gcp = SimpleNamespace(identity=lambda: "ci@p")
    assert kserve.deploy(uc, env, gcp, dry_run=True, log=lambda *_: None)["dry_run"] and fake.isvc is None
    assert kserve.deploy(uc, env, gcp, log=lambda *_: None)["status"] == "deployed"
    assert fake.isvc["spec"]["predictor"]["containers"][0]["env"][0]["value"] == VERSION["artifactUri"]
    assert kserve.deploy(uc, env, gcp, log=lambda *_: None)["status"] == "live"
    assert kserve.KServe(uc, env, gcp).predict([[1, 2]]) == {"predictions": [[0.1, 0.9]]}
    path, body = fake.posted[-1]
    assert path.endswith(f"/services/{kserve.service_name(uc)}-predictor:80/proxy/predictions/model")
    assert body == {"instances": [[1, 2]]}


def test_there_is_no_canary_on_kserve(repo, fake):
    uc, env = serving_env(repo)
    with pytest.raises(ConfigError, match="no canary"):
        kserve.deploy(uc, env, SimpleNamespace(identity=lambda: None), canary=True)


def test_gates_still_decide_what_is_served(repo, fake, monkeypatch):
    from pipewright.models import gates

    uc, env = serving_env(repo)
    monkeypatch.setattr(gates, "verify", lambda *a, **k: ["auc 0.6 is below 0.7"])
    with pytest.raises(ConfigError, match="not served"):
        kserve.deploy(uc, env, SimpleNamespace(identity=lambda: None), log=lambda *_: None)
    assert fake.isvc is None


# Phase 6: KServe serves only what was approved, as deploy-model on Agent Platform does.
def test_kserve_refuses_a_first_version_nobody_approved(repo, fake, monkeypatch):
    uc, env = serving_env(repo)
    monkeypatch.setattr(kserve, "default_version", lambda uc, env, gcp: {**VERSION, "labels": {"approval": "pending"}})
    with pytest.raises(ConfigError, match="nobody has approved it"):
        kserve.deploy(uc, env, SimpleNamespace(identity=lambda: None), log=lambda *_: None)
    assert fake.isvc is None


def test_kserve_refuses_a_version_that_is_not_the_one_approved(repo, fake, monkeypatch):
    from pipewright.models import integrity

    uc, env = serving_env(repo)
    check = integrity.Check(problems=["model.pt: changed after the approval (sha256 0123456789abcdef approved)"])
    monkeypatch.setattr(integrity, "verify", lambda *a, **k: check)
    said = []
    with pytest.raises(ConfigError, match="not what was approved"):
        kserve.deploy(uc, env, SimpleNamespace(identity=lambda: None), log=said.append)
    assert fake.isvc is None and any(line.startswith("approval: NOT what was approved") for line in said)


# -- 1.4.1 (FR-101 item 2): pods load the approved copy and check it before the server starts ---------------------

def _run_pod_check(monkeypatch, objects, spec):
    """Runs the init container's script against fake Cloud Storage objects {name: (generation, bytes)}."""
    import io
    import json
    import urllib.request

    from pipewright.models.integrity import POD_CHECK

    def urlopen(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req
        if "metadata.google.internal" in url:
            return io.BytesIO(json.dumps({"access_token": "t"}).encode())
        name = urllib.parse.unquote(url.split("/o/", 1)[1].split("?", 1)[0])
        gen, data = objects[name]
        if "alt=media" in url:
            class R(io.BytesIO):
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False
            return R(data)
        return io.BytesIO(json.dumps({"generation": str(gen)}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setenv("PIPEWRIGHT_APPROVED_FILES", json.dumps(spec))
    code = 0
    try:
        exec(compile(POD_CHECK, "pod_check", "exec"), {"__name__": "__main__"})
    except SystemExit as e:
        code = e.code
    return code


def test_the_pod_check_passes_the_approved_copy_and_stops_a_swapped_one(monkeypatch, capsys):
    import hashlib

    good = hashlib.sha256(b"weights").hexdigest()
    spec = {"folder": "gs://art/pr/m-approved-v4-abcd", "files": [{"file": "model.pt", "generation": "7",
                                                                 "sha256": good}]}
    assert _run_pod_check(monkeypatch, {"pr/m-approved-v4-abcd/model.pt": (7, b"weights")}, spec) == 0
    assert "1 approved files checked" in capsys.readouterr().out
    assert _run_pod_check(monkeypatch, {"pr/m-approved-v4-abcd/model.pt": (8, b"other")}, spec) == 1
    out = capsys.readouterr().out
    assert "NOT SERVED" in out and "generation 8 is not the approved copy 7" in out
    assert _run_pod_check(monkeypatch, {"pr/m-approved-v4-abcd/model.pt": (7, b"other")}, spec) == 1
    assert "not the approved" in capsys.readouterr().out


def test_a_pinned_inference_service_points_at_the_copy_and_has_the_check():
    from types import SimpleNamespace

    from pipewright.models.kserve import inference_service

    uc = SimpleNamespace(name="churn", tenant="t", model={"name": "churn-classifier", "serving": {
        "machine": "n2-standard-4", "replicas": {"min": 1, "max": 2}}})
    env = SimpleNamespace(artifact_registry={"docker": "europe-west2-docker.pkg.dev/p/r"})
    version = {"versionId": "4", "name": "projects/p/models/m@4", "artifactUri": "gs://art/pr/m",
               "containerSpec": {"imageUri": "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"}}
    s = {"namespace": "serving-t", "mirror": None, "service_account": "serving", "exposure": "cluster",
         "verifier_image": None}
    pinned = {"folder": "gs://art/pr/m-approved-v4-abcd", "files": [{"file": "model.pt", "generation": "7",
                                                                     "sha256": "a" * 64}]}
    body = inference_service(uc, env, version, s, pinned)
    pred = body["spec"]["predictor"]
    env_vars = {e["name"]: e["value"] for e in pred["containers"][0]["env"]}
    assert env_vars["AIP_STORAGE_URI"] == "gs://art/pr/m-approved-v4-abcd"
    assert pred["initContainers"][0]["name"] == "pipewright-check"
    assert pred["initContainers"][0]["image"] == pred["containers"][0]["image"]
    plain = inference_service(uc, env, version, s)
    assert "initContainers" not in plain["spec"]["predictor"]
    assert {e["name"]: e["value"] for e in plain["spec"]["predictor"]["containers"][0]["env"]}["AIP_STORAGE_URI"] \
        == "gs://art/pr/m"


def test_a_mirrored_pod_image_is_judged_by_the_image_it_mirrors():
    """Found live in 1.4.1: a private cluster pulls from the mirror, whose manifest digest differs from the source's;
    the watch compares the source image with the approval, not the mirror path (which never equals it)."""
    from pipewright.models import watch

    mirror = "us-central1-docker.pkg.dev/p/images/mirror"
    src = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"
    assert kserve.mirrored_from(f"{mirror}/{src}", mirror + "/") == src
    assert kserve.mirrored_from(src, mirror) == src
    assert kserve.mirrored_from("us-central1-docker.pkg.dev/p/images/own:1", mirror) == \
        "us-central1-docker.pkg.dev/p/images/own:1"
    assert kserve.mirrored_from(f"{mirror}/{src}", None) == f"{mirror}/{src}"
    isvc = {"metadata": {"name": "churn", "annotations": {"pipewright.dev/model-resource": "projects/p/models/1@2"},
                         "labels": {"pipewright.dev/usecase": "churn"}},
            "spec": {"predictor": {"containers": [{"image": f"{mirror}/{src}", "env": []}]}}}
    assert watch._kserve_use(isvc, "serving", {}, None, mirror).image == src
