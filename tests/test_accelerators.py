"""Pipewright 1.2: GPU limits for steps (FR-58), GPU serving (FR-61) and scale to zero (FR-62), each within what the
environment's manifest allows, and off unless set."""
import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.models import deploy

POLICY = {"steps": {"types": ["NVIDIA_TESLA_T4"], "max_count": 2}, "serving": {"types": ["NVIDIA_L4"], "max_count": 1}}


def resolved(repo, policy=None, steps=None, serving=None):
    if policy is not None:
        repo.manifests["dev"]["policies"]["accelerators"] = policy
    if steps:
        repo.usecase["spec"]["steps"] = steps
    if serving:
        repo.usecase["spec"].setdefault("model", {})["serving"] = {"enabled": True, **serving}
        repo.manifests["dev"]["tenants"]["churn"]["online_serving"] = True
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def problems(repo, **kw) -> str:
    with pytest.raises(ConfigError) as e:
        resolved(repo, **kw)
    return str(e.value)


def test_steps_may_ask_for_any_gpu_until_the_platform_lists_them(repo):
    gpu = {"train-model": {"gpu": {"type": "NVIDIA_A100_80GB", "count": 4}}}
    assert resolved(repo, steps=gpu).steps["train-model"]["gpu"]["count"] == 4          # as in 1.x
    text = problems(repo, policy=POLICY, steps=gpu)
    assert "steps.train-model.gpu.type NVIDIA_A100_80GB: dev allows NVIDIA_TESLA_T4" in text
    assert "steps.train-model.gpu.count 4: at most 2 in dev" in text
    ok = {"train-model": {"gpu": {"type": "NVIDIA_TESLA_T4", "count": 2}}}
    assert resolved(repo, policy=POLICY, steps=ok)


def test_serving_on_a_gpu_needs_the_environment_to_list_it(repo):
    serving = {"machine": "g2-standard-8", "accelerator": {"type": "NVIDIA_L4"}}
    assert "dev lists no serving accelerators" in problems(repo, serving=serving)
    uc = resolved(repo, policy=POLICY, serving=serving)
    assert uc.model["serving"]["accelerator"] == {"type": "NVIDIA_L4"}
    assert "at most 1 in dev" in problems(repo, policy=POLICY, serving={**serving, "accelerator": {
        "type": "NVIDIA_L4", "count": 2}})


def test_scale_to_zero_is_opt_in(repo):
    uc = resolved(repo, serving={"replicas": {"min": 0, "max": 2}})
    assert any("deploys one replica (as in 1.x)" in w for w in uc.warnings)
    uc = resolved(repo, serving={"replicas": {"min": 0, "max": 2}, "scale_to_zero": True})
    assert not uc.warnings and uc.model["serving"]["scale_to_zero"] is True
    assert "scale_to_zero needs replicas.min 0" in problems(repo, serving={"scale_to_zero": True})


def test_a_1_x_use_case_resolves_without_the_new_keys(repo):
    serving = resolved(repo).model["serving"]
    assert "accelerator" not in serving and "scale_to_zero" not in serving


def test_the_scale_to_zero_request():
    body = deploy.scale_to_zero_request("projects/p/locations/r/models/m@3", "churn-v3", "g2-standard-8", 2, 10,
                                        {"111": 70, "222": 30}, {"type": "NVIDIA_L4"}, "run@p")
    d = body["deployedModel"]
    assert d["model"] == "projects/p/locations/r/models/m@3" and d["serviceAccount"] == "run@p"
    assert d["dedicatedResources"] == {
        "machineSpec": {"machineType": "g2-standard-8", "acceleratorType": "NVIDIA_L4", "acceleratorCount": 1},
        "minReplicaCount": 0, "maxReplicaCount": 2, "scaleToZeroSpec": {"idleScaledownPeriod": "900s"}}
    assert body["trafficSplit"] == {"0": 10, "111": 63, "222": 27}
    assert deploy.scale_to_zero_request("m@1", "x", "n1-standard-2", 1, 100, {"9": 100}, None, None)[
        "trafficSplit"] == {"0": 100}


def test_deploying_waits_for_the_operation():
    from types import SimpleNamespace

    from pipewright.config.environment import Environment  # noqa: F401  (type only)

    calls = []

    class Gcp:
        def post(self, url, body):
            calls.append(url)
            return SimpleNamespace(ok=True, body={"name": "projects/p/locations/r/operations/1"})

        def get(self, url):
            calls.append(url)
            done = len(calls) > 2
            return SimpleNamespace(ok=True, body={"done": done})

    env = SimpleNamespace(region="europe-west2")
    deploy.deploy_scaling_to_zero(Gcp(), env, "projects/p/locations/r/endpoints/5", "m@1", "x", "n1-standard-2", 1,
                                  100, {}, None, None, sleep=lambda s: None)
    assert calls[0] == ("https://europe-west2-aiplatform.googleapis.com/v1beta1/projects/p/locations/r/endpoints/5"
                        ":deployModel")
    assert calls[-1].endswith("/v1beta1/projects/p/locations/r/operations/1")
