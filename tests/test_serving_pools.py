"""FR-105 (1.4.2): model co-hosting on shared pools, only when a use case asks; without it nothing changes."""
from types import SimpleNamespace

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.serving import pools, prices

TF = "europe-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-15:latest"
TORCH = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"
RUNNER = "ml-runner-churn@acme-ml-dev.iam.gserviceaccount.com"


def pool(**kw):
    return {"tenant": "churn", "machine": "n1-standard-4", "image": TORCH, "min_replicas": 1, "max_replicas": 2, **kw}


def setup(repo, serving=None, pools_=None, env="dev", manifest=None, endpoints=None):
    if env != "dev":
        repo.add_env(env)
    m = repo.manifests[env]
    if pools_ is not None or endpoints is not None:
        m["serving"] = {**({"pools": pools_} if pools_ is not None else {}),
                        **({"endpoints": endpoints} if endpoints is not None else {})}
    m.update(manifest or {})
    repo.usecase["spec"]["environments"][env] = {"model": {"serving": {"enabled": True, **(serving or {})}}}
    path = repo.write()
    return lambda: resolve_usecase(path, load_environment(repo.root, env))


def test_without_a_pool_nothing_changes(repo):
    uc = setup(repo, pools_={"shared": pool()})()
    assert "pool" not in uc.model["serving"]
    assert "pool" not in (uc.ml_config().get("serving") or {})
    assert not any("pool" in w for w in uc.warnings)


def test_a_use_case_joins_a_declared_pool(repo):
    uc = setup(repo, {"pool": "shared"}, {"shared": pool()})()
    assert uc.ml_config()["serving"]["pool"] == "shared"
    assert any("about $" in w and "a month at its minimum" in w for w in uc.warnings)
    assert any("one machine" in w for w in uc.warnings)          # min 1 outside production: a warning only


def test_an_unknown_pool_or_another_tenants_is_refused(repo):
    with pytest.raises(ConfigError, match="has no such pool"):
        setup(repo, {"pool": "nope"}, {"shared": pool()})()
    with pytest.raises(ConfigError, match="belongs to tenant 'fraud'"):
        setup(repo, {"pool": "shared"}, {"shared": pool(tenant="fraud")})()


def test_the_pool_sets_the_machines(repo):
    with pytest.raises(ConfigError, match="remove model.serving.machine"):
        setup(repo, {"pool": "shared", "machine": "n1-standard-8"}, {"shared": pool()})()


def test_custom_images_stay_on_dedicated_machines(repo):
    with pytest.raises(ConfigError, match="Custom serving images are supported on dedicated deployments"):
        setup(repo, {"pool": "shared"}, {"shared": pool(image="europe-west2-docker.pkg.dev/acme/ml/serve:1")})()


def test_production_keeps_two_machines_and_no_preview_unless_allowed(repo):
    with pytest.raises(ConfigError, match="keeps at least 2 on a pool"):
        setup(repo, {"pool": "shared"}, {"shared": pool(image=TF)}, env="prod")()
    with pytest.raises(ConfigError, match="PyTorch co-hosting is a Google Preview"):
        setup(repo, {"pool": "shared"}, {"shared": pool(min_replicas=2)}, env="prod")()
    uc = setup(repo, {"pool": "shared"}, {"shared": pool(min_replicas=2)}, env="prod",
               manifest={"policies": {"serving": {"pools": {"allow_preview": True}}}})()
    assert uc.ml_config()["serving"]["pool"] == "shared"


def test_one_kind_of_endpoint_per_pool(repo):
    endpoints = {"churn": {"name": "churn-endpoint", "private": True}}
    with pytest.raises(ConfigError, match="serves standard endpoints and churn's is psc"):
        setup(repo, {"pool": "shared"}, {"shared": pool()}, endpoints=endpoints)()
    uc = setup(repo, {"pool": "shared"}, {"shared": pool(network="psc")}, endpoints=endpoints)()
    assert any("private endpoint" in w for w in uc.warnings)


def test_terraform_pools_are_refused_where_cmek_is_required(repo):
    with pytest.raises(ConfigError, match="cannot set a pool's encryption key"):
        setup(repo, {"pool": "shared"}, {"shared": pool(managed_by="terraform")},
              manifest={"security": {"kms_key": "projects/p/locations/r/keyRings/k/cryptoKeys/a"}})()


def test_image_kinds():
    assert pools.image_kind(TF) == "tensorflow" and pools.image_kind(TORCH) == "pytorch"
    assert pools.image_kind(TORCH.replace(":latest", "@sha256:" + "a" * 64)) == "pytorch"
    assert pools.image_kind("us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest") is None
    assert pools.image_kind("europe-west2-docker.pkg.dev/acme/ml/serve:1") is None


def test_a_registered_version_must_fit_its_pool():
    assert pools.model_problems("shared", pool(), TORCH, {}) == []
    out = pools.model_problems("shared", pool(), "europe-west2-docker.pkg.dev/acme/ml/serve:1",
                               {"PIPEWRIGHT_ONLINE_FEATURES": "{}"})
    assert "not the pool's" in out[0] and "serving.json" in out[1]


def test_prices():
    assert prices.machine_hour("n1-standard-2") == pytest.approx(0.157, abs=0.001)
    assert prices.machine_hour("n1-standard-4") == pytest.approx(0.314, abs=0.002)
    assert prices.machine_hour("a2-highgpu-1g") is None
    assert "about $229 a month" in prices.pool_cost({"machine": "n1-standard-4", "min_replicas": 1})


class Gcp:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def identity(self):
        return "ci@acme-ml-dev.iam.gserviceaccount.com"

    def _do(self, method, url, body=None, **params):
        self.calls.append((method, url, body, params))
        for (m, part), resp in self.answers.items():
            if m == method and part in url:
                status, out = resp() if callable(resp) else resp
                return SimpleNamespace(ok=200 <= status < 300, status=status, body=out)
        return SimpleNamespace(ok=True, status=200, body={})

    def get(self, url, **p):
        return self._do("GET", url, **p)

    def post(self, url, body, **p):
        return self._do("POST", url, body, **p)

    def patch(self, url, body, **p):
        return self._do("PATCH", url, body, **p)

    def delete(self, url, **p):
        return self._do("DELETE", url, **p)


def env(**serving_pools):
    return SimpleNamespace(name="dev", project="acme-ml-dev", region="europe-west2", artifacts_bucket="art",
                           policies={}, observability={},
                           security={"kms_key": "projects/p/locations/r/keyRings/k/cryptoKeys/a"},
                           tenants={"churn": {"runner_service_account": RUNNER}}, serving={"pools": serving_pools})


def no_lock(monkeypatch):
    calls = []
    monkeypatch.setattr(pools, "acquire",
                        lambda gcp, e, name, what, log=print, sleep=None: calls.append(("lock", name)))
    monkeypatch.setattr(pools, "release", lambda gcp, e, name: calls.append(("unlock", name)))
    from pipewright.observability import events

    monkeypatch.setattr(events, "emit", lambda e, *a, **k: calls.append(("event", e["data"]["phase"])))
    return calls


def test_apply_creates_a_pool_with_cmek_and_the_tenants_identity(monkeypatch):
    no_lock(monkeypatch)
    gcp = Gcp({("GET", "deploymentResourcePools/shared"): (404, {})})
    steps = pools.apply(gcp, env(shared=pool()), log=lambda m: None, sleep=lambda s: None)
    assert steps == [{"pool": "shared", "action": "create", "detail": ""}]
    create = next(c for c in gcp.calls if c[0] == "POST")
    body = create[2]
    assert body["deploymentResourcePoolId"] == "shared"
    drp = body["deploymentResourcePool"]
    assert drp["serviceAccount"] == RUNNER and drp["encryptionSpec"]["kmsKeyName"].endswith("/cryptoKeys/a")
    assert drp["dedicatedResources"]["minReplicaCount"] == 1 and drp["dedicatedResources"]["maxReplicaCount"] == 2


def test_apply_resizes_in_place_under_the_lock_and_announces(monkeypatch):
    calls = no_lock(monkeypatch)
    current = {"dedicatedResources": {"machineSpec": {"machineType": "n1-standard-4"}, "minReplicaCount": 1,
                                      "maxReplicaCount": 2}}
    gcp = Gcp({("GET", "deploymentResourcePools/shared:query"): (200, {}),
               ("GET", "deploymentResourcePools/shared"): (200, current)})
    pools.apply(gcp, env(shared=pool(min_replicas=2, max_replicas=3)), log=lambda m: None, sleep=lambda s: None)
    patch = next(c for c in gcp.calls if c[0] == "PATCH")
    assert patch[3]["updateMask"].count("replica_count") == 2 and patch[2]["dedicatedResources"]["maxReplicaCount"] == 3
    assert calls == [("lock", "shared"), ("event", "starting"), ("event", "settled"), ("unlock", "shared")]


def test_a_machine_change_is_refused_never_rebuilt(monkeypatch):
    no_lock(monkeypatch)
    current = {"dedicatedResources": {"machineSpec": {"machineType": "n1-standard-2"}, "minReplicaCount": 1,
                                      "maxReplicaCount": 2}}
    gcp = Gcp({("GET", "deploymentResourcePools/shared"): (200, current)})
    with pytest.raises(ConfigError, match="a pool's machines cannot change"):
        pools.apply(gcp, env(shared=pool()), log=lambda m: None, sleep=lambda s: None)
    assert not any(c[0] in ("PATCH", "DELETE") for c in gcp.calls)


def test_the_lock_waits_for_another_change(monkeypatch):
    import json
    import time

    from pipewright.gcp import storage

    tries = {"n": 0}

    def write(gcp, bucket, name, text, content_type="application/json", if_generation_match=None):
        tries["n"] += 1
        if tries["n"] < 3:
            raise storage.PreconditionFailed("held")
        return 1

    monkeypatch.setattr(storage, "gcs_write", write)
    monkeypatch.setattr(storage, "gcs_read", lambda gcp, b, n: (json.dumps({"by": "other", "change": "deploy x",
                                                                            "since": time.time()}), 5))
    said = []
    pools.acquire(Gcp({}), env(shared=pool()), "shared", "deploy y", log=said.append, sleep=lambda s: None)
    assert tries["n"] == 3 and "waiting for another change" in said[0]
    tries["n"] = -100
    with pytest.raises(ConfigError, match="pool shared is busy"):
        pools.acquire(Gcp({}), env(shared=pool()), "shared", "deploy y", log=said.append, sleep=lambda s: None,
                      wait_seconds=60)


def test_pool_deploy_request_names_the_pools_identity():
    from pipewright.models.deploy import pool_deploy_request

    body = pool_deploy_request("projects/p/locations/r/models/m@3", "m-v3", "projects/p/locations/r/"
                               "deploymentResourcePools/shared", RUNNER, 10, {"111": 100})
    dm = body["deployedModel"]
    assert dm["sharedResources"].endswith("/shared") and dm["serviceAccount"] == RUNNER
    assert "dedicatedResources" not in dm and body["trafficSplit"] == {"0": 10, "111": 90}


def test_a_pool_models_settings_stay_in_its_files():
    """The churn example: no container variables on a pool; the handler reads serving.json only."""
    import os

    path = os.path.join(os.path.dirname(__file__), "..", "examples", "churn", "components", "register.py")
    text = open(path, encoding="utf-8").read()
    assert 'if online and not (ml_config.get("serving") or {}).get("pool") else None' in text
    handler = open(path.replace("register.py", "torchserve_handler.py"), encoding="utf-8").read()
    assert 'and not serving.get("pool")' in handler and '.get("mode") == "gateway"' in handler
    train = open(path.replace("register.py", "train.py"), encoding="utf-8").read()
    assert 'serving["pool"] = ml_config["serving"]["pool"]' in train


def test_an_environment_unsets_the_use_cases_machine_to_join_a_pool(repo):
    # GA QA (found live): a use case that sets a machine (the churn example does) could not join a pool in one
    # environment: the override could not remove the machine
    repo.usecase["spec"].setdefault("model", {})["serving"] = {"enabled": True, "machine": "n1-standard-4"}
    uc = setup(repo, {"pool": "shared", "machine": None}, {"shared": pool()})()
    assert uc.model["serving"]["pool"] == "shared" and uc.model["serving"]["machine"] == "n1-standard-2"
    with pytest.raises(ConfigError, match="null only with model.serving.pool"):
        setup(repo, {"machine": None})()
