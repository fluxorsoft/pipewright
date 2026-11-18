"""FR-136: `deploy-model --undeploy` works for a use case that no longer validates, from the live release's record."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import deploy


def _contract(tenant="retail", model="lapse-risk-classifier"):
    pipeline = SimpleNamespace(static_parameters={"ml_config": {"model_name": model, "online_features": {}}})
    return SimpleNamespace(release_id="r1", usecase="lapse-risk", tenant=tenant, environment="test",
                           pipelines={"training": {}}, metadata={"model": model}, pipeline=lambda name: pipeline)


def _patch(monkeypatch, pointer=True, contract=None):
    from pipewright.release import service

    monkeypatch.setattr(service, "read_pointer", lambda gcp, env, uc: (
        (SimpleNamespace(contract="releases/lapse-risk/r1.json"), 1) if pointer else (None, 0)))
    monkeypatch.setattr(service, "read_contract", lambda gcp, env, name: contract or _contract())


def test_undeploy_reads_the_live_release_when_the_use_case_does_not_validate(monkeypatch):
    _patch(monkeypatch)
    said = []
    env = SimpleNamespace(name="test", kserve=None)
    uc = deploy.released_for_undeploy(env, "lapse-risk", None, ConfigError("spec.data: bad\nmore"), log=said.append)
    assert (uc.name, uc.tenant, uc.model["name"], uc.model["serving"]["target"]) == (
        "lapse-risk", "retail", "lapse-risk-classifier", "vertex")
    assert uc.released_only and deploy.endpoint_name(uc) == "lapse-risk-endpoint"
    assert "does not validate (spec.data: bad)" in said[0] and "release r1" in said[0]


def test_undeploy_without_a_live_release_says_what_to_do(monkeypatch):
    _patch(monkeypatch, pointer=False)
    with pytest.raises(ConfigError, match="no live release in test"):
        deploy.released_for_undeploy(SimpleNamespace(name="test", kserve=None), "lapse-risk", None,
                                     ConfigError("spec.data: bad"), log=lambda *_: None)


def test_undeploy_finds_an_inference_service_on_kserve(monkeypatch):
    _patch(monkeypatch)
    from pipewright.models import kserve

    class Fake:
        def __init__(self, uc, env, gcp):
            assert uc.tenant == "retail"

        def current(self):
            return {"metadata": {"name": "lapse-risk"}}

    monkeypatch.setattr(kserve, "KServe", Fake)
    env = SimpleNamespace(name="test", kserve={"namespaces": {"retail": "retail-serving"}})
    uc = deploy.released_for_undeploy(env, "lapse-risk", None, ConfigError("bad"), log=lambda *_: None)
    assert uc.model["serving"]["target"] == "kserve"


def test_only_undeploy_falls_back(monkeypatch, tmp_path):
    """A deploy, promote or canary still needs a use case that validates: only taking a model down falls back."""
    import importlib

    main = importlib.import_module("pipewright.cli.main")

    monkeypatch.setattr(main, "load_environment", lambda root, env: SimpleNamespace(name="test"))
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda: SimpleNamespace())
    monkeypatch.setattr(main, "_selected", lambda root, names: {"lapse-risk": tmp_path})

    def broken(path, env):
        raise ConfigError("spec.data: bad")

    monkeypatch.setattr(main, "resolve_usecase", broken)
    called = []
    monkeypatch.setattr(deploy, "released_for_undeploy", lambda *a, **k: called.append(a) or SimpleNamespace(
        name="lapse-risk", tenant="retail", ml={}, model={"name": "m", "serving": {"target": "vertex"}},
        released_only=True))
    args = SimpleNamespace(root=str(tmp_path), env="test", usecase="lapse-risk", undeploy=False, approve=False,
                           dry_run=True, canary=False, promote=False, undeploy_canary=False, keep_lookup_service=False)
    with pytest.raises(ConfigError, match="spec.data: bad"):
        main.cmd_deploy_model(args)
    assert not called

    gone = []
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda: SimpleNamespace(session=SimpleNamespace(credentials=None)))
    monkeypatch.setattr(deploy, "undeploy", lambda uc, env, creds: gone.append(uc.name))
    monkeypatch.setattr(main, "_remove_lookup_service", lambda *a: None)
    monkeypatch.setattr(main, "_health_sync", lambda *a, **k: pytest.fail("no health sync from a release's record"))
    args.undeploy = True
    assert main.cmd_deploy_model(args) == 0
    assert called and gone == ["lapse-risk"]
