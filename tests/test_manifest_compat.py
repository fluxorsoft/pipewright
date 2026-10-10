"""Manifest forward compatibility (R-05): keys a newer Pipewright added are ignored and reported; `requires` refuses
a version that is too old, so a policy is never silently skipped."""
import json

import pytest

from pipewright.config import load_environment
from pipewright.config.environment import check_requires, environment_from_manifest
from pipewright.config.schema import prune_unknown
from pipewright.errors import ConfigError


def test_unknown_keys_are_ignored_and_reported(repo):
    m = repo.manifests["dev"]
    m["fleet_view"] = {"endpoint": "https://console.example"}                   # a new top-level block
    m["tenants"]["churn"]["notebook_quota"] = 4                                 # a new tenant key
    m["security"] = {"kms_key": None, "colour": "blue"}                         # a new key in a known block
    repo.write()
    env = load_environment(repo.root, "dev")
    assert set(env.manifest_ignored) == {"fleet_view", "tenants.churn.notebook_quota", "security.colour"}
    assert "notebook_quota" not in env.tenants["churn"] and env.security == {"kms_key": None}


def test_known_keys_are_still_validated(repo):
    repo.manifests["dev"]["fleet_view"] = {}
    repo.manifests["dev"]["security"] = {"kms_key": "aiplatform"}              # known key, bad value
    repo.write()
    with pytest.raises(ConfigError, match="kms_key"):
        load_environment(repo.root, "dev")


def test_prune_follows_refs_and_optional_blocks():
    doc = {"ray": {"mode": "per_run", "limits": {"max_workers": 2, "max_gpus": 1}, "pools": []},
           "features": None, "tenants": {"t": {"ray": {"enabled": True, "spot": True}}}}
    out, dropped = prune_unknown(doc, "manifest")
    assert dropped == ["ray.limits.max_gpus", "ray.pools", "tenants.t.ray.spot"]
    assert out["ray"] == {"mode": "per_run", "limits": {"max_workers": 2}} and out["features"] is None


@pytest.mark.parametrize("spec, ok", [(">=0.1", True), (">=99.0", False), ("<0.1", False), (None, True)])
def test_requires_names_the_pipewright_a_manifest_needs(monkeypatch, spec, ok):
    import pipewright

    monkeypatch.setattr(pipewright, "__version__", "0.4.0.dev0")
    m = {"requires": spec} if spec else {}
    if ok:
        check_requires(m, "m.json")
    else:
        with pytest.raises(ConfigError, match=f"needs Pipewright {spec}, and this is 0.4.0.dev0"):
            check_requires(m, "m.json")


def test_a_bad_specifier_is_reported(monkeypatch):
    with pytest.raises(ConfigError, match="is not a version specifier"):
        check_requires({"requires": "the new one"}, "m.json")


def test_platform_services_get_the_same_rules(repo, tmp_path, monkeypatch):
    import pipewright

    monkeypatch.setattr(pipewright, "__version__", "0.4.0")
    m = dict(repo.manifests["dev"], fleet_view={})
    path = tmp_path / "environment.json"
    path.write_text(json.dumps(m), encoding="utf-8")
    assert environment_from_manifest(str(path)).manifest_ignored == ("fleet_view",)
    path.write_text(json.dumps(dict(m, requires=">=0.5")), encoding="utf-8")
    with pytest.raises(ConfigError, match="needs Pipewright >=0.5"):
        environment_from_manifest(str(path))


def test_the_launcher_refuses_a_manifest_that_needs_a_newer_version(repo, monkeypatch):
    import pipewright
    from pipewright.launcher import registry

    monkeypatch.setattr(pipewright, "__version__", "0.4.0")
    texts = {"pipewright/environment.json": json.dumps(dict(repo.manifests["dev"], fleet_view={}))}
    monkeypatch.setattr(registry, "gcs_read", lambda gcp, bucket, name: (texts.get(name), None))
    r = registry.Registry(None, "gs://b/pipewright/environment.json")       # unknown keys: loaded, logged
    assert "fleet_view" in r.manifest
    texts["pipewright/environment.json"] = json.dumps(dict(repo.manifests["dev"], requires=">=0.5"))
    with pytest.raises(ConfigError, match="needs Pipewright >=0.5"):
        registry.Registry(None, "gs://b/pipewright/environment.json")
