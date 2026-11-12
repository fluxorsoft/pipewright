"""Promotion takes the release that trained a version from its run when the version's own label names an earlier
release of the same commit (1.4.3 GA QA, live: the component image of a same-commit re-release says the old one)."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import registry
from pipewright.release import promote


@pytest.fixture
def source(monkeypatch):
    version = {"versionId": "1", "versionAliases": ["default"],
               "labels": {"release": "c9f57c514102", "run_id": "run-r2441"}}
    monkeypatch.setattr(promote, "_find_model", lambda gcp, env, name: {"name": "m"})
    monkeypatch.setattr(promote, "_versions", lambda gcp, env, model: [version])
    return SimpleNamespace(name="ent", region="us-central1", project="p")


def test_the_run_says_the_promoted_release_trained_it(monkeypatch, source):
    ran = {"r2441": "c9f57c514102-f0f6f3e0"}
    monkeypatch.setattr(registry, "run_release", lambda env, gcp, run: ran.get(run, ""))
    assert promote.source_version(None, source, "m", "c9f57c514102-f0f6f3e0", None)[0] == "1"


def test_another_release_is_still_refused(monkeypatch, source):
    monkeypatch.setattr(registry, "run_release", lambda env, gcp, run: "c9f57c514102")
    with pytest.raises(ConfigError, match="was trained by release"):
        promote.source_version(None, source, "m", "c9f57c514102-f0f6f3e0", None)


def test_an_unreadable_run_keeps_the_refusal(monkeypatch, source):
    monkeypatch.setattr(registry, "run_release", lambda env, gcp, run: "")
    with pytest.raises(ConfigError, match="was trained by release"):
        promote.source_version(None, source, "m", "c9f57c514102-f0f6f3e0", None)
