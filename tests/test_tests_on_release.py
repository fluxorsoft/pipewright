"""FR-66: tests.on_release makes `pipewright release` run tests.command before it builds anything (Pipewright 1.2.0).
Off by default: a 1.x use case releases without running its tests, as before."""
from types import SimpleNamespace

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.release import service


class Built(Exception):
    """Raised where the release would start building: the tests ran (or not) before it."""


def release(repo, monkeypatch, tests):
    repo.usecase["spec"]["tests"] = tests
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    env = load_environment(repo.root, "dev")
    ran = []
    monkeypatch.setattr("pipewright.testing.runner.run", lambda uc, log=print: ran.append(uc.name) or 3)
    monkeypatch.setattr(service, "release_id_for", lambda d: ("abc123", "abc123full"))
    monkeypatch.setattr(service, "read_pointer", lambda gcp, env, name: (None, 0))
    monkeypatch.setattr(service.tables_sync, "sync", lambda *a, **k: (_ for _ in ()).throw(Built()))
    gcp = SimpleNamespace(identity=lambda: "ci@acme", session=SimpleNamespace(credentials=None))
    return uc, lambda: service.release(uc, env, gcp, log=lambda *a: None), ran


def test_a_1_x_use_case_releases_without_running_its_tests(repo, monkeypatch):
    uc, run, ran = release(repo, monkeypatch, {"command": "python -m pytest -q"})
    assert uc.tests_on_release is False and uc.to_dict()["tests"] == {"command": "python -m pytest -q"}
    with pytest.raises(Built):
        run()
    assert ran == []


def test_failing_tests_stop_the_release_before_anything_is_built(repo, monkeypatch):
    uc, run, ran = release(repo, monkeypatch, {"command": "python -m pytest -q", "on_release": True})
    assert uc.to_dict()["tests"]["on_release"] is True
    with pytest.raises(ConfigError, match=r"tests.command failed \(exit 3\): nothing was built or released"):
        run()
    assert ran == ["churn"]
