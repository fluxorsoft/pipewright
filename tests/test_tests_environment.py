"""FR-82: tests.environment: components runs tests.command in a virtual environment built from the components'
requirements, cached by their hash (Pipewright 1.2). The default stays Pipewright's own interpreter."""
import sys

from pipewright.config import load_environment, resolve_usecase
from pipewright.testing import runner


def resolved(repo, tests):
    repo.usecase["spec"]["tests"] = tests
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_the_default_is_pipewrights_interpreter(repo, monkeypatch):
    uc = resolved(repo, {"command": "python -m pytest -q"})
    assert uc.tests_environment == "pipewright" and uc.to_dict()["tests"] == {"command": "python -m pytest -q"}
    seen = {}
    monkeypatch.setattr(runner.subprocess, "run", lambda cmd, **k: seen.update(cmd=cmd, env=k["env"]) or
                        type("R", (), {"returncode": 0})())
    assert runner.run(uc, log=lambda *a: None) == 0
    assert sys.executable in seen["cmd"] and seen["env"]["PIPEWRIGHT_USECASE"] == "churn"


def test_components_builds_once_and_runs_there(repo, monkeypatch, tmp_path):
    uc = resolved(repo, {"command": "python -m pytest -q", "environment": "components"})
    assert uc.to_dict()["tests"]["environment"] == "components"
    (uc.components_dir / "requirements.txt").write_text("kfp==2.17.0\nnumpy\n", encoding="utf-8")
    monkeypatch.setenv("PIPEWRIGHT_CACHE", str(tmp_path))
    calls = []

    def fake_run(cmd, **k):
        calls.append(cmd)
        if isinstance(cmd, list) and cmd[1:3] == ["-m", "venv"]:
            python = runner._python(runner.Path(cmd[3]))
            python.parent.mkdir(parents=True)
            python.write_text("")
        return type("R", (), {"returncode": 0})()

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    assert runner.run(uc, log=lambda *a: None) == 0
    built = [c for c in calls if isinstance(c, list)]
    assert len(built) == 2 and built[1][-2:] == ["-r", str(uc.components_dir / "requirements.txt")]
    venv = next((tmp_path / "test-envs").iterdir())
    assert str(runner._python(venv)) in calls[-1]                       # `python` is the environment's
    calls.clear()
    assert runner.run(uc, log=lambda *a: None) == 0 and len(calls) == 1  # cached: nothing built again
    (uc.components_dir / "requirements.txt").write_text("kfp==2.17.0\nnumpy\npandas\n", encoding="utf-8")
    runner.run(uc, log=lambda *a: None)
    assert len(list((tmp_path / "test-envs").iterdir())) == 2            # new requirements, new environment
