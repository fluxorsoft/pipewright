"""pipewright test: runs each use case's tests.command in its folder (the team's own unit tests).

tests.environment (Pipewright 1.2, FR-82): `pipewright` (the default, as in 1.x) runs the command with Pipewright's
own interpreter; `components` runs it in a virtual environment built from components/requirements.txt (numpy,
pandas, torch...), cached by the requirements' hash, so a test that imports a component passes in CI as on a laptop.
"""

from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from pipewright.config import ResolvedUseCase
from pipewright.errors import ConfigError


def cache_dir() -> Path:
    return Path(os.environ.get("PIPEWRIGHT_CACHE") or Path.home() / ".cache" / "pipewright") / "test-envs"


def _requirements_text(path: Path, seen: set | None = None) -> str:
    """The file and every `-r` file it includes, so a change in any of them is a new environment."""
    seen = seen if seen is not None else set()
    if path in seen or not path.is_file():
        return ""
    seen.add(path)
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        line = line.split(" #", 1)[0].strip()
        if line.startswith(("-r ", "--requirement ")):
            text += _requirements_text(path.parent / line.split(maxsplit=1)[1], seen)
    return text


def _python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def components_env(uc: ResolvedUseCase, log=print) -> Path:
    """The cached virtual environment with the components' requirements; built the first time it is needed."""
    requirements = uc.requirements or (uc.components_dir / "requirements.txt")
    if not requirements.is_file():
        raise ConfigError(f"{uc.name}: tests.environment components needs {requirements}")
    key = hashlib.sha256(f"{sys.version_info[:2]}\n{_requirements_text(requirements)}".encode()).hexdigest()[:16]
    venv = cache_dir() / key
    if _python(venv).is_file():
        return venv
    log(f"BUILD {uc.name}: test environment from {requirements.name} (cached as {venv})")
    building = venv.with_name(f"{key}.building-{os.getpid()}")
    shutil.rmtree(building, ignore_errors=True)
    subprocess.run([sys.executable, "-m", "venv", str(building)], check=True)
    code = subprocess.run([str(_python(building)), "-m", "pip", "install", "-q", "-r", str(requirements)],
                          cwd=requirements.parent).returncode
    if code:
        shutil.rmtree(building, ignore_errors=True)
        raise ConfigError(f"{uc.name}: installing {requirements} for the tests failed (exit {code})")
    try:
        building.rename(venv)
    except OSError:                      # another run built it meanwhile
        shutil.rmtree(building, ignore_errors=True)
    return venv


def run(uc: ResolvedUseCase, log=print) -> int | None:
    """Exit code of the command, or None when the use case has no tests.command."""
    if not uc.tests_command:
        log(f"SKIP  {uc.name}: no tests.command in usecase.yaml")
        return None
    venv = components_env(uc, log) if uc.tests_environment == "components" else None
    python = str(_python(venv)) if venv else sys.executable
    log(f"RUN   {uc.name}: {uc.tests_command}  (in {uc.directory}"
        + (", components' requirements)" if venv else ")"))
    command = uc.tests_command
    if command == "python" or command.startswith("python "):
        # The interpreter running Pipewright (or the components' environment), not whatever `python` is on PATH.
        command = subprocess.list2cmdline([python]) if os.name == "nt" else shlex.quote(python)
        command += uc.tests_command[len("python"):]
    env = {**os.environ, "PIPEWRIGHT_USECASE": uc.name, "PIPEWRIGHT_ENVIRONMENT": uc.environment}
    if venv:
        env["VIRTUAL_ENV"] = str(venv)
        env["PATH"] = str(_python(venv).parent) + os.pathsep + env.get("PATH", "")
    sys.stdout.flush()
    # Through the shell, so quoting, pipes and && behave as they do in a terminal.
    code = subprocess.run(command, shell=True, cwd=uc.directory, env=env).returncode
    log(f"{'OK  ' if code == 0 else 'FAIL'}  {uc.name}: exit {code}")
    return code
