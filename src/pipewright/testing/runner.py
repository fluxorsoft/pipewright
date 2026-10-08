"""pipewright test: runs each use case's tests.command in its folder (the team's own unit tests)."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys

from pipewright.config import ResolvedUseCase


def run(uc: ResolvedUseCase, log=print) -> int | None:
    """Exit code of the command, or None when the use case has no tests.command."""
    if not uc.tests_command:
        log(f"SKIP  {uc.name}: no tests.command in usecase.yaml")
        return None
    log(f"RUN   {uc.name}: {uc.tests_command}  (in {uc.directory})")
    command = uc.tests_command
    if command == "python" or command.startswith("python "):
        # The interpreter running Pipewright, not whatever `python` is first on PATH.
        command = subprocess.list2cmdline([sys.executable]) if os.name == "nt" else shlex.quote(sys.executable)
        command += uc.tests_command[len("python"):]
    env = {**os.environ, "PIPEWRIGHT_USECASE": uc.name, "PIPEWRIGHT_ENVIRONMENT": uc.environment}
    sys.stdout.flush()
    # Through the shell, so quoting, pipes and && behave as they do in a terminal.
    code = subprocess.run(command, shell=True, cwd=uc.directory, env=env).returncode
    log(f"{'OK  ' if code == 0 else 'FAIL'}  {uc.name}: exit {code}")
    return code
