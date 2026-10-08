"""CLI plugins: extra `pipewright` commands from other installed packages.

A plugin is an entry point in the `pipewright.commands` group that names a function

    def register(subparsers, common) -> None

`subparsers` is the CLI's subparser collection; `common(parser)` adds the standard --root, --env and --usecase
options. The function adds its parsers and sets `func` on each, exactly as built-in commands do. In pyproject.toml:

    [project.entry-points."pipewright.commands"]
    evidence = "my_package.cli:register"

API_VERSION changes only when this contract does. A plugin that fails to load is reported and skipped: it never
breaks the built-in commands.
"""

from __future__ import annotations

import logging
import sys
from importlib.metadata import entry_points

API_VERSION = 1
GROUP = "pipewright.commands"

log = logging.getLogger("pipewright.plugins")


def discover() -> list:
    return sorted(entry_points(group=GROUP), key=lambda ep: ep.name)


def register_all(subparsers, common) -> dict[str, str]:
    """Registers every plugin's commands. Returns {plugin name: "ok" or the error}."""
    loaded = {}
    for ep in discover():
        try:
            ep.load()(subparsers, common)
            loaded[ep.name] = "ok"
        except Exception as e:  # noqa: BLE001 - a broken plugin must not break the CLI
            loaded[ep.name] = f"{type(e).__name__}: {e}"
            print(f"warning: pipewright plugin {ep.name!r} ({ep.value}) not loaded: {e}", file=sys.stderr)
    return loaded
