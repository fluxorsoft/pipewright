"""pipewright new-usecase: start a use case from a packaged example."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from pipewright.errors import ConfigError

NAME = re.compile(r"^[a-z][a-z0-9-]{0,30}$")


def examples_dir() -> Path:
    """Examples ship next to the package source (pipewright/examples) in a checkout, or inside it when installed."""
    here = Path(__file__).resolve()
    for candidate in (here.parents[1] / "examples", here.parents[3] / "examples"):
        if candidate.is_dir():
            return candidate
    raise ConfigError("Pipewright examples not found")


def list_examples() -> list[str]:
    return sorted(p.name for p in examples_dir().iterdir() if (p / "usecase.yaml.tmpl").is_file())


def new_usecase(root: Path, name: str, tenant: str, owner: str, example: str = "churn",
                table_prefix: str | None = None, model_name: str | None = None) -> Path:
    for label, value in (("name", name), ("tenant", tenant)):
        if not NAME.match(value):
            raise ConfigError(f"{label} {value!r} must be lowercase letters, digits and hyphens, "
                              "starting with a letter")
    src = examples_dir() / example
    if not (src / "usecase.yaml.tmpl").is_file():
        raise ConfigError(f"unknown example {example!r} (available: {', '.join(list_examples())})")
    dest = Path(root) / "usecases" / name
    if dest.exists():
        raise ConfigError(f"{dest} already exists")

    prefix = table_prefix if table_prefix is not None else name.replace("-", "_") + "_"
    values = {"usecase": name, "tenant": tenant, "owner": owner, "table_prefix": prefix,
              "model_name": model_name or f"{name}-classifier"}

    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", "*.tmpl", ".pipewright", ".pytest_cache")
    shutil.copytree(src, dest, ignore=ignore)
    # Every *.tmpl (usecase.yaml.tmpl, config/*.yaml.tmpl, ...) is rendered without its suffix.
    for template in sorted(src.rglob("*.tmpl")):
        text = template.read_text(encoding="utf-8")
        for key, value in values.items():
            text = text.replace("{{" + key + "}}", value)
        leftover = re.findall(r"\{\{(\w+)\}\}", text)
        if leftover:
            raise ConfigError(f"{template.name} has unfilled placeholders: {sorted(set(leftover))}")
        target = dest / template.relative_to(src).with_suffix("")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    gitignore = "# Pipewright build output and Python caches\n.pipewright/\n__pycache__/\n*.pyc\n.pytest_cache/\n"
    (dest / ".gitignore").write_text(gitignore, encoding="utf-8")
    return dest
