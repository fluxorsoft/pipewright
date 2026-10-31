"""pipewright new-usecase: start a use case from a packaged example.

An example may take its code from another one: example.yaml names it (`code_from`) and the paths to copy (`code`),
and the example's own files go on top. The minimal example is the churn example's training code with less config.

The examples are real YAML (Pipewright 1.2.0, FR-64): they read and highlight on GitHub and are schema-checked like a
team's own. example.yaml lists the sample values they use (`sample`: my-churn, my-tenant, my-team...) and the files
that use them (`templated`); new-usecase replaces those values with the new use case's own, in those files only.
Every YAML file written gets a first line pointing the editor at Pipewright's schemas (config/reference.py).
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import yaml

from pipewright.config import reference
from pipewright.errors import ConfigError

EXAMPLE_FILE = "example.yaml"

NAME = re.compile(r"^[a-z][a-z0-9-]{0,30}$")


def examples_dir() -> Path:
    """Examples ship next to the package source (pipewright/examples) in a checkout, or inside it when installed."""
    here = Path(__file__).resolve()
    for candidate in (here.parents[1] / "examples", here.parents[3] / "examples"):
        if candidate.is_dir():
            return candidate
    raise ConfigError("Pipewright examples not found")


def list_examples() -> list[str]:
    return sorted(p.name for p in examples_dir().iterdir() if (p / "usecase.yaml").is_file())


def _meta(src: Path) -> dict:
    path = src / EXAMPLE_FILE
    return (yaml.safe_load(path.read_text(encoding="utf-8")) or {}) if path.is_file() else {}


def render(text: str, sample: dict, values: dict) -> str:
    """The example's sample values replaced by the use case's, in one pass (so a value that equals another sample
    value is never replaced twice); the longest first where one contains another."""
    swap = {sample[k]: values[k] for k in sample if k in values}
    if not swap:
        return text
    pattern = re.compile("|".join(re.escape(v) for v in sorted(swap, key=len, reverse=True)))
    return pattern.sub(lambda m: swap[m.group(0)], text)


def _copy_code(src: Path, dest: Path, ignore) -> None:
    """The paths example.yaml takes from another example."""
    meta = _meta(src)
    if not meta.get("code_from"):
        return
    base = examples_dir() / meta["code_from"]
    for rel in meta.get("code", []):
        source, target = base / rel, dest / rel.rstrip("/")
        if source.is_dir():
            shutil.copytree(source, target, ignore=ignore, dirs_exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def new_usecase(root: Path, name: str, tenant: str, owner: str, example: str = "churn",
                table_prefix: str | None = None, model_name: str | None = None,
                data_yaml: str | None = None, raw_schema: list[dict] | None = None) -> Path:
    """data_yaml and raw_schema (from scaffold/from_bq.py) replace the example's config/data.yaml and raw table
    schema, for a use case drafted from a BigQuery table."""
    for label, value in (("name", name), ("tenant", tenant)):
        if not NAME.match(value):
            raise ConfigError(f"{label} {value!r} must be lowercase letters, digits and hyphens, "
                              "starting with a letter")
    src = examples_dir() / example
    if not (src / "usecase.yaml").is_file():
        raise ConfigError(f"unknown example {example!r} (available: {', '.join(list_examples())})")
    dest = Path(root) / "usecases" / name
    if dest.exists():
        raise ConfigError(f"{dest} already exists")

    prefix = table_prefix if table_prefix is not None else name.replace("-", "_") + "_"
    values = {"usecase": name, "tenant": tenant, "owner": owner, "table_prefix": prefix,
              "model_name": model_name or f"{name}-classifier"}

    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pipewright", ".pytest_cache", EXAMPLE_FILE, "README.md")
    dest.mkdir(parents=True)
    if (src / EXAMPLE_FILE).is_file():
        _copy_code(src, dest, ignore)
    shutil.copytree(src, dest, ignore=ignore, dirs_exist_ok=True)
    # The files that use the example's sample values get the use case's own.
    meta = _meta(src)
    for rel in meta.get("templated") or []:
        text = render((src / rel).read_text(encoding="utf-8"), meta.get("sample") or {}, values)
        (dest / rel).write_text(text, encoding="utf-8")
    if data_yaml is not None:
        (dest / "config").mkdir(exist_ok=True)
        (dest / "config" / "data.yaml").write_text(data_yaml, encoding="utf-8")
        (dest / "schemas" / "raw_customers.json").write_text(json.dumps(raw_schema, indent=2) + "\n",
                                                             encoding="utf-8")
        for sample in (dest / "data").glob("*.csv"):     # the example's sample data does not fit the new contract
            sample.unlink()
        if (dest / "data").is_dir() and not any((dest / "data").iterdir()):
            (dest / "data").rmdir()
    reference.write_editor_schemas(root)
    for path in sorted(dest.rglob("*.yaml")):
        reference.add_modeline(root, path)
    gitignore = "# Pipewright build output and Python caches\n.pipewright/\n__pycache__/\n*.pyc\n.pytest_cache/\n"
    (dest / ".gitignore").write_text(gitignore, encoding="utf-8")
    return dest
