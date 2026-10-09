"""The configuration reference: what each key means, for the editor and for `pipewright explain`.

The schemas say what is valid; schemas/reference.yaml says what each use case key means, one line per dotted key
under `spec`. `annotated()` merges the two, so there is one source for:

  editor schemas   `pipewright schemas` writes them to .pipewright-schemas/ in the MLOps repo; a modeline at the
                   top of each YAML file (`# yaml-language-server: $schema=...`) points the editor at the right one,
                   for completion, hover text and underlined mistakes
  explain          `pipewright explain data.split.strategy` prints the meaning, type, allowed values, default and
                   whether the key may vary per environment
"""

from __future__ import annotations

import copy
import difflib
import json
import os
from dataclasses import dataclass, field
from functools import cache
from importlib import resources
from pathlib import Path

import yaml

from pipewright.config.ml import DEFAULTS
from pipewright.errors import ConfigError

SCHEMAS_DIR = ".pipewright-schemas"
MODELINE = "# yaml-language-server: $schema="

# File kind -> the schema written for it.
EDITOR_SCHEMAS = {
    "usecase": "usecase.schema.json",           # usecases/<name>/usecase.yaml
    "part": "usecase-part.schema.json",         # files the use case includes, such as config/data.yaml
    "overrides": "usecase-overrides.schema.json",  # usecases/<name>/environments/<env>.yaml
    "environment": "environment.schema.json",   # environments/<env>.yaml
}


def _read(name: str) -> str:
    return resources.files("pipewright.config.schemas").joinpath(name).read_text(encoding="utf-8")


@cache
def descriptions() -> dict[str, str]:
    return {str(k): str(v).strip() for k, v in (yaml.safe_load(_read("reference.yaml")) or {}).items()}


def _target(schema: dict, ref: str) -> dict:
    node = schema
    for part in ref.removeprefix("#/").split("/"):
        node = node[part]
    return node


def _follow(schema: dict, node: dict) -> dict:
    seen = set()
    while "$ref" in node and node["$ref"] not in seen:
        seen.add(node["$ref"])
        node = _target(schema, node["$ref"])
    return node


def _children(schema: dict, node: dict) -> tuple[dict, dict]:
    """(node with $ref followed, its named children). Arrays and maps are looked through: `columns.name` is the
    name of each column, `steps.<name>` any step."""
    node = _follow(schema, node)
    if node.get("type") == "array" and isinstance(node.get("items"), dict):
        return _children(schema, node["items"])
    kids = dict(node.get("properties") or {})
    if isinstance(node.get("additionalProperties"), dict):
        kids["<name>"] = node["additionalProperties"]
    return node, kids


def walk(schema: dict, root: dict, prefix: str = ""):
    """Yields (dotted key, raw node) for every key under `root`; a key reached through a loop is not repeated."""
    def go(node, path, depth, refs):
        ref = node.get("$ref")
        if ref in refs or depth > 8:
            return
        _, kids = _children(schema, node)
        for name, child in kids.items():
            key = f"{path}.{name}" if path else name
            yield key, child
            yield from go(child, key, depth + 1, refs | ({ref} if ref else set()))
    yield from go(root, prefix, 0, frozenset())


@cache
def _annotated() -> dict:
    schema = json.loads(_read("usecase.schema.json"))
    text = descriptions()
    spec = schema["properties"]["spec"]
    for key, node in walk(schema, spec):
        if key.startswith("environments.<name>."):
            key = key.removeprefix("environments.<name>.")
        if key in text:
            node["description"] = text[key]        # next to a $ref too: draft 2020-12 allows it
    return schema


def annotated() -> dict:
    """The use case schema with every key's description."""
    return copy.deepcopy(_annotated())


def editor_schemas() -> dict[str, dict]:
    full = annotated()
    spec = full["properties"]["spec"]
    defs = full["$defs"]
    part = {"$schema": full["$schema"], "title": "Pipewright use case: an included configuration file",
            "description": "One or more top-level sections of the use case spec, such as data or evaluation.",
            "type": "object", "additionalProperties": False, "properties": spec["properties"], "$defs": defs}
    overrides = {"$schema": full["$schema"], "title": "Pipewright use case: per-environment overrides",
                 **{k: v for k, v in defs["overrides"].items()}, "$defs": defs}
    environment = json.loads(_read("environment.schema.json"))
    return {EDITOR_SCHEMAS["usecase"]: full, EDITOR_SCHEMAS["part"]: part,
            EDITOR_SCHEMAS["overrides"]: overrides, EDITOR_SCHEMAS["environment"]: environment}


def write_editor_schemas(root: Path) -> list[Path]:
    out = Path(root) / SCHEMAS_DIR
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for name, schema in editor_schemas().items():
        path = out / name
        path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
        written.append(path)
    (out / "README.md").write_text(
        "Written by `pipewright schemas`; do not edit. YAML files point here with a first line such as\n"
        f"`{MODELINE}../../{SCHEMAS_DIR}/usecase.schema.json`, which gives completion, hover help and\n"
        "underlined mistakes in editors with YAML support (VS Code: the Red Hat YAML extension).\n"
        "Run `pipewright schemas` again after upgrading Pipewright.\n", encoding="utf-8")
    return written


def kind_of(root: Path, path: Path) -> str | None:
    """Which editor schema a YAML file in the MLOps repo takes, from where it is."""
    parts = Path(path).resolve().relative_to(Path(root).resolve()).parts
    if len(parts) == 2 and parts[0] == "environments":
        return "environment"
    if len(parts) < 3 or parts[0] != "usecases":
        return None
    if parts[2:] == ("usecase.yaml",):
        return "usecase"
    if len(parts) == 4 and parts[2] == "environments":
        return "overrides"
    return "part"


def modeline(root: Path, path: Path, kind: str) -> str:
    rel = os.path.relpath(Path(root) / SCHEMAS_DIR / EDITOR_SCHEMAS[kind], Path(path).parent)
    return MODELINE + Path(rel).as_posix()


def add_modeline(root: Path, path: Path, kind: str | None = None) -> bool:
    """Puts the schema line first in a YAML file, replacing an older one. True if the file changed."""
    kind = kind or kind_of(root, path)
    if not kind:
        return False
    text = Path(path).read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    if lines and lines[0].startswith(MODELINE):
        lines = lines[1:]
    new = modeline(root, path, kind) + "\n" + "".join(lines)
    if new != text:
        Path(path).write_text(new, encoding="utf-8")
        return True
    return False


# ---------------------------------------------------------------- explain

@dataclass
class Explanation:
    key: str
    description: str = ""
    type: str = ""
    allowed: list = field(default_factory=list)
    limits: str = ""
    default: object = None
    has_default: bool = False
    per_environment: bool = False
    children: list[tuple[str, str]] = field(default_factory=list)


def all_keys() -> list[str]:
    schema = _annotated()
    return [k for k, _ in walk(schema, schema["properties"]["spec"]) if not k.startswith("environments.")]


def _find(schema: dict, root: dict, parts: list[str]) -> dict | None:
    node = root
    for part in parts:
        _, kids = _children(schema, node)
        if part in kids:
            node = kids[part]
        elif "<name>" in kids:
            node = kids["<name>"]
        else:
            return None
    return node


def _default(parts: list[str]):
    node = DEFAULTS
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return False, None
        node = node[part]
    return True, node


def _type(schema: dict, node: dict) -> str:
    t = node.get("type")
    if isinstance(t, list):
        return " or ".join(t)
    if t == "array" and isinstance(node.get("items"), dict):
        inner = _follow(schema, node["items"])
        word = {"object": "mappings", "string": "strings", "integer": "integers", "number": "numbers"}
        return "list" + (f" of {word.get(inner['type'], inner['type'])}" if isinstance(inner.get("type"), str)
                         else "")
    return {"object": "mapping"}.get(t, t or "")


def explain(key: str) -> Explanation:
    schema = _annotated()
    spec = schema["properties"]["spec"]
    parts = [p for p in key.strip().removeprefix("spec.").split(".") if p]
    raw = _find(schema, spec, parts) if parts else None
    if raw is None:
        close = difflib.get_close_matches(key.removeprefix("spec."), all_keys(), n=4, cutoff=0.5)
        hint = f"; did you mean {', '.join(close)}?" if close else "; `pipewright explain --list` shows every key"
        raise ConfigError(f"{key!r} is not a use case key{hint}")
    node, kids = _children(schema, raw)
    resolved = _follow(schema, raw)
    out = Explanation(".".join(parts), raw.get("description") or resolved.get("description") or "")
    out.type = _type(schema, resolved)
    out.allowed = [v for v in node.get("enum", []) if v is not False]
    bounds = [f"{word} {node[k]}" for k, word in (("minimum", "at least"), ("exclusiveMinimum", "above"),
                                                    ("maximum", "at most"), ("exclusiveMaximum", "below"))
              if k in node]
    out.limits = ", ".join(bounds)
    out.has_default, out.default = _default(parts)
    out.per_environment = _find(schema, schema["$defs"]["overrides"], parts) is not None
    for name, child in kids.items():
        text = (child.get("description") or _follow(schema, child).get("description") or "").split(". ")[0]
        text = text.rstrip(".")
        known, value = _default([*parts, name]) if out.has_default and name != "<name>" else (False, None)
        if known and value is not None and not isinstance(value, dict):
            text += f" (default {json.dumps(value)})"
        out.children.append((name, text))
    return out


def render(e: Explanation) -> str:
    lines = [e.key, "", e.description or "(no description)", ""]
    if e.type:
        lines.append(f"type:      {e.type}")
    if e.allowed:
        lines.append(f"allowed:   {', '.join(map(str, e.allowed))}")
    if e.limits:
        lines.append(f"limits:    {e.limits}")
    if e.has_default and not isinstance(e.default, dict):        # a section's defaults are shown per key
        lines.append(f"default:   {'not set' if e.default is None else json.dumps(e.default)}")
    lines.append("per environment: " + ("yes, in environments/<env>.yaml" if e.per_environment
                                        else "no, the same in every environment"))
    if e.children:
        lines += ["", "keys:"]
        width = max(len(n) for n, _ in e.children)
        lines += [f"  {n:<{width}}  {d}" for n, d in e.children]
    return "\n".join(lines)
