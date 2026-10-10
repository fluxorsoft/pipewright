"""JSON Schema validation with readable, path-qualified messages."""

from __future__ import annotations

import json
from functools import cache
from importlib import resources
from typing import Any

from jsonschema import Draft202012Validator

from pipewright.errors import ConfigError

KINDS = ("environment", "usecase", "manifest")


@cache
def _validator(kind: str) -> Draft202012Validator:
    if kind not in KINDS:
        raise ValueError(f"unknown schema kind {kind!r}")
    text = resources.files("pipewright.config.schemas").joinpath(f"{kind}.schema.json").read_text(encoding="utf-8")
    schema = json.loads(text)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _path(parts) -> str:
    out = ""
    for p in parts:
        out += f"[{p}]" if isinstance(p, int) else (f".{p}" if out else str(p))
    return out or "(top level)"


def _message(error) -> str:
    where = _path(error.absolute_path)
    msg = error.message
    parts = list(error.absolute_path)
    if (
        error.validator == "additionalProperties"
        and len(parts) >= 3
        and parts[0] == "spec"
        and parts[1] == "environments"
    ):
        msg += (" (only parameters, steps, triggers, training.hyperparameters, training.tuning, model.serving, "
                "approvals, retraining.enabled, monitoring.alerts and outputs.bigquery.retention_days may vary per "
                "environment)")
    if error.validator == "additionalProperties" and "(True was unexpected)" in msg:
        msg += " (YAML reads an unquoted key on, yes or y as true: quote it, or use retraining.when)"
    return f"{where}: {msg}"


def prune_unknown(document: Any, kind: str) -> tuple[Any, list[str]]:
    """A copy of `document` without the keys the schema does not know, and the dotted paths of those keys.

    Forward compatibility (R-05): a manifest written for a newer Pipewright may carry keys this version has never
    heard of. They are dropped (and reported) instead of failing the whole document; everything this version does
    know is still validated in full. Keys are only dropped where the schema forbids extra properties."""
    schema = _validator(kind).schema
    dropped: list[str] = []

    def deref(node: dict) -> dict:
        while isinstance(node, dict) and "$ref" in node:
            target = schema
            for part in node["$ref"].removeprefix("#/").split("/"):
                target = target[part]
            node = target
        return node

    def walk(value: Any, node: dict, path: str) -> Any:
        node = deref(node)
        if not isinstance(node, dict):
            return value
        for key in ("anyOf", "oneOf"):
            if key in node:
                branch = next((deref(b) for b in node[key] if _fits(value, deref(b))), None)
                return walk(value, branch, path) if branch else value
        if isinstance(value, dict):
            props = node.get("properties") or {}
            extra = node.get("additionalProperties", True)
            out = {}
            for k, v in value.items():
                where = f"{path}.{k}" if path else k
                if k in props:
                    out[k] = walk(v, props[k], where)
                elif extra is False:
                    dropped.append(where)
                elif isinstance(extra, dict):
                    out[k] = walk(v, extra, where)
                else:
                    out[k] = v
            return out
        if isinstance(value, list) and isinstance(node.get("items"), dict):
            return [walk(v, node["items"], f"{path}[{i}]") for i, v in enumerate(value)]
        return value

    return walk(document, schema, ""), dropped


def _fits(value: Any, node: dict) -> bool:
    kind = node.get("type")
    kinds = kind if isinstance(kind, list) else [kind]
    return (isinstance(value, dict) and "object" in kinds) or (value is None and "null" in kinds) or (
        isinstance(value, list) and "array" in kinds) or (kind is None and isinstance(value, dict))


def validate(document: Any, kind: str, source: str) -> None:
    """Raises ConfigError listing every schema violation."""
    errors = sorted(_validator(kind).iter_errors(document), key=lambda e: list(map(str, e.absolute_path)))
    if errors:
        raise ConfigError([_message(e) for e in errors], source=source)
