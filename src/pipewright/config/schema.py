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


def validate(document: Any, kind: str, source: str) -> None:
    """Raises ConfigError listing every schema violation."""
    errors = sorted(_validator(kind).iter_errors(document), key=lambda e: list(map(str, e.absolute_path)))
    if errors:
        raise ConfigError([_message(e) for e in errors], source=source)
