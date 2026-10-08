"""${a.b} references in configuration values."""

from __future__ import annotations

import re
from typing import Any

_REF = re.compile(r"\$\{([a-z_][a-z0-9_.]*)\}")


def _lookup(ctx: dict, dotted: str):
    value: Any = ctx
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            raise KeyError(dotted)
        value = value[part]
    return value


def _flat_keys(ctx: dict, prefix: str = "") -> list[str]:
    out = []
    for k, v in ctx.items():
        if isinstance(v, dict):
            out += _flat_keys(v, f"{prefix}{k}.")
        else:
            out.append(f"{prefix}{k}")
    return out


def interpolate(value: Any, ctx: dict, label: str, problems: list[str]) -> Any:
    """Replaces ${a.b} references in strings. A string that is exactly one reference keeps the value's type."""
    if not isinstance(value, str) or "${" not in value:
        return value
    whole = _REF.fullmatch(value)
    try:
        if whole:
            return _lookup(ctx, whole.group(1))
        return _REF.sub(lambda m: str(_lookup(ctx, m.group(1))), value)
    except KeyError as e:
        known = ", ".join(sorted(_flat_keys(ctx)))
        problems.append(f"{label}: unknown reference ${{{e.args[0]}}} (known: {known})")
        return value


def interpolate_tree(value: Any, ctx: dict, label: str, problems: list[str]) -> Any:
    """interpolate() applied to every string inside nested dicts and lists."""
    if isinstance(value, dict):
        return {k: interpolate_tree(v, ctx, f"{label}.{k}", problems) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_tree(v, ctx, f"{label}[{i}]", problems) for i, v in enumerate(value)]
    return interpolate(value, ctx, label, problems)
