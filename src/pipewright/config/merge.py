"""Merging configuration: deep merge for overrides, strict merge for include files."""

from __future__ import annotations

import copy


def deep_merge(base: dict, override: dict) -> dict:
    """Maps merge key by key; lists and scalars from `override` replace."""
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def merge_parts(spec: dict, part: dict, part_name: str, origins: dict[str, str], problems: list[str]) -> None:
    """Adds an include file's sections to spec. A section defined twice is an error: each section has one home."""
    for key, value in part.items():
        if key in ("include", "environments"):
            problems.append(f"{part_name}: `{key}` belongs in usecase.yaml (or the environments/ folder)")
            continue
        if key in spec:
            problems.append(f"`{key}` is defined in both {origins.get(key, 'usecase.yaml')} and {part_name}")
            continue
        spec[key] = copy.deepcopy(value)
        origins[key] = part_name
