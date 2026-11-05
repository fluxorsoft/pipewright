"""Judgement checks (Pipewright 1.3.0): configuration that is valid but probably wrong for a model.

`validate` already refuses what cannot work. These catch what works and still misleads: the modelling mistakes a
person or a coding agent makes most often. They are warnings, so a use case that passed before still passes; the
one exception is the platform's own gate minimums (`policies.evaluation.min_gates` in the manifest), which are a
policy and refuse like every other policy.

    warnings, problems = judgement.check(ml, env, tenant)
"""

from __future__ import annotations

import re

from pipewright.config.ml import TIME_TYPES

UNSUPERVISED = ("clustering", "decomposition")


def check(ml: dict, env, tenant: str | None = None) -> tuple[list[str], list[str]]:
    warnings: list[str] = []
    problems: list[str] = []
    data = ml.get("data") or {}
    columns = {c["name"]: c for c in ((data.get("contract") or {}).get("columns") or [])}
    task = (data.get("label") or {}).get("task", "binary")
    features = list((ml.get("features") or {}).get("include") or [])
    warnings += _time_split(data, columns, task)
    warnings += _time_as_feature(features, columns, data)
    warnings += _outcome_features(features, data, task)
    warnings += _raw_features(data, env, tenant, task)
    problems += _gate_minimums(ml, env, task)
    return warnings, problems


# An image reference by tag in component code: host/path:tag, not host/path@sha256:...
_IMAGE_BY_TAG = re.compile(r"""["']((?:[a-z0-9-]+\.)+[a-z]{2,}(?::\d+)?/[a-z0-9._/-]+:[A-Za-z0-9._-]+)["']""")


def serving_image_by_tag(components_dir) -> list[str]:
    """Phase 6 (FR-94): a component that registers its serving container by tag. A tag can move after the model was
    approved; a digest cannot. Image references are names, never data."""
    from pathlib import Path

    out = []
    for path in sorted(Path(components_dir).glob("*.py")) if components_dir and Path(components_dir).is_dir() else []:
        text = path.read_text(encoding="utf-8", errors="replace")
        if "serving_container_image_uri" not in text and "imageUri" not in text:
            continue
        for image in sorted(set(_IMAGE_BY_TAG.findall(text))):
            out.append(f"{path.name} registers the serving image by tag ({image}): register it by digest "
                       "(image@sha256:...), so an approved model keeps the image it was approved with; a moved tag "
                       "is refused at deploy (judgement check)")
    return out


def time_column(data: dict, columns: dict) -> str | None:
    """The column that orders the rows in time, when there is one: the entity's cut-off, the split's time column,
    or a required date or timestamp column in the contract."""
    entity = ((data.get("features_from") or {}).get("entity")) or {}
    if entity.get("time_column"):
        return entity["time_column"]
    if (data.get("split") or {}).get("time_column"):
        return data["split"]["time_column"]
    required = [n for n, c in columns.items() if str(c.get("type", "")).lower() in TIME_TYPES and c.get("required")]
    return required[0] if len(required) == 1 else None


def _time_split(data: dict, columns: dict, task: str) -> list[str]:
    split = data.get("split") or {}
    if task in UNSUPERVISED or task == "forecast" or split.get("strategy") == "time":
        return []
    tc = time_column(data, columns)
    if not tc and not data.get("label_source"):
        return []
    on = f" ({tc})" if tc else " (labels come from a time window)"
    fix = f"data.split: {{ strategy: time, time_column: {tc} }}" if tc else "data.split.strategy: time"
    return [f"data.split.strategy is {split.get('strategy', 'stratified')}, but the rows are ordered in time{on}: "
            "a model tested on rows mixed with its training rows learns from the future and looks better than it "
            f"will be. Use {fix} (judgement check)"]


def _time_as_feature(features: list[str], columns: dict, data: dict) -> list[str]:
    entity = ((data.get("features_from") or {}).get("entity")) or {}
    dated = [f for f in features if str((columns.get(f) or {}).get("type", "")).lower() in TIME_TYPES
             and f != entity.get("time_column")]                 # the entity's cut-off has its own warning (FR-68)
    if not dated:
        return []
    return [f"features: {', '.join(dated)} {'is a date' if len(dated) == 1 else 'are dates'}: a model learns when a "
            "row was taken, not what it says, and scores drift as time moves on. Add it to features.exclude, or "
            "derive a duration (days since) in a feature table (judgement check)"]


def _stem(label: str) -> str:
    s = re.sub(r"^(is_|has_|was_|did_)", "", label.lower())
    s = re.sub(r"(_flag|_label|_target|_outcome|_ind)$", "", s)
    s = re.sub(r"(ed|d|s)$", "", s) if len(s) > 4 else s
    return s


def _outcome_features(features: list[str], data: dict, task: str) -> list[str]:
    """Columns named after the label usually describe the outcome itself (churn_date, churn_reason for churned):
    known only after the label, so a model that sees them is told the answer."""
    label = (data.get("label") or {}).get("column")
    if not label or task in UNSUPERVISED or task == "forecast":
        return []
    stem = _stem(label)
    if len(stem) < 4:
        return []
    suspects = [f for f in features if f != label and stem in f.lower()]
    if not suspects:
        return []
    return [f"features: {', '.join(suspects)} {'looks' if len(suspects) == 1 else 'look'} like part of the outcome "
            f"({label}): a column known only after the label tells the model the answer. If it is known before "
            "each row's cut-off, list it in features.include; otherwise exclude it (judgement check)"]


def _raw_features(data: dict, env, tenant: str | None, task: str) -> list[str]:
    """Plain BigQuery tables are allowed as sources, but the pattern for ML features is feature tables and feature
    groups: built once by their owners, shared, kept point-in-time correct. A team that has feature datasets and
    reads every feature from its own table is warned (features.required refuses it instead)."""
    datasets = (((getattr(env, "tenants", None) or {}).get(tenant or "") or {}).get("features") or {}).get("datasets")
    if (not datasets or data.get("features_from") or task in UNSUPERVISED or task == "forecast"
            or ((getattr(env, "features", None) or {}).get("required"))):
        return []
    return [f"data.features_from: tenant {tenant!r} has feature datasets ({', '.join(datasets)}), but every feature "
            "comes from the use case's own table. The pattern for ML features is feature tables and feature groups "
            "through data.features_from; a missing feature goes to the data team (judgement check)"]


def _gate_minimums(ml: dict, env, task: str) -> list[str]:
    """policies.evaluation.min_gates in the manifest: the least each metric's gate may ask in this environment. A
    use case with no gate on such a metric, or a weaker one, is refused here as by any policy."""
    minimums = ((getattr(env, "policies", None) or {}).get("evaluation") or {}).get("min_gates") or {}
    if not minimums or task in UNSUPERVISED:
        return []
    gates = {g["metric"]: g for g in (ml.get("evaluation") or {}).get("gates") or []}
    out = []
    for metric, rule in minimums.items():
        rule = rule if isinstance(rule, dict) else {"min": rule}
        gate = gates.get(metric)
        for bound in ("min", "max"):
            if bound not in rule:
                continue
            have = (gate or {}).get(bound)
            weaker = have is None or (have < rule[bound] if bound == "min" else have > rule[bound])
            if weaker:
                want = f"{bound} {rule[bound]}"
                got = "no such gate" if gate is None else (f"{bound} {have}" if have is not None else f"no {bound}")
                out.append(f"evaluation.gates: {env.name} requires a gate on {metric} with {want} "
                           f"(policies.evaluation.min_gates); this use case has {got}")
    return out
