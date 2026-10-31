"""Evaluation gates, checked by Pipewright itself: a model version becomes the champion or is served only when its
evaluation record passes the use case's gates (evaluation.gates, evaluation.slice_gates, evaluation.champion).

The pipeline's code computes the metrics and may apply the gates too, but Pipewright does not take its word for
it: it recomputes every gate from the recorded numbers, against the configuration in force now. A rewritten
evaluation step that drops a gate is caught here.

The evaluation record is `evaluation.json` in the model version's artifact directory, written by the training
pipeline next to the model (the churn example's evaluate and register steps do this):

    {
      "metrics":  {"auc": 0.81, "calibration_error": 0.03, ...},
      "slices":   {"<column>": {"<value>": {"rows": 120, "auc": 0.79, ...}, ...}, ...},     (for slice gates)
      "champion": {"auc": 0.80, "_version": "3"} | null,     (the champion's metrics on the same test data)
      "decision": "approve" | "reject"                       (informational: Pipewright recomputes it)
    }
"""

from __future__ import annotations

import json

RECORD = "evaluation.json"


def needs_record(evaluation: dict) -> bool:
    """Whether the configuration has anything to check."""
    return bool(evaluation.get("gates") or evaluation.get("slice_gates")
                or (evaluation.get("champion") or {}).get("enabled")
                or ((evaluation.get("fairness") or {}).get("gates")))


def _fmt(v) -> str:
    return f"{v:.4f}" if isinstance(v, (int, float)) else str(v)


def check(record: dict, evaluation: dict, champion_expected: bool = False) -> list[str]:
    """Problems with a version's evaluation record under the given evaluation configuration; empty = passes.
    champion_expected: a champion existed when this version was trained, so a comparison must be recorded."""
    problems: list[str] = []
    metrics = record.get("metrics") or {}
    lower = set(evaluation.get("lower_is_better") or [])

    for gate in evaluation.get("gates") or []:
        m, value = gate["metric"], metrics.get(gate["metric"])
        if value is None:
            problems.append(f"gate {m}: the evaluation record has no {m}")
            continue
        if "min" in gate and value < gate["min"]:
            problems.append(f"gate {m}: {_fmt(value)} is below the minimum {gate['min']}")
        if "max" in gate and value > gate["max"]:
            problems.append(f"gate {m}: {_fmt(value)} is above the maximum {gate['max']}")
        if gate.get("per_series"):           # Pipewright 1.2 (FR-69): every series of a forecast
            problems += _series_problems(record, gate)

    slices = record.get("slices") or {}
    for sg in evaluation.get("slice_gates") or []:
        m, overall = sg["metric"], metrics.get(sg["metric"])
        if overall is None:
            problems.append(f"slice gate {m}: the evaluation record has no overall {m}")
            continue
        if not slices:
            problems.append(f"slice gate {m}: the evaluation record has no slices")
            continue
        for column, values in slices.items():
            for value, entry in (values or {}).items():
                v = (entry or {}).get(m)
                if v is None or (entry or {}).get("rows", 0) < sg.get("min_rows", 1):
                    continue
                gap = (v - overall) if m in lower else (overall - v)
                if gap > sg["max_gap"]:
                    problems.append(f"slice gate {m}: {column}={value} is {_fmt(gap)} worse than overall "
                                    f"(maximum gap {sg['max_gap']})")

    problems += fairness_problems(record, evaluation)

    champ_cfg = evaluation.get("champion") or {}
    if champ_cfg.get("enabled"):
        champion = record.get("champion")
        m = champ_cfg.get("metric") or evaluation.get("primary_metric")
        margin = champ_cfg.get("min_improvement", 0.0) or 0.0
        if not champion:
            if champion_expected:
                problems.append("champion: no comparison with the champion is recorded, but a champion existed "
                                "when this version was trained")
        else:
            new, old = metrics.get(m), champion.get(m)
            if new is None or old is None:
                problems.append(f"champion: the evaluation record has no {m} for "
                                f"{'this version' if new is None else 'the champion'}")
            else:
                diff = (old - new) if m in lower else (new - old)
                if not (diff > margin or (margin == 0 and diff == 0)):
                    problems.append(f"champion: {m} {_fmt(new)} does not beat the champion's {_fmt(old)} by "
                                    f"{margin} (champion version {champion.get('_version', '?')})")
    return problems


def _series_problems(record: dict, gate: dict) -> list[str]:
    m, series = gate["metric"], record.get("per_series")
    if not series:
        return [f"gate {m} per series: the evaluation record has no per-series metrics"]
    out = []
    for name, entry in sorted(series.items()):
        v = (entry or {}).get(m)
        if v is None:
            continue
        if "min" in gate and v < gate["min"]:
            out.append(f"gate {m} per series: {name} {_fmt(v)} is below the minimum {gate['min']}")
        if "max" in gate and v > gate["max"]:
            out.append(f"gate {m} per series: {name} {_fmt(v)} is above the maximum {gate['max']}")
    return out


def worst_series(record: dict, metric: str, lower_is_better: bool = True) -> tuple[str, float] | None:
    """The series with the worst value of metric in a forecast's record, or None without per-series metrics."""
    values = [(k, v[metric]) for k, v in (record.get("per_series") or {}).items() if (v or {}).get(metric) is not None]
    if not values:
        return None
    return (max if lower_is_better else min)(values, key=lambda kv: kv[1])


def read_record(gcp, artifact_uri: str | None) -> dict | None:
    """The evaluation record next to a model version's artifacts, or None when there is none."""
    if not artifact_uri or not artifact_uri.startswith("gs://"):
        return None
    from pipewright.gcp import gcs_read

    bucket, _, prefix = artifact_uri.removeprefix("gs://").partition("/")
    text, _ = gcs_read(gcp, bucket, f"{prefix.rstrip('/')}/{RECORD}".lstrip("/"))
    return json.loads(text) if text else None


def verify(gcp, artifact_uri: str | None, evaluation: dict, champion_expected: bool = False) -> list[str]:
    """check() on the version's stored record; a missing record is a problem when there is anything to check."""
    if not needs_record(evaluation):
        return []
    record = read_record(gcp, artifact_uri)
    if record is None:
        return [f"no evaluation record ({RECORD}) next to the model artifacts at {artifact_uri}: the training "
                "pipeline must write one"]
    return check(record, evaluation, champion_expected)


def fairness_problems(record: dict, evaluation: dict) -> list[str]:
    """evaluation.fairness.gates against the record's fairness measures (FR-53). A measure that could not be taken
    (fewer than two groups with min_rows test rows) fails its gate: a gate is never passed by default."""
    fair = evaluation.get("fairness") or {}
    measured = record.get("fairness") or {}
    out = []
    for gate in fair.get("gates") or []:
        m = gate["metric"]
        for attr in fair.get("attributes") or []:
            value = (measured.get(attr) or {}).get(m)
            if value is None:
                out.append(f"fairness gate {m}: not measured for {attr} (fewer than two groups with "
                           f"{fair.get('min_rows', 30)} test rows)")
            elif value > gate["max"]:
                out.append(f"fairness gate {m}: {attr} is {value:.4f}, above the maximum {gate['max']}")
    return out
