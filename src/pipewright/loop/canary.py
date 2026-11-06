"""The closed retrain loop's online canary with automatic rollback (FR-48 phase 6, D-108; user, 2026-11-06).

After the production approval the loop deploys the candidate as a canary on the use case's Agent Platform endpoint
(`retraining.loop.canary`). After the soak window it compares the canary's error rate and latency with the current
version's (Cloud Monitoring, per deployed model), and the probe's result: healthy, the canary takes all traffic and
the case closes; otherwise the canary is undeployed and the previous version is the default again, automatically.

The probe. A BigQuery ML version is probed by the parity check every deploy runs: the run's kept test rows go to the
canary, and its scores must match ML.PREDICT's (bqml/serving.py). A container model's files are byte for byte the
candidate's that test scored (the phase 6 integrity check, by SHA-256, before the deploy); its probe sends instances
built from the version's training statistics (feature medians and drift-baseline bin edges: aggregates, never a row)
and checks every answer from the canary is a valid score. No request or response is logged or stored: the probe
keeps counts and the score range only.
"""

from __future__ import annotations

import math
from datetime import datetime

DEFAULTS = {"percent": 10, "soak_minutes": 30, "max_error_rate_increase": 0.01, "max_latency_increase": 0.2,
            "probe": True}
METRICS = "aiplatform.googleapis.com/prediction/online/"
PROBE_ROWS = 12


def settings(cfg: dict | None) -> dict | None:
    return None if cfg is None else {**DEFAULTS, **cfg}


def judge(canary: dict, current: dict, cfg: dict, probe: dict | None) -> tuple[bool, str]:
    """(healthy, why) from each deployed model's {predictions, errors, latency_ms} over the soak window."""
    reasons, notes = [], []
    if probe is not None and not probe.get("ok"):
        reasons.append(f"probe: {probe.get('note')}")
    if not canary.get("predictions"):
        reasons.append("the canary served no requests in the soak window: nothing shows it is healthy")
    else:
        rate = canary.get("errors", 0) / canary["predictions"]
        base = (current.get("errors", 0) / current["predictions"]) if current.get("predictions") else 0.0
        if rate - base > cfg["max_error_rate_increase"]:
            reasons.append(f"error rate {rate:.4f} against {base:.4f} for the current version "
                           f"(at most {cfg['max_error_rate_increase']} more)")
        else:
            notes.append(f"error rate {rate:.4f} (current {base:.4f})")
        lat, cur = canary.get("latency_ms"), current.get("latency_ms")
        if lat is not None and cur:
            if lat > cur * (1 + cfg["max_latency_increase"]):
                reasons.append(f"latency {lat:.0f} ms against {cur:.0f} ms (at most {cfg['max_latency_increase']:.0%} "
                               "slower)")
            else:
                notes.append(f"latency {lat:.0f} ms (current {cur:.0f} ms)")
        else:
            notes.append("latency not compared (no requests to the current version in the window)")
    if reasons:
        return False, "; ".join(reasons)
    return True, "; ".join(notes + ([f"probe: {probe.get('note')}"] if probe else []))


def probe_instances(stats: dict) -> list[list]:
    """Instances from the training statistics only: the medians, then each feature at its drift-baseline bin edges
    (low, middle, high) with the others at their medians. Aggregates, never a row."""
    features, medians = stats.get("features") or [], stats.get("medians") or {}
    if not features or any(f not in medians for f in features):
        return []
    base = [medians[f] for f in features]
    out = [list(base)]
    for i, f in enumerate(features):
        edges = ((stats.get("drift_baseline") or {}).get(f) or {}).get("edges") or []
        for e in (edges[:1] + edges[len(edges) // 2:len(edges) // 2 + 1] + edges[-1:]) if edges else []:
            row = list(base)
            row[i] = e
            if row not in out:
                out.append(row)
    return out[:PROBE_ROWS]


def valid_score(value) -> bool:
    """A binary model's answer: a probability (or a list holding one) in [0, 1]."""
    if isinstance(value, dict):
        value = next(iter(value.values()), None)
    if isinstance(value, list):
        value = value[-1] if value else None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and 0.0 <= v <= 1.0


def probe(endpoint, deployed_id: str, instances: list, attempts: int = 60) -> dict:
    """Sends each instance until the canary answered it (an endpoint answers from either deployed model)."""
    if not instances:
        return {"ok": False, "note": "no training statistics to build instances from"}
    answered, bad = 0, 0
    for inst in instances:
        for _ in range(attempts):
            response = endpoint.predict(instances=[inst])
            if str(getattr(response, "deployed_model_id", "")) != str(deployed_id):
                continue
            answered += 1
            bad += 0 if all(valid_score(p) for p in response.predictions) else 1
            break
    if answered < len(instances):
        return {"ok": False, "rows": len(instances), "answered": answered,
                "note": f"the canary answered {answered} of {len(instances)} instances"}
    return {"ok": bad == 0, "rows": len(instances), "answered": answered,
            "note": f"{answered} instances, {bad} invalid answers" if bad else f"{answered} instances, all valid"}


def window(started: str, now: datetime) -> tuple[str, str]:
    return started, now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def endpoint_metrics(gcp, project: str, endpoint_id: str, start: str, end: str) -> dict[str, dict]:
    """{deployed_model_id: {predictions, errors, latency_ms}} over [start, end] from Cloud Monitoring."""
    api = f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
    seconds = max(60, int((datetime.fromisoformat(end.replace("Z", "+00:00"))
                           - datetime.fromisoformat(start.replace("Z", "+00:00"))).total_seconds()))
    out: dict[str, dict] = {}

    def series(metric: str, aligner: str):
        r = gcp.get(api, **{
            "filter": f'metric.type="{METRICS}{metric}" AND resource.type="aiplatform.googleapis.com/Endpoint" '
                      f'AND resource.labels.endpoint_id="{endpoint_id}"',
            "interval.startTime": start, "interval.endTime": end,
            "aggregation.alignmentPeriod": f"{seconds}s", "aggregation.perSeriesAligner": aligner,
            "aggregation.crossSeriesReducer": "REDUCE_SUM",
            "aggregation.groupByFields": "metric.label.deployed_model_id"})
        if not r.ok:
            from pipewright.errors import ConfigError

            raise ConfigError(f"Cloud Monitoring {metric}: HTTP {r.status}")
        return (r.body or {}).get("timeSeries") or []

    for name, metric in (("predictions", "prediction_count"), ("errors", "error_count")):
        for ts in series(metric, "ALIGN_DELTA"):
            d = (ts.get("metric") or {}).get("labels", {}).get("deployed_model_id")
            total = sum(int((p.get("value") or {}).get("int64Value") or 0) for p in ts.get("points") or [])
            out.setdefault(str(d), {})[name] = out.get(str(d), {}).get(name, 0) + total
    for ts in series("prediction_latencies", "ALIGN_DELTA"):
        d = (ts.get("metric") or {}).get("labels", {}).get("deployed_model_id")
        means = [((p.get("value") or {}).get("distributionValue") or {}) for p in ts.get("points") or []]
        counted = [(float(m.get("mean") or 0), int(m.get("count") or 0)) for m in means if m.get("count")]
        if counted:
            n = sum(c for _, c in counted)
            out.setdefault(str(d), {})["latency_ms"] = sum(m * c for m, c in counted) / n
    return out
