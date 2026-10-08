"""pipewright monitor status: what the use case's inference runs found (monitoring.output_table).

Recent runs (drift, prediction drift, performance, alert, retrain request), then the latest run's PSI per
feature against the training baseline, so a team sees what moved without writing SQL.
"""

from __future__ import annotations

import json

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, bigquery


def recent(uc: ResolvedUseCase, env: Environment, limit: int = 10, gcp: Gcp | None = None) -> list[dict]:
    table = (uc.ml.get("monitoring") or {}).get("output_table")
    if not table:
        raise ConfigError(f"{uc.name} has no monitoring.output_table: nothing records monitoring results")
    gcp = gcp or Gcp()
    dataset = uc.resources["bigquery_dataset"]
    sql = (f"SELECT * FROM `{env.project}.{dataset}.{table}` ORDER BY monitored_at DESC LIMIT {int(limit)}")
    return bigquery.rows(bigquery.query(gcp, env.project, sql,
                                        location=bigquery.dataset_location(gcp, env.project, dataset),
                                        kms_key=env.security.get("bigquery_kms_key")))


def _f(v, digits=3) -> str:
    return "-" if v is None else f"{v:.{digits}f}"


def render(uc: ResolvedUseCase, runs: list[dict]) -> str:
    mon = uc.ml["monitoring"]
    out = [f"{uc.name} ({uc.environment}): last {len(runs)} inference run(s) from {mon['output_table']}",
           f"thresholds: feature PSI > {mon['drift']['threshold']}, prediction PSI > "
           f"{mon['prediction_drift']['threshold']}"
           + (f", {mon['performance']['metric']} < {mon['performance']['min']}" if mon["performance"]["enabled"]
              else "")]
    if not runs:
        return "\n".join(out + ["no inference runs recorded yet"])
    out.append(f"  {'monitored (UTC)':<20} {'model':<6} {'rows':>6} {'max PSI':>8} {'pred PSI':>8} "
               f"{'perf':>6} {'alert':<5}  drifted / retrain")
    for r in runs:
        perf = _f(r.get("performance_value")) if r.get("performance_value") is not None else (
            f"n={r['labelled_rows']}" if r.get("labelled_rows") is not None else "-")
        out.append(f"  {str(r['monitored_at'])[:19]:<20} v{r.get('model_version_id') or '?':<5} "
                   f"{r.get('rows_scored') or 0:>6} {_f(r.get('max_feature_psi')):>8} {_f(r.get('prediction_psi')):>8} "
                   f"{perf:>6} {'YES' if r.get('alert') else 'no':<5}  {r.get('drifted_features') or '-'} / "
                   f"{r.get('retrain_status') or '-'}")
    latest = json.loads(runs[0].get("details_json") or "{}")
    psi = latest.get("feature_psi") or {}
    if psi:
        out.append(f"latest run, PSI per feature (> {mon['drift']['threshold']} is drift):")
        width = max(len(k) for k in psi)
        for name, value in sorted(psi.items(), key=lambda kv: -kv[1]):
            bar = "#" * min(40, int(value * 40))
            flag = "  DRIFT" if value > mon["drift"]["threshold"] else ""
            out.append(f"  {name:<{width}} {value:>7.3f} {bar}{flag}")
    for note in latest.get("notes", []):
        out.append(f"note: {note}")
    return "\n".join(out)
