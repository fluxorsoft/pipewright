"""Capture coverage (Pipewright 1.4.3, FR-98 stage 2), from the launcher's hourly tick.

What capture recorded is compared with what was served over the last hour:
  Agent Platform   prediction_count of the endpoint (Cloud Monitoring) against the online log's rows divided by the
                   sample rate (BigQuery)
  KServe           the receiver's kept records (pipewright_capture_kept_total, scraped by Managed Prometheus) against
                   TorchServe's answered requests (Requests2XX), times the sample rate
serving.capture_gap when they part by more than MARGIN; serving.capture_silent when traffic was served and nothing
was recorded. Model health is never inferred from coverage, and a quiet endpoint is not a gap.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

MARGIN = 0.5                     # recorded below half of what the sample rate should give
MIN_SERVED = 200                 # fewer requests in the hour: too few to judge a sample


def judge(served: float, recorded: float, rate: float) -> dict | None:
    """The coverage finding, or None when there is nothing to say."""
    if served < MIN_SERVED:
        return None
    expected = served * rate
    if recorded == 0:
        return {"event": "serving.capture_silent", "served": served, "recorded": 0, "expected": round(expected)}
    if recorded < expected * MARGIN:
        return {"event": "serving.capture_gap", "served": served, "recorded": recorded, "expected": round(expected),
                "share": round(recorded / expected, 3)}
    return None


def agent_platform(gcp, project: str, region: str, endpoint_id: str, table: str, rate: float,
                   now: datetime | None = None, kms_key: str | None = None) -> dict | None:
    now = now or datetime.now(timezone.utc)
    start = now - timedelta(hours=1)
    r = gcp.get(f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries",
                filter=('metric.type="aiplatform.googleapis.com/prediction/online/prediction_count" AND '
                        f'resource.labels.endpoint_id="{endpoint_id}"'),
                **{"interval.startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "interval.endTime": now.strftime("%Y-%m-%dT%H:%M:%SZ")})
    if not r.ok:
        # GA QA: a read that fails is said (it was taken as "nothing to report", so coverage never ran unseen)
        raise RuntimeError(f"served requests not readable from Cloud Monitoring: HTTP {r.status}")
    served = sum(float(list(p["value"].values())[0]) for ts in (r.body or {}).get("timeSeries") or []
                 for p in ts.get("points") or [])
    q = gcp.post(f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries",
                 {"query": f"SELECT COUNT(*) AS n FROM `{table}`, UNNEST(request_payload) WHERE logging_time >= "
                           "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)", "useLegacySql": False,
                  "location": region, "timeoutMs": 30000,
                  **({"destinationEncryptionConfiguration": {"kmsKeyName": kms_key}} if kms_key else {})})
    if not q.ok:
        message = ((q.body or {}).get("error") or {}).get("message", "") if isinstance(q.body, dict) else ""
        raise RuntimeError(f"capture table {table} not readable: HTTP {q.status} {message}"[:300])
    if not (q.body or {}).get("jobComplete"):
        return None
    recorded = float(((q.body.get("rows") or [{}])[0].get("f") or [{"v": 0}])[0]["v"] or 0)
    return judge(served, recorded, rate)


def kserve(prom_query, namespace: str, isvc: str, rate: float) -> dict | None:
    """prom_query(promql) -> a number or None (Cloud Monitoring's PromQL API)."""
    # GA QA (found live, a false capture_gap after a deploy): inference requests only (Requests2XX counts the
    # readiness pings too), and only when both counters existed at the start of the hour: a counter Prometheus first
    # sees inside the window has no zero to start from, so increase() drops what came before its first scrape.
    srv = f'ts_inference_requests_total{{namespace="{namespace}",pod=~"{isvc}-predictor.*"}}'
    kept = f'pipewright_capture_kept_total{{namespace="{namespace}",isvc="{isvc}"}}'
    if prom_query(f"sum({srv} offset 1h)") is None or prom_query(f"sum({kept} offset 1h)") is None:
        return None                                  # too new to judge over the hour
    served = prom_query(f"sum(increase({srv}[1h]))")
    recorded = prom_query(f"sum(increase({kept}[1h]))")
    if served is None:
        return None
    # The receiver keeps a request and its answer: two records per request.
    return judge(served, (recorded or 0) / 2, rate)
