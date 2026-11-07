"""The launcher's online store health (FR-104), on its regular tick.

- `online.stale`: a use case whose online store Pipewright syncs (bigquery, cloudsql) has no sync record newer than
  its limit (the record the sync writes to the artifacts bucket); once when it goes stale, `online.fresh` when it is
  fresh again, as the watch reports transitions.
- `online.billed`: BigQuery lookups by the serving identity on the served table that were billed (BI Engine did not
  serve them: the refresh window, or a reservation that no longer covers the table). Spike (d) showed a per-lookup
  byte cap cannot guard this (BigQuery refuses capped lookups even when BI Engine would serve them), so it is
  detected: from INFORMATION_SCHEMA.JOBS_BY_PROJECT, which needs bigquery.jobs.listAll (the custom role of FR-103).

State lives with the launcher's other cached state; messages carry names, counts and bytes, never a value.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

STALE_HOURS = 48


def _ml(contract) -> dict:
    try:
        return contract.pipeline("training").static_parameters.get("ml_config") or {}
    except Exception:  # noqa: BLE001
        return {}


def synced(live) -> list[tuple]:
    """(contract, online) for live releases whose online store Pipewright syncs."""
    out = []
    for c in live:
        online = _ml(c).get("online_features") or {}
        if online.get("type") in ("bigquery", "cloudsql"):
            out.append((c, online))
    return out


def stale(gcp, bucket: str, live, state: dict, now: datetime | None = None, limit_hours: float = STALE_HOURS):
    from pipewright.gcp.storage import gcs_read
    from pipewright.online import sync

    now = now or datetime.now(timezone.utc)
    events, seen = [], state.setdefault("online_stale", {})
    for c, online in synced(live):
        text, _ = gcs_read(gcp, bucket, sync.record_name(c.tenant, c.usecase))
        rec = json.loads(text) if text else None
        age = ((now - datetime.fromisoformat(rec["synced_at"])).total_seconds() / 3600) if rec else None
        is_stale = age is None or age > limit_hours
        was = seen.get(c.usecase, False)
        if is_stale and not was:
            events.append(("online.stale", c, {"type": online["type"], "last_generation": (rec or {}).get(
                "generation"), "hours": None if age is None else round(age, 1), "limit_hours": limit_hours}))
        elif was and not is_stale:
            events.append(("online.fresh", c, {"type": online["type"], "generation": rec["generation"]}))
        seen[c.usecase] = is_stale
    return events


def billed(gcp, project: str, region: str, live, state: dict, now: datetime | None = None):
    """BigQuery lookups billed since the last look, per use case served from BigQuery."""
    from pipewright.gcp.bigquery import query, rows

    now = now or datetime.now(timezone.utc)
    served = [(c, o) for c, o in synced(live) if o["type"] == "bigquery"]
    if not served:
        return []
    since = state.get("online_billed_since") or now.replace(minute=0, second=0, microsecond=0).isoformat()
    events = []
    for c, online in served:
        sql = (f"SELECT COUNT(*) AS lookups, COUNTIF(total_bytes_billed > 0) AS billed, "
               f"SUM(total_bytes_billed) AS bytes FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA."
               f"JOBS_BY_PROJECT WHERE creation_time > TIMESTAMP(@since) AND user_email = @who "
               f"AND statement_type = 'SELECT' AND EXISTS (SELECT 1 FROM UNNEST(referenced_tables) t WHERE "
               f"CONCAT(t.project_id, '.', t.dataset_id, '.', t.table_id) = @table)")
        params = [{"name": n, "parameterType": {"type": "STRING"}, "parameterValue": {"value": v}}
                  for n, v in (("since", since), ("who", c.runner_service_account), ("table", online["table"]))]
        got = rows(query(gcp, project, sql, params, location=region))
        r = got[0] if got else {}
        if int(r.get("billed") or 0):
            events.append(("online.billed", c, {"table": online["table"], "lookups": int(r.get("lookups") or 0),
                                                "billed_lookups": int(r["billed"]),
                                                "bytes_billed": int(r.get("bytes") or 0), "since": since}))
    state["online_billed_since"] = now.isoformat()
    return events


def tick(gcp, manifest: dict, live, emit) -> dict:
    """Both checks, with the state cached in the artifacts bucket; `emit(event_type, contract, data)`."""
    from pipewright.gcp.storage import PreconditionFailed, gcs_read, gcs_write

    bucket = manifest.get("artifacts_bucket")
    if not bucket or not synced(live):
        return {"checked": 0}
    name = f"runs/cache/online-{manifest.get('environment')}.json"
    text, gen = gcs_read(gcp, bucket, name)
    state = json.loads(text) if text else {}
    found, notes = [], []
    found += stale(gcp, bucket, live, state)
    try:
        found += billed(gcp, manifest["project"], manifest["region"], live, state)
    except Exception as e:  # noqa: BLE001 - said in the result; the stale check still ran
        notes.append(f"billed lookups not checked: {str(e)[:160]}")
    try:
        gcs_write(gcp, bucket, name, json.dumps(state), if_generation_match=gen)
    except PreconditionFailed:
        return {"checked": 0, "skipped": "another tick is under way"}
    for event_type, contract, data in found:
        emit(event_type, contract, data)
    return {"checked": len(synced(live)), "events": [e[0] for e in found], "notes": notes}
