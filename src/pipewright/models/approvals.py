"""The environment's approvals and registrations, read from the event log once and then by what is new (1.4.1, FR-101
item 0).

Before 1.4.1 every integrity check ran one BigQuery query per version with no date filter, so each check scanned the
whole event log, and the log only grows: a watch over 100 use cases every hour would have scanned about 160 TiB a
month after a year. Here the environment's `model.approved` and `model.registered` events are read once, kept, and
refreshed with a query that reads only the partitions published since the last look (the events table is partitioned
by day on publish_time). An approval never changes once written, so what is kept stays true.

The launcher also keeps what it read in its own state file (`runs/cache/approvals-<env>.json` in the artifacts bucket,
which only the launcher writes), so a cold start reads only what is new since the last tick. The CLI never trusts that
file: it reads the event log itself (deploy, approve and promote are rare). Approvals are cross-checked with Cloud
Audit Logs where it matters (FR-97), as before.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone

KINDS = ("model.approved", "model.registered", "model.lock.renewed")
OVERLAP = timedelta(hours=1)            # events reach the table a little after they are published
REFRESH_SECONDS = 60                    # within a process, look again at most this often...
_lock = threading.Lock()
_caches: dict[tuple, dict] = {}
_store: dict = {}                       # {"bucket", "name"}: the launcher's state file, when set
_recent: list[dict] = []                # events this process published, until the event log has them (GA QA)


def remember(event: dict) -> None:
    """A model.approved, model.registered or model.lock.renewed event this process published: counted by its own
    checks at once, as the event log's copy arrives a few seconds later. Never saved in the launcher's state file."""
    if event.get("event_type") not in KINDS:
        return
    with _lock:
        _recent.append({"id": f"local|{len(_recent)}", "at": str(event.get("time") or "")[:19],
                        "type": event["event_type"], "usecase": event.get("usecase"),
                        "environment": event.get("environment"), "data": event.get("data") or {}})


def use_store(bucket: str | None, name: str | None) -> None:
    """The launcher keeps its cache in its own state file (the CLI never calls this)."""
    _store.clear()
    if bucket and name:
        _store.update(bucket=bucket, name=name)


def reset() -> None:
    with _lock:
        _caches.clear()
        _recent.clear()


def _p(name, value, kind="STRING"):
    return {"name": name, "parameterType": {"type": kind}, "parameterValue": {"value": value}}


def _query(gcp, env, dataset: str, since: str | None) -> list[dict]:
    from pipewright.gcp.bigquery import query, rows

    kinds = ", ".join(f"'{k}'" for k in KINDS)
    sql = (f"SELECT message_id, CAST(publish_time AS STRING) AS published, TO_JSON_STRING(data) AS event "
           f"FROM `{env.project}.{dataset}.events` WHERE JSON_VALUE(data, '$.event_type') IN ({kinds}) "
           "AND JSON_VALUE(data, '$.environment') = @env"
           + (" AND publish_time >= TIMESTAMP(@since)" if since else ""))
    params = [_p("env", env.name)] + ([_p("since", since)] if since else [])
    out = []
    for row in rows(query(gcp, env.project, sql, params, kms_key=(env.security or {}).get("bigquery_kms_key"))):
        event = json.loads(row["event"] or "{}")
        out.append({"id": row.get("message_id") or f"{row['published']}|{hash(row['event'])}",
                    "at": (row["published"] or "")[:19], "type": event.get("event_type"),
                    "usecase": event.get("usecase"), "data": event.get("data") or {}})
    return out


def _load_store(gcp) -> dict | None:
    if not _store:
        return None
    from pipewright.gcp import gcs_read

    try:
        text, gen = gcs_read(gcp, _store["bucket"], _store["name"])
    except Exception:  # noqa: BLE001 - an unreadable state file only costs one full read
        return None
    if not text:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return {"through": data.get("through"), "events": {e["id"]: e for e in data.get("events", [])},
            "generation": gen}


def _save_store(gcp, cache: dict) -> None:
    if not _store:
        return
    from pipewright.gcp import PreconditionFailed, gcs_write

    text = json.dumps({"through": cache["through"], "events": list(cache["events"].values())})
    try:
        cache["generation"] = gcs_write(gcp, _store["bucket"], _store["name"], text,
                                        if_generation_match=cache.get("generation") or 0)
    except PreconditionFailed:
        cache["generation"] = None      # another instance wrote it: the next save reads its generation again
    except Exception:  # noqa: BLE001 - the cache is an optimisation; the event log stays the record
        pass


def _refresh(gcp, env, dataset: str, cache: dict) -> None:
    now = datetime.now(timezone.utc)
    since = None
    if cache.get("through"):
        since = (datetime.fromisoformat(cache["through"]) - OVERLAP).strftime("%Y-%m-%d %H:%M:%S")
    for e in _query(gcp, env, dataset, since):
        cache["events"][e["id"]] = e
    cache["through"] = now.isoformat()
    cache["checked"] = time.monotonic()
    cache["queries"] = cache.get("queries", 0) + 1
    _save_store(gcp, cache)


def _cache(gcp, env) -> dict | None:
    """The environment's kept events, loaded or refreshed as needed (call with _lock held)."""
    dataset = (getattr(env, "observability", None) or {}).get("evidence_dataset")
    if not dataset:
        return None
    key = (env.project, dataset, env.name)
    cache = _caches.get(key)
    if cache is None:
        cache = _load_store(gcp) or {"through": None, "events": {}}
        cache.setdefault("checked", 0)
        cache["dataset"] = dataset
        _caches[key] = cache
        _refresh(gcp, env, dataset, cache)
    elif time.monotonic() - cache.get("checked", 0) > REFRESH_SECONDS:
        _refresh(gcp, env, dataset, cache)
    return cache


def events(gcp, env, kind: str, usecase: str, version: str) -> list[dict] | None:
    """`kind` events of one version, newest first, each {"at", **data}. None when the environment keeps no event
    log. A version with nothing kept is looked up again before the answer is no (an approval made a moment ago)."""
    with _lock:
        cache = _cache(gcp, env)
        if cache is None:
            return None

        def found():
            hits = [e for e in cache["events"].values() if e["type"] == kind and e["usecase"] == usecase
                    and str((e["data"] or {}).get("version")) == str(version)]
            if not hits:                  # one this process published, before the event log has it
                hits = [e for e in _recent if e["type"] == kind and e["usecase"] == usecase
                        and e.get("environment") == env.name and str((e["data"] or {}).get("version")) == str(version)]
            return [{"at": e["at"], **e["data"]} for e in sorted(hits, key=lambda e: e["at"], reverse=True)]

        out = found()
        if not out and time.monotonic() - cache.get("checked", 0) > 2:
            _refresh(gcp, env, cache["dataset"], cache)
            out = found()
        return out[:20]


def all_events(gcp, env, kind: str) -> list[dict]:
    """Every kept `kind` event of the environment (for the watch and discovery), oldest first."""
    with _lock:
        cache = _cache(gcp, env)
        if cache is None:
            return []
        hits = [e for e in cache["events"].values() if e["type"] == kind]
        return [{"at": e["at"], "usecase": e["usecase"], **e["data"]} for e in sorted(hits, key=lambda e: e["at"])]


def queries_made(env) -> int:
    dataset = (getattr(env, "observability", None) or {}).get("evidence_dataset")
    return (_caches.get((env.project, dataset, env.name)) or {}).get("queries", 0)
