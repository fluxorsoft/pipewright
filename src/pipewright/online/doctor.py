"""`pipewright doctor` for the online stores of FR-104: what the store is and costs, before anyone relies on it, and
whether it is there, fresh and safe."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pipewright.online import sizing

STALE_HOURS = 48
BQ_PER_TIB = 6.25        # on-demand query price, USD per TiB (list price)


def production(env) -> bool:
    return bool(env.promotion_from) or "prod" in env.name.lower()


def check(uc, env, gcp, report) -> None:
    from pipewright.checks.doctor import FAIL, INFO, OK, WARN, Result
    from pipewright.gcp.storage import gcs_read
    from pipewright.online import sync

    online = uc.ml["online_features"]
    kind = online["type"]
    s = f"Online store of {uc.name}"
    store = ((env.features or {}).get("online_store")) or {}
    if kind == "cloudsql":
        sql = store.get("cloudsql") or {}
        settings = {**sizing.INSTANCE_DEFAULTS, **(sql.get("settings") or {})}
        cost = sizing.instance_monthly_cost(settings)
        tenants = sum(1 for t in env.tenants.values() if (t or {}).get("features", {}).get("sync_job"))
        report.add(s, Result(INFO, "store", f"Cloud SQL {sql.get('instance')} ({settings['edition']}, "
                             f"{settings['tier']}, {settings['availability']})"
                             + (f", about ${cost:.0f} a month" if cost else "")
                             + (f", shared by {tenants} tenants" if tenants > 1 else "")))
        r = gcp.get(f"https://sqladmin.googleapis.com/v1/projects/{sql.get('instance', '::').split(':')[0]}"
                    f"/instances/{sql.get('instance', '::').split(':')[-1]}")
        if r.ok:
            live_tier = ((r.body.get("settings") or {}).get("tier"))
            report.add(s, Result(OK if r.body.get("state") == "RUNNABLE" else WARN, "instance",
                                 f"{r.body.get('state')}, tier {live_tier}"
                                 + ("" if live_tier == settings["tier"] else
                                    f" (the platform file says {settings['tier']})")))
        elif r.status != 403:
            report.add(s, Result(FAIL, "instance", f"{sql.get('instance')}: HTTP {r.status}",
                                 "the platform team creates it (online_store.cloudsql in the platform file)"))
        for w in sizing.instance_warnings(settings, production(env)):
            report.add(s, Result(WARN, "instance tier", w))
        if online.get("schema") == "public" and tenants > 1:
            report.add(s, Result(WARN, "schema", "public, in an instance other tenants share",
                                 "give the team a schema of its own: model.serving.online_store.cloudsql.schema"))
        if online.get("provider", "pipewright") == "pipewright":
            lk = online.get("lookup") or {}
            warm = sizing.lookup_standing_cost(lk)
            report.add(s, Result(INFO, "lookup service",
                                 f"{online['service']}: {lk.get('cpu', 1)} vCPU, {lk.get('memory', '512Mi')}, "
                                 f"{lk.get('min_instances', 0)} to {lk.get('max_instances', 5)} instances"
                                 + (f", warm instances about ${warm:.0f} a month (best effort: Google may restart "
                                    "one)" if warm else ", none warm: the first call after a quiet spell waits for "
                                    "a cold start")))
            got = gcp.get(f"https://run.googleapis.com/v2/projects/{env.project}/locations/{env.region}/services/"
                          f"{online['service']}")
            if got.status == 404:
                report.add(s, Result(WARN, "lookup service", "not created yet",
                                     "deploy-model creates it before it deploys the model"))
            elif got.ok:
                state = (got.body.get("terminalCondition") or {}).get("state")
                report.add(s, Result(OK if state == "CONDITION_SUCCEEDED" else WARN, "lookup service",
                                     f"{state}, {got.body.get('uri')}"))
        else:
            report.add(s, Result(INFO, "lookup", f"the team's own (provider own); {online.get('reader')} reads "
                                 f"{online['schema']}.{online['table']}"))
    elif kind == "bigquery":
        gib = (store.get("bigquery") or {}).get("bi_engine_gib")
        report.add(s, Result(INFO, "store", f"BigQuery {online['table']} with BI Engine"
                             + (f" ({gib} GiB reserved, about ${gib * 0.052 * sizing.HOURS:.0f} a month for the "
                                "environment)" if gib else "")))
        p, d, t = online["table"].split(".")
        table = gcp.get(f"https://bigquery.googleapis.com/bigquery/v2/projects/{p}/datasets/{d}/tables/{t}")
        res = gcp.get(f"https://bigqueryreservation.googleapis.com/v1/projects/{env.project}/locations/"
                      f"{env.region}/biReservation")
        size = int((res.body or {}).get("size") or 0) if res.ok else 0
        if res.ok and not size:
            report.add(s, Result(FAIL, "BI Engine", "no reservation in this project and region: every lookup "
                                 "would be billed as a full table read", "the platform team sets the reservation"))
        if table.ok:
            nbytes = int(table.body.get("numBytes") or 0)
            # BigQuery bills at least 10 MiB a query (found live: a 0.4 MiB table's unaccelerated lookups billed 10 MiB)
            per = max(nbytes, 10 * 2**20) / 2**40 * BQ_PER_TIB
            preferred = [f"{x.get('projectId')}.{x.get('datasetId')}.{x.get('tableId')}"
                         for x in ((res.body or {}).get("preferredTables") or [])] if res.ok else []
            if res.ok and size and nbytes > size:
                report.add(s, Result(FAIL, "BI Engine", f"the table is {nbytes / 2**30:.2f} GiB, the reservation "
                                     f"{size / 2**30:.2f} GiB", "the platform team raises the reservation"))
            elif res.ok and preferred and online["table"] not in preferred:
                report.add(s, Result(FAIL, "BI Engine", "the table is not one of the reservation's preferred "
                                     "tables", "the platform team adds it"))
            elif res.ok and size:
                report.add(s, Result(OK, "BI Engine", f"covers {online['table']} ({nbytes / 2**20:.0f} MiB)"))
            report.add(s, Result(INFO, "refresh window", f"for minutes after each sync, lookups BI Engine has not "
                                 f"reloaded are billed as a full read (at least 10 MiB): about ${per:.5f} each (spike: up "
                                 "to 10 minutes for 1M rows)"))
        else:
            report.add(s, Result(WARN, "served table", f"{online['table']}: not synced yet",
                                 "run `pipewright features sync` or wait for the online_sync schedule"))
    elif kind == "http":
        report.add(s, Result(INFO, "store", f"the team's own, {online.get('url')}: Pipewright checks the names and "
                             "types it answers, not what happens inside it"))
        return
    if online.get("export"):
        report.add(s, Result(INFO, "Parquet export", f"on: {online['export']['uri']}, the last "
                             f"{online['export'].get('keep', 3)} generations kept"))
    text, _ = gcs_read(gcp, env.artifacts_bucket, sync.record_name(uc.tenant, uc.name))
    if not text:
        report.add(s, Result(WARN, "last sync", "none yet",
                             "add a schedule with pipeline online_sync, or run `pipewright features sync`"))
        return
    rec = json.loads(text)
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(rec["synced_at"])).total_seconds() / 3600
    contract = (rec.get("feature_contract") or {}).get("version")
    report.add(s, Result(OK if age <= STALE_HOURS else WARN, "last sync",
                         f"{rec['generation']}: {rec['rows']} entities, {age:.0f} h ago"
                         + (f", feature contract {contract}" if contract else ""),
                         "" if age <= STALE_HOURS else "older than two days: check the online_sync schedule"))
