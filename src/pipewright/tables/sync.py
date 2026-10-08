"""pipewright tables sync: create and evolve a use case's BigQuery tables.

Pipelines only write rows; this is the only thing that creates or changes tables. Breaking changes are refused.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pipewright.config import Environment, ResolvedUseCase
from pipewright.gcp import Gcp, error_text
from pipewright.tables import external
from pipewright.tables.plan import TablePlan, patch_body, plan_table, table_resource

BQ = "https://bigquery.googleapis.com/bigquery/v2"


@dataclass
class SyncResult:
    plan: TablePlan
    applied: bool
    error: str | None = None


def _label(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", value.lower())[:63]


def sync(uc: ResolvedUseCase, env: Environment, gcp: Gcp | None = None, dry_run: bool = False) -> list[SyncResult]:
    gcp = gcp or Gcp()
    dataset = uc.resources["bigquery_dataset"]
    base = f"{BQ}/projects/{env.project}/datasets/{dataset}/tables"
    labels = {"pipewright-usecase": _label(uc.name), "pipewright-tenant": _label(uc.tenant)}
    results = []
    store = uc.outputs.get("store", "bigquery")
    gcs = uc.outputs.get("gcs") or {}
    for table in uc.tables.values():
        resp = gcp.get(f"{base}/{table.name}")
        if resp.status not in (200, 404):
            results.append(SyncResult(TablePlan(table.name, "blocked"), False, error_text(resp)))
            continue
        live = resp.body if resp.ok else None
        if store == "gcs":
            results.append(_sync_external(gcp, table, live, base, env, dataset, gcs, labels, dry_run))
            continue
        plan = plan_table(table, live, uc.retention_days)
        if store == "both":
            plan.notes.append(f"also written as Parquet under {gcs.get('uri')}{table.name}/")
        if plan.action in ("none", "blocked") or dry_run:
            results.append(SyncResult(plan, False))
            continue
        retention = uc.retention_days if table.partition else None
        if plan.action == "create":
            r = gcp.post(base, table_resource(table, env.project, dataset, retention, labels))
        else:
            r = gcp.patch(f"{base}/{table.name}", patch_body(table, retention))
        results.append(SyncResult(plan, r.ok, None if r.ok else error_text(r)))
    return results


def _sync_external(gcp: Gcp, table, live, base: str, env: Environment, dataset: str, gcs: dict, labels: dict,
                   dry_run: bool) -> SyncResult:
    uri = gcs["uri"]
    if not gcs.get("external_tables", True):
        p = TablePlan(table.name, "none")
        p.notes.append(f"Parquet only under {uri}{table.name}/ (external_tables: false)")
        return SyncResult(p, False)
    plan = external.plan(table, live, uri)
    if table.partition:
        plan.notes.append("partition settings do not apply to GCS outputs (files are split by dt= and run=)")
    if plan.action in ("none", "blocked") or dry_run:
        return SyncResult(plan, False)
    bucket, _, name = external.seed_object(uri, table.name).removeprefix("gs://").partition("/")
    r = gcp.session.post(f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o",
                         params={"uploadType": "media", "name": name}, data=external.seed_bytes(table),
                         headers={"Content-Type": "application/vnd.apache.parquet"}, timeout=60)
    if r.status_code != 200:
        return SyncResult(plan, False, f"schema seed upload failed: {r.status_code} {r.text[:200]}")
    body = external.resource(table, env.project, dataset, uri, labels)
    resp = gcp.post(base, body) if plan.action == "create" else gcp.put(f"{base}/{table.name}", body)
    return SyncResult(plan, resp.ok, None if resp.ok else error_text(resp))


def render(results: list[SyncResult], uc: ResolvedUseCase, dry_run: bool) -> str:
    store = uc.outputs.get("store", "bigquery")
    lines = [f"\nTables for {uc.name} in {uc.resources['bigquery_dataset']} ({uc.environment}, store {store})"
             + ("  [dry run]" if dry_run else "")]
    for r in results:
        p = r.plan
        if r.error:
            lines.append(f"  FAIL  {p.table}: {r.error}")
        elif p.action == "none":
            lines.append(f"  OK    {p.table}: up to date")
        elif p.action == "blocked":
            lines.append(f"  FAIL  {p.table}: " + "; ".join(p.blockers[:-1]))
            lines.append(f"        -> {p.blockers[-1]}")
        else:
            verb = "would " + p.action if dry_run else (p.action + "d" if p.action.endswith("e") else p.action)
            lines.append(f"  {'PLAN' if dry_run else 'DONE'}  {p.table}: {verb}: " + "; ".join(p.changes))
        for n in p.notes:
            lines.append(f"  INFO  {p.table}: {n}")
    return "\n".join(lines)
