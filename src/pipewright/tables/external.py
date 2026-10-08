"""outputs.store: gcs. Tables are Parquet files under the tenant's outputs location; `tables sync` keeps a BigQuery
external table over each prefix so the same SQL keeps working.

    <uri><table>/dt=YYYY-MM-DD/run=<run_id>/part-0.parquet      written by pipewright_runtime.write_table
    <uri><table>/dt=1970-01-01/run=_schema/part-0.parquet       zero-row schema seed, written here

The external table has an explicit schema (the use case's schema file), so files written before a column was added
read that column as NULL. Evolution is additive only, as for BigQuery tables.
"""

from __future__ import annotations

import io

from pipewright.config import Table
from pipewright.tables.plan import TablePlan, _flatten

PARTITION_KEYS = ("dt", "run")


def seed_object(uri: str, table: str) -> str:
    return f"{uri}{table}/dt=1970-01-01/run=_schema/part-0.parquet"


def seed_bytes(table: Table) -> bytes:
    import pyarrow.parquet as pq  # the gcp extra

    from pipewright.build.templates.pipewright_runtime import arrow_schema

    fields = [{"name": f["name"], "type": f["type"].upper(), "mode": (f.get("mode") or "NULLABLE").upper()}
              for f in table.fields]
    buf = io.BytesIO()
    pq.write_table(arrow_schema(fields).empty_table(), buf)
    return buf.getvalue()


def resource(table: Table, project: str, dataset: str, uri: str, labels: dict) -> dict:
    fields = [{k: v for k, v in f.items() if k in ("name", "type", "mode", "description")} for f in table.fields]
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": table.name},
        "schema": {"fields": fields},
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET", "autodetect": False,
            "sourceUris": [f"{uri}{table.name}/*"],
            "hivePartitioningOptions": {"mode": "AUTO", "sourceUriPrefix": f"{uri}{table.name}/"},
        },
        "labels": {**labels, "pipewright-store": "gcs"},
    }
    if table.description:
        body["description"] = table.description
    return body


def plan(table: Table, live: dict | None, uri: str) -> TablePlan:
    p = TablePlan(table.name, "none")
    if live is None:
        p.action = "create"
        p.changes.append(f"create external table over {uri}{table.name}/ ({len(table.fields)} columns, Parquet)")
        return p
    if live.get("type") != "EXTERNAL":
        p.action = "blocked"
        p.blockers += [f"a BigQuery table named {table.name} already exists, and outputs.store gcs needs an external "
                       "table of that name",
                       "rename the table in usecase.yaml, use store: both, or migrate and drop the old table"]
        return p
    have = {k: v for k, v in _flatten(live.get("schema", {}).get("fields", [])).items() if k not in PARTITION_KEYS}
    want = _flatten(table.fields)
    for name in have:
        if name not in want:
            p.blockers.append(f"column {name} removed")
        elif have[name][0] != want[name][0]:
            p.blockers.append(f"column {name} changes type {have[name][0]} -> {want[name][0]}")
    added = [n for n in want if n not in have]
    if p.blockers:
        p.action = "blocked"
        p.blockers.append("Parquet files already written keep their columns: add new columns instead, or start a "
                          "new table (e.g. <name>_v2)")
        return p
    source = (live.get("externalDataConfiguration") or {}).get("sourceUris", [])
    if added or source != [f"{uri}{table.name}/*"]:
        p.action = "update"
        p.changes += [f"add column {n} ({want[n][0]}, NULLABLE in older files)" for n in added]
        if source != [f"{uri}{table.name}/*"]:
            p.changes.append(f"source -> {uri}{table.name}/*")
    return p
