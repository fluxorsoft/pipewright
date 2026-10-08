"""Table plans: compare a use case's table schemas with BigQuery.

Additive changes are allowed; anything that would break existing data or readers is blocked with advice.
`pipewright doctor` shows the plan; `pipewright tables sync` (step 5) applies it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipewright.config import Table

_TYPE_ALIASES = {"FLOAT64": "FLOAT", "INT64": "INTEGER", "BOOL": "BOOLEAN", "STRUCT": "RECORD"}
_MS_PER_DAY = 86_400_000


def _norm_type(t: str) -> str:
    t = t.upper()
    return _TYPE_ALIASES.get(t, t)


def _flatten(fields: list, prefix: str = "") -> dict[str, tuple[str, str]]:
    """name (dotted for nested records) -> (type, mode)."""
    out: dict[str, tuple[str, str]] = {}
    for f in fields:
        name = prefix + f["name"]
        out[name] = (_norm_type(f["type"]), (f.get("mode") or "NULLABLE").upper())
        if f.get("fields"):
            out.update(_flatten(f["fields"], name + "."))
    return out


@dataclass
class TablePlan:
    table: str
    action: str                       # create | update | none | blocked
    changes: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def plan_table(desired: Table, live: dict | None, retention_days: int | None = None) -> TablePlan:
    """`live` is the BigQuery tables.get resource, or None if the table does not exist."""
    if retention_days and not desired.partition:
        # Table expiration would delete the whole table, so retention only applies to partitions.
        note = "retention_days ignored: it only applies to partitioned tables"
        retention_days = None
    else:
        note = None
    plan = _plan(desired, live, retention_days)
    if note:
        plan.notes.append(note)
    return plan


def _plan(desired: Table, live: dict | None, retention_days: int | None) -> TablePlan:
    if live is None:
        details = [f"{len(desired.fields)} columns"]
        if desired.partition:
            details.append(f"partitioned by {desired.partition['field']} ({desired.partition['type']})")
        if desired.cluster:
            details.append(f"clustered by {', '.join(desired.cluster)}")
        if retention_days:
            details.append(f"retention {retention_days} days")
        return TablePlan(desired.name, "create", [f"create table: {'; '.join(details)}"])

    plan = TablePlan(desired.name, "none")
    want = _flatten(desired.fields)
    have = _flatten(live.get("schema", {}).get("fields", []))

    for name, (wtype, wmode) in want.items():
        if name not in have:
            if wmode == "REQUIRED":
                plan.blockers.append(
                    f"new column {name} is REQUIRED: BigQuery can only add NULLABLE or REPEATED columns "
                    "to an existing table; make it NULLABLE or create a new table version"
                )
            else:
                plan.changes.append(f"add column {name} ({wtype}, {wmode})")
            continue
        htype, hmode = have[name]
        if wtype != htype:
            plan.blockers.append(f"column {name} changes type {htype} -> {wtype}")
        if hmode != wmode:
            if hmode == "REQUIRED" and wmode == "NULLABLE":
                plan.changes.append(f"relax column {name} REQUIRED -> NULLABLE")
            else:
                plan.blockers.append(f"column {name} changes mode {hmode} -> {wmode}")
    for name in have:
        if name not in want:
            plan.blockers.append(f"column {name} exists in BigQuery but not in the schema file (removal)")

    live_part = live.get("timePartitioning")
    want_part = desired.partition
    live_part_view = (live_part.get("field"), live_part.get("type")) if live_part else None
    want_part_view = (want_part["field"], want_part["type"]) if want_part else None
    if live_part_view != want_part_view:
        plan.blockers.append(f"partitioning changes {live_part_view or 'none'} -> {want_part_view or 'none'}")

    live_cluster = live.get("clustering", {}).get("fields", [])
    if list(desired.cluster) != list(live_cluster):
        plan.changes.append(f"clustering {live_cluster or 'none'} -> {list(desired.cluster) or 'none'}")

    if retention_days:
        want_ms = str(retention_days * _MS_PER_DAY)
        have_ms = (live_part or {}).get("expirationMs") if want_part else None
        if want_part and have_ms != want_ms:
            plan.changes.append(f"partition expiration -> {retention_days} days")

    if plan.blockers:
        plan.action = "blocked"
        plan.blockers.append(f"create a new table instead (for example {desired.name}_v2) and migrate")
    elif plan.changes:
        plan.action = "update"
    return plan


# ------------------------------------------------------------------ apply --

def _partitioning(desired: Table, retention_days: int | None) -> dict | None:
    if not desired.partition:
        return None
    body = {"type": desired.partition["type"], "field": desired.partition["field"]}
    if retention_days:
        body["expirationMs"] = str(retention_days * _MS_PER_DAY)
    return body


def table_resource(desired: Table, project: str, dataset: str, retention_days: int | None,
                   labels: dict | None = None) -> dict:
    """BigQuery table resource for create (tables.insert)."""
    body = {
        "tableReference": {"projectId": project, "datasetId": dataset, "tableId": desired.name},
        "schema": {"fields": desired.fields},
    }
    part = _partitioning(desired, retention_days)
    if part:
        body["timePartitioning"] = part
    if desired.cluster:
        body["clustering"] = {"fields": list(desired.cluster)}
    if desired.description:
        body["description"] = desired.description
    if labels:
        body["labels"] = labels
    return body


def patch_body(desired: Table, retention_days: int | None) -> dict:
    """Body for tables.patch after an additive plan: full desired schema, clustering, partition expiration."""
    body: dict = {"schema": {"fields": desired.fields}}
    part = _partitioning(desired, retention_days)
    if part:
        body["timePartitioning"] = part
    body["clustering"] = {"fields": list(desired.cluster)} if desired.cluster else None
    if desired.description:
        body["description"] = desired.description
    return body
