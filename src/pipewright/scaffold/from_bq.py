"""pipewright new-usecase --from-bq: draft a use case's data contract from a BigQuery table.

The table's schema gives the contract's columns and types, and the raw table's schema (the copy ingest keeps);
the label's distinct values are checked against what the example's model predicts (0 or 1). The draft is a
starting point: ranges, allowed values, uniqueness and personal data are for the team to add, and the file says so.
"""

from __future__ import annotations

import re

from pipewright.errors import ConfigError

# BigQuery type -> data contract type. Others (RECORD, REPEATED, BYTES, GEOGRAPHY, JSON, ...) are left out.
CONTRACT_TYPES = {
    "STRING": "string", "INTEGER": "integer", "INT64": "integer", "FLOAT": "float", "FLOAT64": "float",
    "NUMERIC": "float", "BIGNUMERIC": "float", "BOOLEAN": "boolean", "BOOL": "boolean", "DATE": "date",
    "DATETIME": "timestamp", "TIMESTAMP": "timestamp", "TIME": "string",
}
# Legacy names, as the example's table schema files use them.
TABLE_TYPES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}
NUMERIC = ("integer", "float", "boolean")
INGEST_COLUMNS = [                       # what the example's ingest step adds to each raw row
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_uri", "type": "STRING", "mode": "REQUIRED"},
    {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
IDENTIFIER = re.compile(r"(^|_)(id|uuid|email|e_mail|phone|name|first_name|last_name|address|postcode|zip)($|_)")
TABLE_REF = re.compile(r"^(?:bq://)?([a-z][a-z0-9-]{4,29})[.:]([A-Za-z0-9_]+)\.([A-Za-z0-9_$-]+)$")
MAX_LABEL_VALUES = 10


def parse_ref(ref: str) -> tuple[str, str, str]:
    m = TABLE_REF.match(ref.strip())
    if not m:
        raise ConfigError(f"--from-bq {ref!r}: give the table as project.dataset.table")
    return m.groups()


def read_table(gcp, ref: str, label: str) -> tuple[list[dict], list | None]:
    """(the table's schema fields, the label's distinct values; None when there are more than MAX_LABEL_VALUES)."""
    from pipewright.gcp.bigquery import BQ, query, rows
    from pipewright.gcp.session import error_text

    project, dataset, table = parse_ref(ref)
    r = gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}/tables/{table}")
    if not r.ok:
        raise ConfigError(f"cannot read table {project}.{dataset}.{table}: {error_text(r)}")
    fields = (r.body.get("schema") or {}).get("fields") or []
    if label not in {f["name"] for f in fields}:
        raise ConfigError(f"table {project}.{dataset}.{table} has no column {label!r} for the label "
                          f"(columns: {', '.join(f['name'] for f in fields)})")
    res = query(gcp, project, f"SELECT DISTINCT `{label}` AS v FROM `{project}.{dataset}.{table}` "
                              f"WHERE `{label}` IS NOT NULL LIMIT {MAX_LABEL_VALUES + 1}",
                location=r.body.get("location"))
    values = [row["v"] for row in rows(res)]
    return fields, (values if len(values) <= MAX_LABEL_VALUES else None)


def _label_values(label: str, kind: str, values: list | None) -> list:
    if kind == "boolean":
        return [False, True]
    if kind != "integer" or values is None or not set(values) <= {0, 1}:
        shown = "more than 10 values" if values is None else sorted(values, key=str)
        raise ConfigError(f"the label {label!r} ({kind}, {shown}) is not a 0/1 label: the example's model predicts "
                          "0 or 1. Use a table (or a view) whose label is 0/1 or true/false")
    return [0, 1]


def draft(fields: list[dict], label: str, label_values: list | None, ref: str) -> tuple[str, list[dict], list[str]]:
    """(config/data.yaml text, the raw table's schema, notes for the person running the command)."""
    columns, skipped, notes = [], [], []
    for f in fields:
        kind = CONTRACT_TYPES.get(f["type"])
        if kind is None or f.get("mode") == "REPEATED":
            skipped.append(f["name"])
            continue
        columns.append((f["name"], kind, f.get("mode") == "REQUIRED"))
    clash = sorted({c["name"] for c in INGEST_COLUMNS} & {name for name, _, _ in columns})
    if clash:
        raise ConfigError(f"table columns {clash} clash with the columns ingest adds to the raw table: "
                          "use a view that renames them")
    kinds = {name: kind for name, kind, _ in columns}
    values = _label_values(label, kinds[label], label_values)
    time_column = next((n for n, k, _ in columns if k in ("date", "timestamp") and n != label), None)
    exclude = [n for n, k, _ in columns if n not in (label, time_column) and k not in NUMERIC]
    features = [n for n, k, _ in columns if n not in (label, time_column) and k in NUMERIC]
    if not features:
        raise ConfigError(f"table {ref} has no numeric columns besides the label: the example's model needs at "
                          "least one numeric feature")
    identifiers = [n for n, _, _ in columns if IDENTIFIER.search(n.lower())]

    width = max(len(n) for n, _, _ in columns) + 1
    lines = [f"# Drafted by `pipewright new-usecase --from-bq {ref}`: review it before the first run.",
             "# Add what the table's schema cannot say: ranges (min, max), allowed values, unique keys and min_rows.",
             "data:", "  contract:", "    columns:"]
    for name, kind, required in columns:
        extra = ", required: true" if required else ""
        if name == label:
            extra += f", allowed: [{', '.join(str(v).lower() for v in values)}]"
        lines.append(f"      - {{ name: {name + ',':<{width}} type: {kind}{extra} }}")
    if skipped:
        lines.append(f"    allow_extra_columns: true            # not in the contract (nested or unsupported type): "
                     f"{', '.join(skipped)}")
    lines.append(f"  label: {{ column: {label} }}")
    lines.append(f"  split: {{ strategy: time, time_column: {time_column} }}       # the newest rows are the test set"
                 if time_column else "  split: { strategy: stratified }      # keeps the label's proportions")
    lines += ["", "features:"]
    lines.append(f"  exclude: [{', '.join(exclude)}]" + ("       # not numeric: the example's model takes numbers"
                                                        if exclude else ""))
    lines += ["", "data_protection:",
              "  personal_data_columns: []             # columns that identify a person: never features, erasable"]
    if identifiers:
        looks = "looks like an identifier" if len(identifiers) == 1 else "look like identifiers"
        lines[-1] += f"\n  # review: {', '.join(identifiers)} {looks}"
        notes.append(f"{', '.join(identifiers)} {looks}: list personal data in data_protection.personal_data_columns")
    if skipped:
        notes.append(f"left out of the contract (nested or unsupported type): {', '.join(skipped)}")
    notes.append(f"features: {', '.join(features)}")

    raw = [{"name": f["name"], "type": TABLE_TYPES.get(f["type"], f["type"]), "mode": f.get("mode") or "NULLABLE"}
           for f in fields if f["name"] in kinds]
    return "\n".join(lines) + "\n", raw + [dict(c) for c in INGEST_COLUMNS], notes
