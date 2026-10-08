"""Pipewright runtime helpers for pipeline components. Copied into every component image by `pipewright release`
(next to the use case's own modules), so components import it without depending on the pipewright package:

    from pipewright_runtime import emit_event, log, read_source, write_table

read_source   reads a run's source_uri: gs:// CSV or Parquet, or bq:// table or view
write_table   writes rows to the use case's store (outputs.store): BigQuery, Parquet on GCS, or both
log           one structured JSON log line with the standard Pipewright labels
emit_event    a lifecycle event: a structured log line, and a Pub/Sub message when the platform set an events topic

Everything here takes the `ml_config` the component received; it needs only google-cloud-* and pandas/pyarrow,
which the component image already has. Owned by the platform: do not edit in the use case.
"""

from __future__ import annotations

import base64
import io
import json
import os
import re
import sys
from datetime import datetime, timezone

GCS_FORMAT_VERSION = 1
PARTITION_KEYS = ("dt", "run")        # gs://<uri><table>/dt=YYYY-MM-DD/run=<run_id>/part-0.parquet


# ------------------------------------------------------------------ logging --

def labels(ml_config: dict, run_id: str | None = None, **extra) -> dict:
    """The standard labels every log line and event carries."""
    platform = ml_config.get("platform") or {}
    out = {"pipewright-usecase": ml_config.get("usecase"), "pipewright-tenant": platform.get("tenant"),
           "pipewright-environment": ml_config.get("environment"),
           "pipewright-release": os.environ.get("PIPEWRIGHT_RELEASE"), "run-id": run_id}
    out.update(extra)
    return {k: str(v) for k, v in out.items() if v not in (None, "")}


def log(message: str, ml_config: dict, severity: str = "INFO", run_id: str | None = None, **fields) -> None:
    """Cloud Logging reads JSON lines on stdout as structured entries (jsonPayload + labels); log routers can then
    send them anywhere (Dynatrace, Datadog, Splunk, BigQuery) without code changes here."""
    entry = {"severity": severity, "message": message,
             "logging.googleapis.com/labels": labels(ml_config, run_id), **fields}
    print(json.dumps(entry, default=str), flush=True)


def emit_event(event_type: str, ml_config: dict, run_id: str | None = None, **data) -> dict:
    """Never raises: a monitoring hiccup must not fail an ML step."""
    event = {"event_type": event_type, "time": datetime.now(timezone.utc).isoformat(),
             "usecase": ml_config.get("usecase"), "environment": ml_config.get("environment"),
             "tenant": (ml_config.get("platform") or {}).get("tenant"),
             "release": os.environ.get("PIPEWRIGHT_RELEASE"), "run_id": run_id, "data": data}
    log(f"pipewright event {event_type}", ml_config, "NOTICE", run_id, pipewright_event=event)
    topic = (ml_config.get("platform") or {}).get("events_topic")
    if topic:
        try:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession

            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/pubsub"])
            attributes = {k: str(event[k]) for k in ("event_type", "usecase", "environment", "tenant") if event[k]}
            message = {"data": base64.b64encode(json.dumps(event, default=str).encode()).decode(),
                       "attributes": attributes}
            # Regional endpoint: private topics enforce in-region storage in transit.
            region = (ml_config.get("platform") or {}).get("region")
            host = f"{region}-pubsub.googleapis.com" if region else "pubsub.googleapis.com"
            r = AuthorizedSession(creds).post(f"https://{host}/v1/{topic}:publish",
                                              json={"messages": [message]}, timeout=30)
            if r.status_code != 200:
                log(f"event {event_type} not published: {r.status_code} {r.text[:200]}", ml_config, "WARNING", run_id)
        except Exception as e:  # noqa: BLE001
            log(f"event {event_type} not published: {type(e).__name__}: {e}", ml_config, "WARNING", run_id)
    return event


# ------------------------------------------------------------------ sources --
# Same rules as pipewright.runs.sources (kept in step by a test).
_BQ = re.compile(r"^bq://(?:([a-z][a-z0-9-]{4,61}[a-z0-9])\.)?([A-Za-z_][A-Za-z0-9_]{0,1023})\.([A-Za-z0-9_$-]{1,1024})$")


def bq_table(source_uri: str, default_project: str) -> str:
    """bq://project.dataset.table or bq://dataset.table -> project.dataset.table"""
    m = _BQ.match(source_uri)
    if not m:
        raise ValueError(f"{source_uri!r} is not bq://project.dataset.table (or bq://dataset.table)")
    project, dataset, table = m.groups()
    return f"{project or default_project}.{dataset}.{table}"


def bigquery_client(project: str, ml_config: dict | None = None):
    """A BigQuery client whose queries encrypt their results with the environment's key, when it has one
    (ml_config.platform.bigquery_kms_key): private environments enforce CMEK on every query."""
    from google.cloud import bigquery

    key = ((ml_config or {}).get("platform") or {}).get("bigquery_kms_key")
    config = bigquery.QueryJobConfig(
        destination_encryption_configuration=bigquery.EncryptionConfiguration(kms_key_name=key)) if key else None
    return bigquery.Client(project=project, default_query_job_config=config)


def read_source(source_uri: str, project: str, ml_config: dict | None = None):
    """A run's input as a DataFrame. gs://.../*.parquet is read as Parquet, any other gs:// object as CSV;
    bq:// reads a table or a view with a query, so filtering and joins can live in a view."""
    import pandas as pd

    if source_uri.startswith("bq://"):
        ref = bq_table(source_uri, project)
        df = bigquery_client(project, ml_config).query(f"SELECT * FROM `{ref}`").to_dataframe(
            create_bqstorage_client=False)
        for col in df.columns:               # BigQuery DATE arrives as dbdate: use ISO text, as CSV would give
            if str(df[col].dtype) == "dbdate":
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
        return df
    if source_uri.startswith("gs://"):
        from google.cloud import storage

        bucket, _, name = source_uri.removeprefix("gs://").partition("/")
        payload = storage.Client(project=project).bucket(bucket).blob(name).download_as_bytes()
        return pd.read_parquet(io.BytesIO(payload)) if name.endswith(".parquet") else pd.read_csv(io.BytesIO(payload))
    raise ValueError(f"{source_uri!r}: source_uri must start with gs:// or bq://")


# ------------------------------------------------------------------- tables --

def _arrow_type(kind: str):
    import pyarrow as pa

    return {"STRING": pa.string(), "INTEGER": pa.int64(), "INT64": pa.int64(), "FLOAT": pa.float64(),
            "FLOAT64": pa.float64(), "NUMERIC": pa.float64(), "BOOLEAN": pa.bool_(), "BOOL": pa.bool_(),
            "TIMESTAMP": pa.timestamp("us", tz="UTC"), "DATE": pa.date32(), "JSON": pa.string()}[kind]


def arrow_schema(fields: list[dict]):
    import pyarrow as pa

    return pa.schema([pa.field(f["name"], _arrow_type(f["type"]), nullable=f.get("mode") != "REQUIRED")
                      for f in fields])


def conform(df, fields: list[dict]):
    """Columns in schema order with schema types; unknown columns dropped, missing NULLABLE ones added."""
    import pandas as pd

    out = pd.DataFrame(index=df.index)
    for f in fields:
        name, kind = f["name"], f["type"]
        if name not in df.columns:
            if f.get("mode") == "REQUIRED":
                raise ValueError(f"column {name} is REQUIRED but missing")
            out[name] = None
            continue
        col = df[name]
        if kind == "TIMESTAMP":
            col = pd.to_datetime(col, utc=True, errors="coerce")
        elif kind == "DATE":
            col = pd.to_datetime(col, errors="coerce").dt.date
        elif kind in ("INTEGER", "INT64"):
            col = pd.to_numeric(col, errors="coerce").astype("Int64")
        elif kind in ("FLOAT", "FLOAT64", "NUMERIC"):
            col = pd.to_numeric(col, errors="coerce").astype("float64")
        elif kind in ("BOOLEAN", "BOOL"):
            col = col.astype("boolean")
        else:
            col = col.astype("object").where(col.notna(), None).map(lambda v: v if v is None else str(v))
        out[name] = col
    return out


def gcs_object(ml_config: dict, table: str, run_id: str, when: datetime | None = None) -> str:
    uri = ml_config["outputs"]["gcs"]["uri"]
    day = (when or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
    return f"{uri}{table}/dt={day}/run={run_id}/part-0.parquet"


def _write_gcs(df, table: str, fields: list[dict], ml_config: dict, project: str, run_id: str) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage

    data = pa.Table.from_pandas(df, schema=arrow_schema(fields), preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(data, buf, compression="snappy")
    target = gcs_object(ml_config, table, run_id)
    bucket, _, name = target.removeprefix("gs://").partition("/")
    # One object per run and table: a retried step overwrites its own file instead of adding rows twice.
    storage.Client(project=project).bucket(bucket).blob(name).upload_from_string(
        buf.getvalue(), content_type="application/vnd.apache.parquet")
    return target


def _write_bigquery(df, table: str, ml_config: dict, project: str, dataset: str) -> str:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    ref = client.get_table(f"{project}.{dataset}.{table}")
    known = [f.name for f in ref.schema if f.name in df.columns]
    # Load job, not streaming: free, and rows are visible to DML (erasure) at once.
    client.load_table_from_dataframe(df[known], ref, job_config=bigquery.LoadJobConfig(
        schema=ref.schema, write_disposition="WRITE_APPEND")).result()
    return f"bq://{project}.{dataset}.{table}"


def write_table(table: str, rows, ml_config: dict, project: str, dataset: str, run_id: str) -> list[str]:
    """Appends rows (a DataFrame or a list of dicts) to `table` in the configured store; returns where they went."""
    import pandas as pd

    outputs = ml_config.get("outputs") or {"store": "bigquery", "tables": {}}
    fields = outputs["tables"].get(table)
    if fields is None:
        raise ValueError(f"{table} is not in outputs.bigquery.tables of {ml_config.get('usecase')}")
    df = conform(rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows)), fields)
    written = []
    if outputs["store"] in ("bigquery", "both"):
        written.append(_write_bigquery(df, table, ml_config, project, dataset))
    if outputs["store"] in ("gcs", "both"):
        written.append(_write_gcs(df, table, fields, ml_config, project, run_id))
    log(f"wrote {len(df)} rows to {table}", ml_config, run_id=run_id, table=table, rows=len(df), locations=written)
    return written


if __name__ == "__main__":       # `python pipewright_runtime.py` prints the format version (image smoke test)
    print(GCS_FORMAT_VERSION, file=sys.stdout)
