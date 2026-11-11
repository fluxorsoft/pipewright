"""Pipewright runtime helpers for pipeline components. Copied into every component image by `pipewright release`
(next to the use case's own modules), so components import it without depending on the pipewright package:

    from pipewright_runtime import emit_event, log, read_source, write_table

read_source   reads a run's source_uri: gs:// CSV or Parquet, or bq:// table or view; with data.features_from, the
              source is the entity table and features are joined point in time from the feature tables; with
              labels=True and data.label_source, the label comes from the label table within its window; a
              snapshot://<run_id> source replays the rows an earlier run read (data.snapshot_table)
write_table   writes rows to the use case's store (outputs.store): BigQuery, Parquet on GCS, or both
write_snapshot  writes the rows a run read to data.snapshot_table, tagged with the run, so the run can be replayed
fairness_measures  bias measures per attribute on the test set (evaluation.fairness); fairness_problems its gates
write_lineage writes lineage.json for a registered model version: release, config digest, rows, sources, evaluation
log           one structured JSON log line with the standard Pipewright labels
emit_event    a lifecycle event: a structured log line, and a Pub/Sub message when the platform set an events topic
ray_cluster   a Ray on Agent Platform cluster for this step of this run (steps.<step>.ray), deleted when the block ends
ray_job       runs a Ray job on that cluster and waits for it (needs ray[default] and google-cloud-aiplatform[ray])
approved_download  downloads exactly the approved generation of each model file and checks its SHA-256 (1.4.1)
folder_files  every file of a model folder with its generation and SHA-256, as an approval records them (1.4.1)

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


# Who each event a step sends is for (Pipewright 1.4.3): its Pub/Sub message carries pw_<persona> = "1", so a team
# subscribes with the filter attributes:pw_<persona>. The same as pipewright.observability.catalogue (checked by
# tests/test_event_catalogue.py); this file cannot import the package.
EVENT_PERSONAS = {
    "model.registered": ("governance", "ml"),
    "model.registration_skipped": ("ml",),
    "monitoring.insufficient_data": ("ml",),                  # 1.4.3 (FR-98)
    "monitoring.attribution_drift": ("governance", "ml"),     # 1.4.3 (FR-98)
    "model.lineage.recorded": ("governance",),
    "training.evaluated": ("governance", "ml"),
    "data.snapshot.written": ("data", "governance", "ml"),
    "data.snapshot.replayed": ("governance", "ml"),
    "data.features.resolved": ("data", "ml"),
    "data.labels.resolved": ("data", "ml"),
    "monitoring.recorded": ("ml",),
    "monitoring.alert": ("ml", "product", "sre"),
    "retrain.requested": ("ml",),
    "synthetic.generated": ("data", "governance", "ml"),
    "synthetic.evaluated": ("governance", "ml"),
    "synthetic.rejected": ("governance", "ml"),
    "ray.cluster.used": ("finops", "ml", "sre"),
}


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
            attributes.update({f"pw_{p}": "1" for p in EVENT_PERSONAS.get(event_type, ())})
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
# Value-free errors (NFR-18, D-65): a data engine's message can quote the rows it failed on. Errors from Google's
# clients on the data paths keep the column and the rule, never the value. Same rules as pipewright.errors.value_free
# (kept in step by a test).
_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_AFTER_VALUE = re.compile(r"(?i)\b(values?|got|found|literal)(\s*[:=]\s*)(?![`'\"])[^\s,;)\]]+")
HIDDEN = "..."


def value_free(text: str) -> str:
    """The message with data values taken out."""
    text = _QUOTED.sub(lambda m: m.group(0)[0] + HIDDEN + m.group(0)[0], str(text))
    return _AFTER_VALUE.sub(lambda m: m.group(1) + m.group(2) + HIDDEN, text)


def _value_free_errors(fn):
    """Errors from Google's clients (BigQuery, Cloud Storage) leave fn with their data values taken out."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if type(e).__module__.startswith("google."):
                raise RuntimeError(f"{fn.__name__}: {type(e).__name__}: {value_free(str(e))}") from None
            raise
    return wrapper


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


@_value_free_errors
def read_source(source_uri: str, project: str, ml_config: dict | None = None, run_id: str | None = None,
                labels: bool = False):
    """A run's input as a DataFrame. gs://.../*.parquet is read as Parquet, any other gs:// object as CSV;
    bq:// reads a table or a view with a query, so filtering and joins can live in a view.

    With data.features_from in ml_config the source is the entity table (bq:// only) and each row gets the latest
    values of the named features at or before its cut-off time; a `data.features.resolved` event records what was
    read.

    labels=True (training's ingest step) with data.label_source: the label comes from the label table, 1 when an
    event falls within the window after the row's cut-off, and rows whose window has not closed by the newest label
    event are dropped; a `data.labels.resolved` event records the anchor. Without data.label_source it does nothing.

    snapshot://<run_id> replays exactly the rows that run read, from data.snapshot_table: no feature or label join
    runs again, since the snapshot already holds their result."""
    import pandas as pd

    data = (ml_config or {}).get("data") or {}
    features_from = data.get("features_from")
    label_source = data.get("label_source") if labels else None
    if source_uri.startswith(SNAPSHOT_SCHEME):
        return _read_snapshot(source_uri, project, ml_config or {}, run_id)
    if (features_from or label_source) and not source_uri.startswith("bq://"):
        what = "data.features_from" if features_from else "data.label_source"
        raise ValueError(f"{source_uri!r}: with {what} the run's source is the entity table, a bq:// table or view")
    if source_uri.startswith("bq://"):
        ref = bq_table(source_uri, project)
        client = bigquery_client(project, ml_config)
        source_ref = ref
        if label_source:
            ref = _labelled_entities(client, ref, project, label_source, data, ml_config or {}, run_id)
        if features_from:
            df = _read_with_features(client, ref, project, features_from, ml_config or {}, run_id,
                                     recorded_as=source_ref)
        else:
            df = client.query(f"SELECT * FROM `{ref}`").to_dataframe(create_bqstorage_client=False)
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


def needs_labels(source_uri: str, project: str, ml_config: dict) -> bool:
    """True when a source read outside training's ingest step (the synthetic pipeline's) should take its labels from
    data.label_source, as training does: the use case has one and the bq:// table lacks the label column, so it is the
    entity table rather than rows labelled already (Pipewright 1.2.0, FR-65). A source holding the label is read as
    in 1.x."""
    data = (ml_config or {}).get("data") or {}
    label = (data.get("label") or {}).get("column")
    if not data.get("label_source") or not label or not source_uri.startswith("bq://"):
        return False
    table = bigquery_client(project, ml_config).get_table(bq_table(source_uri, project))
    return label not in {f.name for f in table.schema}


# ---------------------------------------------------- labels and snapshots --
# A label source (data.label_source) turns events into labels: a row is 1 when the label table has an event for its
# key at or after the row's cut-off and before cut-off + window_days, else 0. A row whose window ends after the
# newest event in the label table is still maturing: it is dropped, 1s and 0s alike. The anchor is that newest
# event, not today, so late-arriving events never turn real positives into 0s. A `date` cut-off is the start of that
# day (UTC), as for features.
#
# A run's snapshot (data.snapshot_table) is the per-run copy of what its ingest step read, tagged with run_id;
# snapshot://<run_id> reads exactly those rows back.

SNAPSHOT_SCHEME = "snapshot://"
SNAPSHOT_COLUMNS = ("run_id", "source_uri", "ingested_at")      # added by write_snapshot, dropped on replay
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def label_query(entity_ref: str, label_source: dict, label_column: str, anchor: str, project: str) -> str:
    """The SQL for read_source(labels=True): the entity rows whose window has closed by `anchor` (an ISO timestamp,
    the newest label event), with `label_column` from the label table."""
    table = bq_table(label_source["table"], project)
    keys, cut, days = label_source["key"], label_source["cutoff_column"], int(label_source["window_days"])
    t = f"CAST(l.{_q(label_source['time_column'])} AS TIMESTAMP)"
    c = f"CAST(e.{_q(cut)} AS TIMESTAMP)"
    match = " AND ".join(f"l.{_q(k)} = e.{_q(k)}" for k in keys)
    return (f"SELECT e.*, IF(EXISTS(SELECT 1 FROM `{table}` AS l WHERE {match} AND {t} >= {c} "
            f"AND {t} < TIMESTAMP_ADD({c}, INTERVAL {days} DAY)), 1, 0) AS {_q(label_column)}\n"
            f"FROM `{entity_ref}` AS e\n"
            f"WHERE TIMESTAMP_ADD({c}, INTERVAL {days} DAY) <= TIMESTAMP '{anchor}'")


def _labelled_entities(client, entity_ref: str, project: str, label_source: dict, data: dict, ml_config: dict,
                       run_id: str | None) -> str:
    """Runs the label query and returns the table holding its result (the query's own result table), so the
    feature join, when there is one, reads it like any entity table."""
    label_column = (data.get("label") or {}).get("column")
    if not label_column:
        raise ValueError("data.label_source needs data.label.column")
    columns = [f.name for f in client.get_table(entity_ref).schema]
    if label_column in columns:
        raise ValueError(f"{entity_ref} already has the label column {label_column!r}: with data.label_source the "
                         "label comes from the label table")
    for col in [*label_source["key"], label_source["cutoff_column"]]:
        if col not in columns:
            raise ValueError(f"{entity_ref} has no column {col!r} (data.label_source)")
    table = bq_table(label_source["table"], project)
    tcol = _q(label_source["time_column"])
    row = list(client.query(f"SELECT MAX(CAST({tcol} AS TIMESTAMP)) AS anchor FROM `{table}`").result())[0]
    if row["anchor"] is None:
        raise ValueError(f"{table} has no label events: nothing to anchor the label window on")
    anchor = row["anchor"].isoformat()
    total = list(client.query(f"SELECT COUNT(*) AS n FROM `{entity_ref}`").result())[0]["n"]
    job = client.query(label_query(entity_ref, label_source, label_column, anchor, project))
    job.result()
    dest = job.destination
    ref = f"{dest.project}.{dest.dataset_id}.{dest.table_id}"
    counts = list(client.query(f"SELECT COUNT(*) AS n, COUNTIF({_q(label_column)} = 1) AS positives "
                               f"FROM `{ref}`").result())[0]
    record = {"entity_table": entity_ref, "label_table": table, "key": label_source["key"],
              "time_column": label_source["time_column"], "cutoff_column": label_source["cutoff_column"],
              "window_days": int(label_source["window_days"]), "anchor": anchor, "query_job": job.job_id,
              "entity_rows": int(total), "rows": int(counts["n"]), "positives": int(counts["positives"]),
              "dropped_open_window": int(total) - int(counts["n"])}
    if record["rows"] and record["positives"] in (0, record["rows"]):
        only = "1" if record["positives"] else "0"
        log(f"every labelled row is {only}: the {record['window_days']}-day window over {table} gives one class only, "
            "so no classifier can learn from it and metrics such as auc cannot be computed (a shorter window, or "
            "another label table)", ml_config, "WARNING", run_id)
    log(f"labels from {table}: {record['rows']} of {total} rows have a closed {record['window_days']}-day window "
        f"(anchor {anchor}), {record['positives']} positive", ml_config, run_id=run_id)
    emit_event("data.labels.resolved", ml_config, run_id, **record)
    return ref


def snapshot_table_ref(ml_config: dict, project: str) -> str:
    """project.dataset.table of data.snapshot_table, in the use case's dataset."""
    table = ((ml_config.get("data") or {}).get("snapshot_table"))
    if not table:
        raise ValueError(f"{ml_config.get('usecase')} sets no data.snapshot_table: there is no snapshot to replay")
    dataset = (ml_config.get("platform") or {}).get("bigquery_dataset")
    return f"{project}.{dataset}.{table}"


def _read_snapshot(source_uri: str, project: str, ml_config: dict, run_id: str | None):
    from google.cloud import bigquery

    replayed = source_uri.removeprefix(SNAPSHOT_SCHEME)
    if not _RUN_ID.match(replayed):
        raise ValueError(f"{source_uri!r} is not snapshot://<run id>")
    if ((ml_config.get("outputs") or {}).get("store") or "bigquery") == "gcs":
        raise ValueError("replaying a snapshot needs the BigQuery store (outputs.store bigquery or both)")
    ref = snapshot_table_ref(ml_config, project)
    client = bigquery_client(project, ml_config)
    schema = client.get_table(ref).schema
    columns = [f.name for f in schema if f.name not in SNAPSHOT_COLUMNS]
    job = client.query(f"SELECT {', '.join(_q(c) for c in columns)} FROM `{ref}` WHERE `run_id` = @run_id",
                       job_config=bigquery.QueryJobConfig(query_parameters=[
                           bigquery.ScalarQueryParameter("run_id", "STRING", replayed)]))
    df = job.to_dataframe(create_bqstorage_client=False)
    if df.empty:
        raise ValueError(f"no rows of run {replayed} in {ref}: nothing to replay")
    for col in df.columns:
        if str(df[col].dtype) == "dbdate":
            import pandas as pd

            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
    # FR-85: equal to the original run's data.snapshot.written fingerprint when the rows are the same rows.
    types = [{"name": f.name, "type": getattr(f, "field_type", "STRING")} for f in schema]
    fingerprint = rows_fingerprint(df, types, _snapshot_key(ml_config))
    emit_event("data.snapshot.replayed", ml_config, run_id, replayed_run=replayed, table=ref, rows=len(df),
               query_job=job.job_id, fingerprint=fingerprint)
    return df


def rows_fingerprint(df, fields: list[dict], key: list[str] | None = None) -> str:
    """A digest of rows that does not depend on their order or on how BigQuery hands types back (Pipewright 1.2.0,
    FR-85): each value is written by its schema type (DATE as YYYY-MM-DD, TIMESTAMP in UTC, numbers with 15
    significant digits, empty as empty), rows are sorted (by `key` first), and the lines are hashed. The snapshot's
    own columns (run_id, source_uri, ingested_at) are left out, so a run and its replay compare equal."""
    import hashlib

    import pandas as pd

    cols = [f for f in fields if f.get("name") in df.columns and f.get("name") not in SNAPSHOT_COLUMNS]
    out = pd.DataFrame(index=df.index)
    for f in cols:
        col, kind = df[f["name"]], str(f.get("type", "STRING")).upper()
        if kind == "DATE":
            text = pd.to_datetime(col, errors="coerce").dt.strftime("%Y-%m-%d")
        elif kind in ("TIMESTAMP", "DATETIME"):
            text = pd.to_datetime(col, errors="coerce", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        elif kind in ("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
            text = pd.to_numeric(col, errors="coerce").map(lambda v: "" if pd.isna(v) else format(float(v), ".15g"))
        else:
            text = col.map(lambda v: "" if v is None or (isinstance(v, float) and v != v) else str(v))
        out[f["name"]] = text.fillna("")
    order = [c for c in (key or []) if c in out.columns] + [c for c in out.columns if c not in (key or [])]
    lines = out.sort_values(order, kind="stable").astype(str).agg("\x1f".join, axis=1) if len(out) else []
    digest = hashlib.sha256()
    digest.update(("\x1f".join(order) + "\n").encode("utf-8"))
    for line in lines:
        digest.update((line + "\n").encode("utf-8"))
    return f"sha256:{digest.hexdigest()}"


def _snapshot_key(ml_config: dict) -> list[str]:
    return list(((ml_config.get("data") or {}).get("contract") or {}).get("unique") or [])


def write_snapshot(rows, ml_config: dict, project: str, dataset: str, run_id: str, source_uri: str) -> list[str]:
    """Writes the rows a run read to data.snapshot_table with run_id, source_uri and ingested_at, so
    `pipewright run --replay <run_id>` can read exactly these rows again. Returns where they went."""
    import pandas as pd

    table = (ml_config.get("data") or {}).get("snapshot_table")
    if not table:
        raise ValueError(f"{ml_config.get('usecase')} sets no data.snapshot_table")
    fields = ((ml_config.get("outputs") or {}).get("tables") or {}).get(table) or []
    missing = [c for c in SNAPSHOT_COLUMNS if c not in {f.get("name") for f in fields}]
    if missing:
        raise ValueError(f"data.snapshot_table {table}: its schema needs the columns {', '.join(missing)}")
    df = (rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))).copy()
    fingerprint = rows_fingerprint(df, fields, _snapshot_key(ml_config))     # FR-85: what a replay compares with
    df["run_id"], df["source_uri"], df["ingested_at"] = run_id, source_uri, datetime.now(timezone.utc)
    where = write_table(table, df, ml_config, project, dataset, run_id)
    emit_event("data.snapshot.written", ml_config, run_id, table=table, rows=len(df), source_uri=source_uri,
               locations=where, fingerprint=fingerprint)
    return where


# ----------------------------------------------------------------- features --
# The point-in-time join is plain BigQuery SQL, run as the runner with the environment's key: for every distinct
# (entity, cut-off) of the entity table, the newest row of each feature table dated at or before the cut-off.
# Joining back on (entity, cut-off) keeps every entity row, duplicates included, and is deterministic.

def _q(name: str) -> str:
    return f"`{name}`"


def features_query(entity_ref: str, entity_columns: list[str], features_from: dict, project: str) -> str:
    """The SQL for read_source with data.features_from. `entity_columns` are the entity table's columns: without
    the cut-off column (a table being scored), every row is cut off at the query time, so it gets the latest values."""
    entity = features_from["entity"]
    keys, tc = entity["columns"], entity["time_column"]
    missing = [c for c in keys if c not in entity_columns]
    if missing:
        raise ValueError(f"the entity table {entity_ref} has no column {', '.join(missing)} "
                         "(data.features_from.entity.columns)")
    cutoff = (lambda alias: f"CAST({alias}{_q(tc)} AS TIMESTAMP)") if tc in entity_columns else \
        (lambda _alias: "CURRENT_TIMESTAMP()")
    ctes = ["entity AS (SELECT * FROM " + _q(entity_ref) + ")",
            "entity_keys AS (SELECT DISTINCT " + ", ".join(_q(k) for k in keys) + f", {cutoff('')} AS __pw_cutoff "
            "FROM entity)"]
    select, joins = ["entity.*"], []
    for i, src in enumerate(features_from["sources"]):
        table = bq_table(src["table"], project)
        fkeys = src.get("entity_columns") or keys
        fts = src.get("time_column", "feature_timestamp")
        prefix = src.get("prefix", "")
        clash = [prefix + c for c in src["features"] if prefix + c in entity_columns]
        if clash:
            raise ValueError(f"the entity table {entity_ref} already has {', '.join(clash)}, which "
                             f"data.features_from.sources[{i}] adds; drop them from the entity table or set a prefix")
        on = " AND ".join(f"f.{_q(fk)} = k.{_q(k)}" for fk, k in zip(fkeys, keys, strict=True))
        columns = src.get("columns") or {}             # a registered feature's column, when it differs from its name
        values = ", ".join(f"f.{_q(columns.get(c, c))} AS {_q(prefix + c)}" for c in src["features"])
        ctes.append(
            f"f{i} AS (SELECT " + ", ".join(f"k.{_q(k)}" for k in keys) + f", k.__pw_cutoff, TRUE AS __pw_hit, "
            f"{values} FROM entity_keys AS k JOIN {_q(table)} AS f ON {on} "
            f"AND CAST(f.{_q(fts)} AS TIMESTAMP) <= k.__pw_cutoff "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY " + ", ".join(f"k.{_q(k)}" for k in keys)
            + f", k.__pw_cutoff ORDER BY f.{_q(fts)} DESC) = 1)")
        select += [f"f{i}.{_q(prefix + c)}" for c in src["features"]] + [f"f{i}.__pw_hit IS NOT NULL AS __pw_hit_{i}"]
        joins.append(f"LEFT JOIN f{i} ON " + " AND ".join(f"f{i}.{_q(k)} = entity.{_q(k)}" for k in keys)
                     + f" AND f{i}.__pw_cutoff = {cutoff('entity.')}")
    return ("WITH " + ",\n".join(ctes) + "\nSELECT " + ", ".join(select) + "\nFROM entity\n" + "\n".join(joins))


# Feature groups (Agent Platform Feature Store, Feature Registry): a source may name a registered group instead of a
# table. The registry is read at run time, so a group its owners point at a new table is followed; the group gives
# the table, the entity and time columns, and each registered feature's column.
_REGISTRY = "https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/featureGroups/{group}"


def group_source(src: dict, group: dict, features: list[dict]) -> dict:
    """A feature_group source as the table source it stands for, from the group and its registered features."""
    bq = group.get("bigQuery") or {}
    uri = (bq.get("bigQuerySource") or {}).get("inputUri")
    if not uri:
        raise ValueError(f"feature group {src['feature_group']} has no BigQuery source")
    registered = {f["name"].rsplit("/", 1)[-1]: f.get("versionColumnName") or f["name"].rsplit("/", 1)[-1]
                  for f in features}
    missing = [f for f in src["features"] if f not in registered]
    if missing:
        raise ValueError(f"feature group {src['feature_group']} has no feature {', '.join(missing)} "
                         f"(registered: {', '.join(sorted(registered)) or 'none'})")
    out = {"table": uri, "entity_columns": list(bq.get("entityIdColumns") or ["entity_id"]),
           "time_column": (bq.get("timeSeries") or {}).get("timestampColumn") or "feature_timestamp",
           "features": list(src["features"]), "columns": {f: registered[f] for f in src["features"]},
           "feature_group": group.get("name", src["feature_group"])}
    if src.get("prefix"):
        out["prefix"] = src["prefix"]
    return out


def _registry_get(session, url: str, what: str) -> dict:
    r = session.get(url, timeout=60)
    if r.status_code == 404:
        raise ValueError(f"{what} not found")
    if r.status_code != 200:
        raise ValueError(f"{what}: {r.status_code} {r.text[:300]}")
    return r.json()


def resolve_feature_groups(features_from: dict, project: str, region: str, session=None) -> dict:
    """features_from with every feature_group source read from the Feature Registry and turned into a table source."""
    if not any("feature_group" in s for s in features_from["sources"]):
        return features_from
    if session is None:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        session = AuthorizedSession(creds)
    sources = []
    for src in features_from["sources"]:
        if "feature_group" not in src:
            sources.append(src)
            continue
        url = _REGISTRY.format(region=src.get("region") or region, project=src.get("project") or project,
                               group=src["feature_group"])
        what = f"feature group {src['feature_group']} (project {src.get('project') or project})"
        group = _registry_get(session, url, what)
        features, token = [], None
        while True:
            page = _registry_get(session, f"{url}/features?pageSize=100" + (f"&pageToken={token}" if token else ""),
                                 f"features of {what}")
            features += page.get("features", [])
            token = page.get("nextPageToken")
            if not token:
                break
        sources.append(group_source(src, group, features))
    return {**features_from, "sources": sources}


def _check_feature_datasets(features_from: dict, project: str, ml_config: dict) -> None:
    """The platform's feature datasets for the tenant (ml_config.platform.feature_datasets), when it lists them."""
    allowed = (ml_config.get("platform") or {}).get("feature_datasets")
    if allowed is None:
        return
    for src in features_from["sources"]:
        dataset = bq_table(src["table"], project).rsplit(".", 1)[0]
        if dataset not in allowed:
            via = f" (feature group {src['feature_group'].rsplit('/', 1)[-1]})" if src.get("feature_group") else ""
            raise ValueError(f"{dataset}{via} is not a feature dataset of this tenant ({', '.join(allowed) or 'none'})")


def _read_with_features(client, entity_ref: str, project: str, features_from: dict, ml_config: dict,
                        run_id: str | None, recorded_as: str | None = None):
    """recorded_as: the entity table the run named, when entity_ref is the label window's result over it."""
    features_from = resolve_feature_groups(features_from, project, (ml_config.get("platform") or {}).get("region"))
    _check_feature_datasets(features_from, project, ml_config)
    entity_columns = [f.name for f in client.get_table(entity_ref).schema]
    job = client.query(features_query(entity_ref, entity_columns, features_from, project))
    df = job.to_dataframe(create_bqstorage_client=False)
    tc = features_from["entity"]["time_column"]
    sources = []
    for i, src in enumerate(features_from["sources"]):
        hit = df.pop(f"__pw_hit_{i}")
        prefix = src.get("prefix", "")
        sources.append({"table": bq_table(src["table"], project),
                        "time_column": src.get("time_column", "feature_timestamp"),
                        "features": [prefix + c for c in src["features"]],
                        **({"feature_group": src["feature_group"], "columns": src["columns"]}
                           if src.get("feature_group") else {}),
                        "rows_matched": int(hit.sum()), "rows": len(df)})
    record = {"entity_table": recorded_as or entity_ref,
              **({"labelled": True} if recorded_as and recorded_as != entity_ref else {}),
              "cutoff": tc if tc in entity_columns else "query time",
              "query_job": job.job_id, "read_at": (job.started or datetime.now(timezone.utc)).isoformat(),
              "sources": sources}
    for s in sources:
        if s["rows"] and s["rows_matched"] < s["rows"]:
            log(f"{s['rows'] - s['rows_matched']} of {s['rows']} entity rows have no value in {s['table']} at or "
                "before their cut-off; their features from it are empty", ml_config, "WARNING", run_id)
    emit_event("data.features.resolved", ml_config, run_id, **record)
    return df


# ----------------------------------------------------------------- fairness --
# evaluation.fairness (FR-53): per attribute, each group's selection rate, true and false positive rates at the
# decision threshold, and the largest gap between groups. Groups with fewer than min_rows test rows are listed
# without rates and take no part in the gaps.


def fairness_measures(df, label: str, score: str, threshold: float, ml_config: dict) -> dict:
    """{attribute: {"groups": {value: {rows, selection_rate, tpr, fpr}}, "<measure>": gap or None}}; {} without
    evaluation.fairness."""
    fair = (ml_config.get("evaluation") or {}).get("fairness")
    if not fair:
        return {}
    min_rows = int(fair.get("min_rows") or 30)
    out = {}
    for attr in fair["attributes"]:
        if attr not in df.columns:
            raise ValueError(f"evaluation.fairness: the test set has no column {attr!r} (keep it in the data, "
                             "it need not be a feature)")
        groups, rates = {}, []
        for value, g in df.groupby(attr, dropna=False):
            entry = {"rows": int(len(g))}
            if len(g) >= min_rows:
                y = g[label].astype(int)
                hat = (g[score] >= threshold).astype(int)
                pos, neg = hat[y == 1], hat[y == 0]
                entry.update(selection_rate=float(hat.mean()), tpr=float(pos.mean()) if len(pos) else None,
                             fpr=float(neg.mean()) if len(neg) else None)
                rates.append(entry)
            key = "(missing)" if value != value else (int(value) if isinstance(value, float) and value.is_integer()
                                                       else value)
            groups[str(key)] = entry
        out[attr] = {"groups": groups, **fairness_gaps(rates, fair["metrics"])}
    return out


def fairness_gaps(rates: list[dict], metrics: list[str]) -> dict:
    """The measures from groups' rates ({selection_rate, tpr, fpr}); None when fewer than two groups have one."""
    def gap(key):
        values = [r[key] for r in rates if r.get(key) is not None]
        return max(values) - min(values) if len(values) >= 2 else None

    dp, tpr, fpr = gap("selection_rate"), gap("tpr"), gap("fpr")
    measures = {"demographic_parity_difference": dp, "equal_opportunity_difference": tpr,
                "equalized_odds_difference": None if tpr is None or fpr is None else max(tpr, fpr)}
    return {m: measures[m] for m in metrics}


def fairness_problems(measured: dict, ml_config: dict) -> list[str]:
    """The fairness gates a run's measures fail (the same check `pipewright model approve` makes)."""
    fair = (ml_config.get("evaluation") or {}).get("fairness") or {}
    out = []
    for gate in fair.get("gates") or []:
        m = gate["metric"]
        for attr in fair.get("attributes") or []:
            value = (measured.get(attr) or {}).get(m)
            if value is None:
                out.append(f"fairness gate {m}: not measured for {attr} (fewer than two groups with "
                           f"{fair.get('min_rows', 30)} test rows)")
            elif value > gate["max"]:
                out.append(f"fairness gate {m}: {attr} is {value:.4f}, above the maximum {gate['max']}")
    return out


# ------------------------------------------------------------------ lineage --
# A model version's lineage (FR-55): lineage.json next to its evaluation record holds what only the run knows: the
# release it ran, a digest of its ml_config (and its SQL files), the exact rows it trained on (its snapshot, or
# BigQuery ML's rows table), the sources those came from and its evaluation. `pipewright lineage` adds the release
# contract, the snapshot's row count and read time and the registry's approvals.

LINEAGE_SCHEMA = "pipewright.lineage/1"


def lineage_record(ml_config: dict, run_id: str, source_uri: str, model: dict, evaluation: dict,
                   evaluation_uri: str | None = None, rows_table: str | None = None) -> dict:
    """model: {"name", "version"} as registered. No data values: names of tables, columns and windows only."""
    import hashlib

    data = ml_config.get("data") or {}
    project = (ml_config.get("platform") or {}).get("project")
    replay_of = source_uri.removeprefix(SNAPSHOT_SCHEME) if source_uri.startswith(SNAPSHOT_SCHEME) else None
    rows: dict = {}
    if data.get("snapshot_table"):
        rows["snapshot"] = {"table": snapshot_table_ref(ml_config, project), "run_id": run_id,
                            **({"replay_of": replay_of} if replay_of else {})}
    if rows_table:
        rows["table"] = rows_table
    sources: dict = {}
    if source_uri.startswith("bq://"):
        sources["entity_table"] = bq_table(source_uri, project)
    elif not replay_of:
        sources["uri"] = source_uri
    features_from = data.get("features_from") or {}
    if features_from:
        sources["features"] = {
            "time_column": (features_from.get("entity") or {}).get("time_column"),
            "sources": [{k: v for k, v in src.items() if k in ("table", "feature_group", "features", "time_column",
                                                                 "prefix")}
                        for src in features_from.get("sources") or []]}
    if data.get("label_source"):
        sources["labels"] = dict(data["label_source"])
    primary = (ml_config.get("evaluation") or {}).get("primary_metric")
    champion = evaluation.get("champion") or {}
    bqml = ml_config.get("bqml") or {}
    return {
        "schema": LINEAGE_SCHEMA, "usecase": ml_config.get("usecase"),
        "tenant": (ml_config.get("platform") or {}).get("tenant"), "project": project, "run_id": run_id,
        "release": os.environ.get("PIPEWRIGHT_RELEASE"), "recorded_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "config": {"ml_config_sha256": hashlib.sha256(json.dumps(ml_config, sort_keys=True, default=str)
                                                      .encode("utf-8")).hexdigest(),
                   **({"sql": {k: bqml[k]["sha256"] for k in ("transform", "prepare") if bqml.get(k)}}
                      if bqml else {})},
        "data": {"source_uri": source_uri, **({"replay_of": replay_of} if replay_of else {}), "rows": rows,
                 "sources": sources},
        "evaluation": {"record": evaluation_uri, "decision": evaluation.get("decision"), "primary_metric": primary,
                       "value": (evaluation.get("metrics") or {}).get(primary),
                       **({"champion": {"version": champion.get("_version"), "value": champion.get(primary)}}
                          if champion else {}),
                       "reasons": evaluation.get("reasons") or []},
    }


def write_lineage(ml_config: dict, run_id: str, directory_uri: str, record: dict) -> str:
    """Writes lineage.json into directory_uri (gs://, the folder of the evaluation record) and emits
    `model.lineage.recorded`. Returns where, or "" (logged as an error) when it could not be written."""
    from google.cloud import storage

    bucket, _, prefix = directory_uri.removeprefix("gs://").partition("/")
    uri = f"gs://{bucket}/{prefix.rstrip('/')}/lineage.json"
    try:
        blob = storage.Client(project=record.get("project")).bucket(bucket).blob(f"{prefix.rstrip('/')}/lineage.json")
        blob.upload_from_string(json.dumps(record, indent=2, default=str), content_type="application/json")
    except Exception as e:  # noqa: BLE001 - the version is registered already: say so, do not fail the run
        log(f"lineage.json not written to {uri}: {type(e).__name__}: {value_free(str(e))[:300]}", ml_config,
            "ERROR", run_id)
        return ""
    emit_event("model.lineage.recorded", ml_config, run_id, lineage=uri, model=record.get("model"))
    return uri


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


@_value_free_errors
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


# --------------------------------------------------------------------- ray --
# A cluster per Ray step of a run. The launcher creates it (it can act as the runner identity; the runner cannot
# act as itself) from the live release's settings, so a step only names itself. Names: kept in step with
# pipewright.ray_clusters.settings.cluster_id by a test.

_RAY_NAME = re.compile(r"[^a-z0-9-]+")


def ray_cluster_id(usecase: str, run_id: str, step: str) -> str:
    import hashlib

    digest = hashlib.sha256(f"{usecase}|{run_id}|{step}".encode()).hexdigest()[:12]
    stem = _RAY_NAME.sub("-", f"{usecase}-{step}".lower()).strip("-")[:44].rstrip("-")
    return f"pw-{stem}-{digest}"


def ray_kube_name(usecase: str, run_id: str, step: str) -> str:
    """The RayCluster name on GKE (orchestrator kfp)."""
    import hashlib

    digest = hashlib.sha256(f"{usecase}|{run_id}|{step}".encode()).hexdigest()[:12]
    stem = _RAY_NAME.sub("-", f"{usecase}-{step}".lower()).strip("-")[:24].rstrip("-")
    return f"pw-{stem}-{digest}"


def _session():
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


_METADATA = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"


def _metadata_id_token(audience: str, headers: dict, tried: list) -> str | None:
    """One pass over the metadata server's accounts (named ones first, then `default`); None if none issues one."""
    import requests

    listed = requests.get(f"{_METADATA}/", headers=headers, timeout=10)
    names = [n.strip("/") for n in listed.text.split()] if listed.status_code == 200 else []
    if listed.status_code != 200:
        tried.append(f"list {listed.status_code}")
    for name in [n for n in names if n != "default"] + (["default"] if "default" in names else []):
        r = requests.get(f"{_METADATA}/{name}/identity", params={"audience": audience, "format": "full"},
                         headers=headers, timeout=10)
        if r.status_code == 200 and r.text.count(".") == 2:
            return r.text.strip()
        tried.append(f"{name}: {r.status_code} {r.text.strip()[:80]}")
    return None


def _iam_id_token(audience: str, tried: list) -> str | None:
    """An ID token the IAM Credentials API signs for this identity, with its access token (which Agent Platform
    workers always have). Needs iam.serviceAccounts.getOpenIdToken on itself."""
    import requests

    headers = {"Metadata-Flavor": "Google"}
    email = requests.get(f"{_METADATA}/default/email", headers=headers, timeout=10)
    if email.status_code != 200:
        tried.append(f"email {email.status_code}")
        return None
    session = _session()
    r = session.post(f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{email.text.strip()}"
                     ":generateIdToken", json={"audience": audience, "includeEmail": True}, timeout=30)
    if r.status_code == 200:
        return r.json()["token"]
    tried.append(f"iam generateIdToken {r.status_code} {r.text.strip()[:120]}")
    return None


def id_token(audience: str, attempts: int = 3, wait: float = 10.0) -> str:
    """An ID token for this identity, to call the launcher.

    On an Agent Platform worker: the metadata server's identity endpoint, which is not reliable there (it refuses
    tokens on some workers, "please provide a user-specified service account"), then the IAM Credentials API, signed
    for this identity with its access token. Elsewhere (workstations, Cloud Run): the usual call."""
    import time

    import requests

    headers, tried = {"Metadata-Flavor": "Google"}, []
    try:
        requests.get(f"{_METADATA}/", headers=headers, timeout=5)
    except requests.RequestException:
        import google.auth.transport.requests
        import google.oauth2.id_token

        return google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), audience)
    for attempt in range(attempts):
        token = _metadata_id_token(audience, headers, tried) or _iam_id_token(audience, tried)
        if token:
            return token
        if attempt < attempts - 1:
            time.sleep(wait)
    raise RuntimeError(f"no ID token for {audience} after {attempts} tries (the runner needs "
                       "roles/iam.serviceAccountOpenIdTokenCreator on itself): " + "; ".join(tried[-4:]))


def _ray_request(ml_config: dict, action: str, step: str, run_id: str) -> None:
    """Asks the launcher to create or delete the step's cluster: over Pub/Sub inside a private perimeter (the
    message carries this identity's ID token), otherwise by calling the launcher."""
    platform = ml_config.get("platform") or {}
    url = (platform.get("launcher_url") or "").rstrip("/")
    if not url:
        raise RuntimeError("Ray clusters need the launcher, and the environment manifest has no launcher.url")
    token = id_token(url)
    body = {"action": action, "usecase": ml_config["usecase"], "run_id": run_id, "step": step}
    topic = platform.get("run_requests_topic")
    if topic:
        message = {"data": base64.b64encode(json.dumps({**body, "kind": "ray.cluster", "id_token": token}).encode())
                   .decode(), "attributes": {"usecase": ml_config["usecase"], "kind": "ray.cluster"}}
        region = platform.get("region")
        host = f"{region}-pubsub.googleapis.com" if region else "pubsub.googleapis.com"
        r = _session().post(f"https://{host}/v1/{topic}:publish", json={"messages": [message]}, timeout=60)
    else:
        import requests

        r = requests.post(f"{url}/ray/clusters", json=body, timeout=120,
                          headers={"Authorization": f"Bearer {token}"})
    if r.status_code != 200:
        raise RuntimeError(f"Ray cluster {action} for {step} refused: {r.status_code} {r.text[:300]}")


def _ray_state(name: str, region: str) -> dict:
    r = _session().get(f"https://{region}-aiplatform.googleapis.com/v1/{name}", timeout=30)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


class _RayCluster:
    def __init__(self, ml_config: dict, step: str, run_id: str):
        platform = ml_config.get("platform") or {}
        steps = (platform.get("ray") or {}).get("steps") or {}
        if step not in steps:
            raise ValueError(f"step {step!r} has no Ray cluster: set steps.{step}.ray in the use case "
                             f"(Ray steps: {', '.join(steps) or 'none'})")
        self.ml_config, self.step, self.run_id = ml_config, step, run_id
        self.region = platform["region"]
        self.minutes = int(steps[step]["max_minutes"])
        self.kube = platform.get("orchestrator") == "kfp"
        standing = (platform.get("ray") or {}).get("standing") or {}
        self.standing = (platform.get("ray") or {}).get("mode") == "standing"
        if self.standing:
            # The tenant's standing cluster (private-gke): kept by the platform, reached inside the cluster.
            self.name = standing["address"].rstrip("/")
            self.cluster, self.image = standing.get("cluster"), standing.get("image")
        elif self.kube:
            # KubeRay on the environment's GKE cluster: the step reaches the head's dashboard inside the cluster.
            name = ray_kube_name(ml_config["usecase"], run_id, step)
            self.name = f"http://{name}-head-svc.{platform.get('kubernetes_namespace') or 'kubeflow'}.svc:8265"
        else:
            self.name = (f"projects/{platform['project']}/locations/{self.region}/persistentResources/"
                         f"{ray_cluster_id(ml_config['usecase'], run_id, step)}")

    def _state(self) -> dict:
        """{"state": RUNNING | PENDING | ERROR, ...}, or {} when there is no cluster yet."""
        if not self.kube:
            return _ray_state(self.name, self.region)
        import socket

        import requests

        try:
            r = requests.get(f"{self.name}/api/version", timeout=10)
        except requests.RequestException:
            # The head's Service exists (its name resolves) before the head is ready: the cluster is starting
            # (a new Ray node and its image can take minutes); no name yet means no cluster yet.
            try:
                socket.gethostbyname(self.name.split("//", 1)[-1].rsplit(":", 1)[0])
                return {"state": "PENDING"}
            except OSError:
                return {}
        return {"state": "RUNNING"} if r.status_code == 200 else {"state": "PENDING"}

    def _label(self) -> str:
        return self.name.split("//", 1)[-1].split("-head-svc", 1)[0] if self.kube else self.name.rsplit("/", 1)[-1]

    def __enter__(self) -> str:
        import time

        if self.standing:
            deadline = time.time() + 600
            while self._state().get("state") != "RUNNING":
                if time.time() > deadline:
                    raise RuntimeError(f"the tenant's standing Ray cluster {self.cluster} does not answer at "
                                       f"{self.name}: ask the platform team")
                time.sleep(15)
            namespace = (self.ml_config.get("platform") or {}).get("kubernetes_namespace") or "kubeflow"
            emit_event("ray.cluster.used", self.ml_config, self.run_id, step=self.step, mode="standing",
                       cluster=f"kubernetes:{namespace}/rayclusters/{self.cluster}", service_account=_identity(),
                       image=self.image)
            log(f"using the standing Ray cluster {self.cluster} ({self.step})", self.ml_config, run_id=self.run_id)
            return self.name

        log(f"asking for Ray cluster {self._label()} ({self.step})", self.ml_config,
            run_id=self.run_id, ray_cluster=self.name)
        _ray_request(self.ml_config, "create", self.step, self.run_id)
        deadline, state = time.time() + 60 * min(self.minutes, 45), "PENDING"
        asked, resends = time.time(), 0
        while time.time() < deadline:
            current = self._state()
            state = current.get("state", "PENDING")
            if state == "RUNNING":
                log(f"Ray cluster {self._label()} is running", self.ml_config, run_id=self.run_id)
                return self.name
            if state == "ERROR":
                break
            if not current and time.time() - asked > 180:
                # Still no cluster: the request may have been dropped (a dead-lettered message, a refusal). The ID is
                # fixed for the run, so asking again is safe; after three tries, stop and say where to look.
                if resends == 3:
                    raise RuntimeError(f"the launcher has not created Ray cluster {self.name} after "
                                       f"{resends + 1} requests: see the launcher's logs ('ray create')")
                _ray_request(self.ml_config, "create", self.step, self.run_id)
                asked, resends = time.time(), resends + 1
                log(f"asked again for Ray cluster {self._label()} ({resends})", self.ml_config,
                    "WARNING", self.run_id)
            time.sleep(20)
        self.__exit__(None, None, None)
        error = {} if self.kube else _ray_state(self.name, self.region).get("error") or {}
        raise RuntimeError(f"Ray cluster {self.name} did not start (state {state}): {error.get('message', '')}")

    def __exit__(self, *exc) -> bool:
        if self.standing:
            return False            # the platform keeps it (it scales its workers down when idle)
        try:
            _ray_request(self.ml_config, "delete", self.step, self.run_id)
            log(f"Ray cluster {self._label()} deleted", self.ml_config, run_id=self.run_id)
        except Exception as e:  # noqa: BLE001 - the launcher's cleanup removes it once the run ends
            log(f"Ray cluster delete failed (cleanup removes it after the run): {e}", self.ml_config, "WARNING",
                self.run_id)
        return False


def _identity() -> str | None:
    """This step's service account email (the metadata server), for the evidence."""
    import requests

    try:
        r = requests.get(f"{_METADATA}/default/email", headers={"Metadata-Flavor": "Google"}, timeout=5)
        return r.text.strip() if r.status_code == 200 else None
    except requests.RequestException:
        return None


def ray_cluster(ml_config: dict, step: str, run_id: str) -> _RayCluster:
    """with ray_cluster(ml_config, "tune-on-ray", run_id) as cluster: ray_job(ml_config, cluster, ...)

    The cluster's size is the release's steps.<step>.ray; it is deleted when the block ends, even on errors."""
    return _RayCluster(ml_config, step, run_id)


def ray_job(ml_config: dict, cluster: str, entrypoint: str, run_id: str | None = None, runtime_env: dict | None = None,
            timeout_minutes: int = 60) -> str:
    """Runs `entrypoint` on the cluster's head node (the use case's modules are importable there), streams its
    status, and returns its logs. Raises when the job fails or runs out of time."""
    import time

    from ray.job_submission import JobStatus, JobSubmissionClient

    # On GKE `cluster` is the head's dashboard address; on Agent Platform its resource name.
    client = JobSubmissionClient(cluster if cluster.startswith("http") else f"vertex_ray://{cluster}")
    runtime_env = dict(runtime_env or {})
    if ((ml_config.get("platform") or {}).get("ray") or {}).get("mode") == "standing":
        # A standing cluster runs the tenant's image, not this release's: ship the release's component code.
        runtime_env.setdefault("working_dir", os.path.dirname(os.path.abspath(__file__)))
        runtime_env.setdefault("excludes", ["__pycache__", "*.pt", "*.mar", "*.parquet"])
    job = client.submit_job(entrypoint=entrypoint, runtime_env=runtime_env)
    log(f"Ray job {job}: {entrypoint}", ml_config, run_id=run_id, ray_job=job)
    deadline, status = time.time() + 60 * timeout_minutes, None
    while time.time() < deadline:
        status = client.get_job_status(job)
        if status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED):
            break
        time.sleep(15)
    logs = client.get_job_logs(job)
    print(logs[-20000:], flush=True)
    if status != JobStatus.SUCCEEDED:
        if status not in (JobStatus.FAILED, JobStatus.STOPPED):
            client.stop_job(job)
        raise RuntimeError(f"Ray job {job} ended {status}: {logs[-1000:]}")
    return logs


# -- the registry (Pipewright FR-124) ----------------------------------------------------------------------------------

# ------------------------------------------------------------------ online window (1.4.3, FR-98) --

def parse_instance(instance) -> tuple[list | dict | None, str | None]:
    """(the row, the prediction ID) of one logged instance. An instance may be a plain row, {"instances": [row],
    "_pw_prediction_id": ...} (the churn handler's form before 1.4.3), or {"_pw_prediction_id", "_pw_unit", "row"}
    (the 1.4.3 caller contract, spike 3)."""
    if isinstance(instance, str):
        try:
            instance = json.loads(instance)
        except ValueError:
            return None, None
    if isinstance(instance, dict):
        pid = instance.get("_pw_prediction_id")
        if "row" in instance:
            return instance["row"], pid
        if instance.get("instances"):
            return instance["instances"][0], pid
        return {k: v for k, v in instance.items() if not k.startswith("_pw_")}, pid
    return instance, None


def parse_prediction(answer):
    if isinstance(answer, str):
        try:
            answer = json.loads(answer)
        except ValueError:
            return None
    if isinstance(answer, list):
        answer = answer[0] if answer else None
    if isinstance(answer, dict):
        for key in ("score", "probability", "prediction", "value"):
            if key in answer:
                return answer[key]
        return None
    return answer


def _champion_deployed_model(ml_config: dict, version: str) -> str | None:
    """The deployed model serving the version on the use case's endpoint (Agent Platform)."""
    platform = ml_config.get("platform") or {}
    name = ml_config.get("endpoint_name") or f"{ml_config['usecase']}-endpoint"
    api = f"https://{platform['region']}-aiplatform.googleapis.com/v1/projects/{platform['project']}/locations/" \
          f"{platform['region']}"
    r = _session().get(f"{api}/endpoints", params={"filter": f'display_name="{name}"'}, timeout=60)
    for ep in (r.json().get("endpoints") or []) if r.status_code == 200 else []:
        for dm in ep.get("deployedModels") or []:
            if str(dm.get("modelVersionId")) == str(version):
                return dm["id"]
    return None


def online_window(ml_config: dict, project: str, source_uri: str, features: list[str], model_version: str,
                  client=None, deployed_model: str | None = None, isvc: str | None = None) -> tuple:
    """What the deployed version received and answered in the last monitoring.online.window_hours, as rows of the
    model's features plus `prediction` (and `_pw_prediction_id` when the caller sent one), de-duplicated on the
    prediction ID, at most model.serving.capture.max_per_day a day. Returns (rows, summary); summary says
    insufficient_data when fewer than min_samples rows are usable: never "no drift" (spike 5).

    Agent Platform's log (one row per request, request_payload and response_payload arrays in order) is unnested by
    offset; only the champion's deployed model is read. KServe's records (the log sink's table) pair each request
    and answer by their CloudEvent ID; only the use case's InferenceService is read."""
    from google.cloud import bigquery

    online = (ml_config.get("monitoring") or {}).get("online") or {}
    hours, min_samples = float(online.get("window_hours", 24)), int(online.get("min_samples", 500))
    cap = int(((ml_config.get("serving_capture") or {}).get("max_per_day")) or 100000)
    table = bq_table(source_uri, project)
    client = client or bigquery.Client(project=project)
    params = [bigquery.ScalarQueryParameter("minutes", "INT64", int(hours * 60)),
              bigquery.ScalarQueryParameter("cap", "INT64", int(cap * max(hours / 24, 1)))]
    columns = {f.name for f in client.get_table(table).schema}
    if "request_payload" in columns:
        deployed_model = deployed_model or _champion_deployed_model(ml_config, model_version)
        params.append(bigquery.ScalarQueryParameter("dm", "STRING", deployed_model or ""))
        sql = f"""
            SELECT req AS instance, response_payload[SAFE_OFFSET(i)] AS answer
            FROM `{table}`, UNNEST(request_payload) AS req WITH OFFSET AS i
            WHERE logging_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
              AND (@dm = '' OR deployed_model_id = @dm)
            LIMIT @cap"""
        source = "agent_platform"
    else:
        params.append(bigquery.ScalarQueryParameter("isvc", "STRING", isvc or ""))
        rec = "jsonPayload.pipewright_online_log"
        sql = f"""
            WITH r AS (SELECT {rec}.ce_id AS id, {rec}.ce_type AS t, {rec}.body AS body
                       FROM `{table}`
                       WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
                         AND (@isvc = '' OR {rec}.isvc = @isvc))
            SELECT q.body AS request, a.body AS response
            FROM r q JOIN r a ON q.id = a.id
            WHERE ENDS_WITH(q.t, 'request') AND ENDS_WITH(a.t, 'response')
            LIMIT @cap"""
        source = "kserve"
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params,
                                                               labels=labels(ml_config)))
    found = [dict(r.items()) for r in job.result()]
    if source == "kserve":                 # one record per request: its instances and predictions, in order
        pairs = []
        for r in found:
            try:
                q, a = json.loads(r["request"]), json.loads(r["response"])
            except (TypeError, ValueError):
                continue
            for inst, ans in zip((q or {}).get("instances") or [], (a or {}).get("predictions") or [], strict=False):
                pairs.append({"instance": inst, "answer": ans})
        found = pairs
    rows, seen, skipped = [], set(), 0
    for r in found:
        row, pid = parse_instance(r["instance"])
        pred = parse_prediction(r["answer"])
        if isinstance(row, dict):
            row = [row.get(f) for f in features]
        if row is None or pred is None or len(row) != len(features):
            skipped += 1                   # an answer that was an error, or a row of another shape
            continue
        if pid and pid in seen:
            continue                       # a retried call logged twice
        if pid:
            seen.add(pid)
        try:
            rows.append({**dict(zip(features, [None if v is None else float(v) for v in row], strict=True)),
                         "prediction": float(pred), **({"_pw_prediction_id": pid} if pid else {})})
        except (TypeError, ValueError):
            skipped += 1
    summary = {"source": source, "table": table, "window_hours": hours, "records": len(found), "rows": len(rows),
               "skipped": skipped, "min_samples": min_samples, "insufficient_data": len(rows) < min_samples,
               "deployed_model": deployed_model, "isvc": isvc, "model_version": model_version}
    return rows, summary


# ------------------------------------------------------------------ attribution (1.4.3, FR-98 stage 4) --

def permutation_shapley(predict, rows, background, permutations: int = 2, seed: int = 42):
    """Shapley values of each row's prediction by sampled feature permutations (each with its reverse, as SHAP's
    permutation explainer does), against background rows. Model-agnostic: predict(array of rows) -> scores. numpy
    only, so the component image needs no SHAP (about 165 MB). Returns an array (rows, features)."""
    import numpy as np

    x, bg = np.asarray(rows, dtype="float64"), np.asarray(background, dtype="float64")
    n, f = x.shape
    rng = np.random.default_rng(seed)
    out = np.zeros((n, f))
    orders = []
    for _ in range(max(1, permutations)):
        o = rng.permutation(f)
        orders += [o, o[::-1]]
    for i in range(n):
        batch, marks = [], []
        for order in orders:
            z = bg.copy()
            batch.append(z.copy())
            for j in order:
                z[:, j] = x[i, j]
                batch.append(z.copy())
            marks.append(order)
        preds = np.asarray(predict(np.concatenate(batch).astype("float32")), dtype="float64").reshape(-1)
        step = len(bg)
        for k, order in enumerate(marks):
            chunk = preds[k * (f + 1) * step:(k + 1) * (f + 1) * step].reshape(f + 1, step).mean(axis=1)
            for pos, j in enumerate(order):
                out[i, j] += chunk[pos + 1] - chunk[pos]
        out[i] /= len(marks)
    return out


def attribution_shares(predict, rows, background, features: list[str], seed: int = 42) -> dict:
    """Each feature's share of the mean absolute attribution on rows (summing to 1)."""
    import numpy as np

    values = np.abs(permutation_shapley(predict, rows, background, seed=seed)).mean(axis=0)
    total = float(values.sum()) or 1.0
    return {f: round(float(v) / total, 4) for f, v in zip(features, values, strict=True)}


def attribution_drift(baseline: dict, now: dict, top: int = 3) -> tuple[float, list[dict]]:
    """(total variation distance between the share vectors, the features that moved most)."""
    keys = sorted(set(baseline) | set(now))
    tvd = 0.5 * sum(abs(now.get(k, 0.0) - baseline.get(k, 0.0)) for k in keys)
    moved = sorted(keys, key=lambda k: -abs(now.get(k, 0.0) - baseline.get(k, 0.0)))[:top]
    return round(tvd, 4), [{"feature": k, "training": baseline.get(k, 0.0), "now": now.get(k, 0.0)} for k in moved]


def registration_by_launcher(ml_config: dict) -> bool:
    """policies.registration: launcher (Pipewright 1.4.3, FR-100): this runner cannot write the registry."""
    return (ml_config.get("platform") or {}).get("registration") == "launcher"


def register_version(ml_config: dict, run_id: str, request: dict, wait_minutes: int = 20) -> dict:
    """Asks the launcher to register this run's model version (FR-100) and waits for its answer:
    {"status": "registered", "resource", "version", "default", ...}. A refusal raises, saying why.

    request: display_name, artifact_uri, serving_image, env (dict or None), labels, version_description,
    evaluation {uri, sha256} (the record next to the model files), metrics, files. The launcher checks the run, the
    files and the gates itself and decides the default from the release's policy. Over Pub/Sub inside a private
    perimeter (the message carries this identity's ID token), otherwise by calling the launcher; the answer is also
    written to gs://<artifacts>/runs/registration/<tenant>/run-<run_id>.json, which the step reads."""
    import time

    platform = ml_config.get("platform") or {}
    url = (platform.get("launcher_url") or "").rstrip("/")
    if not url:
        raise RuntimeError("registration through the launcher needs the launcher, and the manifest has no "
                           "launcher.url")
    token = id_token(url)
    body = {**request, "usecase": ml_config["usecase"], "run_id": run_id}
    topic = platform.get("run_requests_topic")
    if not topic:
        import requests

        r = requests.post(f"{url}/models/register", data=json.dumps(body, default=str), timeout=1200,
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        if r.status_code == 200:
            return r.json()
        if r.status_code in (400, 403):
            raise RuntimeError(f"the launcher refused to register this version: {r.json().get('reason', r.text)}")
        log(f"registration call returned {r.status_code}: {r.text[:300]}; waiting for the answer", ml_config,
            "WARNING", run_id)
    else:
        message = {"data": base64.b64encode(json.dumps({**body, "kind": "model.registration", "id_token": token},
                                                       default=str).encode()).decode(),
                   "attributes": {"usecase": ml_config["usecase"], "kind": "model.registration"}}
        region = platform.get("region")
        host = f"{region}-pubsub.googleapis.com" if region else "pubsub.googleapis.com"
        r = _session().post(f"https://{host}/v1/{topic}:publish", json={"messages": [message]}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"registration request not sent: {r.status_code} {r.text[:300]}")
    from google.cloud import storage

    alias = re.sub(r"[^a-z0-9-]", "-", f"run-{run_id}".lower())[:127]
    name = f"runs/registration/{platform.get('tenant')}/{alias}.json"
    blob = storage.Client(project=platform.get("project")).bucket(platform["artifacts_bucket"]).blob(name)
    deadline = time.time() + 60 * wait_minutes
    while time.time() < deadline:
        if blob.exists():
            answer = json.loads(blob.download_as_text())
            if answer.get("status") == "registered":
                return answer
            raise RuntimeError(f"the launcher refused to register this version: {answer.get('reason')}")
        time.sleep(15)
    raise RuntimeError(f"no answer from the launcher after {wait_minutes} minutes (gs://"
                       f"{platform['artifacts_bucket']}/{name}): see the launcher's logs ('registration')")


def registry_model_id(display_name: str) -> str:
    """The model ID a first version is registered under: the display name as an ID (a letter first, [a-z0-9-], at
    most 63). Fixed, so two runs registering a first version at once cannot make two models: the second one's upload
    is refused with a conflict and it registers as a version of the first one's model. pipewright.models.registry
    keeps the same rule (a test keeps them equal)."""
    rid = re.sub(r"[^a-z0-9-]", "-", display_name.lower()).strip("-")
    rid = rid if rid[:1].isalpha() else f"m-{rid}"
    return rid[:63].rstrip("-")


def registry_models(aiplatform, display_name: str) -> list:
    """The models with the display name, oldest first: the oldest is the use case's model. A second one is what a
    race between two first versions left before Pipewright FR-124."""
    models = aiplatform.Model.list(filter=f'display_name="{display_name}"')
    return sorted(models, key=lambda m: (str(getattr(m, "create_time", "") or ""), m.resource_name))


# -- the approved bytes (Pipewright 1.4.1, FR-101) -------------------------------------------------------------------

def folder_files(uri: str, project: str) -> dict:
    """Every file under a model folder with its generation and SHA-256, and one digest over them all: the same record
    an approval keeps (integrity.model_files), taken here by the register step at the moment Agent Platform copies
    the folder. Names and digests only, never contents."""
    import hashlib

    from google.cloud import storage

    bucket_name, _, prefix = uri.removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    client = storage.Client(project=project)
    files = []
    for blob in sorted(client.list_blobs(bucket_name, prefix=prefix), key=lambda b: b.name):
        if blob.name == prefix:
            continue
        h = hashlib.sha256()
        with client.bucket(bucket_name).blob(blob.name, generation=blob.generation).open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        files.append({"file": blob.name[len(prefix):], "generation": int(blob.generation), "size": int(blob.size or 0),
                      "sha256": h.hexdigest()})
    digest = hashlib.sha256(json.dumps([[f["file"], f["sha256"]] for f in files]).encode()).hexdigest()
    return {"folder": uri.rstrip("/"), "count": len(files), "sha256": digest, "files": files}


def approved_download(approved_files: str | dict, dest: str, project: str, only: list[str] | None = None) -> list[str]:
    """Downloads each approved file by its generation into dest and checks its SHA-256; raises if one differs or its
    generation is gone, so a run never scores with bytes nobody approved (a new version put over an approved file
    does not change the approved generation). approved_files: the run's `approved_files` parameter (JSON).
    only: the file names wanted (others are skipped). Returns the names downloaded."""
    import hashlib

    from google.api_core.exceptions import NotFound
    from google.cloud import storage

    spec = json.loads(approved_files) if isinstance(approved_files, str) else approved_files
    bucket_name, _, prefix = spec["folder"].removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    client = storage.Client(project=project)
    os.makedirs(dest, exist_ok=True)
    got = []
    for f in spec["files"]:
        if only is not None and f["file"] not in only:
            continue
        target = os.path.join(dest, f["file"])
        os.makedirs(os.path.dirname(target) or dest, exist_ok=True)
        blob = client.bucket(bucket_name).blob(prefix + f["file"], generation=int(f["generation"]))
        try:
            blob.download_to_filename(target)
        except NotFound:
            raise RuntimeError(f"{f['file']}: the approved generation {f['generation']} is gone (versioning off, or "
                               "deleted): re-approve a version whose files exist") from None
        h = hashlib.sha256()
        with open(target, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != f["sha256"]:
            raise RuntimeError(f"{f['file']}: generation {f['generation']} has sha256 {h.hexdigest()[:16]}, not the "
                               f"approved {f['sha256'][:16]}: not used")
        got.append(f["file"])
    return got


# ---------------------------------------------------------------------------------- A/B tests (FR-57) --
# The same assignment as pipewright.abtest.assign (a test keeps them equal): the component image has no pipewright
# package, so the few lines live here too.

def ab_spec(ab_test: str | dict | None) -> dict | None:
    """The run's ab_test parameter (JSON from the launcher), or None when no test is running."""
    if not ab_test:
        return None
    spec = json.loads(ab_test) if isinstance(ab_test, str) else ab_test
    return spec if spec.get("name") else None


def _ab_bucket(name: str, unit) -> int:
    import hashlib

    return int.from_bytes(hashlib.sha256(f"{name}:{unit}".encode()).digest()[:7], "big")


def ab_arm(name: str, unit, treatment_bp: int, layer: dict | None = None) -> str | None:
    """control or treatment: first 7 bytes of SHA-256(name:unit) below treatment_bp * 2^56 // 10000. With a layer
    (FR-123), None for a unit outside the test's range of the layer: not in the test, scored by the default."""
    if unit is None or unit == "":
        return "control"
    if layer:
        b = _ab_bucket(f"layer/{layer['name']}", unit)
        if not int(layer["start_bp"]) * 2**56 // 10000 <= b < int(layer["end_bp"]) * 2**56 // 10000:
            return None
    return "treatment" if _ab_bucket(name, unit) < int(treatment_bp) * 2**56 // 10000 else "control"


def _ab_sql_bucket(name: str, unit_sql: str) -> str:
    quoted = name.replace("\\", "\\\\").replace("'", "\\'")
    return (f"CAST(CONCAT('0x', TO_HEX(SUBSTR(SHA256(CONCAT('{quoted}', ':', CAST({unit_sql} AS STRING))), 1, 7))) "
            f"AS INT64)")


def ab_sql_arm(name: str, unit_sql: str, treatment_bp: int, layer: dict | None = None) -> str:
    """The same arm in BigQuery SQL; NULL outside the test's range of its layer."""
    arm = (f"IF({_ab_sql_bucket(name, unit_sql)} < {int(treatment_bp) * 2**56 // 10000}, 'treatment', "
           f"'control')")
    if layer:
        b = _ab_sql_bucket(f"layer/{layer['name']}", unit_sql)
        lo, hi = int(layer["start_bp"]) * 2**56 // 10000, int(layer["end_bp"]) * 2**56 // 10000
        arm = f"IF({b} >= {lo} AND {b} < {hi}, {arm}, NULL)"
    return f"IF({unit_sql} IS NULL OR CAST({unit_sql} AS STRING) = '', 'control', {arm})"


if __name__ == "__main__":       # `python pipewright_runtime.py` prints the format version (image smoke test)
    print(GCS_FORMAT_VERSION, file=sys.stdout)
