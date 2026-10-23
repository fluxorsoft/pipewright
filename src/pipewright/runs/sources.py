"""What a run's `source_uri` may be. The launcher, `pipewright run` and schedule configuration check it here;
pipewright_runtime.read_source (in component images) keeps the same rules.

    gs://bucket/path/file.csv        a landing file, or any CSV or Parquet object
    gs://bucket/path/file.parquet
    bq://project.dataset.table       a BigQuery table or view (filter in a view, e.g. the last 12 months)
    bq://dataset.table               in the project the pipeline runs in
    snapshot://<run_id>              only from `pipewright run --replay`: the rows that run read (data.snapshot_table)
"""

from __future__ import annotations

import re

GCS = re.compile(r"^gs://[a-z0-9][a-z0-9._-]*/\S+$")     # shape only; GCS itself enforces bucket naming
BQ = re.compile(r"^bq://(?:([a-z][a-z0-9-]{4,61}[a-z0-9])\.)?([A-Za-z_][A-Za-z0-9_]{0,1023})\.([A-Za-z0-9_$-]{1,1024})$")


SNAPSHOT = re.compile(r"^snapshot://[A-Za-z0-9_-]{1,128}$")


def snapshot_uri(run_id: str) -> str:
    """The source that replays a run's snapshot."""
    uri = f"snapshot://{run_id}"
    if not SNAPSHOT.match(uri):
        raise ValueError(f"{run_id!r} is not a run ID (letters, digits, - and _)")
    return uri


def problem(source_uri: str | None, allow_snapshot: bool = False) -> str | None:
    """None when the URI is usable, otherwise what is wrong with it. A snapshot:// source is accepted only where
    the caller allows it (`pipewright run --replay`), never in schedules or retrain configuration."""
    if not source_uri:
        return "source_uri is required"
    if allow_snapshot and source_uri.startswith("snapshot://"):
        return None if SNAPSHOT.match(source_uri) else f"{source_uri!r} is not snapshot://<run id>"
    if source_uri.startswith("gs://"):
        return None if GCS.match(source_uri) else f"{source_uri!r} is not a gs://bucket/object path"
    if source_uri.startswith("bq://"):
        return None if BQ.match(source_uri) else (f"{source_uri!r} is not bq://project.dataset.table "
                                                  "(or bq://dataset.table)")
    return f"{source_uri!r}: source_uri must start with gs:// (CSV or Parquet) or bq:// (table or view)"


def bq_table(source_uri: str, default_project: str) -> str:
    """bq://... -> project.dataset.table"""
    m = BQ.match(source_uri)
    if not m:
        raise ValueError(problem(source_uri))
    project, dataset, table = m.groups()
    return f"{project or default_project}.{dataset}.{table}"
