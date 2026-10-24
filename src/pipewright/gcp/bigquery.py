"""BigQuery queries over REST (jobs.query), with named parameters and polling for long jobs."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pipewright.errors import ConfigError, value_free
from pipewright.gcp.session import Gcp, error_text

BQ = "https://bigquery.googleapis.com/bigquery/v2"


def _value(field: dict, cell):
    v = cell.get("v") if isinstance(cell, dict) else cell
    if v is None:
        return None
    kind = field["type"]
    if kind in ("INTEGER", "INT64"):
        return int(v)
    if kind in ("FLOAT", "FLOAT64", "NUMERIC"):
        return float(v)
    if kind in ("BOOLEAN", "BOOL"):
        return v == "true"
    if kind == "TIMESTAMP":                      # REST returns epoch seconds as a string
        return datetime.fromtimestamp(float(v), tz=timezone.utc).isoformat()
    return v


def dataset_location(gcp: Gcp, project: str, dataset: str) -> str | None:
    r = gcp.get(f"{BQ}/projects/{project}/datasets/{dataset}")
    return r.body.get("location") if r.ok else None


def query(gcp: Gcp, project: str, sql: str, params: list[dict] | None = None, location: str | None = None,
          kms_key: str | None = None) -> dict:
    """Runs SQL; returns the finished jobs.query response (rows, schema, numDmlAffectedRows). kms_key encrypts the
    results (required where the CMEK organization policy covers BigQuery: manifest security.bigquery_kms_key)."""
    body = {"query": sql, "useLegacySql": False, "timeoutMs": 60000}
    if kms_key:
        body["destinationEncryptionConfiguration"] = {"kmsKeyName": kms_key}
    if params:
        body.update(parameterMode="NAMED", queryParameters=params)
    if location:
        body["location"] = location
    r = gcp.post(f"{BQ}/projects/{project}/queries", body)
    if not r.ok:
        raise ConfigError(value_free(error_text(r)))   # NFR-18
    res = r.body
    while not res.get("jobComplete"):
        time.sleep(2)
        ref = res["jobReference"]
        r = gcp.get(f"{BQ}/projects/{project}/queries/{ref['jobId']}", location=ref.get("location"), timeoutMs=60000)
        if not r.ok:
            raise ConfigError(value_free(error_text(r)))   # NFR-18
        res = r.body
    return res


def rows(res: dict) -> list[dict]:
    """Result rows as dicts with typed scalar values."""
    fields = res.get("schema", {}).get("fields", [])
    return [{f["name"]: _value(f, c) for f, c in zip(fields, row["f"], strict=True)} for row in res.get("rows", [])]
