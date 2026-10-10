"""Pipewright runtime helpers for pipeline components. Copied into every component image by `pipewright release`
(next to the use case's own modules), so components import it without depending on the pipewright package:

    from pipewright_runtime import emit_event, log, read_source, write_table

read_source   reads a run's source_uri: gs:// CSV or Parquet, or bq:// table or view; with data.features_from, the
              source is the entity table and features are joined point in time from the feature tables
write_table   writes rows to the use case's store (outputs.store): BigQuery, Parquet on GCS, or both
log           one structured JSON log line with the standard Pipewright labels
emit_event    a lifecycle event: a structured log line, and a Pub/Sub message when the platform set an events topic
ray_cluster   a Ray on Vertex AI cluster for this step of this run (steps.<step>.ray), deleted when the block ends
ray_job       runs a Ray job on that cluster and waits for it (needs ray[default] and google-cloud-aiplatform[ray])

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


def read_source(source_uri: str, project: str, ml_config: dict | None = None, run_id: str | None = None):
    """A run's input as a DataFrame. gs://.../*.parquet is read as Parquet, any other gs:// object as CSV;
    bq:// reads a table or a view with a query, so filtering and joins can live in a view.

    With data.features_from in ml_config the source is the entity table (bq:// only) and each row gets the latest
    values of the named features at or before its cut-off time; a `data.features.resolved` event records what was
    read."""
    import pandas as pd

    features_from = ((ml_config or {}).get("data") or {}).get("features_from")
    if features_from and not source_uri.startswith("bq://"):
        raise ValueError(f"{source_uri!r}: with data.features_from the run's source is the entity table, a bq:// "
                         "table or view")
    if source_uri.startswith("bq://"):
        ref = bq_table(source_uri, project)
        client = bigquery_client(project, ml_config)
        if features_from:
            df = _read_with_features(client, ref, project, features_from, ml_config or {}, run_id)
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
                        run_id: str | None):
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
    record = {"entity_table": entity_ref, "cutoff": tc if tc in entity_columns else "query time",
              "query_job": job.job_id, "read_at": (job.started or datetime.now(timezone.utc)).isoformat(),
              "sources": sources}
    for s in sources:
        if s["rows"] and s["rows_matched"] < s["rows"]:
            log(f"{s['rows'] - s['rows_matched']} of {s['rows']} entity rows have no value in {s['table']} at or "
                "before their cut-off; their features from it are empty", ml_config, "WARNING", run_id)
    emit_event("data.features.resolved", ml_config, run_id, **record)
    return df


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
        self.name = (f"projects/{platform['project']}/locations/{self.region}/persistentResources/"
                     f"{ray_cluster_id(ml_config['usecase'], run_id, step)}")

    def __enter__(self) -> str:
        import time

        log(f"asking for Ray cluster {self.name.rsplit('/', 1)[-1]} ({self.step})", self.ml_config,
            run_id=self.run_id, ray_cluster=self.name)
        _ray_request(self.ml_config, "create", self.step, self.run_id)
        deadline, state = time.time() + 60 * min(self.minutes, 45), "PENDING"
        asked, resends = time.time(), 0
        while time.time() < deadline:
            current = _ray_state(self.name, self.region)
            state = current.get("state", "PENDING")
            if state == "RUNNING":
                log(f"Ray cluster {self.name.rsplit('/', 1)[-1]} is running", self.ml_config, run_id=self.run_id)
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
                log(f"asked again for Ray cluster {self.name.rsplit('/', 1)[-1]} ({resends})", self.ml_config,
                    "WARNING", self.run_id)
            time.sleep(20)
        self.__exit__(None, None, None)
        error = _ray_state(self.name, self.region).get("error") or {}
        raise RuntimeError(f"Ray cluster {self.name} did not start (state {state}): {error.get('message', '')}")

    def __exit__(self, *exc) -> bool:
        try:
            _ray_request(self.ml_config, "delete", self.step, self.run_id)
            log(f"Ray cluster {self.name.rsplit('/', 1)[-1]} deleted", self.ml_config, run_id=self.run_id)
        except Exception as e:  # noqa: BLE001 - the launcher's cleanup removes it once the run ends
            log(f"Ray cluster delete failed (cleanup removes it after the run): {e}", self.ml_config, "WARNING",
                self.run_id)
        return False


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

    client = JobSubmissionClient(f"vertex_ray://{cluster}")
    job = client.submit_job(entrypoint=entrypoint, runtime_env=runtime_env or {})
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


if __name__ == "__main__":       # `python pipewright_runtime.py` prints the format version (image smoke test)
    print(GCS_FORMAT_VERSION, file=sys.stdout)
