"""The online sync (FR-104): the latest feature values per entity, from BigQuery to the served store.

    1. latest   one query: each source's newest row per entity at or before now, joined on the entity key, written
                to a BigQuery table in the tenant's dataset. For type bigquery this is the served table (BI Engine
                serves it); for cloudsql it is the generation the next steps move.
    2. extract  a free extract job to Parquet, under the export location when the export is on (published, kept
                `export.keep` generations), else under the tenant's staging prefix (deleted after the load).
    3. manifest each file's generation and SHA-256, the row count, schema, entity key, as-of time and the feature
                contract version (FR-103), written beside the files; never rewritten.
    4. load     (cloudsql) the files, each checked against its SHA-256 as it is read, copied into a staging table
                with COPY; the row count checked against the manifest; the key indexed; SELECT granted to the
                reader; then one transaction renames staging into place (spike (d): 3 ms, no failed lookups), with
                a lock timeout and retries. `sync.mode: changed` applies only the rows whose values changed, with
                upserts and deletes in one transaction, so the database works less while lookups run.
    5. record   a `_pipewright_sync` row in the schema (the manifest's digest, contract version, rows, as-of), a
                sync record in the artifacts bucket for the watch and `doctor`, and an `online.synced` event.

It runs as the tenant's runner (it reads the feature tables already), never as the launcher, so the launcher never
reads feature data. Every message names tables, generations, digests and counts, never an entity ID or a value.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

from pipewright.errors import ConfigError

BQ = "https://bigquery.googleapis.com/bigquery/v2"
GCS = "https://storage.googleapis.com/storage/v1"
SYNC_TABLE = "_pipewright_sync"
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_PG_TYPES = {"STRING": "text", "INT64": "bigint", "INTEGER": "bigint", "FLOAT64": "double precision",
             "FLOAT": "double precision", "NUMERIC": "numeric", "BIGNUMERIC": "numeric", "BOOL": "boolean",
             "BOOLEAN": "boolean", "TIMESTAMP": "timestamptz", "DATE": "date", "DATETIME": "timestamp"}


def _q(name: str) -> str:
    return f"`{name}`"


def generation_id(now: datetime | None = None) -> str:
    return "g" + (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")


# ------------------------------------------------------------------------------------------------ 1. latest --

def latest_query(features_from: dict, features: list[str], key: str, project: str, as_of: str) -> str:
    """Each source's newest row per entity at or before `as_of`, the sources joined on the entity key. `features_from`
    has its feature groups already resolved to tables (the runtime's resolve_feature_groups)."""
    from pipewright.build.templates.pipewright_runtime import bq_table

    wanted, ctes, joins, select, stamps, names = set(features), [], [], {}, [], []
    for i, src in enumerate(features_from["sources"]):
        prefix, columns = src.get("prefix", ""), src.get("columns") or {}
        mine = [c for c in src["features"] if prefix + c in wanted]
        if not mine:
            continue
        fkey = (src.get("entity_columns") or [key])[0]
        ts = src.get("time_column", "feature_timestamp")
        values = ", ".join(f"{_q(columns.get(c, c))} AS {_q(prefix + c)}" for c in mine)
        ctes.append(f"s{i} AS (SELECT CAST({_q(fkey)} AS STRING) AS {_q(key)}, CAST({_q(ts)} AS TIMESTAMP) AS __ts, "
                    f"{values} FROM {_q(bq_table(src['table'], project))} "
                    f"WHERE CAST({_q(ts)} AS TIMESTAMP) <= TIMESTAMP '{as_of}' "
                    f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {_q(fkey)} ORDER BY {_q(ts)} DESC) = 1)")
        joins.append(f"LEFT JOIN s{i} USING ({_q(key)})")
        select.update({prefix + c: f"s{i}.{_q(prefix + c)}" for c in mine})
        stamps.append(f"s{i}.__ts")
        names.append(f"s{i}")
    if not ctes:
        raise ConfigError("the online sync has no feature to serve: no source provides the model's features")
    keys = " UNION DISTINCT ".join(f"SELECT {_q(key)} FROM {n}" for n in names)
    floor = "TIMESTAMP '0001-01-01 00:00:00+00'"
    newest = stamps[0] if len(stamps) == 1 else "GREATEST(" + ", ".join(f"IFNULL({s}, {floor})" for s in stamps) + ")"
    cols = [select[f] for f in features if f in select]
    return ("WITH " + ",\n".join(ctes) + f",\nkeys AS ({keys})\n"
            f"SELECT {_q(key)}, {', '.join(cols)}, {newest} AS feature_timestamp\nFROM keys\n" + "\n".join(joins))


def run_query_to_table(gcp, project: str, location: str, sql: str, table: str, kms_key: str | None = None) -> dict:
    """A query job writing `table` (project.dataset.table), replaced; returns the finished job."""
    p, d, t = table.split(".")
    cfg = {"query": {"query": sql, "useLegacySql": False, "writeDisposition": "WRITE_TRUNCATE",
                     "createDisposition": "CREATE_IF_NEEDED",
                     "destinationTable": {"projectId": p, "datasetId": d, "tableId": t}}}
    if kms_key:
        cfg["query"]["destinationEncryptionConfiguration"] = {"kmsKeyName": kms_key}
    if getattr(gcp, "job_labels", None):
        cfg["labels"] = dict(gcp.job_labels)
    return _job(gcp, project, location, cfg, f"the latest values into {table}")


def _job(gcp, project: str, location: str, cfg: dict, what: str) -> dict:
    from pipewright.gcp.session import error_text

    r = gcp.post(f"{BQ}/projects/{project}/jobs", {"configuration": cfg,
                                                   "jobReference": {"projectId": project, "location": location}})
    if not r.ok:
        raise ConfigError(f"{what}: {error_text(r)}")
    job = r.body
    while job.get("status", {}).get("state") != "DONE":
        time.sleep(2)
        r = gcp.get(f"{BQ}/projects/{project}/jobs/{job['jobReference']['jobId']}", location=location)
        if not r.ok:
            raise ConfigError(f"{what}: {error_text(r)}")
        job = r.body
    err = job["status"].get("errorResult")
    if err:
        from pipewright.build.templates.pipewright_runtime import value_free

        raise ConfigError(f"{what}: {value_free(err.get('message', 'failed'))}")
    return job


def table_info(gcp, table: str) -> dict:
    p, d, t = table.split(".")
    r = gcp.get(f"{BQ}/projects/{p}/datasets/{d}/tables/{t}")
    if not r.ok:
        raise ConfigError(f"cannot read {table}")
    return r.body


# ----------------------------------------------------------------------------------------------- 2. extract --

def extract(gcp, project: str, location: str, table: str, prefix: str) -> list[dict]:
    """Parquet files of `table` under `prefix` (gs://bucket/path); returns each file's name, generation, size."""
    p, d, t = table.split(".")
    _job(gcp, project, location, {"extract": {
        "sourceTable": {"projectId": p, "datasetId": d, "tableId": t},
        "destinationUris": [prefix.rstrip("/") + "/part-*.parquet"], "destinationFormat": "PARQUET",
        "compression": "SNAPPY"}}, f"the Parquet extract of {table}")
    bucket, path = _split(prefix)
    files = []
    for o in _list(gcp, bucket, path.rstrip("/") + "/part-"):
        files.append({"name": o["name"], "generation": o["generation"], "bytes": int(o.get("size", 0))})
    return sorted(files, key=lambda f: f["name"])


def _split(uri: str) -> tuple[str, str]:
    rest = uri[len("gs://"):]
    bucket, _, path = rest.partition("/")
    return bucket, path


def _list(gcp, bucket: str, prefix: str) -> list[dict]:
    out, token = [], None
    while True:
        params = {"prefix": prefix, "fields": "items(name,generation,size),nextPageToken"}
        if token:
            params["pageToken"] = token
        r = gcp.get(f"{GCS}/b/{bucket}/o", **params)
        if not r.ok:
            raise ConfigError(f"cannot list gs://{bucket}/{prefix}")
        out += r.body.get("items", [])
        token = r.body.get("nextPageToken")
        if not token:
            return out


def read_file(gcp, bucket: str, name: str, generation: str) -> bytes:
    r = gcp.session.get(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}", params={"alt": "media",
                                                                                "generation": generation}, timeout=600)
    if r.status_code != 200:
        raise ConfigError(f"cannot read gs://{bucket}/{name}#{generation}")
    return r.content


# ---------------------------------------------------------------------------------------------- 3. manifest --

def manifest(files: list[dict], digests: dict, rows: int, schema: list[dict], key: str, as_of: str,
             generation: str, usecase: str, environment: str, contract: dict | None, latest_job: str) -> dict:
    m = {"kind": "pipewright.online.generation", "version": 1, "usecase": usecase, "environment": environment,
         "generation": generation, "as_of": as_of, "entity_key": key, "rows": rows,
         "schema": [{"name": f["name"], "type": f["type"]} for f in schema],
         "files": [{**f, "sha256": digests[f["name"]]} for f in files], "latest_job": latest_job,
         "feature_contract": contract or None}
    return m


def manifest_sha256(m: dict) -> str:
    return hashlib.sha256(json.dumps(m, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


# -------------------------------------------------------------------------------------------------- 4. load --

def pg_type(bq_type: str) -> str:
    return _PG_TYPES.get(bq_type.upper(), "text")


def create_sql(schema_name: str, table: str, schema: list[dict], key: str) -> list[str]:
    cols = ", ".join(f'"{f["name"]}" {pg_type(f["type"])}' + (" NOT NULL" if f["name"] == key else "")
                     for f in schema)
    return [f'DROP TABLE IF EXISTS "{schema_name}"."{table}"', f'CREATE TABLE "{schema_name}"."{table}" ({cols})']


def parquet_csv_batches(data_files, schema: list[dict], batch_rows: int = 100000):
    """CSV text for COPY, in batches, from Parquet file contents."""
    import pyarrow.parquet as pq

    names = [f["name"] for f in schema]
    buf, w, n = io.StringIO(), None, 0
    w = csv.writer(buf)
    for content in data_files:
        for batch in pq.ParquetFile(io.BytesIO(content)).iter_batches(20000, columns=names):
            for row in batch.to_pylist():
                w.writerow(["" if row[c] is None else (row[c].isoformat() if hasattr(row[c], "isoformat")
                                                         else row[c]) for c in names])
                n += 1
                if n % batch_rows == 0:
                    yield buf.getvalue()
                    buf = io.StringIO()
                    w = csv.writer(buf)
    if buf.tell():
        yield buf.getvalue()


def copy_rows(conn, schema_name: str, table: str, schema: list[dict], csv_batches) -> None:
    cols = ", ".join(f'"{f["name"]}"' for f in schema)
    for text in csv_batches:
        conn.run(f'COPY "{schema_name}"."{table}" ({cols}) FROM STDIN WITH (FORMAT csv, NULL \'\')',
                 stream=io.StringIO(text))


def swap(conn, schema_name: str, table: str, staging: str, lock_timeout: str = "2s", tries: int = 5,
         sleep=time.sleep) -> dict:
    """Staging renamed into place in one transaction (spike (d): 3 ms, lookups never see half a load)."""
    conn.run(f"SET lock_timeout = '{lock_timeout}'")
    old = f"{table}__old"
    t, last = time.perf_counter(), None
    for attempt in range(1, tries + 1):
        try:
            conn.run("BEGIN")
            conn.run(f'DROP TABLE IF EXISTS "{schema_name}"."{old}"')
            exists = conn.run("SELECT 1 FROM pg_tables WHERE schemaname = :s AND tablename = :t",
                              s=schema_name, t=table)
            if exists:
                conn.run(f'ALTER TABLE "{schema_name}"."{table}" RENAME TO "{old}"')
            conn.run(f'ALTER TABLE "{schema_name}"."{staging}" RENAME TO "{table}"')
            conn.run("COMMIT")
            conn.run(f'DROP TABLE IF EXISTS "{schema_name}"."{old}"')
            return {"ms": round((time.perf_counter() - t) * 1000, 1), "tries": attempt}
        except Exception as e:  # noqa: BLE001 - a lock timeout: roll back and try again
            last = e
            try:
                conn.run("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            sleep(min(2 ** attempt, 10))
    raise ConfigError(f"the swap into {schema_name}.{table} did not get its lock in {tries} tries "
                      f"({type(last).__name__})")


def apply_changes(conn, schema_name: str, table: str, staging: str, schema: list[dict], key: str) -> dict:
    """sync.mode changed: the staging rows that differ upserted into the live table and removed keys deleted, in
    one transaction. Staging holds the full generation; only differing rows are written."""
    cols = [f["name"] for f in schema]
    q = lambda c: f'"{c}"'  # noqa: E731
    live, stg = f'"{schema_name}"."{table}"', f'"{schema_name}"."{staging}"'
    sets = ", ".join(f"{q(c)} = EXCLUDED.{q(c)}" for c in cols if c != key)
    differs = " OR ".join(f"l.{q(c)} IS DISTINCT FROM s.{q(c)}" for c in cols if c != key)
    conn.run("BEGIN")
    try:
        upserted = conn.run(
            f"WITH changed AS (SELECT s.* FROM {stg} s LEFT JOIN {live} l USING ({q(key)}) "
            f"WHERE l.{q(key)} IS NULL OR {differs}) "
            f"INSERT INTO {live} ({', '.join(map(q, cols))}) SELECT {', '.join(map(q, cols))} FROM changed "
            f"ON CONFLICT ({q(key)}) DO UPDATE SET {sets} RETURNING 1")
        deleted = conn.run(f"DELETE FROM {live} l WHERE NOT EXISTS (SELECT 1 FROM {stg} s WHERE s.{q(key)} = "
                           f"l.{q(key)}) RETURNING 1")
        conn.run("COMMIT")
    except Exception:
        conn.run("ROLLBACK")
        raise
    conn.run(f"DROP TABLE IF EXISTS {stg}")
    return {"upserted": len(upserted or []), "deleted": len(deleted or [])}


def record_sql(schema_name: str) -> list[str]:
    return [f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{SYNC_TABLE}" (table_name text, generation text, '
            "manifest_sha256 text, contract_version text, contract_sha256 text, rows bigint, as_of timestamptz, "
            "synced_at timestamptz, mode text)"]


class Loaded(dict):
    pass


def load_cloudsql(conn, online: dict, m: dict, data_files, readers: list[str], mode: str = "full") -> Loaded:
    """Steps 4 and 5 in the database: staging, COPY, checks, key, grants, swap (or changed rows), the record."""
    s, table, key = online["schema"], online["table"], online["key"]
    for label, ident in (("schema", s), ("table", table), ("entity key", key)):
        if not _IDENT.match(ident):
            raise ConfigError(f"the {label} {ident!r} is not a PostgreSQL identifier")
    staging = f"{table}__staging"
    out = Loaded(timings={})
    t = time.perf_counter()
    conn.run(f'CREATE SCHEMA IF NOT EXISTS "{s}"')
    owner = conn.run("SELECT pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid = "
                     "c.relnamespace WHERE n.nspname = :s AND c.relname = :t", s=s, t=table)
    me = conn.run("SELECT current_user")[0][0]
    if owner and owner[0][0] != me:
        raise ConfigError(f"{s}.{table} belongs to another identity ({owner[0][0]}): Pipewright never writes a table "
                          "another tenant owns; choose another schema or table name")
    for sql in create_sql(s, staging, m["schema"], key):
        conn.run(sql)
    copy_rows(conn, s, staging, m["schema"], data_files)
    out["timings"]["copy_s"] = round(time.perf_counter() - t, 1)
    count = conn.run(f'SELECT count(*) FROM "{s}"."{staging}"')[0][0]
    if count != m["rows"]:
        conn.run(f'DROP TABLE IF EXISTS "{s}"."{staging}"')
        raise ConfigError(f"{s}.{staging} has {count} rows, the manifest {m['rows']}: the generation was not loaded")
    t = time.perf_counter()
    conn.run(f'ALTER TABLE "{s}"."{staging}" ADD PRIMARY KEY ("{key}")')
    conn.run(f'ANALYZE "{s}"."{staging}"')
    out["timings"]["index_s"] = round(time.perf_counter() - t, 1)
    live_exists = conn.run("SELECT 1 FROM pg_tables WHERE schemaname = :s AND tablename = :t", s=s, t=table)
    if mode == "changed" and live_exists:
        out.update(apply_changes(conn, s, table, staging, m["schema"], key))
    else:
        for r in readers:
            conn.run(f'GRANT USAGE ON SCHEMA "{s}" TO "{r}"')
            conn.run(f'GRANT SELECT ON "{s}"."{staging}" TO "{r}"')
        out["swap"] = swap(conn, s, table, staging)
    for sql in record_sql(s):
        conn.run(sql)
    contract = m.get("feature_contract") or {}
    conn.run(f'INSERT INTO "{s}"."{SYNC_TABLE}" VALUES (:t, :g, :d, :cv, :cs, :r, :a, now(), :mode)',
             t=table, g=m["generation"], d=manifest_sha256(m), cv=contract.get("version"),
             cs=contract.get("sha256"), r=m["rows"], a=m["as_of"], mode=mode)
    for r in readers:
        conn.run(f'GRANT USAGE ON SCHEMA "{s}" TO "{r}"')
        conn.run(f'GRANT SELECT ON "{s}"."{SYNC_TABLE}" TO "{r}"')
    out["rows"] = count
    return out


# ----------------------------------------------------------------------------------------------- the whole --

def feature_contract(ml_config: dict) -> dict | None:
    section = ml_config.get("feature_contract")
    if not section:
        return None
    from pipewright.models.definitions import contract_digest

    return {"version": section.get("version"), "sha256": contract_digest(section)}


def record_name(tenant: str, usecase: str, generation: str | None = None) -> str:
    base = f"online-sync/{tenant}/{usecase}"
    return f"{base}/{generation}.json" if generation else f"{base}/latest.json"


def _delete_prefix(gcp, uri: str) -> int:
    bucket, path = _split(uri)
    n = 0
    for o in _list(gcp, bucket, path.rstrip("/") + "/"):
        if gcp.delete(f"{GCS}/b/{bucket}/o/{quote(o['name'], safe='')}").ok:
            n += 1
    return n


def _prune(gcp, base: str, keep: int) -> list[str]:
    """Generations beyond the newest `keep` under an export location, deleted; returns their names."""
    bucket, path = _split(base)
    gens = sorted({o["name"][len(path.rstrip("/")) + 1:].split("/", 1)[0]
                   for o in _list(gcp, bucket, path.rstrip("/") + "/g")})
    gone = [g for g in gens if re.match(r"^g[0-9]{8}T[0-9]{6}Z$", g)][:-keep] if keep else []
    for g in gone:
        _delete_prefix(gcp, f"{base.rstrip('/')}/{g}")
    return gone


def connector(env, gcp, online: dict):
    """connect() for the sync: Cloud SQL over its private IP as the caller (IAM database login: the access token is
    the password), SSL verified against the instance's server CA from the Cloud SQL Admin API."""
    def connect():
        import pg8000.native

        from pipewright.online.server import iam_user, ssl_context

        store = ((env.features or {}).get("online_store") or {}).get("cloudsql") or {}
        project, _region, name = online["instance"].split(":")
        r = gcp.get(f"https://sqladmin.googleapis.com/v1/projects/{project}/instances/{name}")
        if not r.ok:
            raise ConfigError(f"cannot read the Cloud SQL instance {online['instance']} (needs cloudsql.instances.get)")
        host = store.get("private_ip") or next((a["ipAddress"] for a in r.body.get("ipAddresses", [])
                                                if a.get("type") == "PRIVATE"), None)
        if not host:
            raise ConfigError(f"the Cloud SQL instance {online['instance']} has no private IP")
        ca = (r.body.get("serverCaCert") or {}).get("cert")
        conn = pg8000.native.Connection(iam_user(gcp.identity() or ""), host=host, password=gcp.access_token(),
                                        database=online.get("database", "features"), ssl_context=ssl_context(ca),
                                        timeout=60)
        return conn
    return connect


def sync_bigtable_views(online: dict, gcp, log=print, wait_s: int = 1800, sleep=time.sleep) -> list[dict]:
    """FR-110 (Pipewright 1.4.2): starts a sync of each feature view of the tenant's Bigtable online store and waits
    for it. A new view had no sync until its schedule (06:00) in 1.4.2's re-run of the book's Chapter 13, so the
    store served nothing, and `doctor` said "start a sync" without a way to. Returns one entry per view."""
    from pipewright.gcp.session import error_text

    region = online["store"].split("/")[3]
    api = f"https://{region}-aiplatform.googleapis.com/v1"
    out = []
    for view in online.get("views") or []:
        name = f"{online['store']}/featureViews/{view['view']}"
        r = gcp.post(f"{api}/{name}:sync", {})
        if not r.ok:
            raise ConfigError(f"view {view['view']}: the sync did not start: {error_text(r)}")
        sync_name = r.body["featureViewSync"]
        log(f"view {view['view']}: sync {sync_name.rsplit('/', 1)[-1]} started")
        waited, got = 0, {}
        while waited <= wait_s:
            g = gcp.get(f"{api}/{sync_name}")
            got = g.body if g.ok else {}
            if got.get("finalStatus") is not None or (got.get("runTime") or {}).get("endTime"):
                break
            sleep(10)
            waited += 10
        status = got.get("finalStatus") or {}
        if status.get("code"):
            raise ConfigError(f"view {view['view']}: the sync failed: {status.get('message', '')[:300]} (a first "
                              "sync can fail while a new grant reaches every part of Google Cloud: try again in a "
                              "minute)")
        if not (got.get("runTime") or {}).get("endTime"):
            raise ConfigError(f"view {view['view']}: the sync had not finished after {wait_s} s; doctor shows it "
                              "once it has")
        rows = (got.get("syncSummary") or {}).get("rowSynced")
        log(f"view {view['view']}: synced{f' {rows} rows' if rows else ''}")
        out.append({"view": view["view"], "sync": sync_name, "rows": int(rows) if rows else None})
    return out


def run(ml_config: dict, env, tenant: str, usecase: str, gcp, connect=None, now: datetime | None = None,
        log=print, release: str | None = None, load: bool = True, event_stream=None) -> dict:
    """One sync of a use case's online store. `connect()` opens the Cloud SQL connection (IAM login) for cloudsql;
    load=False exports only (`pipewright features export`)."""
    from pipewright.build.templates.pipewright_runtime import resolve_feature_groups
    from pipewright.gcp.storage import gcs_write
    from pipewright.observability import events
    online = ml_config.get("online_features") or {}
    kind = online.get("type")
    # 1.4.2: an export needs no store of Pipewright's: a team's own store (type http) is fed by it (found writing the
    # book's Chapter 23, where `features export` refused the very case ONLINE_STORES.md describes)
    export_only = not load or kind not in ("bigquery", "cloudsql")
    if kind not in ("bigquery", "cloudsql") and not (online.get("export") and kind == "http"):
        raise ConfigError(f"{usecase} has no online store Pipewright syncs (model.serving.online_store.type is "
                          f"{kind or 'bigtable'}; bigquery and cloudsql are synced, and an http store's Parquet "
                          "export with export.enabled)")
    now = now or datetime.now(timezone.utc)
    gen, as_of = generation_id(now), now.strftime("%Y-%m-%d %H:%M:%S+00")
    project, location = env.project, env.region
    dataset = (env.tenants.get(tenant) or {}).get("bigquery_dataset")
    kms = (env.security or {}).get("bigquery_kms_key")
    timings, t0 = {}, time.perf_counter()
    ff = resolve_feature_groups(ml_config["data"]["features_from"], project, location, gcp.session)
    sql = latest_query(ff, online["features"], online["key"], project, as_of)
    from pipewright.online.config import table_name

    table = online.get("table") or table_name(usecase)
    latest = table if kind == "bigquery" else f"{project}.{dataset}.{table}__latest"
    t = time.perf_counter()
    job = run_query_to_table(gcp, project, location, sql, latest, kms)
    timings["latest_s"] = round(time.perf_counter() - t, 1)
    info = table_info(gcp, latest)
    rows = int(info.get("numRows", 0))
    schema = info["schema"]["fields"]
    log(f"latest values: {rows} entities, {len(schema) - 2} features into {latest} "
        f"({int(job['statistics']['query'].get('totalBytesBilled', 0)) / 2**20:.1f} MiB billed)")
    export = online.get("export")
    out = {"usecase": usecase, "tenant": tenant, "environment": env.name, "type": kind, "generation": gen,
           "as_of": as_of, "rows": rows, "latest_table": latest, "latest_job": job["jobReference"]["jobId"],
           "bytes_billed": int(job["statistics"]["query"].get("totalBytesBilled", 0)),
           "feature_contract": feature_contract(ml_config), "release": release}
    m = None
    if kind == "cloudsql" or export:
        base = export["uri"] if export else (f"gs://{env.artifacts_bucket}/online-export/{tenant}/{usecase}/_staging")
        prefix = f"{base.rstrip('/')}/{gen}"
        t = time.perf_counter()
        files = extract(gcp, project, location, latest, prefix)
        timings["extract_s"] = round(time.perf_counter() - t, 1)
        bucket, _ = _split(prefix)
        contents, digests = [], {}
        t = time.perf_counter()
        for f in files:
            data = read_file(gcp, bucket, f["name"], f["generation"])
            digests[f["name"]] = hashlib.sha256(data).hexdigest()
            contents.append(data)
        timings["read_s"] = round(time.perf_counter() - t, 1)
        m = manifest(files, digests, rows, schema, online["key"], as_of, gen, usecase, env.name,
                     out["feature_contract"], out["latest_job"])
        _, path = _split(prefix)
        gcs_write(gcp, bucket, f"{path}/manifest.json", json.dumps(m, indent=1), if_generation_match=0)
        out.update(files=len(files), bytes=sum(f["bytes"] for f in files), manifest=f"{prefix}/manifest.json",
                   manifest_sha256=manifest_sha256(m))
        log(f"extract: {len(files)} Parquet files, {out['bytes'] / 2**20:.1f} MiB, "
            f"manifest {out['manifest_sha256'][:12]}")
        if kind == "cloudsql" and load and not export_only:
            readers = []
            if online.get("provider") == "own" and online.get("reader"):
                readers.append(online["reader"].removesuffix(".gserviceaccount.com"))
            sa = (((env.tenants.get(tenant) or {}).get("features") or {}).get("lookup_service_account"))
            if online.get("provider", "pipewright") == "pipewright" and sa:
                readers.append(sa.removesuffix(".gserviceaccount.com"))
            t = time.perf_counter()
            conn = (connect or connector(env, gcp, online))()
            loaded = load_cloudsql(conn, online, m, parquet_csv_batches(contents, m["schema"]), readers,
                                   (online.get("sync") or {}).get("mode", "full"))
            timings["load_s"] = round(time.perf_counter() - t, 1)
            timings.update(loaded.pop("timings", {}))
            out["load"] = dict(loaded)
            log(f"cloudsql: {online['schema']}.{online['table']} " + (
                f"swapped in {loaded['swap']['ms']} ms" if "swap" in loaded else
                f"{loaded.get('upserted', 0)} rows changed, {loaded.get('deleted', 0)} removed"))
        if export:
            out["pruned"] = _prune(gcp, export["uri"], int(export.get("keep", 3)))
        else:
            _delete_prefix(gcp, prefix)
    timings["total_s"] = round(time.perf_counter() - t0, 1)
    out["timings"] = timings
    out["synced_at"] = datetime.now(timezone.utc).isoformat()
    text = json.dumps(out, indent=1, default=str)
    gcs_write(gcp, env.artifacts_bucket, record_name(tenant, usecase, gen), text)
    gcs_write(gcp, env.artifacts_bucket, record_name(tenant, usecase), text)
    events.emit(events.event("online.synced", usecase, env.name, tenant, release,
                             **{k: out[k] for k in ("type", "generation", "rows", "as_of", "timings")
                                if k in out}, manifest_sha256=out.get("manifest_sha256"),
                             feature_contract=out["feature_contract"]),
                topic=(env.observability or {}).get("events_topic"), gcp=gcp, region=env.region,
                stream=event_stream)       # the CLI passes a sink: the line is for Cloud Logging, in the job
    return out
