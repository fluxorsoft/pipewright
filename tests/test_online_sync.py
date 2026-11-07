"""FR-104: the online sync, BigQuery to the served store."""
import io
import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipewright.errors import ConfigError
from pipewright.online import sync

FF = {"entity": {"columns": ["customer_id"], "time_column": "cutoff"},
      "sources": [{"table": "bq://acme-ml-dev.feats.profile", "features": ["tenure", "plan"]},
                  {"table": "bq://acme-ml-dev.feats.support", "features": ["tickets"], "prefix": "s_",
                   "entity_columns": ["cust"], "time_column": "ts", "columns": {"tickets": "num_tickets"}}]}
SCHEMA = [{"name": "customer_id", "type": "STRING"}, {"name": "tenure", "type": "FLOAT"},
          {"name": "s_tickets", "type": "INTEGER"}, {"name": "feature_timestamp", "type": "TIMESTAMP"}]


def test_latest_query_takes_each_sources_newest_row_and_joins_on_the_key():
    sql = sync.latest_query(FF, ["s_tickets", "tenure"], "customer_id", "acme-ml-dev", "2026-11-07 10:00:00+00")
    assert "FROM `acme-ml-dev.feats.profile`" in sql and "FROM `acme-ml-dev.feats.support`" in sql
    assert "PARTITION BY `cust` ORDER BY `ts` DESC" in sql                     # the source's own key and time
    assert "`num_tickets` AS `s_tickets`" in sql                               # a registered column, prefixed
    assert "<= TIMESTAMP '2026-11-07 10:00:00+00'" in sql                       # nothing after the as-of time
    assert "`plan`" not in sql                                                 # only the model's features
    assert "UNION DISTINCT" in sql and "GREATEST(" in sql
    select = sql.split("SELECT `customer_id`, ", 1)[1]
    assert select.index("s_tickets") < select.index("tenure")                   # the model's order


def test_create_sql_types_and_key():
    drop, create = sync.create_sql("team", "t__staging", SCHEMA, "customer_id")
    assert drop == 'DROP TABLE IF EXISTS "team"."t__staging"'
    assert '"customer_id" text NOT NULL' in create and '"tenure" double precision' in create
    assert '"s_tickets" bigint' in create and '"feature_timestamp" timestamptz' in create


def parquet(rows):
    t = pa.table({k: [r[k] for r in rows] for k in rows[0]})
    b = io.BytesIO()
    pq.write_table(t, b)
    return b.getvalue()


def test_parquet_becomes_csv_batches_with_empty_nulls():
    ts = datetime(2026, 11, 7, tzinfo=timezone.utc)
    data = parquet([{"customer_id": "c1", "tenure": 1.5, "s_tickets": None, "feature_timestamp": ts},
                    {"customer_id": "c2", "tenure": None, "s_tickets": 3, "feature_timestamp": ts}])
    batches = list(sync.parquet_csv_batches([data], SCHEMA, batch_rows=1))
    assert batches[0].strip() == "c1,1.5,,2026-11-07T00:00:00+00:00"
    assert batches[1].strip() == "c2,,3,2026-11-07T00:00:00+00:00"


class Conn:
    """Records SQL; answers the few queries the loader reads."""

    def __init__(self, count=2, owner=None, me="runner@acme.iam", fail_rename=0, live=True):
        self.sql, self.count, self.owner, self.me, self.fail_rename, self.live = [], count, owner, me, fail_rename, live

    def run(self, sql, stream=None, **p):
        self.sql.append(sql)
        if sql.startswith("SELECT pg_get_userbyid"):
            return [[self.owner]] if self.owner else []
        if sql == "SELECT current_user":
            return [[self.me]]
        if sql.startswith("SELECT count(*)"):
            return [[self.count]]
        if sql.startswith("SELECT 1 FROM pg_tables"):
            return [[1]] if self.live else []
        if "RENAME TO" in sql and self.fail_rename:
            self.fail_rename -= 1
            raise RuntimeError("canceling statement due to lock timeout")
        if "RETURNING 1" in sql:
            return [[1]]
        return []


ONLINE = {"type": "cloudsql", "schema": "team", "table": "pw_online_churn", "key": "customer_id"}
M = {"generation": "g20261107T100000Z", "rows": 2, "schema": SCHEMA, "as_of": "2026-11-07 10:00:00+00",
     "feature_contract": {"version": "1.2.0", "sha256": "abc"}, "files": []}


def test_a_full_load_checks_counts_grants_and_swaps():
    c = Conn()
    out = sync.load_cloudsql(c, ONLINE, M, ["x"], ["pwl-churn@acme.iam"])
    text = "\n".join(c.sql)
    assert 'CREATE SCHEMA IF NOT EXISTS "team"' in text
    assert 'ALTER TABLE "team"."pw_online_churn__staging" ADD PRIMARY KEY ("customer_id")' in text
    assert 'GRANT SELECT ON "team"."pw_online_churn__staging" TO "pwl-churn@acme.iam"' in text
    assert text.index('RENAME TO "pw_online_churn__old"') < text.index('"pw_online_churn__staging" RENAME TO')
    assert 'INSERT INTO "team"."_pipewright_sync"' in text
    assert out["rows"] == 2 and out["swap"]["tries"] == 1


def test_a_count_that_differs_from_the_manifest_is_not_loaded():
    c = Conn(count=1)
    with pytest.raises(ConfigError, match="has 1 rows, the manifest 2: the generation was not loaded"):
        sync.load_cloudsql(c, ONLINE, M, ["x"], [])
    assert not any("RENAME" in s for s in c.sql)


def test_another_tenants_table_is_never_written():
    with pytest.raises(ConfigError, match="belongs to another identity"):
        sync.load_cloudsql(Conn(owner="other@acme.iam"), ONLINE, M, ["x"], [])


def test_the_swap_retries_a_lock_timeout():
    c = Conn(fail_rename=2)
    got = sync.swap(c, "team", "t", "t__staging", sleep=lambda s: None)
    assert got["tries"] == 3 and c.sql.count("ROLLBACK") == 2
    with pytest.raises(ConfigError, match="did not get its lock in 5 tries"):
        sync.swap(Conn(fail_rename=9), "team", "t", "t__staging", sleep=lambda s: None)


def test_changed_mode_upserts_and_deletes_in_one_transaction():
    c = Conn()
    out = sync.load_cloudsql(c, ONLINE, M, ["x"], [], mode="changed")
    text = "\n".join(c.sql)
    assert "ON CONFLICT (\"customer_id\") DO UPDATE" in text and "IS DISTINCT FROM" in text
    assert "DELETE FROM" in text and not any("RENAME" in s for s in c.sql)
    assert out["upserted"] == 1 and out["deleted"] == 1


def test_identifiers_are_checked():
    with pytest.raises(ConfigError, match="not a PostgreSQL identifier"):
        sync.load_cloudsql(Conn(), {**ONLINE, "table": "x; drop"}, M, [], [])


def test_the_manifest_digest_is_stable():
    m = sync.manifest([{"name": "p/part-0.parquet", "generation": "1", "bytes": 10}], {"p/part-0.parquet": "aa"},
                      2, SCHEMA, "customer_id", "2026", "g1", "churn", "dev", None, "job1")
    assert m["files"][0]["sha256"] == "aa" and m["schema"][1] == {"name": "tenure", "type": "FLOAT"}
    assert sync.manifest_sha256(m) == sync.manifest_sha256(json.loads(json.dumps(m)))


def test_run_wires_the_steps_and_records_the_sync(monkeypatch):
    from types import SimpleNamespace

    from pipewright.build.templates import pipewright_runtime
    from pipewright.gcp import storage
    from pipewright.observability import events

    written, deleted, emitted = {}, [], []
    ts = datetime(2026, 11, 7, tzinfo=timezone.utc)
    data = parquet([{"customer_id": "c1", "tenure": 1.0, "s_tickets": 2, "feature_timestamp": ts},
                    {"customer_id": "c2", "tenure": 2.0, "s_tickets": None, "feature_timestamp": ts}])
    monkeypatch.setattr(pipewright_runtime, "resolve_feature_groups", lambda ff, p, r, s: ff)
    monkeypatch.setattr(sync, "run_query_to_table", lambda *a, **k: {
        "jobReference": {"jobId": "j1"}, "statistics": {"query": {"totalBytesBilled": "1048576"}}})
    monkeypatch.setattr(sync, "table_info", lambda gcp, t: {"numRows": "2", "schema": {"fields": SCHEMA}})
    monkeypatch.setattr(sync, "extract", lambda gcp, p, loc, t, prefix: [
        {"name": prefix.split("/", 3)[3] + "/part-000.parquet", "generation": "7", "bytes": len(data)}])
    monkeypatch.setattr(sync, "read_file", lambda gcp, b, n, g: data)
    monkeypatch.setattr(sync, "_delete_prefix", lambda gcp, uri: deleted.append(uri) or 1)
    monkeypatch.setattr(storage, "gcs_write", lambda gcp, b, n, text, **k: written.update({n: text}) or 1)
    monkeypatch.setattr(events, "emit", lambda e, **k: emitted.append(e) or True)
    env = SimpleNamespace(name="dev", project="acme-ml-dev", region="europe-west2", artifacts_bucket="art",
                          tenants={"churn": {"bigquery_dataset": "churn", "features": {
                              "lookup_service_account": "pwl-churn@acme-ml-dev.iam.gserviceaccount.com"}}},
                          security={}, observability={})
    ml = {"data": {"features_from": FF},
          "online_features": {**ONLINE, "features": ["tenure", "s_tickets"], "provider": "pipewright"},
          "feature_contract": {"version": "1.0.0", "sources": []}}
    conn = Conn()
    out = sync.run(ml, env, "churn", "churn-x", SimpleNamespace(session=None), connect=lambda: conn,
                   now=ts, log=lambda m: None)
    assert out["rows"] == 2 and out["generation"] == "g20261107T000000Z" and out["files"] == 1
    assert out["feature_contract"]["version"] == "1.0.0" and out["load"]["swap"]["tries"] == 1
    assert any('TO "pwl-churn@acme-ml-dev.iam"' in s for s in conn.sql)        # the lookup identity reads it
    manifest_name = "online-export/churn/churn-x/_staging/g20261107T000000Z/manifest.json"
    assert json.loads(written[manifest_name])["files"][0]["sha256"]
    assert "online-sync/churn/churn-x/latest.json" in written
    assert deleted == ["gs://art/online-export/churn/churn-x/_staging/g20261107T000000Z"]   # export off
    assert emitted[0]["event_type"] == "online.synced" and "c1" not in json.dumps(emitted)
