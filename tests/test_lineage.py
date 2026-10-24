"""FR-55: lineage.json per registered version, and its rendering."""
from pipewright.build.templates import pipewright_runtime as rt
from pipewright.models import lineage

ML = {"usecase": "churn", "platform": {"project": "proj-a", "tenant": "t", "bigquery_dataset": "ds"},
      "evaluation": {"primary_metric": "auc"},
      "data": {"snapshot_table": "raw_snap",
               "features_from": {"entity": {"time_column": "as_of"},
                                 "sources": [{"table": "bq://proj-a.f.usage", "features": ["calls"], "secret": "x"}]},
               "label_source": {"table": "bq://proj-a.l.renewals", "key": "customer_id", "window_days": 30}}}
EVAL = {"decision": "approve", "metrics": {"auc": 0.81}, "champion": {"auc": 0.8, "_version": "4"}, "reasons": []}


def test_lineage_record_names_release_rows_sources_and_evaluation(monkeypatch):
    monkeypatch.setenv("PIPEWRIGHT_RELEASE", "abc123")
    r = rt.lineage_record(ML, "r1", "bq://proj-a.ds.entities", {"name": "m", "version": "5"}, EVAL, "gs://b/x/evaluation.json")
    assert r["schema"] == rt.LINEAGE_SCHEMA and r["release"] == "abc123" and r["model"]["version"] == "5"
    assert r["data"]["rows"]["snapshot"] == {"table": "proj-a.ds.raw_snap", "run_id": "r1"}
    assert r["data"]["sources"]["entity_table"] == "proj-a.ds.entities"
    assert r["data"]["sources"]["features"]["sources"] == [{"table": "bq://proj-a.f.usage", "features": ["calls"]}]
    assert r["data"]["sources"]["labels"]["window_days"] == 30
    assert r["evaluation"] == {"record": "gs://b/x/evaluation.json", "decision": "approve", "primary_metric": "auc",
                               "value": 0.81, "champion": {"version": "4", "value": 0.8}, "reasons": []}
    same = rt.lineage_record(dict(reversed(list(ML.items()))), "r1", "bq://proj-a.ds.entities", {}, EVAL)
    assert same["config"]["ml_config_sha256"] == r["config"]["ml_config_sha256"]          # key order does not matter


def test_a_replay_names_the_run_it_replays():
    r = rt.lineage_record(ML, "r2", "snapshot://r1", {"name": "m", "version": "6"}, EVAL)
    assert r["data"]["replay_of"] == "r1" and r["data"]["rows"]["snapshot"]["replay_of"] == "r1"
    assert "entity_table" not in r["data"]["sources"] and "uri" not in r["data"]["sources"]


def test_bqml_lineage_carries_sql_digests_and_rows_table():
    ml = {**ML, "data": {}, "bqml": {"transform": {"sha256": "t1"}, "prepare": {"sha256": "p1"}}}
    r = rt.lineage_record(ml, "r3", "bq://proj-a.ds.e", {"name": "m", "version": "1"}, EVAL, None, "proj-a.ds.rows_r3")
    assert r["config"]["sql"] == {"transform": "t1", "prepare": "p1"}
    assert r["data"]["rows"] == {"table": "proj-a.ds.rows_r3"}


def test_render_follows_a_promoted_version_and_shows_the_rows():
    record = rt.lineage_record(ML, "r1", "bq://proj-a.ds.entities", {"name": "m", "version": "5"}, EVAL)
    text = lineage.render({"model": "m", "version": "2", "environment": "prod", "default": True, "created": "c",
                           "promoted_from": {"environment": "test", "version": "5"}, "lineage": record,
                           "release": {"release_id": "abc", "commit": "deadbeef", "templates": {"t": "sha256:1"}},
                           "snapshot": {"table": "proj-a.ds.raw_snap", "run_id": "r1", "rows": 120, "read_at": "t0"}})
    assert "promoted from test v5" in text and "commit deadbeef" in text
    assert "120 rows, read t0" in text and "features bq://proj-a.f.usage: calls" in text
    assert "vs champion v4 0.8" in text
    old = lineage.render({"model": "m", "version": "1", "environment": "prod", "lineage": None, "note": "no lineage"})
    assert old.endswith("no lineage")
