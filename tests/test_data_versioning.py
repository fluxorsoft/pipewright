"""Per-run data versioning (data.snapshot_table, D-110) and the label maturity window (data.label_source, D-111)."""
import pytest

from pipewright.build.templates import pipewright_runtime as rt
from pipewright.errors import ConfigError
from pipewright.runs import launch, sources
from tests.test_config_ml import ml_repo, problems, resolve

LABELS = {"table": "bq://sales.purchases", "time_column": "purchased_at", "window_days": 20}


# ------------------------------------------------------------- label window --

def test_label_query_counts_events_within_the_window_and_drops_open_windows():
    spec = {**LABELS, "key": ["customer_id"], "cutoff_column": "snapshot_date"}
    sql = rt.label_query("p.d.signups", spec, "churned", "2026-10-01T00:00:00+00:00", "p")
    assert "FROM `p.sales.purchases` AS l WHERE l.`customer_id` = e.`customer_id`" in sql
    # an event at or after the cut-off and before cut-off + 20 days makes the row a 1
    assert ("CAST(l.`purchased_at` AS TIMESTAMP) >= CAST(e.`snapshot_date` AS TIMESTAMP) AND "
            "CAST(l.`purchased_at` AS TIMESTAMP) < TIMESTAMP_ADD(CAST(e.`snapshot_date` AS TIMESTAMP), "
            "INTERVAL 20 DAY)), 1, 0) AS `churned`") in sql
    # rows whose window ends after the newest event are dropped, 1s and 0s alike, anchored on that event
    assert ("WHERE TIMESTAMP_ADD(CAST(e.`snapshot_date` AS TIMESTAMP), INTERVAL 20 DAY) <= "
            "TIMESTAMP '2026-10-01T00:00:00+00:00'") in sql


def test_label_source_takes_key_and_cut_off_from_the_use_case(repo):
    ml_repo(repo)
    spec = repo.usecase["spec"]
    spec["data"]["label_source"] = dict(LABELS)
    spec["data"]["contract"] = {**spec["data"]["contract"], "unique": []}   # a copy: CONTRACT is shared
    with pytest.raises(ConfigError, match="data.label_source.key"):
        resolve(repo)
    spec["data"]["label_source"]["key"] = ["customer_id"]
    ls = resolve(repo).ml["data"]["label_source"]
    assert ls["key"] == ["customer_id"] and ls["cutoff_column"] == "snapshot_date"   # from data.split.time_column


def test_label_source_is_checked(repo):
    ml_repo(repo)
    data = repo.usecase["spec"]["data"]
    data["label_source"] = {**LABELS, "key": ["nope"], "cutoff_column": "tenure_months"}
    text = problems(repo)
    assert "data.label_source.key: column 'nope' is not in data.contract.columns" in text
    assert "data.label_source.cutoff_column 'tenure_months' must be a date or timestamp column" in text
    data["label_source"] = {**LABELS, "key": ["churned"]}
    assert "data.label_source.key: 'churned' is the label" in problems(repo)


def test_read_source_applies_labels_only_when_asked(monkeypatch):
    called = []
    monkeypatch.setattr(rt, "bigquery_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(rt, "_labelled_entities", lambda *a, **k: called.append(a[1]) or "p.anon.t")
    ml_config = {"data": {"label": {"column": "churned"}, "label_source": {**LABELS, "key": ["customer_id"],
                                                                          "cutoff_column": "snapshot_date"}}}
    rt.read_source("bq://d.signups", "p", ml_config)                   # scoring and 1.0 callers: no label step
    assert called == []
    rt.read_source("bq://d.signups", "p", ml_config, labels=True)
    assert called == ["p.d.signups"]


# ------------------------------------------------------------------ snapshot --

def test_snapshot_table_must_be_an_output_table(repo):
    ml_repo(repo)
    repo.usecase["spec"]["data"]["snapshot_table"] = "raw_customers"
    assert "data.snapshot_table 'raw_customers' is not in outputs.bigquery.tables" in problems(repo)
    repo.usecase["spec"]["data"]["snapshot_table"] = "predictions"
    assert resolve(repo).ml["data"]["snapshot_table"] == "predictions"


def test_a_1_0_use_case_resolves_without_the_new_keys(repo):
    data = resolve(ml_repo(repo)).ml["data"]
    assert "snapshot_table" not in data and "label_source" not in data


def test_snapshot_sources_only_where_allowed():
    assert sources.snapshot_uri("r1f4397341600") == "snapshot://r1f4397341600"
    with pytest.raises(ValueError):
        sources.snapshot_uri("bad id!")
    assert sources.problem("snapshot://r1") is not None                    # schedules, retrain, --source-uri
    assert sources.problem("snapshot://r1", allow_snapshot=True) is None   # only pipewright run --replay
    assert launch.body("churn", "training", "snapshot://r1", None, None, replay=True)["source_uri"] == "snapshot://r1"
    with pytest.raises(ConfigError):
        launch.body("churn", "training", "snapshot://r1", None, None)


def test_write_snapshot_tags_the_rows_and_records_them(monkeypatch):
    pd = pytest.importorskip("pandas")
    written, events = [], []
    monkeypatch.setattr(rt, "write_table", lambda table, df, *a: written.append((table, df)) or [f"bq://p.d.{table}"])
    monkeypatch.setattr(rt, "emit_event", lambda kind, cfg, rid, **data: events.append((kind, data)))
    fields = [{"name": n} for n in ("customer_id", "run_id", "source_uri", "ingested_at")]
    cfg = {"usecase": "churn", "data": {"snapshot_table": "raw"}, "outputs": {"tables": {"raw": fields}}}
    rt.write_snapshot(pd.DataFrame({"customer_id": ["a", "b"]}), cfg, "p", "d", "r9", "bq://p.d.signups")
    table, df = written[0]
    assert table == "raw" and set(df["run_id"]) == {"r9"} and set(df["source_uri"]) == {"bq://p.d.signups"}
    assert events[0] == ("data.snapshot.written", {"table": "raw", "rows": 2, "source_uri": "bq://p.d.signups",
                                                   "locations": ["bq://p.d.raw"]})
    cfg["outputs"]["tables"]["raw"] = fields[:1]
    with pytest.raises(ValueError, match="needs the columns run_id, source_uri, ingested_at"):
        rt.write_snapshot(pd.DataFrame({"customer_id": ["a"]}), cfg, "p", "d", "r9", "bq://p.d.signups")


def test_replay_reads_exactly_that_runs_rows(monkeypatch):
    pd = pytest.importorskip("pandas")
    client = FakeClient(pd.DataFrame({"customer_id": ["a"], "tenure_months": [3]}))
    monkeypatch.setattr(rt, "bigquery_client", lambda *a, **k: client)
    events = []
    monkeypatch.setattr(rt, "emit_event", lambda kind, cfg, rid, **data: events.append((kind, data)))
    cfg = {"usecase": "churn", "data": {"snapshot_table": "raw", "features_from": {"entity": {}}},
           "platform": {"bigquery_dataset": "churn"}}
    df = rt.read_source("snapshot://r9", "p", cfg, "r10", labels=True)    # no feature or label join on replay
    assert list(df["customer_id"]) == ["a"]
    assert client.sql == "SELECT `customer_id`, `tenure_months` FROM `p.churn.raw` WHERE `run_id` = @run_id"
    assert events[0][0] == "data.snapshot.replayed" and events[0][1]["replayed_run"] == "r9"
    with pytest.raises(ValueError, match="not snapshot://<run id>"):
        rt.read_source("snapshot://bad id", "p", cfg)


class FakeClient:
    def __init__(self, df=None):
        self.df, self.sql = df, None

    def get_table(self, ref):
        class T:
            schema = [type("F", (), {"name": n}) for n in ("customer_id", "tenure_months", *rt.SNAPSHOT_COLUMNS)]
        return T()

    def query(self, sql, job_config=None):
        self.sql, df = sql, self.df

        class Job:
            job_id = "job-1"

            def to_dataframe(self, **_):
                import pandas as pd

                return df if df is not None else pd.DataFrame({"customer_id": []})
        return Job()
