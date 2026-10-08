"""outputs.store (bigquery | gcs | both), the component runtime helper, external tables, events."""

import io
import json

import pytest

from pipewright.config import Table, load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.observability import events
from pipewright.tables import external

FIELDS = [{"name": "run_id", "type": "STRING", "mode": "REQUIRED"}, {"name": "auc", "type": "FLOAT"},
          {"name": "evaluated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}, {"name": "day", "type": "DATE"},
          {"name": "rows", "type": "INTEGER"}, {"name": "alert", "type": "BOOLEAN"}]


def gcs_repo(repo, store="gcs", **gcs):
    repo.manifests["dev"]["tenants"]["churn"]["outputs_uri"] = "gs://acme-ml-dev-artifacts/outputs/churn/"
    repo.usecase["spec"]["outputs"]["store"] = store
    if gcs:
        repo.usecase["spec"]["outputs"]["gcs"] = gcs
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_store_defaults_to_bigquery(repo):
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.outputs["store"] == "bigquery" and uc.outputs["gcs"]["uri"] is None
    assert uc.ml_config()["outputs"]["tables"]["predictions"][0] == {"name": "customer_id", "type": "STRING",
                                                                    "mode": "REQUIRED"}


def test_gcs_store_uses_the_tenant_outputs_location(repo):
    uc = gcs_repo(repo)
    assert uc.outputs["gcs"] == {"format": "parquet", "external_tables": True,
                                 "uri": "gs://acme-ml-dev-artifacts/outputs/churn/churn/"}
    assert uc.ml_config()["platform"]["tenant"] == "churn"


def test_gcs_store_needs_an_outputs_location(repo):
    repo.usecase["spec"]["outputs"]["store"] = "gcs"
    with pytest.raises(ConfigError, match="has no outputs_uri in the manifest"):
        resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_sql_reads_need_external_tables(repo):
    repo.usecase["spec"]["retraining"] = {"enabled": False}
    repo.usecase["spec"]["monitoring"] = {"output_table": "model_metrics", "drift": {"enabled": True}}
    repo.usecase["spec"]["pipelines"] = {"training": repo.usecase["spec"].pop("pipeline"),
                                         "inference": {"file": "pipeline.py", "function": "infer"}}
    with pytest.raises(ConfigError, match="external_tables is false, but pipewright monitor status"):
        gcs_repo(repo, external_tables=False)


def test_external_table_plan_and_resource():
    t = Table("model_metrics", None, FIELDS, None, [], "metrics")
    uri = "gs://b/outputs/churn/churn/"
    assert external.plan(t, None, uri).action == "create"
    live = {"type": "EXTERNAL", "schema": {"fields": FIELDS[:-1] + [{"name": "dt", "type": "DATE"},
                                                                    {"name": "run", "type": "STRING"}]},
            "externalDataConfiguration": {"sourceUris": [f"{uri}model_metrics/*"]}}
    p = external.plan(t, live, uri)
    assert p.action == "update" and p.changes == ["add column alert (BOOLEAN, NULLABLE in older files)"]
    assert external.plan(t, {**live, "schema": {"fields": FIELDS}}, uri).action == "none"
    retyped = {**live, "schema": {"fields": [{"name": "auc", "type": "STRING"}]}}
    assert "column auc changes type STRING -> FLOAT" in external.plan(t, retyped, uri).blockers[0]
    assert external.plan(t, {"type": "TABLE"}, uri).action == "blocked"
    body = external.resource(t, "p", "d", uri, {"pipewright-usecase": "churn"})
    cfg = body["externalDataConfiguration"]
    assert cfg["sourceUris"] == [f"{uri}model_metrics/*"] and cfg["hivePartitioningOptions"]["mode"] == "AUTO"
    assert cfg["autodetect"] is False and body["schema"]["fields"][0]["name"] == "run_id"


def test_schema_seed_is_an_empty_parquet_file():
    pq = pytest.importorskip("pyarrow.parquet")
    data = external.seed_bytes(Table("m", None, FIELDS, None, [], None))
    t = pq.read_table(io.BytesIO(data))
    assert t.num_rows == 0 and t.schema.names == [f["name"] for f in FIELDS]
    assert not t.schema.field("run_id").nullable and str(t.schema.field("evaluated_at").type) == "timestamp[us, tz=UTC]"
    assert external.seed_object("gs://b/o/", "m") == "gs://b/o/m/dt=1970-01-01/run=_schema/part-0.parquet"


def test_runtime_conform_and_layout():
    pd = pytest.importorskip("pandas")
    from pipewright.build.templates import pipewright_runtime as rt

    df = rt.conform(pd.DataFrame([{"run_id": "r1", "auc": "0.8", "evaluated_at": "2026-10-05T10:00:00Z",
                                   "day": "2026-10-04", "rows": "12", "extra": 1}]), FIELDS)
    assert list(df.columns) == [f["name"] for f in FIELDS]           # extra dropped, alert added
    assert df["auc"][0] == 0.8 and int(df["rows"][0]) == 12 and str(df["day"][0]) == "2026-10-04"
    assert df["alert"][0] is None
    with pytest.raises(ValueError, match="run_id is REQUIRED"):
        rt.conform(pd.DataFrame([{"auc": 1.0}]), FIELDS)
    cfg = {"outputs": {"gcs": {"uri": "gs://b/outputs/t/uc/"}}}
    from datetime import datetime, timezone

    when = datetime(2026, 10, 5, tzinfo=timezone.utc)
    assert rt.gcs_object(cfg, "predictions", "r9", when) == "gs://b/outputs/t/uc/predictions/dt=2026-10-05/run=r9/part-0.parquet"


def test_runtime_log_and_event_lines(capsys):
    from pipewright.build.templates import pipewright_runtime as rt

    cfg = {"usecase": "churn", "environment": "dev", "platform": {"tenant": "churn", "events_topic": None}}
    e = rt.emit_event("monitoring.alert", cfg, "r1", max_feature_psi=0.85)
    line = json.loads(capsys.readouterr().out.strip())
    assert line["severity"] == "NOTICE" and line["pipewright_event"]["event_type"] == "monitoring.alert"
    assert line["logging.googleapis.com/labels"] == {"pipewright-usecase": "churn", "pipewright-tenant": "churn",
                                                     "pipewright-environment": "dev", "run-id": "r1"}
    assert e["data"] == {"max_feature_psi": 0.85}


class FakePubSub:
    def __init__(self):
        self.calls = []

    def post(self, url, body):
        self.calls.append((url, body))
        return type("R", (), {"ok": True, "status": 200})()


def test_cli_events_publish_with_attributes():
    fake, out = FakePubSub(), io.StringIO()
    e = events.event("model.approved", "churn", "prod", "churn", None, None, version="4")
    assert events.emit(e, "projects/p/topics/pipewright-events", fake, stream=out) is True
    url, body = fake.calls[0]
    assert url == "https://pubsub.googleapis.com/v1/projects/p/topics/pipewright-events:publish"
    regional = FakePubSub()  # private topics enforce in-region storage: publish to the regional endpoint
    events.emit(e, "projects/p/topics/pipewright-events", regional, stream=io.StringIO(), region="europe-west2")
    assert regional.calls[0][0] == "https://europe-west2-pubsub.googleapis.com/v1/projects/p/topics/pipewright-events:publish"
    assert body["messages"][0]["attributes"] == {"event_type": "model.approved", "usecase": "churn",
                                                 "environment": "prod", "tenant": "churn"}
    assert json.loads(out.getvalue())["pipewright_event"]["data"] == {"version": "4"}
    assert events.emit(e, None, fake, stream=io.StringIO()) is False and len(fake.calls) == 1


def test_bigquery_rows_are_typed():
    from pipewright.gcp import bigquery

    res = {"schema": {"fields": [{"name": "t", "type": "TIMESTAMP"}, {"name": "n", "type": "INTEGER"},
                                 {"name": "x", "type": "FLOAT"}, {"name": "b", "type": "BOOLEAN"},
                                 {"name": "s", "type": "STRING"}]},
           "rows": [{"f": [{"v": "1.791226826589602E9"}, {"v": "3"}, {"v": "0.5"}, {"v": "true"}, {"v": None}]}]}
    assert bigquery.rows(res) == [{"t": "2026-10-05T19:00:26.589602+00:00", "n": 3, "x": 0.5, "b": True, "s": None}]
