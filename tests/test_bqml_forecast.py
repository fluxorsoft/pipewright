"""BigQuery ML forecasting (Pipewright 1.1): ARIMA_PLUS, and AI.FORECAST where the platform allows it."""
import json

from pipewright.bqml.steps import bqml_runtime as rt
from tests.test_bqml_config import bqml_repo, problems, resolve

SERIES = {"columns": [{"name": "store", "type": "string"}, {"name": "day", "type": "date"},
                      {"name": "units", "type": "float", "min": 0}], "min_rows": 50}
FORECASTS = [{"name": n, "type": t} for n, t in (
    ("store", "STRING"), ("forecast_timestamp", "TIMESTAMP"), ("forecast_value", "FLOAT64"),
    ("prediction_interval_lower_bound", "FLOAT64"), ("prediction_interval_upper_bound", "FLOAT64"),
    ("model_version_id", "STRING"), ("run_id", "STRING"), ("scored_at", "TIMESTAMP"))]


def forecast_repo(repo, **forecast):
    bqml_repo(repo)
    spec = repo.usecase["spec"]
    spec["bqml"] = {"forecast": {"horizon": 14, "id_columns": ["store"], **forecast}}
    spec["data"] = {"contract": SERIES, "label": {"column": "units", "task": "forecast"},
                    "split": {"strategy": "time", "time_column": "day", "test_fraction": 0.1}}
    spec["data_protection"] = {}
    spec.pop("evaluation", None)
    spec["monitoring"] = {"prediction_drift": {"enabled": True}, "output_table": "model_metrics"}
    spec["model"]["serving"] = {"enabled": False}
    return repo


def test_arima_plus_resolves_from_the_forecast_section(repo, monkeypatch):
    forecast_repo(repo)
    monkeypatch.setattr("tests.test_bqml_config.SCORES", FORECASTS)
    uc = resolve(repo)
    section, ev = uc.ml["bqml"], uc.ml["evaluation"]
    assert section["model_type"] == "arima_plus" and not section["explain"]
    assert section["forecast"] == {"engine": "arima_plus", "horizon": 14, "id_columns": ["store"],
                                   "confidence_level": 0.95}
    assert ev["primary_metric"] == "mae" and ev["metrics"] == ["mae", "mape", "rmse"]
    assert not ev["champion"]["enabled"] and uc.ml["features"]["include"] == []
    assert uc.model["promotion"] == "retrain"


def test_what_a_forecast_cannot_use(repo, monkeypatch):
    monkeypatch.setattr("tests.test_bqml_config.SCORES", FORECASTS)
    forecast_repo(repo)
    spec = repo.usecase["spec"]
    spec["evaluation"] = {"champion": {"enabled": True}}
    spec["monitoring"]["drift"] = {"enabled": True}
    spec["environments"]["dev"]["model"]["serving"] = {"enabled": True, "target": "vertex"}
    text = problems(repo)
    assert "ARIMA_PLUS forecasts from the end of its own training data" in text
    assert "monitoring.drift: a forecast has no features" in text
    assert "model.serving: a forecast is not served online" in text
    forecast_repo(repo)
    repo.usecase["spec"]["data"]["split"] = {"strategy": "random"}
    assert "a forecast holds out the latest period" in problems(repo)
    forecast_repo(repo)
    repo.usecase["spec"]["bqml"]["forecast"]["id_columns"] = ["region"]
    assert "bqml.forecast.id_columns: column 'region' is not in data.contract.columns" in problems(repo)


def test_ai_forecast_needs_the_platforms_switch(repo, monkeypatch):
    """C-08's one exception: off unless the environment's policies.bqml.ai_forecast is true."""
    monkeypatch.setattr("tests.test_bqml_config.SCORES", FORECASTS)
    forecast_repo(repo, engine="ai_forecast")
    assert "an exception to C-08 that dev has not switched on (platform policy policies.bqml.ai_forecast)" in \
        problems(repo)
    repo.manifests["dev"]["policies"] = {"bqml": {"ai_forecast": True, "allowed_model_types": ["logistic_reg"]}}
    section = resolve(repo).ml["bqml"]
    assert section["model_type"] == "ai_forecast" and section["forecast"]["model"] == "TimesFM 2.5"
    repo.usecase["spec"]["bqml"].update({"model_type": "arima_plus", "options": {"auto_arima": True}})
    text = problems(repo)
    assert "AI.FORECAST trains no model; remove model_type" in text
    assert "bqml.options.auto_arima: AI.FORECAST takes its settings from bqml.forecast" in text


def test_the_forecast_section_is_for_forecasts_only(repo):
    bqml_repo(repo)
    repo.usecase["spec"]["bqml"]["forecast"] = {"horizon": 7}
    assert "bqml.forecast: for data.label.task forecast; this use case is binary" in problems(repo)


ML = {"usecase": "demand", "platform": {"bigquery_dataset": "pw", "region": "us-central1"},
      "data": {"label": {"column": "units", "task": "forecast"},
               "split": {"strategy": "time", "time_column": "day", "test_fraction": 0.1}},
      "features": {"include": [], "exclude": []}, "inference": {},
      "bqml": {"model_type": "arima_plus", "options": {"data_frequency": "DAILY"}, "registry_id": "demand",
               "records": "gs://b/pipeline-root/t/demand/bqml/",
               "forecast": {"engine": "arima_plus", "horizon": 14, "id_columns": ["store"],
                            "confidence_level": 0.9}},
      "evaluation": {"metrics": ["mae", "rmse"], "slices": []}}
AI = {**ML["bqml"]["forecast"], "engine": "ai_forecast", "model": "TimesFM 2.5", "context_window": 512}


def test_arima_plus_trains_on_the_series_before_the_held_out_period():
    sql = rt.forecast_create_sql(ML, "p.d.m", "p.d.rows", 30)
    assert "model_type = 'arima_plus'" in sql and "time_series_timestamp_col = 'day'" in sql
    assert "time_series_data_col = 'units'" in sql and "time_series_id_col = 'store'" in sql
    assert "horizon = 30" in sql and "data_frequency = 'DAILY'" in sql          # covers the held-out period
    assert "SELECT `store`, `day`, `units`\nFROM `p.d.rows`\nWHERE NOT _pw_test" in sql
    assert "horizon = 14" in rt.forecast_create_sql(ML, "p.d.m", "p.d.rows", 5)


def test_forecast_sql_for_each_engine():
    arima = rt.forecast_sql(ML, ML["bqml"]["forecast"], "p.d.m", "p.d.rows", "TRUE", 14)
    assert "ML.FORECAST(MODEL `p.d.m`, STRUCT(14 AS horizon, 0.9 AS confidence_level))" in arima
    assert arima.startswith("-- ARIMA_PLUS") and "SELECT `store`, forecast_timestamp" in arima
    ai = rt.forecast_sql(ML, AI, "", "p.d.rows", "NOT _pw_test", 30)
    assert "(SELECT `store`, `day`, `units` FROM `p.d.rows` WHERE NOT _pw_test)" in ai
    assert "data_col => 'units', timestamp_col => 'day', model => 'TimesFM 2.5', id_cols => ['store']," in ai
    assert "horizon => 30, confidence_level => 0.9, context_window => 512)" in ai
    assert "ai_forecast_status AS _pw_status" in ai


def test_evaluation_joins_the_forecast_to_what_happened():
    sql = rt.forecast_table_sql(ML, "SELECT 1", "p.d.test", {"a": "b"}, "p.d.rows")
    assert "SELECT f.*, f.forecast_value AS _pw_prediction, a.`units`" in sql
    assert "ON a._pw_t = f.forecast_timestamp AND a.`store` = f.`store`" in sql
    assert "CAST(`day` AS TIMESTAMP) AS _pw_t, `units` FROM `p.d.rows` WHERE _pw_test" in sql
    scoring = rt.forecast_table_sql(ML, "SELECT 1", "p.d.scored", {})
    assert "JOIN" not in scoring and scoring.rstrip().endswith("FROM f")


def test_evaluate_forecast_measures_the_held_out_period():
    calls = []

    class Jobs:
        labels = {}

        def run(self, sql, what):
            calls.append(what)
            if what == "held-out period":
                return [{"steps": 30}]
            if what == "forecast status":
                return [{"failed": 0, "status": None}]
            if what == "metrics":
                return [{"row_count": 60, "mae": 1.5, "rmse": 2.25}]
            return []

    got = rt.evaluate_forecast(Jobs(), ML, ML["bqml"]["forecast"], "p.d.m", "p.d.rows", "p.d.test")
    assert got == {"metrics": {"mae": 1.5, "rmse": 2.25}, "rows": 60, "slices": {}, "horizon": 30}
    assert calls[:2] == ["held-out period", "forecast of the held-out period"]


def test_a_failed_series_fails_the_forecast():
    class Jobs:
        def run(self, sql, what):
            return [{"failed": 3, "status": "The time series data is too short."}]

    try:
        rt.forecast_failures(Jobs(), "p.d.t")
    except RuntimeError as e:
        assert "failed for 3 rows: The time series data is too short." in str(e)
    else:
        raise AssertionError("no error")


def test_an_ai_forecast_version_has_no_model_files():
    class Response:
        def __init__(self, status, body):
            self.status_code, self._body, self.text = status, body, json.dumps(body)

        def json(self):
            return self._body

    sent = []

    class Session:
        def get(self, url, timeout):
            if url.endswith("/models/demand"):
                return Response(404, {})
            return Response(200, {"done": True, "response": {"model": "m", "modelVersionId": "1"}})

        def post(self, url, json, timeout):
            sent.append((url, json))
            return Response(200, {"name": "projects/p/locations/r/operations/1"})

    version = rt.upload_version("p", "us-central1", "demand", {"pipewright-engine": "ai-forecast"}, "settings",
                                "k", session=Session())
    url, body = sent[0]
    assert version == "1" and url.endswith("/locations/us-central1/models:upload")
    assert body == {"model": {"displayName": "demand", "description": "settings", "versionDescription": "settings",
                              "labels": {"pipewright-engine": "ai-forecast"},
                              "encryptionSpec": {"kmsKeyName": "k"}}, "modelId": "demand"}
    assert "containerSpec" not in body["model"] and "artifactUri" not in body["model"]


def test_a_forecast_has_no_features_to_take_from_feature_tables(repo, monkeypatch):
    """features.required: every model feature from a feature table; a forecast has none."""
    monkeypatch.setattr("tests.test_bqml_config.SCORES", FORECASTS)
    forecast_repo(repo)
    repo.manifests["dev"]["features"] = {"required": True}
    assert resolve(repo).ml["features"]["include"] == []


def test_arima_plus_is_refit_on_the_whole_series_before_registration():
    """Backtest, then refit: the registered model forecasts from the latest data, not from the held-out period."""
    sql = rt.forecast_create_sql(ML, "p.d.m_refit", "p.d.rows", 14, refit=True)
    assert sql.rstrip().endswith("SELECT `store`, `day`, `units`\nFROM `p.d.rows`") and "_pw_test" not in sql
    assert "horizon = 14" in sql and "data_frequency = 'DAILY'" in sql


def test_a_bqml_version_gets_a_description_like_a_container_one():
    """model list shows 'run <id>, <metric>=<value>' for BigQuery ML versions too (found in the 1.1.0 QA)."""
    sent = []

    class Session:
        def get(self, url, timeout):
            return type("R", (), {"json": lambda self: {"labels": {"a": "1"}}})()

        def patch(self, url, params, json, timeout):
            sent.append((params, json))
            return type("R", (), {"status_code": 200, "text": ""})()

    rt.label_version("p", "r", "m", "2", {"b": "2"}, session=Session(), description="run r1, auc=0.8100")
    rt.label_version("p", "r", "m", "2", {"c": "3"}, session=Session())
    assert sent[0] == ({"updateMask": "labels,versionDescription"},
                       {"labels": {"a": "1", "b": "2"}, "versionDescription": "run r1, auc=0.8100"})
    assert sent[1] == ({"updateMask": "labels"}, {"labels": {"a": "1", "c": "3"}})
