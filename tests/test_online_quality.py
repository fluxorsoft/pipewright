"""Live quality (FR-98 stage 3, 1.4.3): online predictions recorded by prediction ID, outcomes joined to them, the
monitor step's online mode."""

from types import SimpleNamespace

from pipewright.build.templates import pipewright_runtime as rt

ML = {"usecase": "churn-kq", "platform": {"bigquery_dataset": "pw_churn", "bigquery_kms_key": "k1"},
      "serving_capture": {"retention_days": 7},
      "monitoring": {"online": {"outcomes": {"table": "churn_outcomes_online", "key": "prediction_id",
                                             "label": "churned"}}}}


class Client:
    def __init__(self):
        self.loaded = None

    def load_table_from_json(self, data, table, job_config=None):
        self.loaded = (data, table, job_config)
        return SimpleNamespace(result=lambda: None)


def test_only_predictions_with_an_id_are_recorded_and_expire():
    c = Client()
    rows = [{"a": 1.0, "prediction": 0.7, "_pw_prediction_id": "p1"}, {"a": 2.0, "prediction": 0.2}]
    assert rt.record_online_predictions(ML, "acme", rows, "3", "r1", client=c) == 1
    data, table, cfg = c.loaded
    assert table == "acme.pw_churn.churn_kq_online_predictions"
    assert data == [{"prediction_id": "p1", "model_version": "3", "prediction": 0.7,
                     "recorded_at": data[0]["recorded_at"], "run_id": "r1"}]
    assert cfg.time_partitioning.expiration_ms == 7 * 86400 * 1000
    assert cfg.destination_encryption_configuration.kms_key_name == "k1"
    assert "a" not in data[0]                                          # no features kept
    assert rt.record_online_predictions(ML, "acme", [{"prediction": 0.1}], "3", "r1", client=c) == 0


def test_the_outcomes_join():
    sql = rt.online_outcomes_sql(ML, "acme")
    assert "acme.pw_churn.churn_kq_online_predictions" in sql and "acme.pw_churn.churn_outcomes_online" in sql
    assert "lb.prediction_id = pr.prediction_id" in sql and "pr.model_version = @version" in sql
    assert rt.online_outcomes_sql({**ML, "monitoring": {"online": {}}}, "acme") is None


def test_the_monitor_step_takes_online_and_defaults_to_batch():
    import inspect
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parents[1] / "examples/churn/components"))
    import monitor

    params = inspect.signature(monitor.check_batch.python_func).parameters
    assert params["online"].default is False
