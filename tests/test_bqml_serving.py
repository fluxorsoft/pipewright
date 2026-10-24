"""BigQuery ML online serving: endpoint scores against ML.PREDICT before a version keeps its traffic."""
import json
from types import SimpleNamespace

import pytest

from pipewright.bqml import serving
from pipewright.errors import ConfigError

LABELS = {"pipewright-engine": "bqml", "pipewright-run": "r1", "pipewright-bq-model": "churn_bqml_r1"}


def uc(task="binary"):
    return SimpleNamespace(name="churn-bqml", platform={"project": "proj-a"},
                           resources={"bigquery_dataset": "pw_churn"},
                           ml={"data": {"label": {"column": "churned", "task": task, "positive": 1}}})


ENV = SimpleNamespace(project="proj-a", security={"bigquery_kms_key": "k"})


class Gcp:
    def get(self, url, **_):
        assert url.endswith("/datasets/pw_churn/models/churn_bqml_r1")
        return SimpleNamespace(ok=True, status=200, body={"featureColumns": [{"name": "tenure"}, {"name": "plan"}]})


def expected(monkeypatch, scores):
    seen = {}

    def query(gcp, project, sql, params, kms_key=None):
        seen["sql"] = sql
        return [{"instance": json.dumps({"tenure": i, "plan": "a"}), "expected": s} for i, s in enumerate(scores)]

    monkeypatch.setattr("pipewright.gcp.bigquery.query", query)
    monkeypatch.setattr("pipewright.gcp.bigquery.rows", lambda result: result)
    return seen


class Endpoint:
    def __init__(self, answers):
        self.answers = list(answers)

    def predict(self, instances):
        model, probs = self.answers.pop(0)
        return SimpleNamespace(deployed_model_id=model, predictions=[
            {"churned_values": ["1", "0"], "churned_probs": [p, 1 - p], "predicted_churned": ["1" if p > .5 else "0"]}
            for p in probs])


def test_the_new_deployment_must_score_as_ml_predict(monkeypatch):
    seen = expected(monkeypatch, [0.9, 0.2])
    endpoint = Endpoint([("old", [0.5, 0.5]), ("new", [0.9 + 5e-9, 0.2])])     # a canary: the old one answers first
    got = serving.parity(Gcp(), uc(), ENV, LABELS, endpoint, "new")
    assert got["ok"] and got["rows"] == 2 and got["max_difference"] < 1e-8
    assert "`proj-a.pw_churn.churn_bqml_bqml_test_r1`" in seen["sql"] and "`tenure`, `plan`" in seen["sql"]
    expected(monkeypatch, [0.9, 0.2])
    assert not serving.parity(Gcp(), uc(), ENV, LABELS, Endpoint([("new", [0.7, 0.2])]), "new")["ok"]


def test_parity_is_never_assumed(monkeypatch):
    expected(monkeypatch, [])
    with pytest.raises(ConfigError, match="no kept test predictions"):
        serving.parity(Gcp(), uc(), ENV, LABELS, Endpoint([]), "new")
    expected(monkeypatch, [0.9])
    with pytest.raises(ConfigError, match="never answered from the new deployed model"):
        serving.parity(Gcp(), uc(), ENV, LABELS, Endpoint([("old", [0.9])] * 3), "new", attempts=3)


def test_endpoint_answers_per_task():
    assert serving.endpoint_value({"predicted_churned": [3.5]}, uc("regression")) == 3.5
    assert serving.endpoint_value({"predicted_churned": ["gold"]}, uc("multiclass")) == "gold"
    assert serving.compare(["a", "b"], ["a", "c"], "multiclass") == 0.5
    with pytest.raises(ConfigError, match="no probability for churned=1"):
        serving.endpoint_value({"churned_values": ["yes", "no"], "churned_probs": [1, 0]}, uc())
