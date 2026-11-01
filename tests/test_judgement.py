"""Judgement checks (Pipewright 1.3.0): valid configuration that probably misleads a model, and the platform's gate
minimums (policies.evaluation.min_gates)."""

from types import SimpleNamespace

from pipewright.config import judgement
from tests.test_config_ml import ml_repo, problems, resolve

COLUMNS = [
    {"name": "customer_id", "type": "string", "required": True},
    {"name": "snapshot_date", "type": "date", "required": True},
    {"name": "tenure_months", "type": "integer"},
    {"name": "churned", "type": "integer"},
]
ENV = SimpleNamespace(name="prod", policies={})


def ml(split=None, include=None, label="churned", task="binary", gates=None, **data):
    return {"data": {"contract": {"columns": COLUMNS}, "label": {"column": label, "task": task},
                     "split": split or {"strategy": "random"}, **data},
            "features": {"include": include if include is not None else ["tenure_months"]},
            "evaluation": {"gates": gates or []}}


def warned(m, env=ENV):
    return judgement.check(m, env)[0]


def test_time_ordered_rows_with_a_random_split_are_warned():
    w = warned(ml())
    assert len(w) == 1 and "ordered in time (snapshot_date)" in w[0] and "strategy: time" in w[0]


def test_a_time_split_or_a_table_without_time_is_fine():
    assert not warned(ml(split={"strategy": "time", "time_column": "snapshot_date"}))
    no_time = ml()
    no_time["data"]["contract"]["columns"] = [c for c in COLUMNS if c["name"] != "snapshot_date"]
    assert not warned(no_time)


def test_labels_from_a_window_are_time_ordered_even_without_a_date_column():
    m = ml(label_source={"table": "bq://p.d.events", "window_days": 90})
    m["data"]["contract"]["columns"] = [c for c in COLUMNS if c["name"] != "snapshot_date"]
    assert any("labels come from a time window" in x for x in warned(m))


def test_unsupervised_and_forecast_models_are_not_asked_for_a_time_split():
    assert not warned(ml(task="clustering"))
    assert not warned(ml(task="forecast", split={"strategy": "time", "time_column": "snapshot_date"}))


def test_a_date_as_a_feature_is_warned():
    w = warned(ml(split={"strategy": "time", "time_column": "snapshot_date"},
                  include=["tenure_months", "snapshot_date"]))
    assert len(w) == 1 and "snapshot_date is a date" in w[0]


def test_the_entitys_cut_off_is_left_to_its_own_warning():
    m = ml(split={"strategy": "time", "time_column": "snapshot_date"}, include=["snapshot_date"],
           features_from={"entity": {"columns": ["customer_id"], "time_column": "snapshot_date"}, "sources": []})
    assert not [x for x in warned(m) if "is a date" in x]


def test_columns_named_after_the_label_look_like_the_outcome():
    cols = COLUMNS + [{"name": "churn_reason", "type": "string"}, {"name": "churn_date", "type": "date"}]
    m = ml(split={"strategy": "time", "time_column": "snapshot_date"}, include=["tenure_months", "churn_reason"])
    m["data"]["contract"]["columns"] = cols
    w = warned(m)
    assert len(w) == 1 and "churn_reason looks like part of the outcome (churned)" in w[0]


def test_short_or_unrelated_labels_raise_nothing():
    m = ml(split={"strategy": "time", "time_column": "snapshot_date"}, label="y", include=["tenure_months", "y_score"])
    assert not warned(m)


def test_gate_minimums_refuse_a_missing_or_weaker_gate():
    env = SimpleNamespace(name="prod", policies={"evaluation": {"min_gates": {"auc": 0.7,
                                                                            "calibration_error": {"max": 0.1}}}})
    m = ml(split={"strategy": "time", "time_column": "snapshot_date"}, gates=[{"metric": "auc", "min": 0.65}])
    probs = judgement.check(m, env)[1]
    assert any("auc with min 0.7" in p and "has min 0.65" in p for p in probs)
    assert any("calibration_error with max 0.1" in p and "no such gate" in p for p in probs)
    ok = ml(split={"strategy": "time", "time_column": "snapshot_date"},
            gates=[{"metric": "auc", "min": 0.75}, {"metric": "calibration_error", "max": 0.05}])
    assert not judgement.check(ok, env)[1]


def test_the_policy_refuses_through_validate(repo):
    ml_repo(repo, evaluation={"primary_metric": "auc", "gates": [{"metric": "auc", "min": 0.6}]})
    assert resolve(repo).ml                                 # no policy: fine
    repo.manifests["dev"]["policies"] = {"evaluation": {"min_gates": {"auc": 0.7}}}
    assert "requires a gate on auc with min 0.7" in problems(repo)


def test_warnings_reach_validate(repo):
    ml_repo(repo)
    repo.usecase["spec"]["data"]["split"] = {"strategy": "random"}
    assert any("(judgement check)" in w for w in resolve(repo).warnings)
