"""Regression and multi-class use cases (data.label.task, FR-52): metrics, defaults and directions."""
from pipewright.models import gates
from tests.test_config_ml import ml_repo, problems, resolve
from tests.test_synthetic import resolve as syn_resolve
from tests.test_synthetic import synthetic_repo


def task_repo(repo, task, **evaluation):
    ml_repo(repo)
    spec = repo.usecase["spec"]
    spec["data"]["label"] = {"column": "churned", "task": task}
    if evaluation:
        spec["evaluation"] = evaluation
    return repo


def test_a_1_0_use_case_resolves_as_binary_unchanged(repo):
    ml = resolve(ml_repo(repo)).ml
    assert "task" not in ml["data"]["label"]
    assert ml["evaluation"]["primary_metric"] == "auc" and ml["evaluation"]["lower_is_better"] == [
        "calibration_error", "log_loss"]
    assert ml["training"]["tuning"]["goal"] == "maximize"


def test_regression_defaults_and_direction(repo):
    ml = resolve(task_repo(repo, "regression")).ml
    ev = ml["evaluation"]
    assert ev["primary_metric"] == "rmse" and ev["metrics"] == ["mae", "r2", "rmse"]
    assert ev["lower_is_better"] == ["mae", "rmse"] and ev["champion"]["metric"] == "rmse"
    assert ml["training"]["tuning"]["goal"] == "minimize"
    assert ml["monitoring"]["performance"]["metric"] == "rmse"


def test_regression_split_defaults_to_random_and_refuses_stratified(repo):
    task_repo(repo, "regression")
    del repo.usecase["spec"]["data"]["split"]
    assert resolve(repo).ml["data"]["split"]["strategy"] == "random"
    repo.usecase["spec"]["data"]["split"] = {"strategy": "stratified"}
    assert "strategy stratified needs a class label" in problems(repo)


def test_metrics_must_belong_to_the_task(repo):
    task_repo(repo, "regression", primary_metric="auc", gates=[{"metric": "f1_macro", "min": 0.5}])
    text = problems(repo)
    assert "evaluation.primary_metric 'auc' is not a regression metric" in text
    assert "evaluation.gates: 'f1_macro' is not a regression metric" in text
    task_repo(repo, "binary", primary_metric="rmse")
    assert "evaluation.primary_metric 'rmse' is not one of" in problems(repo)       # the 1.0 message
    task_repo(repo, "ordinal")
    assert "spec.data.label.task: 'ordinal' is not one of" in problems(repo)          # the schema refuses it


def test_multiclass_uses_its_metrics_and_given_values(repo):
    ev = resolve(task_repo(repo, "multiclass")).ml["evaluation"]
    assert ev["primary_metric"] == "f1_macro" and "balanced_accuracy" in ev["metrics"]
    assert ev["lower_is_better"] == ["log_loss"]
    ev = resolve(task_repo(repo, "multiclass", primary_metric="accuracy", metrics=["recall_macro"])).ml["evaluation"]
    assert ev["metrics"] == ["accuracy", "recall_macro"]


def test_gates_check_lower_is_better_metrics_as_maximums(repo):
    ev = resolve(task_repo(repo, "regression", gates=[{"metric": "rmse", "max": 2.0}])).ml["evaluation"]
    assert gates.check({"metrics": {"rmse": 1.5}}, ev) == []
    assert gates.check({"metrics": {"rmse": 2.5}}, ev) == ["gate rmse: 2.5000 is above the maximum 2.0"]


def test_synthetic_utility_gate_is_binary_only(repo):
    synthetic_repo(repo)
    assert "tstr_drop" in syn_resolve(repo).ml["synthetic"]["gates"]
    repo.usecase["spec"]["data"]["label"]["task"] = "regression"
    repo.usecase["spec"]["data"]["split"]["strategy"] = "time"
    assert "tstr_drop" not in syn_resolve(repo).ml["synthetic"]["gates"]
