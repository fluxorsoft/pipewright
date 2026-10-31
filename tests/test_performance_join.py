"""FR-78: performance monitoring joins each outcome to the score made for its cut-off (Pipewright 1.2)."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("kfp")

ROOT = Path(__file__).parents[1]
COMPONENTS = ROOT / "examples" / "churn" / "components"
RUNTIME = ROOT / "src" / "pipewright" / "build" / "templates"


@pytest.fixture
def monitor(monkeypatch):
    for p in (COMPONENTS, RUNTIME):
        monkeypatch.syspath_prepend(str(p))
    sys.modules.pop("monitor", None)
    import monitor as module                                     # noqa: PLC0415 - the example's component
    return module


def run_check(monitor, tmp_path, monkeypatch, labels, data, outcomes):
    """Runs the example's monitoring step with a fake BigQuery that answers from `outcomes`."""
    scored = tmp_path / "scored.parquet"
    pd.DataFrame({"customer_id": ["c1"], "churn_probability": [0.4]}).to_parquet(scored)
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    queries = []

    class Job:
        def __init__(self, sql):
            queries.append(sql)

        def to_dataframe(self):
            return outcomes

    import pipewright_runtime
    monkeypatch.setattr(pipewright_runtime, "bigquery_client",
                        lambda project, cfg: SimpleNamespace(query=lambda sql, job_config=None: Job(sql)))
    ml_config = {"monitoring": {"drift": {"enabled": False}, "prediction_drift": {"enabled": False},
                                "performance": {"enabled": True, "labels": labels, "metric": "auc", "min": 0.7}},
                 "inference": {"output_table": "predictions", "decision_threshold": 0.5}, "data": data}
    report = SimpleNamespace(path=str(tmp_path / "report.json"))
    metrics = SimpleNamespace(log_metric=lambda *a: None)
    out = monitor.check_batch.python_func("p", "d", SimpleNamespace(path=str(scored)), SimpleNamespace(
        path=str(model_dir)), "10", ml_config, report, metrics)
    return queries[-1], json.loads(out.summary)


OUTCOMES = pd.DataFrame({"prob": [0.1, 0.9] * 30, "outcome": [0, 1] * 30})
CUTOFF_DATA = {"features_from": {"entity": {"columns": ["customer_id"], "time_column": "snapshot_date"}}}


def test_with_a_time_column_outcomes_meet_the_score_for_their_cut_off(monitor, tmp_path, monkeypatch):
    sql, summary = run_check(monitor, tmp_path, monkeypatch,
                             {"table": "outcomes", "key": "customer_id", "label": "returned",
                              "time_column": "scored_for"}, CUTOFF_DATA, OUTCOMES)
    assert "DATE(lb.scored_for) = pr.scored_for" in sql and "lb.customer_id = pr.customer_id" in sql
    assert "PARTITION BY pr.customer_id, pr.scored_for" in sql
    assert summary["performance"]["auc"] == 1.0 and not any("key only" in n for n in summary["notes"])


def test_without_one_the_old_join_is_kept_and_the_row_says_why(monkeypatch, monitor, tmp_path):
    sql, summary = run_check(monitor, tmp_path, monkeypatch,
                             {"table": "outcomes", "key": "customer_id", "label": "returned"}, CUTOFF_DATA, OUTCOMES)
    assert "USING (customer_id)" in sql and "PARTITION BY customer_id ORDER BY pr.scored_at DESC" in sql
    assert any("joined by key only" in n for n in summary["notes"])


def test_data_without_a_cut_off_is_joined_by_key_without_a_note(monkeypatch, monitor, tmp_path):
    _, summary = run_check(monitor, tmp_path, monkeypatch,
                           {"table": "outcomes", "key": "customer_id", "label": "churned"}, {}, OUTCOMES)
    assert not summary["notes"]


def test_validate_warns_only_for_data_with_a_cut_off(repo):
    from tests.test_config_ml import ml_repo, resolve

    perf = {"enabled": True, "labels": {"table": "predictions", "key": "customer_id", "label": "churned"}}
    ml_repo(repo, monitoring={"performance": perf, "output_table": "model_metrics"},
            inference={"output_table": "predictions"})
    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"), "inference": {"file": "pipeline.py", "function": "infer"}}
    data = spec["data"]
    cutoff = (data.get("split") or {}).get("time_column")
    if cutoff:                                                  # make the fixture's data have no cut-off first
        data["split"] = {"strategy": "random"}
    assert not resolve(repo).warnings
    data["split"] = {"strategy": "time", "time_column": "snapshot_date"}
    assert any("by key only" in w for w in resolve(repo).warnings)
    spec["monitoring"]["performance"]["labels"]["time_column"] = "snapshot_date"
    assert not resolve(repo).warnings
