"""FR-86 (Pipewright 1.2): machine-readable fleet state (--json on status, model list, monitor status, retrain list)
and BigQuery ML runs in a metrics table (bqml.metrics_table), with the container examples' column names."""
import json
import sys
from pathlib import Path

from pipewright import bqml
from pipewright.bqml.config import METRICS_COLUMNS
from pipewright.cli.main import build_parser
from tests.test_bqml_config import bqml_repo, problems, resolve, write

sys.path.insert(0, str(bqml.STEPS_DIR))
import bqml_runtime as rt  # noqa: E402

CHURN_METRICS = Path(__file__).parent.parent / "examples" / "churn" / "schemas" / "model_metrics.json"


def with_metrics(repo, schema):
    bqml_repo(repo, metrics_table="metrics")
    repo.usecase["spec"]["outputs"]["bigquery"]["tables"]["metrics"] = "schemas/metrics.json"
    path = write(repo)
    (path.parent / "schemas" / "metrics.json").write_text(json.dumps(schema), encoding="utf-8")
    return repo


def test_the_metrics_row_matches_the_churn_examples_table():
    churn = {f["name"] for f in json.loads(CHURN_METRICS.read_text())}
    assert set(METRICS_COLUMNS) <= churn
    ml = {"evaluation": {"primary_metric": "auc"}, "bqml": {"registry_id": "churn-bqml"}, "environment": "dev"}
    record = {"decision": "reject", "reasons": ["auc 0.61 < 0.7"], "metrics": {"auc": 0.61},
              "champion": {"auc": 0.74, "_version": "3"}, "rows": {"test": 200}, "training_stats": {"rows": 800}}
    row = rt.metrics_row(ml, "r9", "p.d.churn_bqml_rows_r9", record, "2026-10-31T10:00:00+00:00")
    assert tuple(row) == METRICS_COLUMNS
    assert row["source_uri"] == "bq://p.d.churn_bqml_rows_r9" and row["model_display_name"] == "churn-bqml"
    assert (row["train_rows"], row["test_rows"], row["primary_value"]) == (800, 200, 0.61)
    assert (row["champion_version"], row["champion_value"]) == ("3", 0.74)
    assert json.loads(row["decision_reasons"]) == ["auc 0.61 < 0.7"]


def test_the_metrics_table_is_checked(repo):
    churn = json.loads(CHURN_METRICS.read_text())
    assert resolve(with_metrics(repo, churn)).ml["bqml"]["metrics_table"] == "metrics"
    short = [f for f in churn if f["name"] != "champion_value"]
    assert "the schema lacks champion_value" in problems(with_metrics(repo, short))


def test_a_1_x_bqml_use_case_has_no_metrics_table(repo):
    assert "metrics_table" not in resolve(bqml_repo(repo)).ml["bqml"]


def test_each_fleet_command_takes_json():
    parser = build_parser()
    for argv in (["status", "--env", "dev", "--json"],
                 ["model", "list", "--env", "dev", "--usecase", "churn", "--json"],
                 ["monitor", "status", "--env", "dev", "--usecase", "churn", "--json"],
                 ["retrain", "list", "--env", "dev", "--usecase", "churn", "--json"]):
        assert parser.parse_args(argv).json is True, argv


def test_status_json(repo, monkeypatch, capsys):
    import importlib
    from types import SimpleNamespace

    from pipewright.release import service
    from pipewright.release.contract import Pointer

    main = importlib.import_module("pipewright.cli.main")
    repo.write()
    pointer = Pointer("churn", "abc123", "contracts/churn/abc123.json", "2026-10-31T09:00:00Z", "ci@acme",
                      [{"release_id": "old1", "updated_at": "2026-10-30T09:00:00Z"}], {"via": "github", "actor": "a"})
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda *a, **k: None)
    monkeypatch.setattr(service, "read_pointer", lambda gcp, env, name: (pointer, 1))
    monkeypatch.setattr(service, "read_contract", lambda gcp, env, name: SimpleNamespace(
        contract_version=3, pipeline_names=["training", "inference"]))
    assert main.main(["status", "--root", str(repo.root), "--env", "dev", "--json"]) == 0
    (row,) = json.loads(capsys.readouterr().out)
    assert row["usecase"] == "churn" and row["release"] == "abc123" and row["approval"]["via"] == "github"
    assert row["pipelines"] == ["training", "inference"] and row["history"][0]["release_id"] == "old1"
