import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.data import erase
from pipewright.errors import ConfigError
from pipewright.models.deploy import check_approval
from pipewright.testing import runner


def uc_for(repo, **spec_changes):
    repo.usecase["spec"].update(spec_changes)
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_erase_targets_tables_with_the_key(repo, tmp_path):
    uc = uc_for(repo, data_protection={"personal_data_columns": [], "erasure": {"key_column": "customer_id"}})
    key, tables = erase.targets(uc)
    assert key == "customer_id" and tables == [("predictions", "STRING")]     # model_metrics has no customer_id
    f = tmp_path / "keys.txt"
    f.write_text("# subjects\nC1\nC2\n\nC1\n", encoding="utf-8")
    assert erase.read_keys(["C3"], str(f)) == ["C1", "C2", "C3"]
    with pytest.raises(ConfigError, match="give the keys"):
        erase.read_keys(None, None)
    text = erase.render(uc, [erase.EraseResult("predictions", 3, False)], 2, confirm=False)
    assert "predictions: 3 row(s) would be deleted" in text and "C1" not in text and "--confirm" in text


def test_erase_needs_the_section(repo):
    with pytest.raises(ConfigError, match="no data_protection.erasure"):
        erase.targets(uc_for(repo))


class FakeGcp:
    """Answers BigQuery jobs.query with a count, then a DML result."""

    def __init__(self):
        self.queries = []

    def identity(self):
        return "tester@acme.example"

    def get(self, url, **params):
        return type("R", (), {"ok": True, "body": {"location": "europe-west2"}})()

    def post(self, url, body):
        self.queries.append(body)
        result = {"jobComplete": True, "rows": [{"f": [{"v": "4"}]}]}
        if body["query"].startswith("DELETE"):
            result = {"jobComplete": True, "numDmlAffectedRows": "4"}
        return type("R", (), {"ok": True, "body": result})()


def test_erase_counts_then_deletes_with_parameters(repo):
    uc = uc_for(repo, data_protection={"personal_data_columns": [], "erasure": {"key_column": "customer_id"}})
    env = load_environment(repo.root, "dev")
    gcp = FakeGcp()
    dry = erase.erase(uc, env, ["C1", "C2"], confirm=False, gcp=gcp)
    assert dry == [erase.EraseResult("predictions", 4, False)] and len(gcp.queries) == 1
    done = erase.erase(uc, env, ["C1", "C2"], confirm=True, gcp=gcp)
    assert done == [erase.EraseResult("predictions", 4, True)]
    delete = gcp.queries[-1]
    assert delete["query"] == "DELETE FROM `acme-ml-dev.churn.predictions` WHERE customer_id IN UNNEST(@keys)"
    assert delete["queryParameters"][0]["parameterValue"]["arrayValues"] == [{"value": "C1"}, {"value": "C2"}]
    assert delete["location"] == "europe-west2"


def test_promote_approval_uses_its_own_key(repo):
    uc = uc_for(repo)        # promote_model manual, deploy_model auto in dev
    check_approval(uc, None, False)                                   # deploy_model: auto
    with pytest.raises(ConfigError, match="approvals.promote_model is manual"):
        check_approval(uc, None, False, key="promote_model")
    check_approval(uc, "someone@acme.example", True, key="promote_model")   # group approvers are not checked


def test_runner(repo):
    uc = uc_for(repo, tests={"command": "python -c \"import os, sys; sys.exit(0 if os.environ['PIPEWRIGHT_USECASE'] "
                                        "== 'churn' else 3)\""})
    lines = []
    assert runner.run(uc, log=lines.append) == 0 and lines[-1].startswith("OK")
    assert runner.run(uc_for(repo, tests={"command": "python -c \"raise SystemExit(4)\""}), log=lines.append) == 4
    repo.usecase["spec"].pop("tests")
    assert runner.run(uc_for(repo), log=lines.append) is None


def test_monitor_status_render(repo):
    from pipewright.monitoring import status

    uc = uc_for(repo, monitoring={"drift": {"enabled": True}, "output_table": "model_metrics"},
                pipelines={"training": repo.usecase["spec"].pop("pipeline"),
                           "inference": {"file": "pipeline.py", "function": "infer"}})
    run = {"monitored_at": "2026-10-05T18:30:00", "model_version_id": "4", "rows_scored": 1000,
           "max_feature_psi": 0.853, "prediction_psi": 0.88, "labelled_rows": 0, "alert": True,
           "drifted_features": "monthly_charges", "retrain_status": "requested (drift): churn-r1",
           "details_json": '{"feature_psi": {"monthly_charges": 0.853, "tenure_months": 0.01}, "notes": []}'}
    text = status.render(uc, [run])
    assert "v4" in text and "YES" in text and "requested (drift)" in text
    assert "monthly_charges   0.853" in text and "DRIFT" in text and "n=0" in text
    assert "no inference runs recorded yet" in status.render(uc, [])
