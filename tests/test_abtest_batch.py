"""FR-57 phase A: what a scoring run gets while a test runs, the predictions table's columns, the platform policy,
and the component runtime's copy of the assignment."""
import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from pipewright.abtest import assign, scoring
from pipewright.abtest import config as abc

RUNTIME = Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py"


def runtime():
    spec = importlib.util.spec_from_file_location("pw_runtime_copy", RUNTIME)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_component_runtime_assigns_exactly_as_the_package():
    rt = runtime()
    for unit in [str(i) for i in range(5000)] + ["", None, "ä-ü"]:
        assert rt.ab_arm("lapse-v7", unit, 2000) == assign.arm("lapse-v7", unit, 2000)
    assert rt.ab_sql_arm("lapse-v7", "`customer_id`", 2000) == assign.sql_arm("lapse-v7", "`customer_id`", 2000)
    assert rt.ab_spec("") is None and rt.ab_spec('{"name": "t"}') == {"name": "t"}


# -- the launcher's parameter -----------------------------------------------------------------------------------

def _contract():
    return SimpleNamespace(usecase="churn", tenant="retention", environment="prod", release_id="R1",
                           project="p", region="europe-west2", metadata={"model": "churn-classifier"})


def _test(state="running", bp=2000):
    return {"name": "lapse-v7", "usecase": "churn", "unit": "customer_id", "mode": "ab", "state": state,
            "arms": {"control": {"version": "5"}, "treatment": {"version": "7"}},
            "allocation": [{"at": "t0", "treatment_bp": 3000}, {"at": "t1", "treatment_bp": bp}]}


FILES = {"folder": "gs://b/m/7/", "files": [{"file": "model.pt", "generation": "1", "sha256": "ab"}]}


def test_a_running_test_passes_the_treatment_and_its_approved_files(monkeypatch):
    monkeypatch.setattr(scoring, "active_test", lambda gcp, bucket, uc: _test())
    seen = {}

    def gate(gcp, manifest, contract, version):
        seen.update(manifest["policies"]["integrity"], version=version)
        return {"problem": None, "approved_files": FILES, "version": version}

    p, rec = scoring.param(None, {"artifacts_bucket": "b", "policies": {}}, _contract(), gate=gate)
    spec = json.loads(p)
    assert spec == {"name": "lapse-v7", "unit": "customer_id", "treatment_bp": 2000,
                    "treatment": {"version": "7", "approved_files": FILES}}
    assert seen == {"inference": "warn", "pin": "on", "version": "7"}     # checked whatever the policy says
    assert rec == {"test": "lapse-v7", "treatment": "7", "treatment_bp": 2000}


def test_a_paused_test_scores_everyone_with_the_control(monkeypatch):
    monkeypatch.setattr(scoring, "active_test", lambda gcp, bucket, uc: _test("paused"))
    p, _ = scoring.param(None, {"artifacts_bucket": "b"}, _contract(),
                         gate=lambda *a: {"problem": None, "approved_files": FILES})
    assert json.loads(p)["treatment_bp"] == 0


def test_a_treatment_that_fails_its_check_is_never_used_under_refuse(monkeypatch):
    monkeypatch.setattr(scoring, "active_test", lambda gcp, bucket, uc: _test())
    bad = lambda *a: {"problem": "model.pt changed", "approved_files": FILES}       # noqa: E731
    p, rec = scoring.param(None, {"artifacts_bucket": "b"}, _contract(), gate=bad)
    assert p is None and "model.pt changed" in rec["problem"] and rec["scored"] == "control only"
    p, rec = scoring.param(None, {"artifacts_bucket": "b", "policies": {"integrity": {"mode": "warn"}}}, _contract(),
                           gate=bad)
    assert json.loads(p)["treatment"]["approved_files"] == FILES and "(warn)" in rec["problem"]


def test_no_test_no_parameter(monkeypatch):
    monkeypatch.setattr(scoring, "active_test", lambda gcp, bucket, uc: None)
    assert scoring.param(None, {"artifacts_bucket": "b"}, _contract()) == (None, {})


# -- the predictions table and the policy -----------------------------------------------------------------------

def _ml():
    return {"ab_test": {**abc.DEFAULTS, "enabled": True, "publish": {"events": False, "exposures": False},
                        "decision": {"approvals": {"required": ["ml-leads"]}},
                        "duration": {"min_days": 14, "max_days": 60}, "allocation": {"treatment_bp": 2000}},
            "inference": {"output_table": "preds"}}


def _table(*cols):
    return SimpleNamespace(fields=[{"name": c, "type": "STRING"} for c in cols])


def test_the_predictions_table_must_carry_the_arm():
    problems = []
    abc.check_tables(_ml(), {"preds": _table("customer_id")}, True, False, problems)
    assert any("add ab_test and arm" in p for p in problems)
    problems = []
    abc.check_tables(_ml(), {"preds": _table("customer_id", "ab_test", "arm")}, True, False, problems)
    assert problems == []
    problems = []
    abc.check_tables(_ml(), {}, False, False, problems)
    assert any("scores nothing" in p for p in problems)
    problems = []
    ml = copy.deepcopy(_ml())
    ml["ab_test"]["analysis"]["segments"] = ["country"]
    abc.check_tables(ml, {"preds": _table("customer_id", "ab_test", "arm")}, True, False, problems)
    assert any("add country to preds" in p for p in problems)


def test_the_platform_policy():
    ml = _ml()
    problems = []
    abc.apply_policy(ml["ab_test"], {"allowed": True, "max_treatment_bp": 1000, "min_days": 21,
                                     "required_approvers": ["model-risk"]}, "prod", problems)
    assert any("above prod's limit of 1000" in p for p in problems)
    assert any("below prod's minimum of 21" in p for p in problems)
    assert ml["ab_test"]["decision"]["approvals"]["required"] == ["model-risk", "ml-leads"]
    problems = []
    abc.apply_policy({**_ml()["ab_test"], "publish": {"exposures": True}}, {}, "prod", problems)
    assert any("may not leave" in p for p in problems)          # default deny, even with no policy
    problems = []
    abc.apply_policy(_ml()["ab_test"], {"allowed": False}, "prod", problems)
    assert any("not allowed in prod" in p for p in problems)
