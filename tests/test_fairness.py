"""FR-53: fairness measures per attribute, their gates, and the BigQuery ML shaping."""
import pandas as pd

from pipewright.bqml.steps import bqml_runtime as bq
from pipewright.build.templates import pipewright_runtime as rt
from pipewright.models import gates
from tests.test_config_ml import CONTRACT, ml_repo, problems, resolve

GATE = {"metric": "demographic_parity_difference", "max": 0.1}


def test_fairness_resolves_only_when_set(repo):
    uc = resolve(ml_repo(repo))
    assert "fairness" not in uc.ml["evaluation"]                         # a 1.1 use case without it: as in 1.0
    uc = resolve(ml_repo(repo, evaluation={"fairness": {"attributes": ["contract_length_months"],
                                                         "gates": [{"metric": "equalized_odds_difference", "max": 0.2}],
                                                         "metrics": ["demographic_parity_difference"]}}))
    fair = uc.ml["evaluation"]["fairness"]
    assert fair["metrics"] == ["demographic_parity_difference", "equalized_odds_difference"]   # gated: computed
    assert fair["min_rows"] == 30


def test_fairness_attributes_come_from_the_contract_and_need_a_binary_label(repo):
    text = problems(ml_repo(repo, evaluation={"fairness": {"attributes": ["gender", "churned"]}}))
    assert "column 'gender' is not in data.contract.columns" in text and "'churned' is the label" in text
    r = ml_repo(repo, evaluation={"fairness": {"attributes": ["tenure_months"]}})
    r.usecase["spec"]["data"]["label"] = {"column": "monthly_charges", "task": "regression"}
    r.usecase["spec"]["data"]["split"] = {"strategy": "random", "test_fraction": 0.2}
    assert "measured for binary classification" in problems(r)
    assert CONTRACT["columns"][0]["name"] == "customer_id"


def frame():
    # Group a: 4 of 10 selected, TPR 3/5, FPR 1/5. Group b: 8 of 10 selected, TPR 5/5, FPR 3/5. Group c: 2 rows.
    rows = ([("a", 1, 0.9)] * 3 + [("a", 1, 0.1)] * 2 + [("a", 0, 0.8)] + [("a", 0, 0.2)] * 4
            + [("b", 1, 0.9)] * 5 + [("b", 0, 0.7)] * 3 + [("b", 0, 0.1)] * 2 + [("c", 1, 0.9)] * 2)
    return pd.DataFrame(rows, columns=["region", "y", "p"])


def ml(**fair):
    return {"evaluation": {"fairness": {"attributes": ["region"], "min_rows": 5, "gates": [GATE],
                                        "metrics": ["demographic_parity_difference", "equal_opportunity_difference",
                                                    "equalized_odds_difference"], **fair}}}


def test_measures_are_the_largest_gap_between_measured_groups():
    got = rt.fairness_measures(frame(), "y", "p", 0.5, ml())["region"]
    assert got["groups"]["c"] == {"rows": 2}                                # below min_rows: listed, not measured
    assert round(got["demographic_parity_difference"], 6) == 0.4
    assert round(got["equal_opportunity_difference"], 6) == 0.4
    assert round(got["equalized_odds_difference"], 6) == 0.4
    assert rt.fairness_problems({"region": got}, ml()) == [
        "fairness gate demographic_parity_difference: region is 0.4000, above the maximum 0.1"]
    assert rt.fairness_measures(frame(), "y", "p", 0.5, {"evaluation": {}}) == {}


def test_a_measure_that_cannot_be_taken_fails_its_gate():
    got = rt.fairness_measures(frame(), "y", "p", 0.5, ml(min_rows=100))
    assert got["region"]["demographic_parity_difference"] is None
    assert "not measured for region" in rt.fairness_problems(got, ml(min_rows=100))[0]


def test_pipewright_rechecks_fairness_gates_from_the_record():
    evaluation = {**ml()["evaluation"], "gates": [], "slice_gates": []}
    assert gates.needs_record(evaluation)
    record = {"metrics": {}, "fairness": {"region": {"demographic_parity_difference": 0.05}}}
    assert gates.check(record, evaluation) == []
    record["fairness"]["region"]["demographic_parity_difference"] = 0.3
    assert gates.check(record, evaluation) == [
        "fairness gate demographic_parity_difference: region is 0.3000, above the maximum 0.1"]
    assert "not measured" in gates.check({"metrics": {}}, evaluation)[0]      # a record without fairness fails


def test_bigquery_ml_shapes_fairness_as_the_container_path(monkeypatch):
    class Jobs:
        def run(self, sql, what):
            assert "`region`" in sql and "_pw_score >= 0.5" in sql
            return [{"_pw_group": "a", "row_count": 10, "selection_rate": 0.4, "tpr": 0.6, "fpr": 0.2},
                    {"_pw_group": "b", "row_count": 10, "selection_rate": 0.8, "tpr": 1.0, "fpr": 0.6},
                    {"_pw_group": "c", "row_count": 2, "selection_rate": 1.0, "tpr": 1.0, "fpr": None}]

    monkeypatch.setitem(__import__("sys").modules, "pipewright_runtime", rt)
    config = {**ml(), "data": {"label": {"column": "y", "positive": 1}}}
    config["evaluation"]["decision_threshold"] = 0.5
    got = bq.fairness(Jobs(), config, "p.d.preds")["region"]
    want = rt.fairness_measures(frame(), "y", "p", 0.5, ml())["region"]
    assert got["groups"]["c"] == {"rows": 2}
    for m in ("demographic_parity_difference", "equal_opportunity_difference", "equalized_odds_difference"):
        assert round(got[m], 6) == round(want[m], 6)
