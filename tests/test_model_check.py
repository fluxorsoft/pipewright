"""FR-72 (Pipewright 1.2.0): `pipewright model check` measures a version against today's gates, filling the slices and
fairness its own record lacks from the newest challenger that measured it as the champion."""
from types import SimpleNamespace

from pipewright.models import check, registry

EVALUATION = {"primary_metric": "auc", "gates": [{"metric": "auc", "min": 0.7}],
              "slice_gates": [{"metric": "auc", "max_gap": 0.1, "min_rows": 10}], "champion": {"enabled": False}}
OLD = {"metrics": {"auc": 0.78}}                                   # trained before the slice gate: no slices
SLICES = {"region": {"north": {"auc": 0.75, "rows": 200}, "south": {"auc": 0.61, "rows": 150}}}
CHALLENGER = {"metrics": {"auc": 0.74}, "champion": {"auc": 0.78, "_version": "5", "_slices": SLICES}}


def fake(monkeypatch, records):
    monkeypatch.setattr(registry, "_resource", lambda uc, env, creds: "projects/p/locations/r/models/m")
    monkeypatch.setattr(registry, "artifact_uri", lambda resource, version, uc=None: f"gs://b/v{version}")
    monkeypatch.setattr(check.gates, "read_record", lambda gcp, uri: records.get(uri.rsplit("/v", 1)[-1]))
    monkeypatch.setattr(registry, "versions", lambda uc, env, creds, check_gates=True, limit=10: [
        SimpleNamespace(version_id=v) for v in sorted(records, key=int, reverse=True)])
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda creds=None: None)
    return SimpleNamespace(ml={"evaluation": EVALUATION}, model={"name": "churn"}, environment="prod")


def test_a_champion_from_before_the_slice_gate_is_measured_by_its_challenger(monkeypatch):
    uc = fake(monkeypatch, {"5": OLD, "6": CHALLENGER})
    result = check.check(uc, None, None, "5")
    assert result.filled == ["slices"] and result.measured_by == "6"
    assert result.problems == ["slice gate auc: region=south is 0.1700 worse than overall (maximum gap 0.1)"]
    text = check.render(uc, result)
    assert text.startswith("churn v5 (prod) against today's gates: FAIL (1)")
    assert "slices measured on v6's test rows, where v5 was the champion" in text


def test_without_a_challenger_it_says_what_is_unmeasured(monkeypatch):
    uc = fake(monkeypatch, {"5": OLD})
    result = check.check(uc, None, None, "5")
    assert result.problems == ["slice gate auc: the evaluation record has no slices"] and not result.filled
    assert "train a challenger" in check.render(uc, result)


def test_own_measures_win():
    own = {"metrics": {"auc": 0.8}, "slices": {"x": {}}}
    record, filled = check.merged(own, CHALLENGER)
    assert record["slices"] == {"x": {}} and filled == []


def test_a_forecast_gate_per_series_names_the_series(monkeypatch):
    from pipewright.models import gates

    evaluation = {"primary_metric": "mape", "lower_is_better": ["mape"], "champion": {"enabled": False},
                  "gates": [{"metric": "mape", "max": 0.6, "per_series": True}], "slice_gates": []}
    record = {"metrics": {"mape": 0.31},
              "per_series": {"sku-1": {"rows": 7, "mape": 0.2}, "sku-7": {"rows": 7, "mape": 0.84}}}
    assert gates.check(record, evaluation) == ["gate mape per series: sku-7 0.8400 is above the maximum 0.6"]
    assert gates.check({"metrics": {"mape": 0.3}}, evaluation) == [
        "gate mape per series: the evaluation record has no per-series metrics"]
    assert gates.worst_series(record, "mape") == ("sku-7", 0.84)
    uc = fake(monkeypatch, {"3": record})
    uc.ml["evaluation"] = evaluation
    text = check.render(uc, check.check(uc, None, None, "3"))
    assert "worst series  sku-7: mape 0.8400" in text


def test_per_series_is_for_forecasts(repo):
    from tests.test_config_ml import ml_repo, problems

    ml_repo(repo)
    repo.usecase["spec"].setdefault("evaluation", {})["gates"] = [{"metric": "auc", "min": 0.7, "per_series": True}]
    assert "per_series is for forecasts" in problems(repo)
