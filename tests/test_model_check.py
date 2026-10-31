"""FR-72 (Pipewright 1.2): `pipewright model check` measures a version against today's gates, filling the slices and
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
