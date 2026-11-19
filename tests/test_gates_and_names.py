"""Evaluation gates checked by Pipewright from the evaluation record, and .yml files refused with a clear error."""

import pytest
import yaml

from pipewright.config import discover_usecases, load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.models import gates

EVALUATION = {
    "primary_metric": "auc",
    "lower_is_better": ["calibration_error"],
    "gates": [{"metric": "auc", "min": 0.75}, {"metric": "calibration_error", "max": 0.05}],
    "slice_gates": [{"metric": "auc", "max_gap": 0.05, "min_rows": 50}],
    "champion": {"enabled": True, "metric": "auc", "min_improvement": 0.005},
}
GOOD = {
    "metrics": {"auc": 0.81, "calibration_error": 0.03},
    "slices": {"contract": {"12": {"rows": 400, "auc": 0.79}, "1": {"rows": 10, "auc": 0.40}}},
    "champion": {"auc": 0.80, "_version": "3"},
    "decision": "approve",
}


def record(**changes):
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in GOOD.items()}
    for k, v in changes.items():
        out[k] = v
    return out


def test_a_passing_record_passes():
    assert gates.check(GOOD, EVALUATION, champion_expected=True) == []


def test_gates_are_recomputed_not_trusted():
    # The pipeline said "approve", but the numbers fail: Pipewright goes by the numbers.
    bad = record(metrics={"auc": 0.70, "calibration_error": 0.09})
    problems = gates.check(bad, EVALUATION)
    assert any("gate auc: 0.7000 is below the minimum 0.75" in p for p in problems)
    assert any("gate calibration_error: 0.0900 is above the maximum 0.05" in p for p in problems)


def test_a_dropped_metric_fails_its_gate():
    problems = gates.check(record(metrics={"auc": 0.81}), EVALUATION)
    assert problems == ["gate calibration_error: the evaluation record has no calibration_error"]


def test_slice_gates_skip_small_slices_and_catch_large_gaps():
    assert gates.check(GOOD, EVALUATION) == []                      # the 10-row slice is below min_rows
    wide = record(slices={"contract": {"12": {"rows": 400, "auc": 0.70}}})
    assert gates.check(wide, EVALUATION) == [
        "slice gate auc: contract=12 is 0.1100 worse than overall (maximum gap 0.05)"]
    assert gates.check(record(slices={}), EVALUATION) == ["slice gate auc: the evaluation record has no slices"]


def test_the_champion_margin_and_a_missing_comparison():
    close = record(champion={"auc": 0.808, "_version": "3"})
    assert "does not beat the champion's 0.8080 by 0.005" in gates.check(close, EVALUATION)[0]
    no_comparison = record(champion=None)
    assert gates.check(no_comparison, EVALUATION, champion_expected=False) == []   # first model: nothing to beat
    assert "no comparison with the champion" in gates.check(no_comparison, EVALUATION, champion_expected=True)[0]


def test_nothing_configured_needs_no_record():
    assert not gates.needs_record({"gates": [], "slice_gates": [], "champion": {"enabled": False}})
    assert gates.verify(None, None, {"gates": [], "champion": {"enabled": False}}) == []
    assert "no evaluation record" in gates.verify(None, None, EVALUATION)[0]


# ---------------------------------------------------------------- .yml refused, not ignored

def test_yml_override_is_refused_not_ignored(repo):
    path = repo.write()
    (path.parent / "environments").mkdir()
    (path.parent / "environments" / "dev.yml").write_text(yaml.safe_dump({"parameters": {"epochs": 2}}),
                                                          encoding="utf-8")
    with pytest.raises(ConfigError, match=r"environments/dev.yml: Pipewright reads .yaml files; rename it to "
                                          r"environments/dev.yaml"):
        resolve_usecase(path, load_environment(repo.root, "dev"))


def test_yml_include_and_usecase_file_are_refused(repo):
    repo.usecase["spec"]["include"] = ["config/*.yaml"]
    path = repo.write()
    (path.parent / "config").mkdir()
    (path.parent / "config" / "data.yml").write_text("data: {}\n", encoding="utf-8")
    with pytest.raises(ConfigError, match=r"config/data.yml: Pipewright reads .yaml files"):
        resolve_usecase(path, load_environment(repo.root, "dev"))

    other = repo.root / "usecases" / "fraud"
    other.mkdir()
    (other / "usecase.yml").write_text("kind: UseCase\n", encoding="utf-8")
    with pytest.raises(ConfigError, match=r"usecases/fraud/usecase.yml: .*rename it to usecases/fraud/usecase.yaml"):
        discover_usecases(repo.root)


def test_yml_environment_file_is_refused(repo):
    repo.write()
    env_dir = repo.root / "environments"
    (env_dir / "dev.yaml").rename(env_dir / "dev.yml")
    with pytest.raises(ConfigError, match=r"environments/dev.yml: Pipewright reads .yaml files; rename it to "
                                          r"environments/dev.yaml"):
        load_environment(repo.root, "dev")


def test_a_version_trained_alongside_the_champion_needs_no_comparison():
    # 1.4.5 GA QA (live): deploy-model judged "a champion existed" from the version number alone; v1 and v2 were
    # trained together, v1 registered (19:35) after v2's run started (19:20), so v2 had nothing to compare with
    from types import SimpleNamespace

    from pipewright.gcp import Gcp
    from pipewright.models.registry import champion_expected

    class FakeGcp(Gcp):
        def __init__(self):
            pass

        def get(self, url, **params):
            if url.endswith(":listVersions"):
                return SimpleNamespace(ok=True, body={"models": [
                    {"versionId": "1", "versionCreateTime": "2026-11-19T19:35:15Z", "labels": {"run_id": "run-r1"}},
                    {"versionId": "2", "versionCreateTime": "2026-11-19T19:37:22Z", "labels": {"run_id": "run-r2"}}]})
            return SimpleNamespace(ok=True, body={"pipelineJobs": [{"createTime": "2026-11-19T19:20:02Z"}]})

    env = SimpleNamespace(region="europe-west2", project="p", kubernetes=None)
    assert champion_expected(env, FakeGcp(), "projects/p/locations/r/models/m", "2", ["1", "2"]) is False
