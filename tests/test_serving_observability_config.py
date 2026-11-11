"""Model serving observability settings (FR-98, 1.4.3): off and absent by default, defaults when set, the checks,
and the platform's policies.serving rules."""

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from tests.test_config_ml import ml_repo


def serving_repo(repo, serving=None, monitoring=None):
    ml_repo(repo)
    spec = repo.usecase["spec"]
    spec.setdefault("model", {})["serving"] = {"enabled": True, **(serving or {})}
    spec["environments"]["dev"]["model"]["serving"]["enabled"] = True
    spec.setdefault("monitoring", {}).update(monitoring or {})
    return repo


def resolve(repo):
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def problems(repo) -> str:
    with pytest.raises(ConfigError) as e:
        resolve(repo)
    return str(e.value)


def test_absent_means_nothing_changes(repo):
    uc = resolve(serving_repo(repo))
    assert "capture" not in uc.model["serving"]
    assert "online" not in uc.ml["monitoring"] and "health" not in uc.ml["monitoring"]


def test_defaults_when_set(repo):
    uc = resolve(serving_repo(repo, {"capture": {"enabled": True}},
                              {"online": {"enabled": False}, "health": {"enabled": True}}))
    assert uc.model["serving"]["capture"] == {"enabled": True, "sample_rate": 0.05, "max_per_day": 100000,
                                              "retention_days": 30}
    online, health = uc.ml["monitoring"]["online"], uc.ml["monitoring"]["health"]
    assert online["min_samples"] == 500 and online["attribution"] == {"enabled": False, "sample_rows": 500,
                                                                      "threshold": 0.10}
    assert health["scaling_minutes"] == 5 and health["propose_rollback_hours"] == 0
    assert health["latency_p95_ms"] == {"warning": 500, "critical": 1000}


def test_checks(repo):
    found = problems(serving_repo(repo, {"capture": {"enabled": False}}, {
        "online": {"enabled": True, "schedule": "daily"},
        "health": {"enabled": True, "error_rate": {"warning": 0.1, "critical": 0.05}}}))
    for part in ("monitoring.online: needs model.serving.capture.enabled", "schedule must be a cron expression",
                 "needs spec.pipelines.monitoring",
                 "monitoring.health.error_rate: warning is above critical"):
        assert part in found, part


def test_platform_policy(repo):
    serving_repo(repo, {"capture": {"enabled": True, "sample_rate": 0.5, "retention_days": 90}})
    repo.manifests["dev"]["policies"] = {"serving": {"capture": {"max_sample_rate": 0.1, "retention_days": 30},
                                                     "health": {"required": True}}}
    found = problems(repo)
    for part in ("above dev's maximum 0.1", "retention_days 90 is above dev's maximum 30",
                 "requires health policies on every endpoint"):
        assert part in found, part
    repo.manifests["dev"]["policies"] = {"serving": {"capture": {"allowed": False}}}
    assert "capture is not allowed in dev" in problems(repo)


def test_scale_to_zero_warns_and_a_platform_can_refuse_it(repo):
    serving_repo(repo, {"scale_to_zero": True, "replicas": {"min": 0, "max": 2}})
    uc = resolve(repo)
    assert any("answered 429 and dropped" in w for w in uc.warnings)
    repo.manifests["dev"]["policies"] = {"serving": {"scale_to_zero": "refused"}}
    assert "model.serving.scale_to_zero is refused in dev" in problems(repo)


def test_an_outcomes_table_the_use_case_does_not_own_is_pointed_out(repo):
    # GA QA: live quality joins it at run time; one that is not among the use case's tables must exist already
    uc = resolve(serving_repo(repo, {"capture": {"enabled": False}},
                              {"online": {"enabled": False, "outcomes": {"table": "theirs", "label": "churned"}}}))
    assert any("outcomes.table 'theirs' is not one of" in w for w in uc.warnings)
