"""FR-129 (1.4.3): AI build assistance knows what 1.4 added: tools, the context pack's rules, playbooks, the review
bundle and judgement checks."""

from pipewright.abtest import config as abc
from pipewright.abtest.store import TestStore
from pipewright.ai import mcp, review
from pipewright.config import judgement
from pipewright.gcp.session import Response
from pipewright.serving import rollback
from tests.test_ai_context import built
from tests.test_ai_mcp import FakeGcp, logged
from tests.test_ai_review import committed
from tests.test_config_ml import ml_repo

TEST = {"name": "lapse-v7", "usecase": "churn", "environment": "dev", "state": "running", "mode": "ab",
        "started_at": "2026-11-01T06:00:00+00:00", "started_by": "ana@example.com", "release": "r1",
        "arms": {"control": {"version": "5"}, "treatment": {"version": "7"}},
        "allocation": [{"treatment_bp": 1000, "at": "2026-11-01", "by": "ana@example.com"}],
        "analysis": {"at": "2026-11-10", "status": "collecting", "blocked": [], "exposed": {"control": 900,
                                                                                         "treatment": 100},
                     "primary": {"name": "renewed", "diff": 0.01, "ci": [-0.02, 0.04], "p": 0.4},
                     "segments": {"country": {"United Kingdom": {"control": 800}}}},
        "decision": {"required": ["model-risk"], "approvals": [{"group": "model-risk", "winner": "treatment",
                                                                "by": "bo@example.com"}]}}


def _server(repo, gcp=None):
    ml_repo(repo)
    repo.usecase["spec"]["environments"]["dev"]["model"]["serving"]["enabled"] = True
    repo.write()
    fake = gcp or FakeGcp()
    return mcp.Server(repo.root, "dev", gcp_factory=lambda: fake)


def test_the_new_tools_are_offered():
    assert {"ab_status", "serving_health", "features_check"} <= set(mcp.TOOLS)
    assert "integrity_check" not in mcp.TOOLS            # reads model files and audit logs: not for an agent


def test_ab_status_gives_the_test_in_aggregate_and_never_per_value(repo, monkeypatch):
    s = _server(repo)
    monkeypatch.setattr(TestStore, "current", lambda self, uc: TEST)
    out = s.ab_status("churn")
    assert out["ok"] and out["state"] == "running" and out["arms"]["treatment"]["version"] == "7"
    assert out["analysis"]["primary"]["p"] == 0.4 and out["analysis"]["exposed"]["treatment"] == 100
    assert "withheld" in out["analysis"]["segments"]
    text = str(out)
    assert "United Kingdom" not in text and "example.com" not in text      # no value of a column, no person
    assert out["decision"]["groups_decided"] == ["model-risk"] and out["next"]["text"]
    assert logged(s)[-1]["tool"] == "ab_status"


def test_ab_status_without_a_test_says_who_starts_one(repo, monkeypatch):
    s = _server(repo)
    monkeypatch.setattr(TestStore, "current", lambda self, uc: None)
    monkeypatch.setattr(TestStore, "list", lambda self, uc: [{**TEST, "state": "stopped"}])
    out = s.ab_status("churn")
    assert out["active"] is None and out["recent"][0]["state"] == "stopped" and "by a person" in out["note"]


def test_ab_status_names_the_missing_grant(repo, monkeypatch):
    from pipewright.errors import ConfigError

    s = _server(repo)

    def denied(self, uc):
        raise ConfigError("cannot read gs://b/ab-tests/churn/current.json: 403 forbidden")
    monkeypatch.setattr(TestStore, "current", denied)
    out = s.ab_status("churn")
    assert not out["ok"] and "403" in out["error"] and "platform files" in out["hint"]


class HealthGcp(FakeGcp):
    def get(self, url, **params):
        if url.endswith("/alertPolicies"):
            self.filter = params.get("filter")
            return Response(200, {"alertPolicies": [{"enabled": True, "userLabels": {
                "pipewright-signal": "latency", "pipewright-severity": "critical", "pipewright-threshold": "1000",
                "pipewright-model-version": "3", "pipewright-environment": "dev"}}]})
        return super().get(url, **params)


def test_serving_health_shows_settings_policies_and_rollbacks(repo, monkeypatch):
    gcp = HealthGcp()
    s = _server(repo, gcp)
    monkeypatch.setattr(rollback, "cases", lambda g, b, uc: [{"case_id": "rb-1", "status": "open", "version": "3",
                                                              "previous_version": "2", "signal": "errors",
                                                              "by": "ana@example.com"}])
    out = s.serving_health("churn")
    assert out["ok"] and out["serving"]["enabled"] is True
    assert out["policies"] == [{"signal": "latency", "severity": "critical", "threshold": "1000", "version": "3",
                                "arm": None, "enabled": True}]
    assert 'pipewright-environment"="dev"' in gcp.filter and out["rollbacks"][0]["previous_version"] == "2"
    assert "example.com" not in str(out)
    assert "monitoring.health is off" in out["note"]


def test_features_check_runs_the_contract_check(repo, monkeypatch):
    s = _server(repo)
    seen = {}

    def fake_cli(*argv):
        seen["argv"] = argv
        return {"output": "churn: feature contract 1.0.0 matches the live sources in dev"}
    monkeypatch.setattr(s, "_cli", fake_cli)
    assert "matches" in s.features_check("churn")["output"]
    assert seen["argv"] == ("features", "contract", "--usecase", "churn", "--check")


def test_the_pack_states_the_1_4_rules(repo):
    repo.manifests["dev"]["policies"] = {
        "registration": "launcher", "integrity": {"mode": "refuse"},
        "ab_test": {"max_treatment_bp": 2000, "min_days": 14, "required_guardrails": ["complaints"],
                    "required_approvers": ["model-risk"], "overlap": "refuse"},
        "serving": {"capture": {"max_sample_rate": 0.1, "retention_days": 30}, "health": {"required": True},
                    "scale_to_zero": "refused"}}
    built(repo)
    pack = (repo.root / "APP_CONTEXT.md").read_text(encoding="utf-8")
    assert "Registration goes through the launcher" in pack and "register_version" in pack
    assert "at most 2000 basis points" in pack and "`complaints`" in pack and "`model-risk`" in pack
    assert "sample_rate at most 0.1" in pack and "sets `monitoring.health`" in pack
    assert "scale_to_zero` is refused" in pack
    assert "`serving_health`" in pack and "--persona ml" in pack and "integrity check" in pack
    for p in ("run-an-ab-test.md", "add-serving-health.md", "serve-features-online.md", "change-a-feature-contract.md"):
        assert f".pipewright/playbooks/{p}" in pack and (repo.root / ".pipewright" / "playbooks" / p).exists()


def test_a_pack_without_1_4_policies_still_says_how_tests_and_serving_work(repo):
    built(repo)
    pack = (repo.root / "APP_CONTEXT.md").read_text(encoding="utf-8")
    assert "started by a person" in pack and "Registration goes through" not in pack


def test_the_review_bundle_shows_serving_health_capture_and_a_pool(repo):
    repo.usecase["spec"]["environments"]["dev"]["model"]["serving"]["enabled"] = True
    env = committed(repo)
    spec = repo.usecase["spec"]
    spec["monitoring"] = {**(spec.get("monitoring") or {}), "health": {"enabled": True}}
    spec["model"]["serving"] = {**spec["model"]["serving"],
                                "capture": {"enabled": True, "sample_rate": 0.05, "retention_days": 30}}
    repo.write()
    text = review.bundle(repo.root, env)
    assert "+ serving health: alert policies per deployed version" in text
    assert "+ request capture into `churn_online_log`: a sample of 0.05" in text
    assert "`model.serving.capture.sample_rate`: set to `0.05`" in text          # model.serving moves are listed


def _ab(**over):
    t = {**abc.DEFAULTS, "enabled": True, "name": "lapse-v7", "unit": "customer_id"}
    t.update(over)
    return {"ab_test": t}


def test_judgement_on_a_test_that_validates_and_still_misleads():
    w = judgement.serving_and_tests(_ab(duration={"min_days": 3, "max_days": 30}, allocation={"treatment_bp": 8000}),
                                    {})
    text = "\n".join(w)
    assert "no business guardrail" in text and "weekly cycle" in text and "8000" in text
    assert "approvals.required is empty" in text
    good = _ab(guardrails={"business": [{"name": "complaints"}]}, decision={"approvals": {"required": ["risk"]}})
    assert judgement.serving_and_tests(good, {}) == []
    assert judgement.serving_and_tests(_ab(mode="shadow"), {}) == []           # a shadow decides nothing
    assert judgement.serving_and_tests({}, {}) == []                           # off: nothing said


def test_judgement_on_capturing_most_calls():
    w = judgement.serving_and_tests({}, {"serving": {"capture": {"enabled": True, "sample_rate": 1.0}}})
    assert len(w) == 1 and "real people" in w[0]
    assert judgement.serving_and_tests({}, {"serving": {"capture": {"enabled": True, "sample_rate": 0.05}}}) == []


def test_monitor_status_as_an_agent_says_where_the_facts_are(repo, monkeypatch):
    s = _server(repo)
    monkeypatch.setattr(s, "_cli", lambda *a: {"ok": False, "exit_code": 2, "error": "error: Access Denied: Table "
                                               "p:d.churn_model_monitoring: User does not have permission"})
    out = s.monitor_status("churn")
    assert out["refused"].startswith("monitoring results are rows") and "retrain_list" in out["instead"][0]
    assert logged(s)[-1]["outcome"] == "refused"


def test_bootstrap_says_when_a_feature_source_has_no_schema(repo):
    s = _server(repo)
    entity = {"schema": {"fields": [{"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
                                    {"name": "snapshot_date", "type": "DATE", "mode": "REQUIRED"},
                                    {"name": "churned", "type": "INTEGER"}]}}
    real_get = s._gcp_factory().get

    def get(url, **params):
        if url.endswith("/featureGroups/gone"):
            return Response(200, {"bigQuery": {"bigQuerySource": {"inputUri": "bq://acme-ml-dev.features.gone"}}})
        if url.endswith("/tables/gone"):
            return Response(404, {"error": {"message": "Not found: Table"}})
        if url.endswith("/tables/entities"):
            return Response(200, entity)
        return real_get(url, **params)

    s._gcp_factory().get = get
    out = s.bootstrap_usecase("lapse", "churn.entities", "churned",
                              features=[{"feature_group": "gone", "features": ["tenure_months"]}])
    assert any("feature source gone: no schema for tenure_months" in n for n in out["notes"])
