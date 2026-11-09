"""FR-57 phases A4 and A5: decisions, pending items, the analysis verdict, and the service's rules."""
import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pipewright import decisions
from pipewright.abtest import analysis, decide, stats
from pipewright.abtest import config as abc
from pipewright.abtest import service as svc
from pipewright.errors import ConfigError
from tests.test_abtest_core import FakeGcs

T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _test(**kw):
    t = {"name": "lapse-v7", "usecase": "churn", "unit": "customer_id", "mode": "ab", "state": "running",
         "started_at": T0.isoformat(), "arms": {"control": {"version": "5"}, "treatment": {"version": "7"}},
         "allocation": [{"at": T0.isoformat(), "treatment_bp": 2000}],
         "decision": {"required": ["ml-leads", "model-risk"], "approvals": []}, "history": []}
    t.update(kw)
    return t


# -- decisions ---------------------------------------------------------------------------------------------------

def test_every_group_must_agree_before_there_is_a_winner():
    t = decide.record(_test(), "ml-leads", "a@example.com", "treatment", "lift is real", "t1")
    assert "winner" not in t["decision"]
    t = decide.record(t, "model-risk", "b@example.com", "treatment", "fine", "t2")
    assert t["decision"]["winner"] == "treatment" and t["decision"]["decided_at"] == "t2"


def test_disagreement_leaves_it_open_and_one_person_one_voice():
    t = decide.record(_test(), "ml-leads", "a@example.com", "treatment", "x", "t1")
    with pytest.raises(decide.Refused, match="already decided for another group"):
        decide.record(copy.deepcopy(t), "model-risk", "a@example.com", "treatment", "x", "t2")
    with pytest.raises(decide.Refused, match="already decided"):
        decide.record(copy.deepcopy(t), "ml-leads", "c@example.com", "control", "x", "t2")
    t = decide.record(t, "model-risk", "b@example.com", "control", "risk", "t2")
    assert "winner" not in t["decision"] and t["decision"]["disagreement"] == ["control", "treatment"]
    with pytest.raises(decide.Refused, match="not required"):
        decide.record(_test(), "finance", "c@example.com", "treatment", "x", "t1")
    with pytest.raises(decide.Refused, match="stopped"):
        decide.record(_test(state="stopped"), "ml-leads", "c@example.com", "treatment", "x", "t1")


def test_pending_lists_each_group_and_then_the_conclusion():
    res = {"primary": {"name": "renewed", "diff": 0.05, "p": 0.001, "treatment_better": True}, "blocked": [],
           "decision_reasons": ["the planned sample is reached"]}
    items = decisions.ab_items("churn", "prod", [_test(state="decision_required", analysis=res)])
    assert [i.kind for i in items] == ["ab-decide", "ab-decide"] and items[0].gate == "prod-ml-leads"
    assert "--winner" in items[0].command and items[0].command[items[0].command.index("--winner") + 1] == "treatment"
    done = _test(state="decision_required", analysis=res,
                 decision={"required": ["ml-leads"], "approvals": [], "winner": "control"})
    item = decisions.ab_items("churn", "prod", [done])[0]
    assert item.kind == "ab-conclude" and item.command[item.command.index("--winner") + 1] == "control"
    assert decisions.ab_items("churn", "prod", [_test()]) == []


# -- the analysis verdict ------------------------------------------------------------------------------------------

def _section(**analysis_kw):
    s = copy.deepcopy(abc.DEFAULTS)
    s.update({"enabled": True, "name": "lapse-v7", "unit": "customer_id"})
    s["metric"].update({"name": "renewed", "source": "bq://p.d.r", "time_column": "t", "minimum_effect": 0.05})
    s["analysis"].update(analysis_kw)
    return s


def _primary(c_n, c_y, t_n, t_y, c_exp=None, t_exp=None, switched=0):
    agg = lambda n, y: stats.Agg(n, y, y)                       # noqa: E731 - 0/1 outcomes: yy == y
    return {"segments": {"all": {"control": (agg(c_n, c_y), c_exp or c_n), "treatment": (agg(t_n, t_y), t_exp or t_n)}},
            "switched": switched, "first_exposure": None, "newest_outcome": None}


def test_a_split_off_plan_blocks_the_decision():
    r = analysis.evaluate(_test(), _section(), _primary(4000, 1200, 1000, 350, 4000, 1300), {},
                          T0 + timedelta(days=30))
    assert r["status"] == "blocked" and "sample ratio mismatch" in r["blocked"][0]
    r = analysis.evaluate(_test(), _section(), _primary(4000, 1200, 1000, 350, switched=3), {}, T0 + timedelta(days=30))
    assert any("both arms" in b for b in r["blocked"])


def test_no_decision_before_min_days_then_the_sample_decides():
    early = analysis.evaluate(_test(), _section(), _primary(8000, 2400, 2000, 700), {}, T0 + timedelta(days=5))
    assert early["status"] == "continue" and early["primary"]["treatment_better"]
    later = analysis.evaluate(_test(), _section(), _primary(8000, 2400, 2000, 700), {}, T0 + timedelta(days=20))
    assert later["status"] == "decision_required" and "planned sample" in later["decision_reasons"][0]
    small = analysis.evaluate(_test(), _section(), _primary(400, 120, 100, 35), {}, T0 + timedelta(days=20))
    assert small["status"] == "continue" and small["plan"]["progress"] < 1
    late = analysis.evaluate(_test(), _section(), _primary(400, 120, 100, 35), {}, T0 + timedelta(days=61))
    assert "max_days" in late["decision_reasons"][0]


def test_a_sequential_test_may_decide_early_and_its_p_only_falls():
    r = analysis.evaluate(_test(), _section(method="sequential"), _primary(3000, 900, 750, 300), {},
                          T0 + timedelta(days=15))
    p = r["primary"]["p_sequential"]
    assert p < 0.05 and r["status"] == "decision_required" and "sequential" in r["decision_reasons"][0]
    again = analysis.evaluate(_test(), _section(method="sequential"), _primary(3000, 900, 750, 230), {},
                              T0 + timedelta(days=16), previous=r)
    assert again["primary"]["p_sequential"] == p


# -- the service's rules -------------------------------------------------------------------------------------------

def _svc(monkeypatch, record=None, **uc_kw):
    FakeGcs(monkeypatch)
    monkeypatch.setattr(svc, "emit", lambda *a, **k: None)
    uc = SimpleNamespace(name="churn", tenant="retention", environment="prod",
                         ml={"ab_test": _section(), "inference": {"output_table": "preds"}},
                         model={"name": "m", "serving": {"enabled": False}}, pipelines={"inference": 1},
                         resources={"bigquery_dataset": "ds"}, approvals={})
    for k, v in uc_kw.items():
        setattr(uc, k, v)
    env = SimpleNamespace(name="prod", artifacts_bucket="b", project="p", region="r", security={}, policies={},
                          observability={}, tenants={}, approver_groups={})
    s = svc.ABTests(uc, env, SimpleNamespace(), log=lambda m: None)
    if record:
        s.store.create(record)
    return s


def test_the_allocation_may_only_go_down(monkeypatch):
    s = _svc(monkeypatch, _test())
    s.set_allocation(1000, "less risk", "a@example.com")
    with pytest.raises(ConfigError, match="only go down"):
        s.set_allocation(1500, "more", "a@example.com")
    assert [a["treatment_bp"] for a in s.current()["allocation"]] == [2000, 1000]


def test_pause_resume_stop(monkeypatch):
    s = _svc(monkeypatch, _test())
    assert s.set_state("paused", "incident", "a@example.com")["state"] == "paused"
    assert s.set_state("running", "fixed", "a@example.com")["state"] == "running"
    s.set_state("stopped", "enough", "a@example.com")
    assert s.current() is None and s.store.get("churn", "lapse-v7")["state"] == "stopped"
    with pytest.raises(ConfigError, match="no running test"):
        s.set_state("paused", "x", "a@example.com")


def test_the_analysis_refuses_a_changed_promise(monkeypatch):
    s = _svc(monkeypatch, _test(digest="not-the-promise"))
    with pytest.raises(ConfigError, match="differs from what test lapse-v7 promised"):
        s.analyse(query=lambda sql: [])


def test_conclude_waits_for_the_groups(monkeypatch):
    s = _svc(monkeypatch, _test(state="decision_required"))
    with pytest.raises(ConfigError, match="waits for approver group"):
        s.conclude("control", "x", "a@example.com", True)
    s.store.update("churn", "lapse-v7", lambda t: {**t, "decision": {**t["decision"], "winner": "control"}})
    with pytest.raises(ConfigError, match="chose the control"):
        s.conclude("treatment", "x", "a@example.com", True)
    t = s.conclude(None, "the groups chose", "a@example.com", True)
    assert t["state"] == "concluded" and t["decision"]["winner"] == "control"


def test_the_treatment_wins_only_on_evidence_without_groups(monkeypatch):
    s = _svc(monkeypatch, _test(decision={"required": [], "approvals": []}))
    with pytest.raises(ConfigError, match="only on evidence"):
        s.conclude("treatment", "looks good", "a@example.com", True)


def test_plan_sizes_the_test_and_warns_when_it_cannot_finish(monkeypatch):
    s = _svc(monkeypatch)
    assert "give --baseline" in s.plan()["note"]
    p = s.plan(baseline=0.3, per_day=50)
    assert p["needed"]["treatment"] < p["needed"]["control"] and "beyond max_days" in p["warning"]


# -- platform rules (phase C) ---------------------------------------------------------------------------------------

def test_overlapping_tests_on_one_unit_are_refused_or_recorded(monkeypatch):
    s = _svc(monkeypatch)
    s.store.create({**_test(name="other-test"), "usecase": "upsell"})
    s.store.create({**_test(name="by-account", unit="account_id"), "usecase": "fraud"})
    s.env.policies = {}
    assert s._overlaps(_section()) == []
    s.env.policies = {"ab_test": {"overlap": "warn"}}
    found = s._overlaps(_section())
    assert [(o["usecase"], o["ab_test"]) for o in found] == [("upsell", "other-test")]
    s.env.policies = {"ab_test": {"overlap": "refuse"}}
    with pytest.raises(ConfigError, match=r"other-test \(upsell\) already test"):
        s._overlaps(_section())


def test_the_platform_may_require_business_guardrails():
    problems = []
    t = _section()
    abc.apply_policy(t, {"required_guardrails": ["complaints"]}, "prod", problems)
    assert any("requires complaints" in p for p in problems)
    t["guardrails"]["business"] = [{"name": "complaints"}]
    problems = []
    abc.apply_policy(t, {"required_guardrails": ["complaints"]}, "prod", problems)
    assert problems == []


def test_with_cmek_the_results_table_carries_its_key_in_its_options(monkeypatch):
    """Found in the battle test: BigQuery refuses a job-level key on CREATE TABLE (Enterprise, CMEK)."""
    from pipewright.gcp import bigquery

    s = _svc(monkeypatch, _test(digest="d"))
    s.env.security = {"bigquery_kms_key": "projects/p/locations/r/keyRings/k/cryptoKeys/bq"}
    jobs = []
    monkeypatch.setattr(bigquery, "query",
                        lambda gcp, project, sql, location=None, kms_key=None: jobs.append((sql, kms_key)) or [])
    monkeypatch.setattr(bigquery, "rows", lambda job: [])
    monkeypatch.setattr(svc.ab_config, "digest", lambda section: "d")
    nobody = {"control": 0, "treatment": 0}
    monkeypatch.setattr(svc.ab_analysis, "run", lambda *a, **k: {
        "at": "t", "status": "continue", "exposed": nobody, "matured": nobody, "decision_reasons": [], "blocked": []})
    s.analyse()
    create = next((sql, k) for sql, k in jobs if sql.startswith("CREATE"))
    assert "OPTIONS(kms_key_name='projects/p/locations/r/keyRings/k/cryptoKeys/bq')" in create[0] and create[1] is None
    assert all(k is None for sql, k in jobs if sql.startswith("INSERT"))


def test_a_stop_that_could_not_remove_the_arm_can_be_finished(monkeypatch):
    """Found in the battle test: a dropped connection left the router running and `ab stop` refused a second go."""
    s = _svc(monkeypatch, _test())
    calls = []

    class Online:
        def stop(self, t):
            calls.append(t["state"])
            if len(calls) == 1:
                raise ConnectionError("dropped")

    s.online = Online()
    with pytest.raises(ConfigError, match="run `pipewright ab stop --env prod --usecase churn` again"):
        s.set_state("stopped", "enough", "a@example.com")
    assert s.current() is None and s.store.get("churn", "lapse-v7")["state"] == "stopped"
    done = s.set_state("stopped", "finish", "a@example.com")
    assert done["name"] == "lapse-v7" and calls == ["stopped", "stopped"]
    s.online = None
    with pytest.raises(ConfigError, match="no running test"):
        s.set_state("stopped", "x", "a@example.com")
