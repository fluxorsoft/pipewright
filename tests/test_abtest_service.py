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
    assert any("control and later the treatment" in b for b in r["blocked"])


def test_units_moved_to_the_control_are_reported_and_do_not_block():
    # 1.4.3 GA QA (live, KServe): a lowered allocation and a pause sent treatment units to the control; the analysis
    # read them as a broken assignment and blocked the test
    primary = {**_primary(4000, 1200, 4000, 1300), "moved": 177}
    r = analysis.evaluate(_test(), _section(), primary, {}, T0 + timedelta(days=30))
    assert r["moved"] == 177 and not any("broken" in b for b in r.get("blocked") or [])


def test_the_exposure_sql_takes_the_first_arm_and_tells_moves_from_switches():
    sql = analysis.exposures_sql({"name": "t", "unit": "customer_id"}, "p", "d", None, True, [])
    assert "ARRAY_AGG(arm ORDER BY first_at LIMIT 1)[OFFSET(0)] AS arm" in sql
    assert "MIN(IF(arm = 'control', first_at, NULL)) < MIN(IF(arm = 'treatment', first_at, NULL))" in sql   # switched
    assert "MIN(IF(arm = 'treatment', first_at, NULL)) <= MIN(IF(arm = 'control', first_at, NULL))" in sql  # moved


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


# -- FR-122: the feed copy leaves out who acted ------------------------------------------------------------------

def _feed_emit(monkeypatch, publish):
    import base64
    import json

    sent = []
    monkeypatch.setattr("pipewright.observability.events.emit", lambda *a, **k: None)
    gcp = SimpleNamespace(post=lambda url, body: sent.append(body) or SimpleNamespace(ok=True, status=200))
    uc = SimpleNamespace(name="churn", tenant="retention", ml={"ab_test": {"publish": publish}})
    env = SimpleNamespace(name="prod", region="r", observability={},
                          tenants={"retention": {"ab_test_feed": {"topic": "projects/p/topics/feed"}}})
    svc.emit(env, uc, gcp, "ab_test.decided", {"name": "lapse-v7", "release": "R1"}, by="lead@example.com",
             winner="treatment", approvals=[{"group": "ml-leads", "by": "x@example.com"}])
    (body,) = sent
    (message,) = body["messages"]
    return json.loads(base64.b64decode(message["data"])), message["attributes"]


def test_the_feed_copy_leaves_out_who_acted(monkeypatch):
    data, attributes = _feed_emit(monkeypatch, {"events": True})
    assert "by" not in data["data"] and data["data"]["approvals"] == [{"group": "ml-leads"}]
    assert data["data"]["winner"] == "treatment" and attributes["event_type"] == "ab_test.decided"
    assert "lead@example.com" not in str(data) and "x@example.com" not in str(data)
    kept, _ = _feed_emit(monkeypatch, {"events": True, "include_by": True})
    assert kept["data"]["by"] == "lead@example.com"


# -- FR-121: what comes next ----------------------------------------------------------------------------------------

def test_the_next_step_follows_the_test():
    base = {**_test(), "environment": "prod"}
    step = decide.next_step(base)
    assert step["action"] == "analyse" and step["command"] == "pipewright ab analyse --env prod --usecase churn"
    going = {**base, "analysis": {"status": "continue", "age_days": 3.0, "matured": {"control": 900, "treatment": 200},
                                  "plan": {"needed": {"control": 1500, "treatment": 700}},
                                  "duration": {"min_days": 7, "max_days": 60}}}
    text = decide.next_step(going)["text"]
    assert "4 more day(s) to min_days" in text and "about 600 more units" in text and "2026-12-31" in text
    blocked = {**base, "analysis": {"status": "blocked", "blocked": ["sample ratio mismatch: ..."]}}
    assert decide.next_step(blocked)["action"] == "fix" and "sample ratio" in decide.next_step(blocked)["text"]
    due = {**base, "state": "decision_required", "decision": {"required": ["ml-leads"], "approvals": []},
           "analysis": {"status": "decision_required", "decision_reasons": ["the planned sample is reached"],
                        "primary": {"treatment_better": True}}}
    step = decide.next_step(due)
    assert step["action"] == "decide" and "--winner treatment --group ml-leads" in step["command"]
    due["decision"] = {"required": [], "approvals": []}
    assert decide.next_step(due)["action"] == "conclude"
    won = {**base, "state": "concluded", "decision": {"winner": "treatment"}}
    assert decide.next_step(won, default_version="7", served=["5"])["action"] == "deploy"
    assert decide.next_step(won, default_version="7", served=["7"])["action"] == "none"
    assert "a newer approval" in decide.next_step(won, default_version="9")["text"]
    assert "v5 stays the default" in decide.next_step({**won, "decision": {"winner": "control"}})["text"]
    assert decide.next_step({**base, "state": "paused"})["action"] == "resume"


def test_every_analysis_is_kept_in_brief(monkeypatch):
    s = _svc(monkeypatch, _test())
    for i in range(3):
        s.record_result("lapse-v7", {"at": f"2026-11-0{i + 2}T00:00:00", "status": "continue",
                                     "primary": {"name": "lapse", "control": 0.3, "treatment": 0.3 + 0.01 * i,
                                                 "diff": 0.01 * i, "ci": [-0.01, 0.02 * i], "p": 0.5},
                                     "exposed": {"control": 100 * i, "treatment": 20 * i}, "blocked": [],
                                     "matured": {"control": 0, "treatment": 0},
                                     "decision_reasons": []})
    rows = s.current()["results"]
    assert [r["diff"] for r in rows] == [0.0, 0.01, 0.02] and rows[-1]["exposed"] == {"control": 200, "treatment": 40}
    assert "next: " in svc.render_test(s.current())


def test_the_platforms_rules_govern_starting_never_stopping(monkeypatch):
    """Found in the QA battle test: with policies.ab_test.allowed false, `ab stop` was refused; a running test must
    stay stoppable whatever the platform's rules say now."""
    import importlib

    cli = importlib.import_module("pipewright.cli.main")

    seen = {}
    env = SimpleNamespace(name="test", policies={"ab_test": {"allowed": False, "max_treatment_bp": 100,
                                                             "feed_exposures": True}, "deploy_model": "manual"})
    import dataclasses

    @dataclasses.dataclass(frozen=True)
    class Env:
        name: str
        policies: dict

    monkeypatch.setattr(cli, "load_environment", lambda root, name: Env("test", env.policies))
    monkeypatch.setattr(cli, "_selected", lambda root, names: {names[0]: "p"})

    def resolve(path, e):
        seen["policies"] = e.policies
        raise ConfigError("stop here")

    monkeypatch.setattr(cli, "resolve_usecase", resolve)
    for command, rules in (("stop", {"feed_exposures": True}), ("status", {"feed_exposures": True}),
                           ("start", env.policies["ab_test"])):
        args = SimpleNamespace(request=None, env="test", root=".", usecase="churn", ab_command=command)
        with pytest.raises(ConfigError, match="stop here"):
            cli.cmd_ab(args)
        assert seen["policies"]["ab_test"] == rules and seen["policies"]["deploy_model"] == "manual", command


def test_edge_cases_of_the_next_step():
    """QA edge-case review: groups that disagree can never record a winner (each decides once)."""
    base = {**_test(), "environment": "prod", "state": "decision_required",
            "analysis": {"status": "decision_required", "decision_reasons": ["the planned sample is reached"],
                         "primary": {"treatment_better": True}},
            "decision": {"required": ["ml-leads", "model-risk"], "disagreement": ["control", "treatment"],
                         "approvals": [{"group": "ml-leads", "winner": "treatment"},
                                       {"group": "model-risk", "winner": "control"}]}}
    step = decide.next_step(base)
    assert step["action"] == "stop" and "ml-leads chose the treatment" in step["text"]
    assert step["command"].startswith("pipewright ab stop")
    # a shadow test due for a decision concludes with the control
    shadow = {**base, "mode": "shadow", "decision": {"required": [], "approvals": []},
              "analysis": {"status": "decision_required", "decision_reasons": ["max_days (14) is reached"],
                           "mode": "shadow"}}
    assert "--winner control" in decide.next_step(shadow)["command"]


def test_a_shadow_test_takes_no_layer():
    problems = []
    t = _section()
    t["mode"] = "shadow"
    t["layer"] = {"name": "customers", "share_bp": 2000}
    abc.check({"ab_test": t, "task": "binary"}, set(), "churned", lambda *a: None, problems)
    assert any("a shadow test exposes no unit" in p for p in problems)
