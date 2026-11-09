"""FR-57 phase B3: the launcher's tick for A/B tests (results in, serving guardrails, the scheduled analysis)."""
import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pipewright.abtest import config as abc
from pipewright.abtest import service as svc
from pipewright.abtest import watch
from pipewright.abtest.store import TestStore
from pipewright.errors import ConfigError
from tests.test_abtest_core import FakeGcs

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
STARTED = NOW - timedelta(days=2)


def test_cron():
    assert watch.cron_matches("0 6 * * *", datetime(2026, 11, 10, 6, 0))
    assert not watch.cron_matches("0 6 * * *", datetime(2026, 11, 10, 6, 1))
    assert watch.cron_matches("*/15 8-10 * * 1-5", datetime(2026, 11, 10, 9, 45))      # a Tuesday
    assert not watch.cron_matches("*/15 8-10 * * 1-5", datetime(2026, 11, 8, 9, 45))   # a Sunday
    assert watch.cron_matches("0 0 1 * 0", datetime(2026, 11, 8, 0, 0))               # Sunday: either day field
    assert watch.due("0 6 * * *", NOW - timedelta(hours=7), NOW)
    assert not watch.due("0 6 * * *", NOW - timedelta(hours=5), NOW)
    with pytest.raises(ConfigError, match="five cron fields"):
        watch.cron_matches("daily", NOW)


def _line(minutes_ago, instance, **counts):
    return {"_ts": (NOW - timedelta(minutes=minutes_ago)).isoformat(), "instance": instance, **counts}


def test_router_calls_are_differences_per_instance():
    start = NOW - timedelta(minutes=70)
    lines = [_line(72, "a", treatment_calls=100, treatment_instances=100),
             _line(40, "a", treatment_calls=130, treatment_instances=130, dropped=2),
             _line(15, "a", treatment_calls=160, treatment_instances=160, dropped=2),
             _line(30, "b", treatment_calls=25, treatment_instances=50, unassigned=1),    # started in the window
             _line(75, "c", treatment_calls=999)]                                         # idle all window
    sent, loss = watch.router_calls(lines, start)
    assert sent["treatment_calls"] == 60 + 25 and sent["treatment_instances"] == 60 + 50
    assert loss == {"unassigned": 1, "dropped": 2, "mismatched_model": 0}


class Gcp:
    def __init__(self, logs=(), metrics=None, run_ok=True):
        self.logs, self.metrics, self.run_ok, self.posts = list(logs), metrics or {}, run_ok, []
        self.session = SimpleNamespace(credentials=None)

    def post(self, url, body, **k):
        self.posts.append((url, body))
        if url == watch.LOG_API:
            event = "counts" if 'jsonPayload.event="counts"' in body["filter"] else "breaker_tripped"
            entries = [{"jsonPayload": {**line, "event": event}, "timestamp": line["_ts"]}
                       for line in self.logs if line.get("event", "counts") == event]
            return SimpleNamespace(ok=True, status=200, body={"entries": entries})
        return SimpleNamespace(ok=self.run_ok, status=200 if self.run_ok else 403,
                               body={"metadata": {"name": "executions/e1"}})


def _section(**kw):
    s = copy.deepcopy(abc.DEFAULTS)
    s.update({"enabled": True, "name": "lapse-v7", "unit": "customer_id"})
    s["metric"].update({"name": "renewed", "source": "bq://p.d.r", "time_column": "t"})
    s.update(kw)
    return s


def _test(**kw):
    t = {"name": "lapse-v7", "usecase": "churn", "unit": "customer_id", "mode": "ab", "state": "running",
         "started_at": STARTED.isoformat(), "digest": abc.digest(_section()),
         "arms": {"control": {"version": "5"},
                  "treatment": {"version": "7", "endpoint": "projects/1/locations/r/endpoints/42"}},
         "allocation": [{"at": STARTED.isoformat(), "treatment_bp": 2000}],
         "decision": {"required": [], "approvals": []}, "history": []}
    t.update(kw)
    return t


def _metrics(monkeypatch, answered, errors):
    from pipewright.loop import canary

    seen = {}

    def fake(gcp, project, endpoint_id, start, end):
        seen.update(endpoint=endpoint_id, start=start, end=end)
        return {"d1": {"predictions": answered, "errors": errors}}

    monkeypatch.setattr(canary, "endpoint_metrics", fake)
    return seen


MANIFEST = {"project": "p", "region": "r", "environment": "prod", "artifacts_bucket": "b", "artifact_registry": "ar",
            "tenants": {"retention": {"features": {"sync_job": "projects/p/locations/r/jobs/pw-sync-retention"}}}}


def test_an_error_rate_over_the_breakers_limit_is_a_breach(monkeypatch):
    seen = _metrics(monkeypatch, 400, 40)
    counts = [_line(65, "a", treatment_calls=0), _line(11, "a", treatment_calls=400)]
    out = watch.check_serving(Gcp(counts), MANIFEST, _test(), _section(), NOW)
    assert "40 error(s)" in out["breach"] and seen["endpoint"] == "42" and out["bypassed"] == 0
    assert seen["end"] == "2026-11-10T11:50:00Z"


def test_a_trip_waits_for_monitoring_then_is_trusted(monkeypatch):
    _metrics(monkeypatch, 0, 0)
    fresh = [{**_line(3, "a"), "event": "breaker_tripped"}]
    out = watch.check_serving(Gcp(fresh), MANIFEST, _test(), _section(), NOW)
    assert out["breach"] is None and "not in Cloud Monitoring yet" in out["note"]
    old = [{**_line(25, "a"), "event": "breaker_tripped"}]
    assert "breaker tripped" in watch.check_serving(Gcp(old), MANIFEST, _test(), _section(), NOW)["breach"]


def test_calls_the_router_did_not_send_are_bypass(monkeypatch):
    _metrics(monkeypatch, 500, 0)
    counts = [_line(65, "a", treatment_calls=0), _line(11, "a", treatment_calls=100, treatment_instances=100)]
    out = watch.check_serving(Gcp(counts), MANIFEST, _test(), _section(), NOW)
    assert out["bypassed"] == 400 and out["breach"] is None
    _metrics(monkeypatch, 105, 0)
    assert watch.check_serving(Gcp(counts), MANIFEST, _test(), _section(), NOW)["bypassed"] == 0


# -- the tick ------------------------------------------------------------------------------------------------------

def _contract(section):
    target = SimpleNamespace(static_parameters={"ml_config": {
        "ab_test": section, "model_name": "m", "platform": {"bigquery_dataset": "ds"},
        "inference": {"output_table": "preds"}}})
    return SimpleNamespace(usecase="churn", tenant="retention", environment="prod", release_id="R1",
                           pipelines={"training": {}, "inference": {}}, pipeline=lambda name: target)


def _setup(monkeypatch, test):
    from pipewright.gcp import storage

    fake = FakeGcs(monkeypatch)
    for name in ("gcs_read", "gcs_write", "gcs_list"):
        monkeypatch.setattr(storage, name, getattr(__import__("pipewright.abtest.store", fromlist=["x"]), name))
    events = []
    monkeypatch.setattr(svc, "emit", lambda env, uc, gcp, event_type, t, **d: events.append((event_type, d)))
    TestStore(None, "b").create(test)
    return fake, events


def test_a_breach_stops_the_treatment_and_opens_the_decision(monkeypatch):
    fake, events = _setup(monkeypatch, _test(analysis_requested_at=(NOW - timedelta(hours=1)).isoformat()))
    _metrics(monkeypatch, 400, 60)
    refreshed = []
    gcp = Gcp([_line(65, "a", treatment_calls=0), _line(11, "a", treatment_calls=400, unassigned=3)])
    out = watch.tick(gcp, MANIFEST, [_contract(_section())], refresh=refreshed.append, now=NOW)
    t = TestStore(None, "b").get("churn", "lapse-v7")
    assert t["state"] == "decision_required" and t["allocation"][-1]["treatment_bp"] == 0
    assert t["router"]["unassigned"] == 3 and t["guardrails_breached"][0]["kind"] == "technical"
    assert [k for k, _ in events] == ["ab_test.guardrail_breached"] and refreshed
    assert out["tests"][0]["serving"]["breach"] and "analysis" not in out["tests"][0]


def test_the_schedule_starts_the_tenants_job_and_its_result_is_recorded(monkeypatch):
    fake, events = _setup(monkeypatch, _test(arms={"control": {"version": "5"}, "treatment": {"version": "7"}}))
    gcp = Gcp()
    out = watch.tick(gcp, MANIFEST, [_contract(_section())], now=NOW)
    started = out["tests"][0]["analysis"]
    assert started["status"] == "submitted"
    url, body = gcp.posts[-1]
    assert url.endswith("/jobs/pw-sync-retention:run")
    args = body["overrides"]["containerOverrides"][0]["args"]
    assert args[:3] == ["ab", "analyse", "--request"] and args[-1] == "churn"
    request = json.loads(fake.objects[started["request"]][0])
    assert request["test"]["name"] == "lapse-v7" and request["ml_config"]["platform"]["bigquery_dataset"] == "ds"
    assert TestStore(None, "b").get("churn", "lapse-v7")["analysis_requested_at"] == "2026-11-10T12:00:00Z"
    assert "analysis" not in watch.tick(gcp, MANIFEST, [_contract(_section())], now=NOW + timedelta(hours=1))[
        "tests"][0]

    # the job: the analysis as the runner, its result left for the launcher
    from pipewright.abtest import analysis

    monkeypatch.setattr(analysis, "run", lambda *a, **k: {
        "at": NOW.isoformat(), "status": "decision_required", "decision_reasons": ["the planned sample is reached"],
        "blocked": [], "exposed": {"control": 8000, "treatment": 2000}, "primary": {"name": "renewed"}})
    monkeypatch.setattr(svc.ABTests, "_write_result", lambda self, t, r, q: None)
    monkeypatch.setattr(svc.ABTests, "_has_online", lambda self, q: False)
    done = watch.run_request(gcp, f"gs://b/{started['request']}", "churn", query=lambda sql: [])
    assert done["status"] == "decision_required" and done["result"].startswith("online-sync/retention/churn/ab/")
    assert TestStore(None, "b").get("churn", "lapse-v7")["state"] == "running"     # the job does not record
    with pytest.raises(ConfigError, match="not other"):
        watch.run_request(gcp, f"gs://b/{started['request']}", "other")

    out = watch.tick(gcp, MANIFEST, [_contract(_section())], now=NOW + timedelta(hours=2))
    t = TestStore(None, "b").get("churn", "lapse-v7")
    assert out["tests"][0]["ingested"] == [done["result"]] and t["ingested"] == done["result"]
    assert t["state"] == "decision_required" and ("ab_test.analysed", ) == (events[-1][0], )
    assert watch.tick(gcp, MANIFEST, [_contract(_section())], now=NOW + timedelta(hours=3))["tests"][0][
        "ingested"] == []


def test_a_use_case_without_a_test_is_not_checked(monkeypatch):
    _setup(monkeypatch, _test(state="stopped"))
    assert watch.tick(Gcp(), MANIFEST, [_contract(_section())], now=NOW)["checked"] == 0
    assert watch.tick(Gcp(), MANIFEST, [_contract({})], now=NOW)["checked"] == 0


# -- shadow tests (phase D) ----------------------------------------------------------------------------------------

def test_a_shadow_is_summed_from_the_gateways_lines_and_never_won_by_the_treatment(monkeypatch):
    from pipewright.abtest import analysis, decide

    section = _section(mode="shadow")
    test = _test(mode="shadow")
    sql = analysis.shadow_sql(test, "p", "ds")
    assert "jsonPayload.event = 'shadow'" in sql and "pipewright_ab_exposure" in sql
    row = {"calls": 500, "failures": 2, "compared": 4800, "mean_abs_diff": 0.031, "agreement": 0.94,
           "ms_p50": 41.0, "ms_p95": 90.0}
    early = analysis.run(test, section, "p", "ds", None, True, lambda q: [row], now=STARTED + timedelta(days=3))
    assert early["status"] == "continue" and early["shadow"]["failure_rate"] == 0.004
    late = analysis.evaluate_shadow(test, section, row, STARTED + timedelta(days=15))
    assert late["status"] == "decision_required" and "min_days" in late["decision_reasons"][0]
    assert "agreement 94.0%" in analysis.render(late, test)
    broken = analysis.evaluate_shadow(test, section, {**row, "failures": 100}, STARTED + timedelta(days=1))
    assert broken["guardrails"][0]["breached"] and broken["status"] == "decision_required"
    assert analysis.run(test, section, "p", "ds", None, True, lambda q: 1 / 0, now=NOW)["status"] == "blocked"
    row_out = analysis.results_row(test, late, "R1")
    assert row_out["exposed_treatment"] == 500 and row_out["diff"] is None
    with pytest.raises(decide.Refused, match="shadow test shows"):
        decide.record(test, "ml-leads", "a@example.com", "treatment", "x", "t1")


def test_shadow_needs_online_serving():
    problems = []
    abc.check_tables({"ab_test": _section(mode="shadow")}, {}, True, False, problems)
    assert any("mode shadow" in p for p in problems)


def test_the_arms_are_compared_as_the_canary_is(monkeypatch):
    from pipewright.loop import canary

    def fake(gcp, project, endpoint_id, start, end):
        if endpoint_id == "42":                            # the treatment: 2% errors, twice as slow
            return {"t": {"predictions": 500, "errors": 10, "latency_ms": 80.0}}
        return {"c1": {"predictions": 1500, "errors": 3, "latency_ms": 40.0},
                "c2": {"predictions": 500, "errors": 1, "latency_ms": 40.0}}

    monkeypatch.setattr(canary, "endpoint_metrics", fake)
    counts = [_line(65, "a", treatment_calls=0), _line(11, "a", treatment_calls=500)]
    out = watch.check_serving(Gcp(counts), MANIFEST, _test(), _section(), NOW, control_endpoint="projects/1/e/7")
    assert out["breach"].startswith("against the control") and "error rate" in out["breach"]
    assert out["compared"]["control"]["predictions"] == 2000
    loose = _section()
    loose["guardrails"]["technical"].update(max_error_rate_increase=0.05, max_latency_increase=1.5)
    assert watch.check_serving(Gcp(counts), MANIFEST, _test(), loose, NOW,
                               control_endpoint="projects/1/e/7")["breach"] is None


def test_a_kserve_test_is_judged_by_the_routers_counts(monkeypatch):
    from pipewright.loop import canary

    monkeypatch.setattr(canary, "endpoint_metrics", lambda *a: pytest.fail("KServe has no Agent Platform metrics"))
    test = _test(arms={"control": {"version": "5"}, "treatment": {"version": "7",
                                                                   "endpoint": "kserve:serving-retail/lapse-risk-ab"}})
    counts = [_line(65, "a", treatment_calls=0, control_calls=0),
              _line(11, "a", treatment_calls=400, treatment_errors=30, treatment_ms=24000,
                    control_calls=1600, control_errors=4, control_ms=64000)]
    out = watch.check_serving(Gcp(counts), MANIFEST, test, _section(), NOW)
    assert out["answered"] == 400 and out["errors"] == 30 and "30 error(s)" in out["breach"]
    assert out["compared"]["control"]["latency_ms"] == 40 and out["bypassed"] == 0
    calm = [_line(65, "a"), _line(11, "a", treatment_calls=400, treatment_errors=1, treatment_ms=16000,
                                  control_calls=1600, control_errors=4, control_ms=64000)]
    assert watch.check_serving(Gcp(calm), MANIFEST, test, _section(), NOW)["breach"] is None
