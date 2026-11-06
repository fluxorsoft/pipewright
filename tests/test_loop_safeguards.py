"""FR-48 phase 6 (user, 2026-11-06): safeguards for loops started without a person (retraining.mode candidate with
the closed retrain loop): a rate limit, a circuit breaker, drift on consecutive runs, and pause / resume. Held by a
safeguard, the case opens for a person to decide; nothing is lost."""
import json
from dataclasses import replace
from datetime import timedelta

from pipewright.retraining import cases as rc
from tests.test_retraining import CONTRACT, CTX, DRIFT, ML, T0, TWO, gcs, service  # noqa: F401 - gcs is a fixture

AUTO = replace(TWO, mode="candidate", loop=True, required=(), source_uri="bq://p.d.entities",
               safeguards=rc.safeguards({}))
STREAK = {**DRIFT, "recent": [{"alert": False}, {"alert": True}]}          # the run before this one alerted too


def auto_case(cid: str, state: str, days_ago: float) -> dict:
    return {"case_id": cid, "approved_at": rc.iso(T0 - timedelta(days=days_ago)),
            "loop": {"state": state, "history": [{"state": "approved", "by": rc.AUTOMATIC}]}}


def test_off_by_default_and_the_conservative_defaults():
    assert rc.Settings.from_config({"mode": "candidate", "loop": {"enabled": True}}).safeguards is None
    s = rc.Settings.from_config({"mode": "candidate", "loop": {"enabled": True, "safeguards": {"per_days": 14}}})
    assert s.safeguards == {"max_loops": 1, "per_days": 14, "pause_after_failures": 2, "drift_runs": 2}
    # Off: a single drifting run hands the case to the loop at once, as in 1.1.
    out = rc.observe(None, None, DRIFT, replace(AUTO, safeguards=None), T0, CTX)
    assert out.case["loop"]["state"] == "approved"


def test_drift_must_repeat_on_consecutive_runs():
    # Found live in QA: a held first alert opened a case every later alert joined, so drift never started a loop.
    # Below the streak no case opens; the monitoring history keeps the alert for the next run.
    first = rc.observe(None, None, DRIFT, AUTO, T0, CTX, {"recent": 0, "failures": 0})
    assert first.case is None and "2 scoring runs in a row" in first.status and first.status.startswith("no case yet")
    started = rc.observe(None, None, STREAK, AUTO, T0, CTX, {"recent": 0, "failures": 0})
    assert started.case["loop"]["state"] == "approved"
    # An explicit request (a schedule or a landing) is not drift: no streak needed.
    sched = rc.observe(None, None, {**DRIFT, "triggers": ["schedule"]}, AUTO, T0, CTX, {"recent": 0, "failures": 0})
    assert sched.case["loop"]["state"] == "approved"


def test_the_rate_limit_and_the_circuit_breaker():
    now = T0
    cases = [auto_case("rc-a", "closed", 2)]
    g = rc.guard(cases, {}, AUTO, now)
    assert g == {"paused": None, "recent": 1, "failures": 0}
    held = rc.observe(None, None, STREAK, AUTO, now, CTX, g)
    assert "1 automatic loop(s) in the last 7 days" in held.case["held"]
    assert rc.guard(cases, {}, AUTO, now + timedelta(days=4))["recent"] == 1
    assert rc.guard(cases, {}, AUTO, now + timedelta(days=6))["recent"] == 0

    failing = [auto_case("rc-c", "rolled-back", 20), auto_case("rc-b", "rejected", 30), auto_case("rc-a", "closed", 40)]
    g = rc.guard(failing, {}, AUTO, now)
    assert g["failures"] == 2
    held = rc.observe(None, None, STREAK, AUTO, now, CTX, g)
    assert "the last 2 automatic loops ended" in held.case["held"] and "loop resume" in held.case["held"]
    # A person's loop between them is not counted; a closed loop ends the run of failures.
    person = {**auto_case("rc-p", "failed", 25), "loop": {"state": "failed", "history": [{"by": "lead@x"}]}}
    assert rc.guard([failing[0], person, failing[2]], {}, AUTO, now)["failures"] == 1


def test_a_pause_holds_every_automatic_loop():
    paused = {"by": "lead@x", "at": rc.iso(T0), "reason": "investigating data quality"}
    held = rc.observe(None, None, STREAK, AUTO, T0, CTX, rc.guard([], {"paused": paused}, AUTO, T0))
    assert "paused (by lead@x" in held.case["held"] and "investigating data quality" in held.case["held"]


def test_the_service_pauses_after_failures_and_a_person_resumes(gcs):  # noqa: F811 - the fixture
    from pipewright.retraining import store

    ml = {**ML, "retraining": {**ML["retraining"], "mode": "candidate", "approvals": {},
                               "loop": {"enabled": True, "safeguards": {}}}}
    contract = replace(CONTRACT, pipelines={**CONTRACT.pipelines, "inference": {
        **CONTRACT.pipelines["inference"], "static_parameters": {"ml_config": ml}}})
    for c in (auto_case("rc-b", "failed", 20), auto_case("rc-a", "verification-failed", 30)):
        gcs[f"cases/churn/{c['case_id']}.json"] = (json.dumps({**c, "usecase": "churn", "state": "approved"}), 1)
    d, submitted, emitted = service()
    got = d.propose(contract, {"usecase": "churn", "run_id": "r5", "observation": STREAK})
    assert got["state"] == "open" and "loop resume" in got["detail"] and submitted == []
    pointer = json.loads(gcs["cases/churn/current.json"][0])
    assert pointer["paused"]["by"] == "safeguards" and pointer["open"] == got["case_id"]
    assert [e[0] for e in emitted] == ["retrain.proposed", "loop.paused"]

    s = store.CaseStore(None, "b")
    s.set_paused("churn", None, {"by": "lead@x", "at": "t", "reason": "fixed the feed"})
    pointer = json.loads(gcs["cases/churn/current.json"][0])
    assert "paused" not in pointer and pointer["resumed"]["by"] == "lead@x"          # found live: was the pause's
    assert pointer["resumed"]["reason"] == "fixed the feed" and pointer["resumed"]["ended"]["by"] == "safeguards"
    assert pointer["open"] == got["case_id"]                     # the open case is untouched


# -- what the loop's settings need of the use case (FR-48 phase 6) ------------------------------------------------

def test_a_bigquery_ml_loop_promotes_by_files(repo):
    from tests.test_bqml_config import bqml_repo, problems, resolve

    bqml_repo(repo, model_type="logistic_reg")
    repo.usecase["spec"]["retraining"] = {"mode": "propose", "loop": {"enabled": True, "data": "production"}}
    repo.usecase["spec"]["model"]["promotion"] = "model"
    uc = resolve(repo)
    assert uc.ml["retraining"]["loop"] == {"enabled": True, "data": "production"}
    repo.usecase["spec"]["model"].pop("promotion")
    assert "loop promotes its candidate by files" in problems(repo)
    bqml_repo(repo)                                                     # boosted trees: by retraining only
    repo.usecase["spec"]["retraining"] = {"mode": "propose", "loop": {"enabled": True}}
    assert "a boosted_tree_classifier is promoted by retraining" in problems(repo)


def test_the_loops_settings_need_the_loop_and_an_endpoint_for_the_canary(repo):
    from tests.test_config_ml import ml_repo, problems

    ml_repo(repo)
    repo.usecase["spec"]["retraining"] = {"mode": "propose", "loop": {"safeguards": {}}}
    assert "apply only with retraining.loop.enabled" in problems(repo)
    repo.usecase["spec"]["retraining"] = {"mode": "propose", "loop": {"enabled": True, "canary": {"percent": 10}}}
    assert "not served online" in problems(repo)


def test_a_promoted_bigquery_ml_model_with_transform_is_not_served_online(repo):
    """Found live in phase 6 (ent-prod): the endpoint served the exported model without its TRANSFORM; the parity
    check refused it after the deploy. Refused up front now."""
    from tests.test_bqml_config import bqml_repo, problems

    bqml_repo(repo, model_type="logistic_reg", transform="sql/transform.sql")
    repo.usecase["spec"]["model"].update(promotion="model", serving={"enabled": True})
    repo.add_env("prod")
    repo.environments["prod"]["promotion"] = {"from": "dev"}
    assert "does not apply its TRANSFORM" in problems(repo, env="prod")
