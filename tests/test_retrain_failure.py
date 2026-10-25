"""A retrain case whose training fails (Pipewright 1.1): the approval stays, the launcher's tick records how each
attempt ended, transient failures retry by themselves (capped), and a person retries with `retrain retry`."""
import json
from dataclasses import replace
from datetime import timedelta

import pytest

from pipewright.retraining import cases as rc
from tests.test_retraining import CONTRACT, DRIFT, T0, TWO, gcs, opened, service  # noqa: F401 - gcs is a fixture

ONE = replace(TWO, required=("ml-leads",))


def approved(s=ONE):
    case = opened(s)
    out = rc.decide(case, "approve", "lead@x", "ml-leads", s, T0, reason="shift")
    case = out.case
    case["training"] = {"kind": "retrain", "status": "submitted", "job_id": f"churn-{case['case_id']}",
                        "run_id": "r100", "run_key": case["case_id"], "source_uri": "gs://l/samples/churn.csv"}
    return case


def test_a_running_job_changes_nothing():
    case = approved()
    for state in ("PIPELINE_STATE_RUNNING", "PENDING", None):
        assert rc.reconcile(case, state, "", None, ONE, T0).case is None


def test_a_finished_job_records_the_version_or_the_gates():
    case = approved()
    out = rc.reconcile(case, "PIPELINE_STATE_SUCCEEDED", "", "4", ONE, T0)
    t = out.case["training"]
    assert t["outcome"] == "registered" and t["version"] == "4" and out.case["state"] == "approved"
    assert out.events[0][0] == "retrain.training_finished" and out.submit is None
    gated = rc.reconcile(case, "SUCCEEDED", "", None, ONE, T0)
    assert gated.case["training"]["outcome"] == "rejected"               # gate 1 kept it out of the registry
    assert rc.reconcile(gated.case, "SUCCEEDED", "", None, ONE, T0).case is None      # recorded once


def test_a_deterministic_failure_waits_for_a_person():
    out = rc.reconcile(approved(), "PIPELINE_STATE_FAILED", "ValueError: data contract failed: 12 nulls", None,
                       ONE, T0)
    t = out.case["training"]
    assert out.case["state"] == "approved" and t["outcome"] == "failed" and not t["transient"]
    assert out.submit is None and [e[0] for e in out.events] == ["retrain.training_failed"]
    assert "pipewright retrain retry --case" in out.events[0][1]["next"]


def test_transient_failures_retry_by_themselves_twice():
    case = approved()
    cid = case["case_id"]
    capacity = "503 Machine type temporarily unavailable, please retry"
    first = rc.reconcile(case, "FAILED", capacity, None, ONE, T0)
    assert first.submit == {"kind": "retrain", "source_uri": "gs://l/samples/churn.csv", "run_key": f"{cid}-retry-1",
                            "auto": True}
    assert [e[0] for e in first.events] == ["retrain.training_failed", "retrain.training_retried"]
    case = first.case
    for n in (2, 3):              # the next attempt, submitted again (as the service does), fails the same way
        case["training"] = {"kind": "retrain", "status": "submitted", "job_id": f"churn-{cid}-retry-{n - 1}",
                            "run_key": f"{cid}-retry-{n - 1}", "source_uri": "gs://l/samples/churn.csv", "auto": True}
        case["training_attempts"][-1]["auto"] = True
        out = rc.reconcile(case, "FAILED", capacity, None, ONE, T0 + timedelta(hours=n))
        case = out.case
    assert out.submit is None and len(case["training_attempts"]) == 3      # MAX_AUTO_RETRIES: then a person


def test_a_person_retries_a_failed_training():
    case = rc.reconcile(approved(), "FAILED", "KeyError: tenure_months", None, ONE, T0).case
    cid = case["case_id"]
    out = rc.decide(case, "retry", "lead@x", "ml-leads", ONE, T0, reason="fixed the view")
    assert out.submit["run_key"] == f"{cid}-retry-1" and out.events[0][0] == "retrain.training_retried"
    assert out.case["state"] == "approved" and out.case["history"][-1]["action"] == "retry"
    with pytest.raises(rc.DecisionRefused, match="required approver group"):
        rc.decide(case, "retry", "x@y", None, ONE, T0, reason="go")
    running = approved()
    with pytest.raises(rc.DecisionRefused, match="still running"):
        rc.decide(running, "retry", "lead@x", "ml-leads", ONE, T0, reason="go")
    looped = {**case, "loop": {"state": "approved"}}
    with pytest.raises(rc.DecisionRefused, match="closed retrain loop"):
        rc.decide(looped, "retry", "lead@x", "ml-leads", ONE, T0, reason="go")


def test_the_tick_reconciles_and_resubmits(gcs):  # noqa: F811 - the fixture
    now = {"t": T0}
    d, submitted, emitted = service(clock=lambda: now["t"])
    states = {"job": ("PIPELINE_STATE_FAILED", "503 temporarily unavailable")}
    d.job_state = lambda contract, job_id: states["job"]
    d.version_for_run = lambda contract, run: "5"
    cid = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r1", "observation": DRIFT})["case_id"]
    d.decide(CONTRACT, {"case_id": cid, "action": "approve", "reason": "shift", "id_token": "tok-lead"},
             "ml-leads", "topic:ml-leads")
    assert submitted[-1][2] == cid
    now["t"] = T0 + timedelta(hours=1)
    d.tick([CONTRACT])
    case = json.loads(gcs[f"cases/churn/{cid}.json"][0])
    assert case["state"] == "approved" and case["training"]["run_key"] == f"{cid}-retry-1" and case["training"]["auto"]
    assert submitted[-1][2] == f"{cid}-retry-1"
    assert [e[0] for e in emitted][-2:] == ["retrain.training_failed", "retrain.training_retried"]
    states["job"] = ("PIPELINE_STATE_SUCCEEDED", "")
    now["t"] = T0 + timedelta(hours=2)
    d.tick([CONTRACT])
    case = json.loads(gcs[f"cases/churn/{cid}.json"][0])
    assert case["training"]["outcome"] == "succeeded"          # the fake submit gives no run ID: no version lookup
    assert [a["outcome"] for a in case["training_attempts"]] == ["failed", "succeeded"]
    n = len(emitted)
    d.tick([CONTRACT])                                          # nothing more to do
    assert len(emitted) == n


def test_the_failure_emails_go_to_the_decision_recipients(repo):
    from pipewright.alerts import sync
    from tests.test_retraining import monitored, resolve

    monitored(repo, monitoring={"drift": {"enabled": True}, "output_table": "predictions"},
              retraining={"mode": "propose", "when": ["drift"], "source_uri": "gs://l/x.csv",
                          "decisions": {"notify": ["lead@example.com"]}})
    wanted = sync.wanted_policies(resolve(repo))
    assert wanted["retrain.training_failed"] == ["lead@example.com"] and "retrain.training_retried" in wanted


def test_show_tells_how_training_ended_and_how_to_retry():
    from pipewright.retraining.summary import render

    case = rc.reconcile(approved(), "FAILED", "KeyError: tenure_months", None, ONE, T0).case
    text = render(case)
    assert "FAILED: KeyError: tenure_months" in text and f"pipewright retrain retry --case {case['case_id']}" in text
