"""FR-79: a retrain case records what its training did. When the gates refuse the approved training, the case opens
again with the reasons, `retrain.refused` is emitted (and emailable), and no cooldown holds back new cases."""
from datetime import timedelta
from types import SimpleNamespace

from pipewright import adapters
from pipewright.retraining import cases as rc
from pipewright.retraining.client import render_list
from pipewright.retraining.summary import render
from tests.test_retrain_failure import ONE, approved
from tests.test_retraining import CONTRACT, DRIFT, T0, gcs, service  # noqa: F401 - gcs is a fixture

REASONS = ["gate calibration_error=0.1327 above max 0.05", "champion: auc 0.7843 vs 0.7796 (champion version 10)"]


def test_a_refused_training_opens_the_case_again_with_the_gates_reasons():
    # The ebook's Chapter 15: approved, trained, refused on calibration, and the case said nothing of it.
    out = rc.reconcile(approved(), "SUCCEEDED", "", None, ONE, T0, REASONS)
    case = out.case
    assert case["state"] == "open" and case["approvals"] == [] and case["training"]["outcome"] == "rejected"
    assert case["training"]["reasons"] == REASONS and case["training_attempts"][-1]["reasons"] == REASONS
    assert [e[0] for e in out.events] == ["retrain.training_finished", "retrain.refused"]
    assert out.events[1][1]["reasons"] == REASONS and "--source-uri" in out.events[1][1]["next"]
    assert "calibration_error" in case["history"][-1]["reason"]


def test_a_registered_version_keeps_the_approval_and_the_cooldown():
    out = rc.reconcile(approved(), "SUCCEEDED", "", "11", ONE, T0)
    assert out.case["state"] == "approved" and [e[0] for e in out.events] == ["retrain.training_finished"]


def test_show_and_list_say_what_the_training_did():
    case = rc.reconcile(approved(), "SUCCEEDED", "", None, ONE, T0, REASONS).case
    text = render(case)
    assert "rejected by the evaluation gates" in text and REASONS[0] in text and "open again" in text
    listing = render_list("churn", SimpleNamespace(name="test"), [case])
    assert "training" in listing.splitlines()[1] and "refused" in listing.splitlines()[2]


def test_the_tick_clears_the_cooldown_when_the_gates_refuse(gcs):  # noqa: F811 - the fixture
    now = {"t": T0}
    d, submitted, emitted = service(clock=lambda: now["t"])
    d.job_state = lambda contract, job_id: ("PIPELINE_STATE_SUCCEEDED", "")
    d.version_for_run = lambda contract, run: None
    d.reasons_for_job = lambda contract, job_id: REASONS
    cid = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r1", "observation": DRIFT})["case_id"]
    d.decide(CONTRACT, {"case_id": cid, "action": "approve", "reason": "shift", "id_token": "tok-lead"},
             "ml-leads", "topic:ml-leads")
    d.store.update_case("churn", cid, lambda c: {**c, "training": {**c["training"], "run_id": "r2"}})
    now["t"] = T0 + timedelta(hours=1)
    d.tick([CONTRACT])
    pointer, _ = d.store.pointer("churn")
    assert pointer["open"] == cid and pointer["last_approved_at"] is None
    assert d.store.get("churn", cid)["state"] == "open"
    assert any(e[0] == "retrain.refused" for e in emitted)
    # A drift run the next day adds to the case instead of being suppressed by a cooldown.
    now["t"] = T0 + timedelta(days=1)
    out = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r3", "observation": DRIFT})
    assert out.get("case_id") == cid and "cooldown" not in str(out)


def test_the_reasons_come_from_the_evaluate_steps_output():
    body = {"jobDetail": {"taskDetails": [
        {"execution": {"metadata": {"output:decision": "reject"}}},
        {"execution": {"metadata": {"output:reasons": '["gate auc=0.61 below min 0.7"]'}}}]}}
    gcp = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok=True, body=body))
    contract = SimpleNamespace(orchestrator="vertex", region="europe-west2", project="p")
    assert adapters.job_reasons(contract, "job-1", gcp) == ["gate auc=0.61 below min 0.7"]
    assert adapters.job_reasons(SimpleNamespace(orchestrator="kfp"), "job-1", gcp) == []
    missing = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok=False, body={}))
    assert adapters.job_reasons(contract, "job-1", missing) == []


def test_the_refusal_is_an_emailable_decision_event():
    from pipewright.alerts.sync import DECISION_EVENTS

    assert "retrain.refused" in DECISION_EVENTS


def test_an_alert_during_the_cooldown_is_kept_on_the_approved_case(gcs):  # noqa: F811 - the fixture
    now = {"t": T0}
    d, submitted, emitted = service(clock=lambda: now["t"])
    cid = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r1", "observation": DRIFT})["case_id"]
    d.decide(CONTRACT, {"case_id": cid, "action": "approve", "reason": "shift", "id_token": "tok-lead"},
             "ml-leads", "topic:ml-leads")
    now["t"] = T0 + timedelta(days=2)                       # inside the 7-day cooldown
    out = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r9", "observation": DRIFT})
    assert out["status"] == "no-case" and "cooldown" in out["detail"]
    observed = d.store.get("churn", cid)["observations"]
    assert observed[-1]["run_id"] == "r9" and observed[-1]["during"] == "cooldown"
