import base64
import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from pipewright.gcp import PreconditionFailed
from pipewright.retraining import cases as rc
from pipewright.retraining import identity, store
from pipewright.retraining.service import Decisions
from pipewright.retraining.summary import SUMMARY_LIMIT, render, summary
from tests.test_config_ml import ml_repo, problems, resolve
from tests.test_release_parts import INFERENCE, V3

T0 = datetime(2026, 10, 7, 9, 0, tzinfo=timezone.utc)
RUNNER = "runner@p.iam.gserviceaccount.com"
CTX = {"usecase": "churn", "environment": "dev", "tenant": "churn", "release": "abc123", "requested_by": RUNNER,
       "run_id": "r1", "champion": {"version": "3"}}
DRIFT = {"triggers": ["drift"], "run_id": "r1", "max_feature_psi": 0.31, "prediction_psi": 0.05,
         "drifted_features": ["tenure_months"], "feature_psi": {"tenure_months": 0.31, "monthly_charges": 0.02},
         "labelled_rows": 12, "data_checks": {"rows": 500, "errors": [], "null_fractions": {"tenure_months": 0.01}}}
TWO = rc.Settings(mode="propose", when=("drift", "performance"), required=("ml-leads", "model-risk"),
                  source_uri="gs://l/samples/churn.csv", max_snooze_days=14)


# ------------------------------------------------------------------------------------------------- config --

def monitored(repo, **sections):
    ml_repo(repo, **sections)
    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"), "inference": {"file": "pipeline.py", "function": "infer"}}
    return repo


def test_modes_and_the_older_switch(repo):
    monitored(repo, monitoring={"drift": {"enabled": True}, "output_table": "predictions"},
            retraining={"enabled": True, "when": ["drift"], "source_uri": "gs://l/x.csv"})
    r = resolve(repo).ml["retraining"]
    assert (r["mode"], r["mode_source"], r["enabled"]) == ("propose", "enabled", True)
    repo.usecase["spec"]["environments"]["dev"]["retraining"] = {"enabled": False}
    r = resolve(repo).ml["retraining"]
    assert (r["mode"], r["enabled"]) == ("off", False)
    repo.usecase["spec"]["environments"]["dev"]["retraining"] = {"mode": "candidate"}
    repo.usecase["spec"]["retraining"] = {"mode": "propose", "when": ["drift"], "source_uri": "gs://l/x.csv"}
    assert resolve(repo).ml["retraining"]["mode"] == "candidate"
    repo.usecase["spec"]["environments"]["dev"]["retraining"] = {"mode": False}      # YAML: `mode: off`
    assert resolve(repo).ml["retraining"]["mode"] == "off"
    assert rc.Settings.from_config({"mode": False, "enabled": True}).mode == "off"


def test_candidate_needs_manual_promotion(repo):
    monitored(repo, monitoring={"drift": {"enabled": True}, "output_table": "predictions"},
            retraining={"mode": "candidate", "when": ["drift"], "source_uri": "gs://l/x.csv"})
    repo.usecase["spec"]["approvals"]["promote_model"] = "auto"
    assert "a person must promote" in problems(repo)


def test_platform_policy_on_retraining(repo):
    monitored(repo, monitoring={"drift": {"enabled": True}, "output_table": "predictions"},
            retraining={"mode": "candidate", "when": ["drift"], "source_uri": "gs://l/x.csv",
                        "approvals": {"required": ["ml-leads"]}, "snooze": {"max_days": 60}})
    topic = {"topic": "projects/p/topics/approve-x", "subscription": "projects/p/subscriptions/approve-x"}
    repo.manifests["dev"]["approver_groups"] = {"ml-leads": topic}
    repo.manifests["dev"]["policies"] = {"retraining": {"allowed_modes": ["off", "propose"],
                                                        "required_approvers": ["model-risk"], "max_snooze_days": 14}}
    msg = problems(repo)
    assert "retraining.mode candidate is not allowed in dev" in msg
    assert "'model-risk' is not an approver group in dev (registered: ml-leads)" in msg
    assert "snooze.max_days 60 is above the dev limit of 14 days" in msg
    repo.manifests["dev"]["approver_groups"]["model-risk"] = topic
    repo.usecase["spec"]["retraining"].update(mode="propose", snooze={"max_days": 14})
    assert resolve(repo).ml["retraining"]["approvals"]["required"] == ["model-risk", "ml-leads"]


# -------------------------------------------------------------------------------------------------- cases --

def opened(s=TWO):
    out = rc.observe(None, None, DRIFT, s, T0, CTX)
    assert out.events[0][0] == "retrain.proposed" and out.case["state"] == "open"
    return out.case


def test_observe_opens_updates_and_respects_cooldown():
    case = opened()
    assert case["case_id"].startswith("rc-20261007-") and case["required"] == ["ml-leads", "model-risk"]
    again = rc.observe(case, None, {**DRIFT, "run_id": "r2"}, TWO, T0 + timedelta(hours=1), CTX)
    assert not again.events and len(again.case["observations"]) == 2
    assert rc.observe(None, None, {**DRIFT, "triggers": ["schedule"]}, TWO, T0, CTX).case is None
    cool = rc.observe(None, rc.iso(T0 - timedelta(days=2)), DRIFT, TWO, T0, CTX)
    assert cool.case is None and "cooldown" in cool.status
    off = rc.observe(None, None, DRIFT, replace(TWO, mode="off"), T0, CTX)
    assert off.case is None


def test_candidate_mode_trains_at_once():
    out = rc.observe(None, None, DRIFT, replace(TWO, mode="candidate"), T0, CTX)
    assert out.submit == {"kind": "candidate", "source_uri": "gs://l/samples/churn.csv",
                          "run_key": f"{out.case['case_id']}-candidate"}
    case = out.case
    case = rc.decide(case, "approve", "a@x", "ml-leads", TWO, T0, reason="ok").case
    done = rc.decide(case, "approve", "b@x", "model-risk", TWO, T0, reason="ok")
    assert done.case["state"] == "approved" and done.submit is None


def test_quorum_one_identity_per_group():
    case = opened()
    first = rc.decide(case, "approve", "lead@x", "ml-leads", TWO, T0, reason="real change")
    assert first.case["state"] == "open" and first.events[0][1]["waiting_for"] == ["model-risk"]
    with pytest.raises(rc.DecisionRefused, match="one identity counts once"):
        rc.decide(first.case, "approve", "lead@x", "model-risk", TWO, T0, reason="again")
    with pytest.raises(rc.DecisionRefused, match="ml-leads already approved"):
        rc.decide(first.case, "approve", "other@x", "ml-leads", TWO, T0, reason="me too")
    with pytest.raises(rc.DecisionRefused, match="a direct call cannot decide"):
        rc.decide(first.case, "approve", "risk@x", None, TWO, T0, reason="direct")
    done = rc.decide(first.case, "approve", "risk@x", "model-risk", TWO, T0, reason="ok",
                     source_uri="gs://l/window.csv")
    assert done.case["state"] == "approved"
    assert done.submit == {"kind": "retrain", "source_uri": "gs://l/window.csv", "run_key": case["case_id"]}
    with pytest.raises(rc.DecisionRefused, match="already approved"):
        rc.decide(done.case, "decline", "risk@x", "model-risk", TWO, T0, reason="x", because="other")


def test_no_groups_one_approval_and_requester_separation():
    s = replace(TWO, required=())
    case = opened(s)
    with pytest.raises(rc.DecisionRefused, match="proposed this case"):
        rc.decide(case, "approve", RUNNER, None, s, T0, reason="me")
    with pytest.raises(rc.DecisionRefused, match="give a reason"):
        rc.decide(case, "approve", "a@x", None, s, T0)
    assert rc.decide(case, "approve", "a@x", None, s, T0, reason="go").case["state"] == "approved"


def test_snooze_reopens_when_worse_or_expired():
    case = opened()
    with pytest.raises(rc.DecisionRefused, match="--because"):
        rc.decide(case, "snooze", "a@x", "ml-leads", TWO, T0, reason="later", days=3)
    with pytest.raises(rc.DecisionRefused, match="between 1 and 14"):
        rc.decide(case, "snooze", "a@x", "ml-leads", TWO, T0, reason="later", because="seasonal", days=30)
    snoozed = rc.decide(case, "snooze", "a@x", "ml-leads", TWO, T0, reason="holiday", because="seasonal",
                        days=3).case
    assert snoozed["state"] == "snoozed" and snoozed["until"] == rc.iso(T0 + timedelta(days=3))
    same = rc.observe(snoozed, None, {**DRIFT, "run_id": "r2", "max_feature_psi": 0.35}, TWO, T0, CTX)
    assert same.case["state"] == "snoozed" and not same.events
    worse = rc.observe(snoozed, None, {**DRIFT, "run_id": "r3", "max_feature_psi": 0.45}, TWO, T0, CTX)
    assert worse.case["state"] == "open" and worse.events[0][0] == "retrain.reopened"
    assert "rose from 0.310 to 0.450" in worse.events[0][1]["reason"]
    new_feature = rc.observe(snoozed, None, {**DRIFT, "run_id": "r4", "drifted_features": ["monthly_charges"]},
                             TWO, T0, CTX)
    assert "newly drifting: monthly_charges" in new_feature.status
    assert rc.tick(snoozed, TWO, T0 + timedelta(days=1)).case is None
    expired = rc.tick(snoozed, TWO, T0 + timedelta(days=3))
    assert expired.case["state"] == "open" and expired.events[0][1]["reason"] == "snoozed expired"


def test_decline_expires_and_escalation():
    case = opened()
    declined = rc.decide(case, "decline", "a@x", "model-risk", TWO, T0, reason="data bug upstream",
                         because="data-problem").case
    assert declined["state"] == "declined" and declined["until"] == rc.iso(T0 + timedelta(days=14))
    assert rc.tick(declined, TWO, T0 + timedelta(days=14)).case["state"] == "open"
    esc = rc.tick(case, TWO, T0 + timedelta(days=3))
    assert esc.events[0][0] == "retrain.escalated" and esc.case["escalated_at"]
    assert rc.tick(esc.case, TWO, T0 + timedelta(days=4)).case is None      # once


def test_summary_is_short_and_value_free():
    case = opened()
    case["observations"][0]["feature_psi"] = {f"feature_{i}": 0.5 for i in range(80)}
    case["observations"][0]["drifted_features"] = [f"feature_{i}" for i in range(80)]
    text = summary(case, "retrain.proposed")
    assert len(text) <= SUMMARY_LIMIT and "needs a decision" in text and "(+76 more)" in text
    assert text.splitlines()[-1].startswith("Decide: pipewright retrain show")
    assert "Approvers: ml-leads, model-risk" in summary(opened(), "retrain.proposed")
    assert "Options:" in render(opened())


# -------------------------------------------------------------------------------------------- store, service --

@pytest.fixture
def gcs(monkeypatch):
    objects: dict[str, tuple[str, int]] = {}

    def read(gcp, bucket, name):
        return objects.get(name, (None, 0))

    def write(gcp, bucket, name, text, content_type="application/json", if_generation_match=None):
        current = objects.get(name, (None, 0))[1]
        if if_generation_match is not None and if_generation_match != current:
            raise PreconditionFailed(name)
        objects[name] = (text, current + 1)
        return current + 1

    def list_(gcp, bucket, prefix):
        return sorted(n for n in objects if n.startswith(prefix))

    monkeypatch.setattr(store, "gcs_read", read)
    monkeypatch.setattr(store, "gcs_write", write)
    monkeypatch.setattr(store, "gcs_list", list_)
    return objects


ML = {"usecase": "churn", "retraining": {"mode": "propose", "when": ["drift"], "source_uri": "gs://l/x.csv",
                                         "decisions": {"publish": True},
                                         "approvals": {"required": ["ml-leads"]}}}
CONTRACT = replace(V3, pipelines={**V3.pipelines, "inference": {**INFERENCE, "static_parameters": {"ml_config": ML}}})


def service(manifest=None, clock=lambda: T0):
    submitted, emitted = [], []

    def submit(contract, pipeline, source_uri, run_key, trigger):
        submitted.append((pipeline, source_uri, run_key, trigger))
        return {"status": "submitted", "job_id": f"churn-{run_key}"}

    def emit(event_type, contract, data, attributes):
        emitted.append((event_type, data, attributes))

    d = Decisions(store.CaseStore(None, "b"), lambda: manifest or {}, submit, emit,
                  verify=lambda token: {"tok-lead": "lead@x", "tok-risk": "risk@x"}.get(token) or "",
                  clock=clock)
    return d, submitted, emitted


def test_service_proposal_decision_and_pointer(gcs):
    d, submitted, emitted = service({"policies": {"retraining": {"required_approvers": ["model-risk"]}}})
    body = {"usecase": "churn", "run_id": "r1", "observation": DRIFT, "champion": {"version": "3"}}
    first = d.propose(CONTRACT, body)
    cid = first["case_id"]
    assert first["state"] == "open" and emitted[0][0] == "retrain.proposed"
    assert emitted[0][2] == {"case_id": cid, "decision_feed": "true"} and "Case rc-" in emitted[0][1]["summary"]
    assert d.propose(CONTRACT, body)["status"] == "no-case"          # redelivered: recorded once
    assert json.loads(gcs["cases/churn/current.json"][0])["open"] == cid

    refused = d.decide(CONTRACT, {"case_id": cid, "action": "approve", "reason": "x", "id_token": "forged"},
                       "ml-leads", "topic:ml-leads")
    assert refused["status"] == "refused"
    msg = {"case_id": cid, "action": "approve", "reason": "real shift"}
    assert d.decide(CONTRACT, {**msg, "id_token": "tok-lead"}, "ml-leads", "topic:ml-leads")["state"] == "open"
    done = d.decide(CONTRACT, {**msg, "id_token": "tok-risk"}, "model-risk", "topic:model-risk")
    assert done["state"] == "approved" and done["training"]["job_id"] == f"churn-{cid}"
    assert submitted == [("training", "gs://l/x.csv", cid, "retrain")]
    pointer = json.loads(gcs["cases/churn/current.json"][0])
    assert pointer["open"] is None and pointer["last_approved_at"] == rc.iso(T0)
    case = json.loads(gcs[f"cases/churn/{cid}.json"][0])
    assert [h["action"] for h in case["history"]] == ["proposed", "refused", "approve", "approve"]
    assert [e[0] for e in emitted] == ["retrain.proposed", "retrain.approval", "retrain.approved"]
    # Cooldown after an approved retrain.
    assert d.propose(CONTRACT, {**body, "run_id": "r9"})["status"] == "no-case"


def test_service_tick_and_digest(gcs):
    now = {"t": T0}
    d, _, emitted = service(clock=lambda: now["t"])
    cid = d.propose(CONTRACT, {"usecase": "churn", "run_id": "r1", "observation": DRIFT})["case_id"]
    d.decide(CONTRACT, {"case_id": cid, "action": "snooze", "reason": "wait", "because": "seasonal", "days": 2,
                        "id_token": "tok-lead"}, "ml-leads", "topic:ml-leads")
    now["t"] = T0 + timedelta(days=2)
    results = d.tick([CONTRACT], digest=True)
    assert results[0]["detail"] == f"case {cid} re-opened"
    assert [e[0] for e in emitted][-2:] == ["retrain.reopened", "retrain.digest"]
    assert f"{cid} open 0 days, waiting for ml-leads" in emitted[-1][1]["summary"]


def test_store_retries_a_lost_race(gcs, monkeypatch):
    s = store.CaseStore(None, "b")
    calls = {"n": 0}
    real = store.gcs_write

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PreconditionFailed("race")
        return real(*args, **kwargs)

    monkeypatch.setattr(store, "gcs_write", flaky)
    case = opened()
    s.update("churn", lambda current, pointer: (copy.deepcopy(case), {**pointer, "open": case["case_id"]}))
    assert s.current("churn")["case_id"] == case["case_id"]


# --------------------------------------------------------------------------------------------- launcher --

def _token(email):
    part = base64.urlsafe_b64encode(json.dumps({"email": email}).encode()).decode().rstrip("=")
    return f"Bearer x.{part}.SIGNATURE_REMOVED_BY_GOOGLE"


def test_cloud_run_caller():
    assert identity.cloud_run_caller(_token("A@X.com")) == "a@x.com"
    assert identity.cloud_run_caller("Bearer nope") == "" and identity.cloud_run_caller(None) == ""


def test_launcher_routes(gcs, monkeypatch):
    pytest.importorskip("flask")
    from pipewright import adapters
    from pipewright.launcher.app import create_app
    from tests.test_launcher import FakeRegistry

    monkeypatch.setattr(adapters, "submit", lambda *a, **k: "projects/p/locations/r/pipelineJobs/j")
    monkeypatch.setattr(identity, "verify", lambda token, audiences: {"tok-lead": "lead@x"}.get(token) or "")
    reg = FakeRegistry([CONTRACT])
    reg.gcp, reg.bucket = None, "b"
    reg.manifest.update(launcher={"url": "https://l", "service_account": "launcher@p"},
                        approver_groups={"ml-leads": {"topic": "projects/p/topics/a",
                                                      "subscription": "projects/p/subscriptions/a"}})
    client = create_app(reg).test_client()
    proposal = {"kind": "retrain.proposal", "usecase": "churn", "run_id": "r1", "observation": DRIFT}
    data = base64.b64encode(json.dumps(proposal).encode()).decode()
    r = client.post("/pubsub/run", json={"message": {"data": data}})
    assert r.status_code == 200 and r.json["state"] == "open"
    cid = r.json["case_id"]

    decision = {"usecase": "churn", "case_id": cid, "action": "approve", "reason": "go", "id_token": "tok-lead"}
    push = {"message": {"data": base64.b64encode(json.dumps(decision).encode()).decode()},
            "subscription": "projects/p/subscriptions/a"}
    assert client.post("/retraining/decide", json=push, headers={"Authorization": _token("x@p")}).status_code == 403
    r = client.post("/retraining/decide", json=push, headers={"Authorization": _token("launcher@p")})
    assert r.status_code == 200 and r.json["state"] == "approved" and r.json["by"] == "lead@x"
    # A direct call cannot satisfy a required group.
    direct = client.post("/retraining/decide", json={**decision, "action": "snooze"},
                         headers={"Authorization": _token("lead@x")})
    assert direct.status_code == 409
    assert client.post("/retraining/tick", json={"digest": True}).json["results"][-1] == {"usecase": "churn",
                                                                                         "digest": 1}


# ----------------------------------------------------------------------------------------------- alerts --

def test_decision_alert_policies(repo):
    from pipewright.alerts.sync import policy_body, wanted_policies

    monitored(repo, monitoring={"drift": {"enabled": True}, "output_table": "predictions",
                              "alerts": {"notify": ["ops@x.com"]}},
            retraining={"mode": "propose", "when": ["drift"], "source_uri": "gs://l/x.csv",
                        "decisions": {"notify": ["ml@x.com"], "escalate_notify": ["head@x.com"], "digest": False}})
    uc = resolve(repo)
    wanted = wanted_policies(uc)
    assert wanted["monitoring.alert"] == ["ops@x.com"] and wanted["retrain.proposed"] == ["ml@x.com"]
    assert wanted["retrain.escalated"] == ["head@x.com", "ml@x.com"] and "retrain.digest" not in wanted
    body = policy_body(uc, "retrain.proposed", ["c1"], "WARNING")
    assert "```\n${log.extracted_label.summary}\n```" in body["documentation"]["content"]
    assert body["documentation"]["subject"].endswith("(${log.extracted_label.case_id})")
