"""The batch scoring gate (FR-130, Pipewright 1.4.3): approvals.batch_scoring manual holds every scoring run in the
launcher until a person approves it; off by default."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from pipewright.gcp import PreconditionFailed
from pipewright.runs import held
from tests.test_launcher import FakeRegistry
from tests.test_release_parts import V3

pytest.importorskip("flask")

RUNNER = "pw-churn-run@p.iam.gserviceaccount.com"
LAUNCHER = "pw-launcher@p.iam.gserviceaccount.com"
PERSON = "ops@example.com"
NOW = datetime(2026, 11, 12, 6, 0, tzinfo=timezone.utc)


class Bucket:
    """GCS objects with generations, for the store's preconditions."""

    def __init__(self):
        self.objects = {}

    def read(self, gcp, bucket, name):
        text, gen = self.objects.get(name, (None, 0))
        return text, gen

    def write(self, gcp, bucket, name, text, content_type="application/json", if_generation_match=None):
        current = self.objects.get(name, (None, 0))[1]
        if if_generation_match is not None and if_generation_match != current:
            raise PreconditionFailed(name)
        self.objects[name] = (text, current + 1)
        return current + 1

    def list(self, gcp, bucket, prefix):
        return sorted(n for n in self.objects if n.startswith(prefix))


@pytest.fixture
def bucket(monkeypatch):
    b = Bucket()
    monkeypatch.setattr(held, "gcs_read", b.read)
    monkeypatch.setattr(held, "gcs_write", b.write)
    monkeypatch.setattr(held, "gcs_list", b.list)
    return b


def record(**kw):
    r = held.new_record("churn", "h1", {"source_uri": "gs://l/s.csv", "rid": "h1", "trigger": "schedule"},
                        "schedule", 24, NOW)
    return {**r, **kw}


# ----------------------------------------------------------------------------------------------- the rules --

def test_off_by_default_and_the_platform_can_require_it():
    assert held.mode(None, None) == "auto" and held.mode({"promote_model": "manual"}, {}) == "auto"
    assert held.mode({"batch_scoring": "manual"}, {}) == "manual"
    assert held.mode({}, {"batch_scoring": "manual"}) == "manual"
    assert held.mode({"batch_scoring": "auto"}, {"batch_scoring": "manual"}) == "manual"   # validate refuses it too
    assert held.hold_hours(None) == 24 and held.hold_hours({"batch_scoring_hold_hours": 4}) == 4


def test_a_decision_needs_a_person_a_held_run_and_a_reason_to_reject():
    r = record()
    approved = held.decide(r, "approve", PERSON, NOW + timedelta(hours=1), approval={"via": "github"})
    assert approved["state"] == "approved" and approved["decided_by"] == PERSON
    assert approved["history"][-1]["approval"] == {"via": "github"}
    with pytest.raises(held.Refused, match="is approved, not held"):
        held.decide(approved, "reject", PERSON, NOW, reason="x")
    with pytest.raises(held.Refused, match="needs a reason"):
        held.decide(r, "reject", PERSON, NOW)
    with pytest.raises(held.Refused, match="runs pipelines or the launcher"):
        held.decide(r, "approve", RUNNER, NOW, not_allowed={RUNNER})
    with pytest.raises(held.Refused, match="no verified identity"):
        held.decide(r, "approve", "", NOW)
    with pytest.raises(held.Refused, match="action must be"):
        held.decide(r, "submit", PERSON, NOW)


def test_a_hold_expires():
    r = record()
    assert held.state(r, NOW + timedelta(hours=23)) == "held"
    assert held.state(r, NOW + timedelta(hours=24)) == "expired"
    with pytest.raises(held.Refused, match="is expired"):
        held.decide(r, "approve", PERSON, NOW + timedelta(hours=25))


def test_a_run_asked_for_twice_is_held_once(bucket):
    store = held.HeldStore(None, "artifacts")
    first, created = store.create(record())
    again, created_again = store.create(record(trigger="api"))
    assert created and not created_again and again["trigger"] == "schedule"
    assert [r["hold_id"] for r in store.list("churn")] == ["h1"]


def test_validate_refuses_auto_where_the_platform_requires_manual(repo):
    from pipewright.config import load_environment, resolve_usecase
    from pipewright.errors import ConfigError
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    repo.manifests["dev"]["policies"] = {"batch_scoring": "manual"}
    path = repo.write()
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))      # not set: the platform's manual applies
    assert held.mode(uc.approvals, load_environment(repo.root, "dev").policies) == "manual"
    repo.usecase["spec"]["approvals"] = {"batch_scoring": "auto"}
    path = repo.write()
    with pytest.raises(ConfigError, match="batch_scoring: auto is not allowed in dev"):
        resolve_usecase(path, load_environment(repo.root, "dev"))


def test_a_release_without_the_setting_stores_the_same_approvals(repo):
    from pipewright.config import load_environment, resolve_usecase
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert "batch_scoring" not in uc.approvals and "batch_scoring_hold_hours" not in uc.approvals


# ------------------------------------------------------------------------------------------- the launcher --

@pytest.fixture
def gate(monkeypatch, bucket):
    from pipewright import adapters
    from pipewright.launcher import app as launcher_app

    submitted, emitted = [], []
    monkeypatch.setattr(adapters, "submit", lambda contract, target, params, job_id, *a, **k:
                        submitted.append((job_id, params)) or f"projects/p/locations/r/pipelineJobs/{job_id}")
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: emitted.append(event))
    contract = replace(V3, metadata={**(V3.metadata or {}), "approvals": {"batch_scoring": "manual"}})
    registry = FakeRegistry([contract])
    registry.manifest.update({"artifacts_bucket": "artifacts", "launcher": {"service_account": LAUNCHER},
                              "tenants": {V3.tenant: {"runner_service_account": RUNNER}}})
    caller = {"who": PERSON}
    monkeypatch.setattr(launcher_app.identity, "cloud_run_caller", lambda header: caller["who"])
    client = launcher_app.create_app(registry).test_client()
    client.submitted, client.emitted, client.caller, client.registry = submitted, emitted, caller, registry
    return client


def scoring(client, uri="gs://x/y.csv"):
    return client.post("/run", json={"usecase": V3.usecase, "pipeline": "inference", "source_uri": uri})


def test_a_scoring_run_is_held_then_submitted_when_a_person_approves(gate):
    r = scoring(gate)
    assert r.json["status"] == "held" and gate.submitted == []
    hold_id = r.json["hold_id"]
    assert "pipewright scoring approve" in r.json["decide"]
    assert [e["event_type"] for e in gate.emitted] == ["run.held"]
    assert scoring(gate).json["status"] == "held" and len(gate.emitted) == 1       # the same run: held once
    d = gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id, "action": "approve",
                                           "reason": "checked", "approval": {"via": "github",
                                                                             "reviewers": ["reviewer"]}})
    assert d.status_code == 200 and d.json["run"]["status"] == "submitted" and len(gate.submitted) == 1
    assert gate.submitted[0][1]["source_uri"] == "gs://x/y.csv"
    decided = [e for e in gate.emitted if e["event_type"] == "run.hold_decided"][-1]
    assert decided["data"]["by"] == PERSON and decided["data"]["approval"]["reviewers"] == ["reviewer"]
    submitted_event = [e for e in gate.emitted if e["event_type"] == "run.submitted"][-1]
    assert submitted_event["data"]["held"]["approved_by"] == PERSON
    again = gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id, "action": "approve"})
    assert again.status_code == 403 and "approved, not held" in again.json["reason"] and len(gate.submitted) == 1


def test_a_runner_or_the_launcher_cannot_release_a_held_run(gate):
    hold_id = scoring(gate).json["hold_id"]
    for who in (RUNNER, LAUNCHER):
        gate.caller["who"] = who
        d = gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id, "action": "approve"})
        assert d.status_code == 403 and gate.submitted == []
    refused = [e for e in gate.emitted if e["event_type"] == "run.hold_decided"]
    assert [e["data"]["action"] for e in refused] == ["refused", "refused"]


def test_a_rejected_run_never_starts(gate):
    hold_id = scoring(gate).json["hold_id"]
    d = gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id, "action": "reject",
                                           "reason": "source file is last month's"})
    assert d.json["action"] == "reject" and gate.submitted == []
    assert gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id,
                                              "action": "approve"}).status_code == 403


def test_training_is_not_held_and_the_gate_is_off_by_default(gate):
    r = gate.post("/run", json={"usecase": V3.usecase, "source_uri": "gs://x/train.csv"})
    assert r.json["status"] == "submitted" and len(gate.submitted) == 1
    gate.registry._live = [V3]                                 # no batch_scoring: scoring starts at once
    assert scoring(gate, "gs://x/z.csv").json["status"] == "submitted" and len(gate.submitted) == 2
    gate.registry.manifest["policies"] = {"batch_scoring": "manual"}      # the platform turns it on
    assert scoring(gate, "gs://x/w.csv").json["status"] == "held" and len(gate.submitted) == 2


def test_no_person_no_decision(gate):
    hold_id = scoring(gate).json["hold_id"]
    gate.caller["who"] = None
    assert gate.post("/scoring/decide", json={"usecase": V3.usecase, "hold_id": hold_id,
                                              "action": "approve"}).status_code == 401


def test_decisions_pending_asks_for_each_held_run(bucket):
    from types import SimpleNamespace

    from pipewright.decisions import held_items

    store = held.HeldStore(None, "artifacts")
    store.create(record(expires_at=held.iso(datetime.now(timezone.utc) + timedelta(hours=2))))
    store.create(held.new_record("churn", "h0", {}, "landing", 1, datetime.now(timezone.utc) - timedelta(hours=2)))
    env = SimpleNamespace(name="test", policies={}, artifacts_bucket="artifacts")
    uc = SimpleNamespace(approvals={"batch_scoring": "manual"})
    items = held_items("churn", env, uc, None, {"production": "https://launcher"})
    assert [i.id for i in items] == ["test/churn/score-approve/h1"]                 # the expired one is not asked
    assert items[0].gate == "test" and items[0].command[:2] == ["scoring", "approve"]
    assert held_items("churn", env, SimpleNamespace(approvals={}), None, {}) == []
