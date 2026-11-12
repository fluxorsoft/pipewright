"""FR-83: under a manual policy, a first version that became the default by itself is not the champion until a person
approves it (Pipewright 1.2.0). Scoring is unchanged; model list, doctor and deploy-model say so."""
import sys
from types import SimpleNamespace

import pytest

from pipewright.checks import doctor
from pipewright.errors import ConfigError
from pipewright.models import deploy, registry


def test_the_list_marks_a_default_nobody_approved():
    uc = SimpleNamespace(model={"name": "demand-forecast"}, environment="prod")
    rows = [registry.Version("1", True, [], "run r8dd, mae=197.5556", "2026-10-31 00:25:15", {"approval": "pending"}),
            registry.Version("2", False, [], "run r9", "2026-11-01 00:00:00", {"status": "candidate"})]
    text = registry.render(uc, rows)
    assert "v1    DEFAULT" in text and "(not approved: `pipewright model approve`)" in text
    approved = [registry.Version("1", True, [], "run r8dd", "x", {"approval": "approved"})]
    assert "not approved" not in registry.render(uc, approved)


def test_a_former_default_is_not_shown_as_the_champion():
    """Found re-running the book on 1.4.2: the register step labels a new default `champion`, and the label stays
    when a newer version takes over; the list shows the champion only as the default."""
    uc = SimpleNamespace(model={"name": "lapse-risk"}, environment="test")
    rows = [registry.Version("2", True, [], "run r2", "x", {"status": "champion", "approval": "approved"}),
            registry.Version("1", False, [], "run r1", "y", {"status": "champion"}),
            registry.Version("3", False, [], "run r3", "z", {"status": "candidate"})]
    text = registry.render(uc, rows)
    assert "v2    DEFAULT" in text and "champion" not in text and "v3    candidate" in text


def test_deploy_model_refuses_a_default_nobody_approved(monkeypatch):
    plan = deploy.DeployPlan("demand-forecast", "1", "e", "n1-standard-2", {"min": 1, "max": 1}, False, [], [],
                             {"approval": "pending"}, None)
    monkeypatch.setattr(deploy, "plan", lambda uc, env, creds: plan)
    monkeypatch.setattr(deploy, "_aiplatform", lambda env, creds: None)
    uc = SimpleNamespace(name="demand-forecast")
    with pytest.raises(ConfigError, match="nobody has approved it"):
        deploy.deploy(uc, SimpleNamespace(name="prod"), None)


def test_approve_records_the_approval_of_the_first_version(monkeypatch):
    calls, emitted = [], []

    class FakeGcp:
        def __init__(self, creds=None):
            pass

        def get(self, url, **k):
            return SimpleNamespace(ok=True, body={"labels": {"approval": "pending", "status": "champion"}})

        def patch(self, url, body, **k):
            calls.append((url, body, k))
            return SimpleNamespace(ok=True, status=200)

    monkeypatch.setattr("pipewright.gcp.Gcp", FakeGcp)
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: emitted.append(event))
    monkeypatch.setattr(registry, "artifact_uri", lambda *a, **k: None)  # no Vertex AI call for the 1.3.1 digest
    uc = SimpleNamespace(name="demand-forecast", tenant="retail", model={"name": "demand-forecast"},
                         ml={"evaluation": {}}, approvals={})
    env = SimpleNamespace(name="prod", region="europe-west2", observability={}, policies={})
    lines = []
    registry._approve_first_version(uc, env, None, "projects/p/locations/r/models/m", "1", "lead@acme",
                                    lines.append)
    url, body, params = calls[0]
    assert url.endswith("/models/m@1") and body["labels"] == {"approval": "approved", "status": "champion"}
    assert params == {"updateMask": "labels"}
    event = emitted[0]
    assert event["event_type"] == "model.approved" and event["data"]["first_version"] is True
    assert event["data"]["by"] == "lead@acme" and "is approved by lead@acme" in lines[0]


def test_doctor_warns_about_a_default_nobody_approved():
    def get(url, **k):
        if url.endswith("/models"):
            return SimpleNamespace(ok=True, body={"models": [{"name": "projects/p/locations/r/models/m"}]})
        return SimpleNamespace(ok=True, body={"versionId": "1", "labels": {"approval": "pending"}})

    report = doctor.Report()
    uc = SimpleNamespace(name="demand-forecast", model={"name": "demand-forecast"})
    doctor._check_champion_approval(uc, SimpleNamespace(name="prod", region="r", project="p"),
                                    SimpleNamespace(get=get), report, "ML")
    (result,) = report.sections["ML"]
    assert result.status == doctor.WARN and "nobody has approved it" in result.detail
    assert "pipewright model approve --env prod --usecase demand-forecast --version 1" in result.hint


def test_validate_warns_when_the_register_step_predates_it(repo):
    """Found in 1.2.0 QA (GKE): a register step copied before 1.2.0 never labels approval=pending, so the first version
    of a manual-policy use case became the default unapproved and nothing said so."""
    from pipewright.config import load_environment, resolve_usecase

    path = repo.write()
    components = path.parent / "components"
    (components / "register.py").write_text("registered = aiplatform.Model.upload(display_name=name)\n",
                                            encoding="utf-8")
    env = load_environment(repo.root, "dev")
    uc = resolve_usecase(path, env)
    if uc.ml["registry"]["set_default"] != "manual":
        repo.usecase["spec"].setdefault("approvals", {})["promote_model"] = "manual"
        uc = resolve_usecase(repo.write(), env)
    assert any("FR-83" in w and "components/register.py" in w for w in uc.warnings)
    (components / "register.py").write_text('registered = aiplatform.Model.upload(labels={"approval": "pending"})\n',
                                            encoding="utf-8")
    assert not any("FR-83" in w for w in resolve_usecase(path, env).warnings)


def test_the_event_log_decides_when_the_label_is_missing(monkeypatch):
    """Phase 6, found live on private GKE: a register step that predates the label left v1 the default with no
    approval on record. The list says so, and approve records the approval instead of saying it is the default."""
    uc = SimpleNamespace(name="churn-gke", model={"name": "churn-gke-classifier"}, environment="ent")
    rows = [registry.Version("1", True, [], "run r1", "x", {"status": "champion"}, "pass", False)]
    assert "(not approved: `pipewright model approve`)" in registry.render(uc, rows)
    rows[0].approved = True
    assert "not approved" not in registry.render(uc, rows)
    rows[0].approved = None                                # no event log: nothing to say
    assert "not approved" not in registry.render(uc, rows)

    monkeypatch.setattr("pipewright.models.integrity.approval_events", lambda *a: [])
    assert registry.approval_on_record(uc, SimpleNamespace(), None, "1") is False
    monkeypatch.setattr("pipewright.models.integrity.approval_events", lambda *a: [{"by": "lead@acme"}])
    assert registry.approval_on_record(uc, SimpleNamespace(), None, "1") is True
    monkeypatch.setattr("pipewright.models.integrity.approval_events", lambda *a: None)
    assert registry.approval_on_record(uc, SimpleNamespace(), None, "1") is None

    def broken(*a):
        raise RuntimeError("403")
    monkeypatch.setattr("pipewright.models.integrity.approval_events", broken)
    assert registry.approval_on_record(uc, SimpleNamespace(), None, "1") is None


def test_approve_approves_a_default_with_no_approval_on_record(monkeypatch):
    class V:
        version_id, version_aliases = "1", ["default", "run-r1"]

    class Registry:
        def __init__(self, model):
            pass

        def list_versions(self):
            return [V()]

    approved = []
    # faked, never imported: other tests fake google.cloud.aiplatform through sys.modules too
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform.models", SimpleNamespace(ModelRegistry=Registry))
    monkeypatch.setattr(registry, "_resource", lambda *a: "projects/p/locations/r/models/m")
    monkeypatch.setattr(registry, "version_labels", lambda *a: {"1": {"status": "champion"}})
    monkeypatch.setattr(registry, "approval_on_record", lambda *a: False)
    monkeypatch.setattr(registry, "_judged", lambda *a: (SimpleNamespace(note=None, differs=[], approvals={},
                                                                         evaluation={}, release="r"), {}, None))
    monkeypatch.setattr(registry, "_check_rules", lambda uc, *a: uc)
    monkeypatch.setattr(registry.gates, "verify", lambda *a: [])
    monkeypatch.setattr(registry, "_approve_first_version", lambda *a, **k: approved.append(a[4]))
    uc = SimpleNamespace(name="churn-gke", model={"name": "churn-gke-classifier"}, ml={"evaluation": {}})
    lines = []
    registry.approve(uc, SimpleNamespace(name="ent"), None, "1", "lead@acme", True, lines.append)
    assert approved == ["1"] and "no approval of it is on record" in lines[0]

    monkeypatch.setattr(registry, "approval_on_record", lambda *a: True)
    lines.clear()
    registry.approve(uc, SimpleNamespace(name="ent"), None, "1", "lead@acme", True, lines.append)
    assert approved == ["1"] and lines == ["v1 of churn-gke-classifier is already the default"]


def test_decisions_never_deploy_an_unrecorded_default():
    from pipewright import decisions

    default = registry.Version("1", True, [], "run r1", "x", {"status": "champion"}, "pass", False)
    (item,) = decisions.pending_for("churn-gke", "ent", [default], [], deploy={"mode": "on_register",
                                                                               "served": set()})
    assert (item.kind, item.key) == ("model-approve", "v1")     # 1.4.3: asked for approval, never deployed
    default.approved = True
    (item,) = decisions.pending_for("churn-gke", "ent", [default], [], deploy={"mode": "on_register",
                                                                               "served": set()})
    assert item.kind == "deploy-auto"
