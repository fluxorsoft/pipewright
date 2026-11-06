"""FR-84: decisions are kept and shown: lineage reads them from the event log, the release pointer names the person
behind a CI identity, and doctor fails an events topic nobody subscribes to (Pipewright 1.2.0)."""
import json
from types import SimpleNamespace

from pipewright.checks import doctor
from pipewright.models import lineage
from pipewright.release.contract import Pointer, new_pointer

ENV = SimpleNamespace(name="prod", project="acme-ml-prod", security={},
                      observability={"events_topic": "projects/acme-ml-prod/topics/pw-events",
                                     "evidence_dataset": "pw_evidence"})
GITHUB = {"via": "github", "actor": "a-reviewer", "run_url": "https://github.com/acme/ml/actions/runs/7",
          "reviewers": []}


def test_lineage_lists_the_versions_decisions_with_how_each_was_approved(monkeypatch):
    seen = {}
    events = [
        {"published": "2026-10-30 19:42:13.1", "event": json.dumps({"event_type": "release.promoted",
                                                                    "release": "abc123",
                                                                    "data": {"by": "ci@acme", "approval": GITHUB}})},
        {"published": "2026-10-31 01:30:00.0", "event": json.dumps({"event_type": "model.approved", "release": None,
                                                             "data": {"version": "2", "by": "ci@acme",
                                                                      "approval": GITHUB,
                                                                      "source_environment": "test"}})},
        {"published": "2026-10-31 01:50:00.0", "event": json.dumps({"event_type": "model.deployed", "data": {
            "version": "2", "endpoint": "churn-endpoint", "traffic_percent": 100, "by": "ops@acme",
            "approval": {"via": "outside-ci"}}})},
    ]

    def query(gcp, project, sql, params, **kw):
        seen.update(sql=sql, params={p["name"]: p["parameterValue"]["value"] for p in params})
        return {}

    monkeypatch.setattr("pipewright.gcp.bigquery.query", query)
    monkeypatch.setattr("pipewright.gcp.bigquery.rows", lambda res: events)
    out = lineage.decisions(None, ENV, "churn", "2", "abc123")
    assert "acme-ml-prod.pw_evidence.events" in seen["sql"]
    assert " AS at," not in seen["sql"]           # AT is a reserved word in BigQuery (found live)
    assert seen["params"] == {"usecase": "churn", "env": "prod", "version": "2", "release": "abc123"}
    assert [d["event"] for d in out["decisions"]] == ["release.promoted", "model.approved", "model.deployed"]
    assert out["decisions"][1]["source_environment"] == "test"       # a promotion's approval: its upload counts
    text = lineage.render({"model": "churn-classifier", "version": "2", "environment": "prod", "lineage": None,
                           "note": "no lineage.json", **out})
    assert "release abc123 promoted here, by ci@acme (GitHub a-reviewer, https://github.com/acme/ml/actions/runs/7)" \
        in text
    assert "v2 deployed to churn-endpoint (100% of traffic), by ops@acme (outside CI)" in text


def test_without_an_event_log_lineage_says_decisions_were_not_kept():
    env = SimpleNamespace(**{**ENV.__dict__, "observability": {"events_topic": "projects/p/topics/t"}})
    assert "was not kept" in lineage.decisions(None, env, "churn", "2", "abc")["decisions_note"]


def test_the_pointer_records_the_person_behind_a_ci_identity_and_reads_old_pointers():
    p = new_pointer("churn", "r1", "contracts/churn/r1.json", "ci@acme", GITHUB)
    moved = p.moved_to("r2", "contracts/churn/r2.json", "ci@acme", {"via": "outside-ci"})
    assert moved.approval == {"via": "outside-ci"} and moved.history[0]["approval"] == GITHUB
    assert moved.history[0]["updated_by"] == "ci@acme"
    back = Pointer.from_json(moved.to_json())
    assert back.approval == {"via": "outside-ci"} and back.history == moved.history
    old = json.dumps({"usecase": "churn", "release_id": "r0", "contract": "c", "updated_at": "t", "updated_by": "x",
                      "history": []})                                      # written by Pipewright 1.0 or 1.1
    assert Pointer.from_json(old).approval is None
    assert new_pointer("churn", "r1", "c", "me").approval is None          # callers from before 1.2.0


def gcp_answering(subscriptions, ok=True):
    return SimpleNamespace(get=lambda url, **k: SimpleNamespace(
        ok=ok, status=403, body={"subscriptions": subscriptions} if ok else {"error": {"message": "denied"}}))


def results(env, gcp):
    report = doctor.Report()
    doctor._check_event_log(env, gcp, report, "Environment", "ask the platform team")
    return [(r.status, r.detail) for rs in report.sections.values() for r in rs]


def test_doctor_fails_an_events_topic_nobody_subscribes_to():
    # The ebook's Chapter 17: every promotion, approval and deployment had been published and discarded.
    (status, detail), = results(ENV, gcp_answering([]))
    assert status == doctor.WARN and "nothing subscribes to pw-events" in detail


def test_doctor_names_the_table_events_are_kept_in():
    (status, detail), = results(ENV, gcp_answering(["projects/p/subscriptions/pw-events-to-bigquery"]))
    assert status == doctor.OK and "pw_evidence.events" in detail


def test_doctor_warns_when_it_cannot_look():
    (status, _), = results(ENV, gcp_answering([], ok=False))
    assert status == doctor.WARN


# -- the evaluation an approval was judged on (Pipewright 1.3.1) -----------------------------------------------

RECORD = '{"metrics": {"auc": 0.91}, "champion": {"auc": 0.88, "_version": "3"}, "decision": "approve"}'


def test_the_digest_names_the_record_its_generation_and_its_bytes(monkeypatch):
    import hashlib

    from pipewright.models import gates

    seen = {}

    def gcs_read(gcp, bucket, name):
        seen.update(bucket=bucket, name=name)
        return RECORD, 1730000000123

    monkeypatch.setattr("pipewright.gcp.gcs_read", gcs_read)
    d = gates.record_digest(None, "gs://acme-artifacts/pipeline-root/retail/churn/r1/model/")
    assert seen == {"bucket": "acme-artifacts", "name": "pipeline-root/retail/churn/r1/model/evaluation.json"}
    assert d == {"record": "gs://acme-artifacts/pipeline-root/retail/churn/r1/model/evaluation.json",
                 "generation": 1730000000123, "sha256": hashlib.sha256(RECORD.encode()).hexdigest()}
    monkeypatch.setattr("pipewright.gcp.gcs_read", lambda gcp, b, n: (None, 0))
    assert gates.record_digest(None, "gs://b/p") is None
    assert gates.record_digest(None, None) is None and gates.record_digest(None, "bq://not-a-folder") is None


class _Version:
    def __init__(self, vid, default):
        self.version_id, self.version_aliases = vid, (["default"] if default else [])


def _approve_with(monkeypatch, known, record, emitted, aliased):
    """approve() with the Model Registry, the gates and the lock faked; `record` is what _judged returns."""
    import sys

    from pipewright.models import gates, integrity, registry

    class FakeRegistry:
        def __init__(self, model):
            pass

        def list_versions(self):
            return known

        def add_version_aliases(self, new_aliases, version):
            aliased.append((new_aliases, version))

    # a fake SDK module: importing the real one here would leave it loaded for later tests that fake it
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform.models", SimpleNamespace(ModelRegistry=FakeRegistry))
    monkeypatch.setattr(registry, "_resource", lambda uc, env, creds: "projects/p/locations/r/models/m")
    monkeypatch.setattr(registry, "version_labels", lambda *a: {"3": {"approval": "pending"}})
    rules = integrity.Rules({}, {}, "rel1", "release")
    monkeypatch.setattr(registry, "_judged", lambda *a, **k: (rules, record, "gs://b/m4"))
    monkeypatch.setattr(gates, "verify", lambda *a, **k: [])
    monkeypatch.setattr(integrity, "lock", lambda *a, **k: {"locked": 3, "until": "2026-12-05T00:00:00Z"})
    patched = []
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda creds=None: SimpleNamespace(
        get=lambda url, **k: SimpleNamespace(ok=True, body={"labels": {"approval": "pending"}}),
        patch=lambda url, body, **k: patched.append((url, body)) or SimpleNamespace(ok=True, status=200)))
    aliased.append(patched)                  # the test reads it back: aliased[0] is the label patches
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: emitted.append(event))
    uc = SimpleNamespace(name="churn", tenant="retail", model={"name": "churn-classifier"}, ml={"evaluation": {}},
                         approvals={}, environment="prod")
    return uc, SimpleNamespace(**{**ENV.__dict__, "region": "europe-west2", "policies": {}})


def test_an_approval_records_the_evaluation_it_was_judged_on(monkeypatch):
    from pipewright.models import registry

    emitted, aliased = [], []
    digest = {"record": "gs://b/m4/evaluation.json", "generation": 7, "sha256": "ab" * 32}
    record = {"evaluation": digest, "rules": {"release": "rel1", "source": "release"}}
    uc, env = _approve_with(monkeypatch, [_Version("3", True), _Version("4", False)], record, emitted, aliased)
    registry.approve(uc, env, None, "4", "risk@acme", True, log=lambda *a: None)
    patched = aliased.pop(0)
    assert aliased == [(["default"], "4")]
    # a later version approved is no longer labelled pending (found live: deploy-model refused it)
    assert patched and patched[0][0].endswith("@4") and patched[0][1]["labels"]["approval"] == "approved"
    data = emitted[0]["data"]
    assert data["version"] == "4" and data["previous"] == "3" and data["by"] == "risk@acme"
    assert data["evaluation"] == digest and data["rules"]["release"] == "rel1"
    assert data["lock"] == {"locked": 3, "until": "2026-12-05T00:00:00Z"}


def test_an_unreadable_evaluation_never_blocks_the_approval(monkeypatch):
    from pipewright.models import gates, registry

    def broken(gcp, uri):
        raise RuntimeError("403")

    monkeypatch.setattr(registry, "artifact_uri", lambda resource, version, uc=None: "gs://b/m4")
    monkeypatch.setattr(gates, "record_digest", broken)
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda creds=None: None)
    assert registry._evaluated(None, "res", "4", None) == {}
    monkeypatch.setattr(gates, "record_digest", lambda gcp, uri: None)       # a version with no record
    assert registry._evaluated(None, "res", "4", None) == {}


def test_the_first_version_approval_records_its_evaluation_too(monkeypatch):
    from pipewright.models import registry

    emitted = []

    class FakeGcp:
        def __init__(self, creds=None):
            pass

        def get(self, url, **k):
            return SimpleNamespace(ok=True, body={"labels": {"approval": "pending"}})

        def patch(self, url, body, **k):
            return SimpleNamespace(ok=True, status=200)

    monkeypatch.setattr("pipewright.gcp.Gcp", FakeGcp)
    monkeypatch.setattr(registry, "_evaluated", lambda creds, resource, version, uc: {"evaluation": {"sha256": "cd"}})
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: emitted.append(event))
    uc = SimpleNamespace(name="demand", tenant="supply", model={"name": "demand"}, ml={"evaluation": {}}, approvals={})
    env = SimpleNamespace(name="prod", region="r", observability={}, policies={})
    registry._approve_first_version(uc, env, None, "res", "1", "lead@acme", lambda *a: None)
    data = emitted[0]["data"]
    assert data["first_version"] is True and data["evaluation"] == {"sha256": "cd"}
    # Phase 6: a version that names no release is judged by the working copy's rules, and the approval says so
    assert data["rules"]["source"] == "working copy" and data["rules"]["release"] is None


def _approved(evaluation=None):
    data = {"version": "4", "by": "risk@acme", "approval": GITHUB, **({"evaluation": evaluation} if evaluation else {})}
    return {"published": "2026-11-04 09:00:00.0", "event": json.dumps({"event_type": "model.approved", "data": data})}


def test_lineage_says_whether_the_approved_evaluation_is_the_one_on_file(monkeypatch):
    from pipewright.models import gates

    approved = {"record": "gs://b/m4/evaluation.json", "generation": 7, "sha256": "ab" * 32}
    monkeypatch.setattr("pipewright.gcp.bigquery.query", lambda *a, **k: {})
    monkeypatch.setattr("pipewright.gcp.bigquery.rows", lambda res: [_approved(approved), _approved()])
    for now, expected, shown in [({"sha256": "ab" * 32}, "unchanged", "unchanged since"),
                                 ({"sha256": "ff" * 32}, "changed", "CHANGED since"),
                                 (None, "missing", "MISSING now")]:
        out = lineage.decisions(None, ENV, "churn", "4", None)
        monkeypatch.setattr(gates, "record_digest", lambda gcp, folder, now=now: now)
        lineage.check_evaluations(None, out, "gs://b/m4/")
        first, older = out["decisions"]
        assert first["evaluation_now"] == expected and "evaluation_now" not in older     # a 1.3.0 approval: no digest
        text = lineage.render({"model": "churn-classifier", "version": "4", "environment": "prod", "lineage": None,
                               "note": "-", **out})
        assert f"judged on evaluation sha256 {'ab' * 8} (generation 7), {shown}" in text
        assert text.count("judged on evaluation") == 1
