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
                                                                      "approval": GITHUB}})},
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
