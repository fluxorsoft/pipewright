"""GA QA (found live): an approval this process just published counts in its own checks at once; the event log's copy
arrives a few seconds later (the closed loop's canary deploy was refused right after the production approval)."""
from types import SimpleNamespace

from pipewright.models import approvals


def test_a_just_published_approval_counts_until_the_log_has_it(monkeypatch):
    approvals.reset()
    log = []
    monkeypatch.setattr(approvals, "_query", lambda gcp, env, dataset, since: list(log))
    env = SimpleNamespace(project="p", name="prod", observability={"evidence_dataset": "ev"}, security={})
    assert approvals.events(None, env, "model.approved", "churn", "2") == []
    approvals.remember({"event_type": "model.approved", "usecase": "churn", "environment": "prod",
                        "time": "2026-11-12T00:12:45", "data": {"version": "2", "by": "a@x"}})
    assert approvals.events(None, env, "model.approved", "churn", "2")[0]["by"] == "a@x"
    other = SimpleNamespace(project="p", name="test", observability={"evidence_dataset": "ev"}, security={})
    assert approvals.events(None, other, "model.approved", "churn", "2") == []      # another environment: not
    log.append({"id": "m1", "at": "2026-11-12T00:12:50", "type": "model.approved", "usecase": "churn",
                "data": {"version": "2", "by": "a@x", "from": "log"}})
    approvals.reset()
    approvals.remember({"event_type": "model.approved", "usecase": "churn", "environment": "prod",
                        "time": "2026-11-12T00:12:45", "data": {"version": "2", "by": "a@x"}})
    got = approvals.events(None, env, "model.approved", "churn", "2")
    assert len(got) == 1 and got[0]["from"] == "log"                                # the log's copy, once
    approvals.remember({"event_type": "run.submitted", "usecase": "churn", "environment": "prod", "data": {}})
    assert all(r["type"] in approvals.KINDS for r in approvals._recent)
    approvals.reset()
