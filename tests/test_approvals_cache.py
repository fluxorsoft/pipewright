"""1.4.1 (FR-101 item 0): approvals are read once and then by what is new, not one full scan per version."""

from types import SimpleNamespace

import pytest

from pipewright.models import approvals


def env(name="prod"):
    return SimpleNamespace(name=name, project="p", region="r", security={},
                           observability={"evidence_dataset": "pw_evidence"})


def approved(uc, version, at, mid):
    return {"id": mid, "at": at, "type": "model.approved", "usecase": uc,
            "data": {"version": str(version), "by": "ana@example.com"}}


@pytest.fixture
def log(monkeypatch):
    approvals.reset()
    approvals.use_store(None, None)
    calls, table = [], []
    clock = {"t": 1000.0}

    def query(gcp, e, dataset, since):
        calls.append(since)
        return list(table)

    monkeypatch.setattr(approvals, "_query", query)
    monkeypatch.setattr(approvals.time, "monotonic", lambda: clock["t"])
    yield SimpleNamespace(calls=calls, table=table, clock=clock)
    approvals.reset()


def test_many_lookups_cost_one_query(log):
    log.table += [approved("churn", 3, "2026-11-01T10:00:00", "m1"), approved("fraud", 7, "2026-11-02T10:00:00", "m2")]
    for _ in range(50):
        assert approvals.events(None, env(), "model.approved", "churn", "3")[0]["by"] == "ana@example.com"
        assert approvals.events(None, env(), "model.approved", "fraud", "7")[0]["version"] == "7"
    assert log.calls == [None]                                   # one full read, then nothing


def test_a_refresh_reads_only_what_is_new(log):
    log.table.append(approved("churn", 3, "2026-11-01T10:00:00", "m1"))
    approvals.events(None, env(), "model.approved", "churn", "3")
    log.clock["t"] += approvals.REFRESH_SECONDS + 1
    log.table[:] = [approved("churn", 4, "2026-11-07T10:00:00", "m3")]       # only the new partitions come back
    got = approvals.events(None, env(), "model.approved", "churn", "4")
    assert got and log.calls[0] is None and log.calls[1] is not None          # the second query has a since
    assert approvals.events(None, env(), "model.approved", "churn", "3")      # the older approval is still kept


def test_a_version_not_found_is_looked_up_again_once(log):
    approvals.events(None, env(), "model.approved", "churn", "3")             # nothing yet
    log.clock["t"] += 3
    log.table.append(approved("churn", 5, "2026-11-07T11:00:00", "m5"))       # approved a moment ago
    assert approvals.events(None, env(), "model.approved", "churn", "5")
    assert len(log.calls) == 2


def test_no_event_log_means_none(log):
    e = env()
    e.observability = {}
    assert approvals.events(None, e, "model.approved", "churn", "3") is None
    assert approvals.all_events(None, e, "model.approved") == []


def test_the_launcher_keeps_its_cache_in_its_state_file(log, monkeypatch):
    import pipewright.gcp as g

    files = {}

    def read(gcp, bucket, name):
        return files.get(name, (None, 0))

    def write(gcp, bucket, name, text, content_type="application/json", if_generation_match=None):
        gen = files.get(name, (None, 0))[1]
        assert if_generation_match in (gen, None)
        files[name] = (text, gen + 1)
        return gen + 1

    monkeypatch.setattr(g, "gcs_read", read)
    monkeypatch.setattr(g, "gcs_write", write)
    approvals.use_store("art", "runs/cache/approvals-prod.json")
    log.table.append(approved("churn", 3, "2026-11-01T10:00:00", "m1"))
    approvals.events(None, env(), "model.approved", "churn", "3")
    approvals.reset()                                                         # a cold start
    approvals.events(None, env(), "model.approved", "churn", "3")
    assert log.calls[0] is None and log.calls[1] is not None                  # the cold start read only what is new
    approvals.use_store(None, None)
