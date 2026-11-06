"""FR-48 phase 6 (user, 2026-11-06): the loop's online canary with automatic rollback. After the production
approval the candidate serves a share of traffic; after the soak window its error rate and latency are compared
with the current version's, with the probe's result: healthy, it takes all traffic and the case closes; otherwise
it is removed and the previous version is the default again, without a person."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.loop import canary
from tests.test_loop import FakeBackend, loop

T0 = datetime(2026, 11, 6, 10, 0, tzinfo=timezone.utc)
CFG = canary.settings({"percent": 10, "soak_minutes": 30})


class CanaryBackend(FakeBackend):
    def __init__(self, healthy=(True, "error rate 0.0000 (current 0.0000)"), ready=True):
        super().__init__()
        self.healthy, self.ready, self.calls = healthy, ready, []

    def canary_settings(self):
        return CFG

    def canary_ready(self):
        if not self.ready:
            raise ConfigError("churn-endpoint serves no version in prod")

    def deploy_canary(self, version, cfg):
        self.calls.append(("canary", version, cfg["percent"]))
        return {"endpoint": "projects/p/locations/r/endpoints/9", "deployed_model_id": "222",
                "current_deployed_model_ids": ["111"], "probe": {"ok": True, "note": "12 instances, all valid"}}

    def canary_health(self, c):
        self.calls.append(("health", c["deployed_model_id"]))
        return self.healthy

    def promote_canary(self):
        self.calls.append(("promote",))

    def rollback_canary(self, previous):
        self.calls.append(("rollback", previous))
        self.approve("prod", previous)


def to_validated(backend):
    lp = loop(backend, clock=lambda: T0)
    lp.start()
    backend.finish("test", "training", "rc-1", register="8")
    lp.advance()                                                     # candidate
    lp.advance()                                                     # validating
    backend.finish("test", "inference", "rc-1-validate", alert=False)
    assert lp.advance()["state"] == "validated"
    return lp


def test_a_healthy_canary_takes_all_traffic_after_its_soak():
    b = CanaryBackend()
    lp = to_validated(b)
    rec = lp.advance(approve=True)
    assert rec["state"] == "canary" and rec["canary"]["deployed_model_id"] == "222"
    assert rec["promotion"]["previous_version"] == "3"
    assert b.calls == [("canary", rec["promotion"]["production_version"], 10)]
    lp.clock = lambda: T0 + timedelta(minutes=29)
    assert lp.advance()["state"] == "canary" and "soaks until" in lp.log[-1]       # not yet
    lp.clock = lambda: T0 + timedelta(minutes=31)
    rec = lp.advance()
    assert rec["state"] == "closed" and rec["canary"]["result"] == "healthy" and ("promote",) in b.calls
    assert [e for e, _ in b.submitted if e == "prod"] == []        # the canary checks it: no batch verification run


def test_an_unhealthy_canary_is_rolled_back_without_a_person():
    b = CanaryBackend(healthy=(False, "error rate 0.0400 against 0.0010 for the current version"))
    lp = to_validated(b)
    lp.advance(approve=True)
    lp.clock = lambda: T0 + timedelta(minutes=31)
    rec = lp.advance()
    assert rec["state"] == "rolled-back" and rec["rolled_back_to"] == "3" and ("rollback", "3") in b.calls
    assert next(v for v in b.versions_of["prod"] if v["default"])["version"] == "3"
    assert "canary: error rate 0.0400" in rec["reason"] and "loop.rolled-back" in b.events


def test_no_canary_without_the_current_version_on_the_endpoint_and_nothing_changes():
    b = CanaryBackend(ready=False)
    lp = to_validated(b)
    with pytest.raises(ConfigError, match="serves no version"):
        lp.advance(approve=True)
    assert lp.record()["state"] == "validated" and len(b.versions_of["prod"]) == 1   # nothing promoted


def test_the_judgement():
    ok, why = canary.judge({"predictions": 1000, "errors": 2, "latency_ms": 110},
                           {"predictions": 9000, "errors": 9, "latency_ms": 100}, CFG, {"ok": True, "note": "fine"})
    assert ok and "error rate 0.0020 (current 0.0010)" in why and "median latency 110 ms" in why
    bad, why = canary.judge({"predictions": 1000, "errors": 40, "latency_ms": 200},
                            {"predictions": 9000, "errors": 9, "latency_ms": 100}, CFG, None)
    assert not bad and "error rate 0.0400" in why and "median latency 200 ms against 100 ms" in why
    silent, why = canary.judge({}, {"predictions": 10}, CFG, None)
    assert not silent and "served no requests" in why
    probed, why = canary.judge({"predictions": 5}, {}, CFG, {"ok": False, "note": "1 invalid"})
    assert not probed and why.startswith("probe: 1 invalid")


def test_the_probe_uses_aggregates_only_and_answers_from_the_canary():
    stats = {"features": ["tenure", "charges"], "medians": {"tenure": 7.0, "charges": 49.7},
             "drift_baseline": {"tenure": {"edges": [1.0, 5.0, 40.0]}, "charges": {"edges": [10.0, 60.0, 120.0]}}}
    rows = canary.probe_instances(stats)
    assert rows[0] == [7.0, 49.7] and [1.0, 49.7] in rows and [7.0, 120.0] in rows and len(rows) <= canary.PROBE_ROWS
    assert canary.probe_instances({"features": ["x"], "medians": {}}) == []

    answers = iter(["111", "222", "222", "111", "222"] * 10)

    class Endpoint:
        def predict(self, instances):
            return SimpleNamespace(deployed_model_id=next(answers), predictions=[0.42])

    got = canary.probe(Endpoint(), "222", rows[:3])
    assert got == {"ok": True, "rows": 3, "answered": 3, "note": "3 instances, all valid"}
    assert canary.valid_score(0.5) and canary.valid_score([0.1]) and not canary.valid_score({"error": "bad row"})
    assert not canary.valid_score(1.7) and not canary.valid_score(float("nan"))


def test_metrics_per_deployed_model_from_cloud_monitoring():
    seen = []

    def get(url, **params):
        seen.append(params)
        metric = params["filter"].split('metric.type="')[1].split('"')[0].rsplit("/", 1)[-1]
        series = {"prediction_count": [("222", {"int64Value": "500"}), ("111", {"int64Value": "4500"})],
                  "error_count": [("222", {"int64Value": "1"})],
                  "prediction_latencies": [("222", {"doubleValue": 120.0})]}[metric]
        return SimpleNamespace(ok=True, status=200, body={"timeSeries": [
            {"metric": {"labels": {"deployed_model_id": d}}, "points": [{"value": v}]} for d, v in series]})

    got = canary.endpoint_metrics(SimpleNamespace(get=get), "p", "9", "2026-11-06T10:00:00Z", "2026-11-06T10:31:00Z")
    assert got == {"222": {"predictions": 500, "errors": 1, "latency_ms": 120.0}, "111": {"predictions": 4500}}
    assert seen[0]["aggregation.alignmentPeriod"] == "1860s" and 'endpoint_id="9"' in seen[0]["filter"]
    assert seen[2]["aggregation.perSeriesAligner"] == "ALIGN_PERCENTILE_50"                 # the median


def test_the_soak_window_starts_before_the_deploy_so_the_probe_counts():
    """Found live: the window began after the deploy and its probe: a quiet endpoint's canary served nothing in it."""
    b = CanaryBackend()
    lp = to_validated(b)
    deploy = b.deploy_canary

    def slow_deploy(version, cfg):
        lp.clock = lambda: T0 + timedelta(minutes=15)                 # a deploy takes a while
        return deploy(version, cfg)

    b.deploy_canary = slow_deploy
    rec = lp.advance(approve=True)
    assert rec["canary"]["started_at"] == T0.isoformat()


def test_a_cold_canary_is_not_judged_on_latency_until_it_has_served_enough():
    """Found live on ent-prod: 30 requests after a cold start doubled the latency and rolled a healthy canary back."""
    ok, why = canary.judge({"predictions": 30, "errors": 0, "latency_ms": 45},
                           {"predictions": 120, "errors": 0, "latency_ms": 22}, CFG, {"ok": True, "note": "fine"})
    assert ok and "latency not compared (30 canary requests, fewer than 50)" in why


def test_a_run_that_never_starts_fails_the_step_instead_of_hanging():
    """QA finding (QE6): a request the launcher dropped left the loop 'training' for ever."""
    from tests.test_loop import FakeBackend

    b = FakeBackend()
    lp = loop(b, clock=lambda: T0)
    lp.start()
    b.jobs.clear()                                                  # the run never appeared
    lp.clock = lambda: T0 + timedelta(minutes=10)
    assert lp.advance()["state"] == "training"                     # still within the grace period
    lp.clock = lambda: T0 + timedelta(minutes=31)
    rec = lp.advance()
    assert rec["state"] == "failed" and "never started" in rec["history"][-1]["note"]


def test_the_loop_refuses_to_send_what_the_release_does_not_take(monkeypatch):
    """QA finding (QE6): the launcher dropped champion_version for a pipeline without it; the loop could not see."""
    from pipewright.loop.backend import GcpBackend
    from pipewright.release import contract as rc

    b = GcpBackend.__new__(GcpBackend)
    b.usecase, b.gcp = "churn-feats", None
    b.envs = {"test": SimpleNamespace(artifacts_bucket="b")}
    monkeypatch.setattr("pipewright.release.service.read_pointer",
                        lambda gcp, e, u: (SimpleNamespace(release_id="r1"), 1))
    monkeypatch.setattr("pipewright.gcp.gcs_read", lambda gcp, bucket, name: ("{}", 1))
    monkeypatch.setattr(rc.ReleaseContract, "from_json", staticmethod(lambda text: SimpleNamespace(
        pipeline=lambda p: SimpleNamespace(parameters=["project", "region", "source_uri", "run_id"]))))
    with pytest.raises(ConfigError, match="takes no champion_version parameter.*nothing was submitted"):
        b._accepts("test", {"pipeline": "training", "champion_version": "3"})
    b._accepts("test", {"pipeline": "training"})                   # nothing optional sent: nothing to check
