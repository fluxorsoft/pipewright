"""1.4.1 (FR-101 item 8): approved files stay protected while they are needed; never shortened; fail safe = more."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pipewright.models import protection
from pipewright.models.protection import Artifact

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class Gcs:
    """One object generation with retention; optional races (412s) and a refusal."""

    def __init__(self, until=None, mode="Unlocked", races=0, refuse=False, gone=False):
        self.until, self.mode, self.races, self.refuse, self.gone = until, mode, races, refuse, gone
        self.meta, self.patches = 5, []

    def get(self, url, **params):
        if "/o/" not in url:
            return SimpleNamespace(ok=True, status=200, body={"objectRetention": {"mode": "Enabled"}})
        if self.gone:
            return SimpleNamespace(ok=False, status=404, body={})
        ret = {"mode": self.mode, "retainUntilTime": iso(self.until)} if self.until else None
        return SimpleNamespace(ok=True, status=200, body={"generation": "9", "metageneration": str(self.meta),
                                                          **({"retention": ret} if ret else {})})

    def patch(self, url, body, **params):
        self.patches.append((body, params))
        if self.races:
            self.races -= 1
            self.meta += 1                 # someone else changed the metadata
            return SimpleNamespace(ok=False, status=412, body={})
        if params["ifMetagenerationMatch"] != str(self.meta):
            return SimpleNamespace(ok=False, status=412, body={})
        if self.refuse:
            return SimpleNamespace(ok=False, status=403, body={})
        self.until = datetime.fromisoformat(body["retention"]["retainUntilTime"].replace("Z", "+00:00"))
        self.mode, self.meta = body["retention"]["mode"], self.meta + 1
        return SimpleNamespace(ok=True, status=200, body={})


ART = Artifact("art", "pipeline-root/t/run/model/model.pt", "9", "model_file", ["default"], "churn", "4")


def test_the_threshold_has_a_minimum():
    assert protection.threshold_days(30) == 15 and protection.threshold_days(2) == 3
    assert protection.threshold_days(90) == 45


def test_enough_protection_left_is_left_alone():
    g = Gcs(until=NOW + timedelta(days=20))
    got = protection.reconcile_one(g, ART, 30, "unlocked", NOW)
    assert got["status"] == "unchanged" and g.patches == []


def test_under_the_threshold_is_extended_exactly_with_only_retention_in_the_body():
    g = Gcs(until=NOW + timedelta(days=5))
    got = protection.reconcile_one(g, ART, 30, "unlocked", NOW)
    assert got["status"] == "renewed" and got["new"] == iso(NOW + timedelta(days=30))
    body, params = g.patches[0]
    assert list(body) == ["retention"] and params["generation"] == "9" and "ifMetagenerationMatch" in params


def test_a_locked_mode_stays_locked_and_an_expired_one_gets_the_policy_mode():
    g = Gcs(until=NOW + timedelta(days=1), mode="Locked")
    protection.reconcile_one(g, ART, 30, "unlocked", NOW)
    assert g.patches[0][0]["retention"]["mode"] == "Locked"
    g = Gcs(until=None)
    assert protection.reconcile_one(g, ART, 30, "locked", NOW)["status"] == "renewed"
    assert g.patches[0][0]["retention"]["mode"] == "Locked"


def test_retention_is_never_shortened():
    g = Gcs(until=NOW + timedelta(days=60))                 # protected longer than the policy asks
    got = protection.reconcile_one(g, ART, 2, "unlocked", NOW)     # threshold 3: 60 left, nothing to do
    assert got["status"] == "unchanged" and g.patches == []


def test_a_lost_race_is_read_again_and_retried():
    g = Gcs(until=NOW + timedelta(days=2), races=1)
    assert protection.reconcile_one(g, ART, 30, "unlocked", NOW)["status"] == "renewed"
    assert len(g.patches) == 2


def test_a_gone_generation_and_a_refusal_fail_loudly():
    assert protection.reconcile_one(Gcs(gone=True), ART, 30, "unlocked", NOW)["status"] == "failed"
    got = protection.reconcile_one(Gcs(until=NOW + timedelta(days=1), refuse=True), ART, 30, "unlocked", NOW)
    assert got["status"] == "failed" and "403" in got["reason"]


def made(uc, version, at, previous=None, kind="model.approved"):
    return {"usecase": uc, "version": version, "at": at, "previous": previous, "type": kind}


def test_the_previous_default_is_a_candidate_until_its_window_ends():
    timeline = [made("churn", "3", "2026-10-01T10:00:00"), made("churn", "4", "2026-11-01T10:00:00", "3")]
    got, unclear = protection.rollback_candidates("churn", "4", timeline, 30, NOW)
    assert got == [("3", "2026-11-01T10:00:00")] and not unclear
    got, _ = protection.rollback_candidates("churn", "4", timeline, 5, NOW)          # the window has ended
    assert got == []
    assert protection.rollback_candidates("churn", "4", timeline, 0, NOW) == ([], False)   # no rollback window


def test_a_registration_that_was_default_counts_and_finds_the_previous_one():
    timeline = [made("churn", "3", "2026-10-30T10:00:00"),
                made("churn", "4", "2026-11-05T10:00:00", kind="model.registered")]
    got, unclear = protection.rollback_candidates("churn", "4", timeline, 30, NOW)
    assert got == [("3", "2026-11-05T10:00:00")] and not unclear


def test_an_unclear_history_protects_every_candidate_and_is_degraded():
    timeline = [made("churn", "2", "2026-10-20T10:00:00"), made("churn", "3", "2026-11-01T10:00:00", "2")]
    got, unclear = protection.rollback_candidates("churn", "5", timeline, 30, NOW)   # v5 became default in the console
    assert unclear and sorted(v for v, _ in got) == ["2", "3"]


def test_the_summary_is_failed_degraded_or_healthy():
    ok = [{"status": "unchanged", "remaining_days": 20.0}, {"status": "renewed", "remaining_days": 30.0}]
    assert protection.summary(ok, {}, 30)["status"] == "healthy"
    assert protection.summary(ok, {"degraded": ["x"]}, 30)["status"] == "degraded"
    bad = protection.summary(ok + [{"status": "failed"}], {"degraded": ["x"]}, 30)
    assert bad["status"] == "failed" and bad["least_protection_remaining_days"] == 20.0 and bad["renewed"] == 1


def test_the_launcher_reports_a_released_hold_and_an_unexplained_metadata_change(monkeypatch):
    from pipewright.models import approvals

    art = Artifact("art", "pr/m/model.pt", "9", "model_file", ["default"], "churn", "4")
    monkeypatch.setattr(protection, "discover", lambda gcp, env, models=None, now=None: {"artifacts": [art]})
    meta = {"metageneration": "5", "temporaryHold": True}
    monkeypatch.setattr(protection, "_meta", lambda gcp, a: SimpleNamespace(ok=True, body=dict(meta)))
    renewals = []
    monkeypatch.setattr(approvals, "all_events", lambda gcp, env, kind: renewals)
    state = {}
    assert protection.observe(None, None, state) == []                       # the first look records
    meta.update(metageneration="6")                                          # the renewer extended it, recorded
    renewals.append({"bucket": "art", "object": "pr/m/model.pt", "generation": "9", "at": "2099-01-01 00:00:00"})
    assert protection.observe(None, None, state) == []
    meta.update(metageneration="7")                                          # changed, with no renewal recorded
    renewals.clear()
    got = protection.observe(None, None, state)
    assert [c["status"] for c in got] == ["metadata_changed"] and got[0]["severity"] == "incident"
    meta.update(temporaryHold=False)
    assert [c["status"] for c in protection.observe(None, None, state)] == ["hold_released"]


def test_the_watch_records_only_changes():
    from pipewright.models import watch

    u = watch.Use("churn", "churn-classifier", "4", "default", "default")
    assert watch.transitions({}, [u]) == []                                  # new and unchanged: nothing
    u.status, u.severity, u.problems = watch.CHANGED, "incident", ["model.pt: changed after the approval"]
    got = watch.transitions({u.key: {"status": "unchanged"}}, [u])
    assert got[0]["status"] == "changed" and got[0]["previous"] == "unchanged"
    assert watch.transitions({u.key: {"status": "changed"}}, [u]) == []      # still changed: said once
    u.status, u.problems = watch.UNCHANGED, []
    assert watch.transitions({u.key: {"status": "changed"}}, [u])[0]["status"] == "restored"
    assert watch.exit_code([u]) == 0
    u.status = watch.CANNOT
    assert watch.exit_code([u]) == 2
