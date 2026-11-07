"""FR-104: doctor shows each store and its cost, and checks it is there, fresh and safe."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pipewright.checks.doctor import Report
from pipewright.gcp import storage
from pipewright.models import online as online_check
from pipewright.online import doctor

CLOUDSQL = {"type": "cloudsql", "provider": "pipewright", "service": "pwl-churn-x", "schema": "public",
            "table": "pw_online_x", "key": "k", "features": ["a"], "instance": "acme:europe-west2:features",
            "lookup": {"cpu": 1, "memory": "512Mi", "min_instances": 1, "max_instances": 5}}


def env(**kw):
    tenants = {"churn": {"features": {"sync_job": "j1"}}, "fraud": {"features": {"sync_job": "j2"}}}
    return SimpleNamespace(name=kw.get("name", "prod"), project="acme", region="europe-west2", artifacts_bucket="art",
                           promotion_from=kw.get("promotion_from"), tenants=tenants, features={"online_store": {
                               "cloudsql": {"instance": "acme:europe-west2:features",
                                            "settings": {"tier": kw.get("tier", "db-custom-1-3840")}},
                               "bigquery": {"bi_engine_gib": 1}}})


class Gcp:
    def __init__(self, answers):
        self.answers = answers

    def get(self, url, **p):
        for part, (status, body) in self.answers.items():
            if part in url:
                return SimpleNamespace(ok=200 <= status < 300, status=status, body=body)
        return SimpleNamespace(ok=False, status=404, body={})


def run(online, gcp, e, record=None, monkeypatch=None):
    monkeypatch.setattr(storage, "gcs_read", lambda g, b, n: (json.dumps(record), 1) if record else (None, 0))
    report = Report()
    doctor.check(SimpleNamespace(name="x", tenant="churn", ml={"online_features": online}), e, gcp, report)
    return [(r.status, r.check, r.detail) for rs in report.sections.values() for r in rs]


def test_cloudsql_shows_costs_and_warns_about_public_and_shared_core(monkeypatch):
    fresh = {"generation": "g1", "rows": 10, "synced_at": datetime.now(timezone.utc).isoformat(),
             "feature_contract": {"version": "1.0.0"}}
    got = run(CLOUDSQL, Gcp({"sqladmin": (200, {"state": "RUNNABLE", "settings": {"tier": "db-g1-small"}}),
                             "run.googleapis.com": (200, {"terminalCondition": {"state": "CONDITION_SUCCEEDED"},
                                                          "uri": "https://u"})}),
              env(tier="db-g1-small"), fresh, monkeypatch)
    text = " | ".join(f"{s} {c}: {d}" for s, c, d in got)
    assert "about $33 a month (London list price), shared by 2 tenants" in text
    assert "WARN instance tier" in text and "no SLA" in text                 # shared core in production
    assert "WARN schema: public, in an instance other tenants share" in text
    assert "warm instances about $14 a month (best effort" in text
    assert "OK last sync: g1: 10 entities, 0 h ago, feature contract 1.0.0" in text


def test_a_missing_service_and_an_old_sync_are_warned(monkeypatch):
    old = {"generation": "g0", "rows": 1, "synced_at": (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()}
    got = run({**CLOUDSQL, "schema": "team"}, Gcp({"sqladmin": (200, {"state": "RUNNABLE",
                                                                      "settings": {"tier": "db-custom-1-3840"}}),
                                                   "run.googleapis.com": (404, {})}), env(), old, monkeypatch)
    statuses = {c: s for s, c, d in got}
    assert statuses["lookup service"] == "WARN" and statuses["last sync"] == "WARN"
    assert "schema" not in statuses


def test_bigquery_without_a_reservation_fails_and_states_the_refresh_cost(monkeypatch):
    online = {"type": "bigquery", "table": "acme.churn.pw_online_x"}
    got = run(online, Gcp({"tables/pw_online_x": (200, {"numBytes": str(245 * 2**20)}),
                           "biReservation": (200, {})}), env(), None, monkeypatch)
    text = " | ".join(f"{s} {c}: {d}" for s, c, d in got)
    assert "FAIL BI Engine: no reservation" in text and "about $0.00146 each" in text
    got = run(online, Gcp({"tables/pw_online_x": (200, {"numBytes": str(2 * 2**30)}),
                           "biReservation": (200, {"size": str(2**30)})}), env(), None, monkeypatch)
    assert any(s == "FAIL" and "the reservation 1.00 GiB" in d for s, c, d in got)


def test_a_typed_lookup_must_be_this_environments():
    expected = {**CLOUDSQL}
    vars_ = {"PIPEWRIGHT_ONLINE_FEATURES": json.dumps(expected)}
    assert online_check.check(expected, vars_, None, "prod") is None
    other = {"PIPEWRIGHT_ONLINE_FEATURES": json.dumps({**expected, "instance": "acme-test:europe-west2:features"})}
    assert "instance differ" in online_check.check(expected, other, None, "prod")
    assert "carries no online lookup" in online_check.check(expected, {}, None, "prod")
