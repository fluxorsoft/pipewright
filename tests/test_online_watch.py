"""FR-104: the launcher's online store health: stale syncs and billed BigQuery lookups."""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pipewright.gcp import bigquery, storage
from pipewright.online import watch

NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)


def contract(name, online):
    target = SimpleNamespace(static_parameters={"ml_config": {"online_features": online}})
    return SimpleNamespace(usecase=name, tenant="churn", pipeline=lambda n: target,
                           runner_service_account=f"runner-{name}@acme.iam.gserviceaccount.com")


SQL = contract("a", {"type": "cloudsql"})
BQ = contract("b", {"type": "bigquery", "table": "acme.churn.pw_online_b"})
BT = contract("c", {"store": "projects/p/locations/r/featureOnlineStores/s"})


def records(monkeypatch, ages):
    def read(gcp, bucket, name):
        for usecase, hours in ages.items():
            if f"/{usecase}/latest.json" in name and hours is not None:
                return json.dumps({"generation": "g1", "synced_at": (NOW - timedelta(hours=hours)).isoformat()}), 1
        return None, 0
    monkeypatch.setattr(storage, "gcs_read", read)


def test_stale_is_said_once_and_fresh_when_it_recovers(monkeypatch):
    state = {}
    records(monkeypatch, {"a": 50, "b": 1})
    got = watch.stale(None, "art", [SQL, BQ, BT], state, NOW)
    assert [(e[0], e[1].usecase, e[2]["hours"]) for e in got] == [("online.stale", "a", 50.0)]
    assert watch.stale(None, "art", [SQL, BQ, BT], state, NOW) == []          # said once
    records(monkeypatch, {"a": 2, "b": 1})
    assert [e[0] for e in watch.stale(None, "art", [SQL, BQ], state, NOW)] == ["online.fresh"]


def test_a_use_case_never_synced_is_stale(monkeypatch):
    records(monkeypatch, {})
    got = watch.stale(None, "art", [SQL], {}, NOW)
    assert got[0][0] == "online.stale" and got[0][2]["hours"] is None


def test_billed_lookups_by_the_serving_identity(monkeypatch):
    asked = []
    monkeypatch.setattr(bigquery, "query", lambda gcp, project, sql, params, location=None: asked.append(
        (sql, params)) or {})
    monkeypatch.setattr(bigquery, "rows", lambda res: [{"lookups": 40, "billed": 3, "bytes": 3 * 245 * 2**20}])
    state = {}
    got = watch.billed(None, "acme", "europe-west2", [SQL, BQ], state, NOW)
    assert got[0][0] == "online.billed" and got[0][2]["billed_lookups"] == 3
    sql, params = asked[0]
    assert "INFORMATION_SCHEMA.JOBS_BY_PROJECT" in sql and "region-europe-west2" in sql
    assert {p["name"]: p["parameterValue"]["value"] for p in params}["who"] == "runner-b@acme.iam.gserviceaccount.com"
    assert state["online_billed_since"] == NOW.isoformat()


def test_alerts_for_synced_stores(monkeypatch):
    from pipewright.alerts.sync import wanted_policies

    def uc(online):
        return SimpleNamespace(ml={"monitoring": {"alerts": {"notify": ["team@example.com"], "events": []}},
                                   "online_features": online})
    assert set(wanted_policies(uc({"type": "bigquery"}))) == {"online.stale", "online.billed"}
    assert set(wanted_policies(uc({"type": "cloudsql"}))) == {"online.stale"}
    assert wanted_policies(uc({"store": "x"})) == {}
