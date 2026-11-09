"""Pipewright 1.4.2 QA design items (decided at the end of the QA run): the personal-data warning, several SQL files
for one built table, the export schedule for a team's own store, readers in either form, the team store's invoker,
the export readers the platform grants, and undeploy removing the lookup service."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import definitions as d
from pipewright.online import launcher as ol
from tests.test_definitions import SCHEMA, VIEW_SQL, FakeGcp
from tests.test_entity_columns_not_features import with_entity
from tests.test_bqml_config import bqml_repo, resolve
from tests.test_online_launcher import MANIFEST, ONLINE, Gcp, contract
from tests.test_online_store_types import store_repo

TEAM = "team-store@acme-team.iam.gserviceaccount.com"


# -- item 2: a column that names a person, with no personal data declared ------------------------------------------

def test_validate_warns_when_a_person_column_is_not_declared_personal(repo):
    uc = resolve(with_entity(bqml_repo(repo)))
    assert any("personal_data_columns is empty, but customer_id usually identifies a person" in w
               for w in uc.warnings)


def test_no_personal_data_warning_once_something_is_declared(repo):
    with_entity(bqml_repo(repo)).usecase["spec"]["data_protection"]["personal_data_columns"] = ["customer_id"]
    uc = resolve(repo)
    assert not any("usually identif" in w for w in uc.warnings)


# -- item 3: a table several jobs write ------------------------------------------------------------------------------

def test_a_table_written_by_several_jobs_accepts_any_of_their_sql(tmp_path, monkeypatch):
    table = {"type": "TABLE", "schema": {"fields": SCHEMA}, "location": "EU"}
    gcp = FakeGcp({"p-test.feat.spend": table})
    from_ = {"entity": {"columns": ["customer_id"]},
             "sources": [{"name": "spend", "table": "bq://p-test.feat.spend", "features": ["avg_spend_90d"]}]}
    first = d.build(gcp, from_, "p-test", "EU", None, tmp_path)["section"]
    (tmp_path / "sql").mkdir()
    append = VIEW_SQL.replace("90 DAY", "30 DAY")
    (tmp_path / "sql" / "backfill.sql").write_text(VIEW_SQL)
    (tmp_path / "sql" / "append.sql").write_text(append)
    first["sources"][0]["sql_files"] = ["sql/backfill.sql", "sql/append.sql"]
    sec = d.build(gcp, from_, "p-test", "EU", first, tmp_path)["section"]
    src = sec["sources"][0]
    h_backfill, h_append = (d.sha256(d.normalise_sql(s, "p-test")) for s in (VIEW_SQL, append))
    assert src["sql_sha256"] == h_backfill and src["also_sql_sha256"] == [h_append]
    expected = [{**src, "object": "p-test.feat.spend"}]
    for h, ok in ((h_backfill, True), (h_append, True), ("f" * 64, False)):
        monkeypatch.setattr(d, "built_sql", lambda gcp, ref, loc, kms=None, h=h: {"sql_sha256": h, "job": "j"})
        assert (d.check(gcp, expected, None, "p-test", "EU", built_tables=True)[0] == []) is ok
    one = {**sec, "sources": [{k: v for k, v in src.items() if k != "also_sql_sha256"}]}
    assert d.contract_digest(one) != d.contract_digest(sec)     # the second job's SQL is part of the contract


# -- items 4 and 5: the export schedule for a team's own store, readers in either form -------------------------------

def _schedule(spec):
    spec["spec"].setdefault("triggers", {})["schedules"] = [
        {"name": "export", "cron": "0 3 * * *", "timezone": "Europe/London", "pipeline": "online_sync"}]


def test_an_http_store_with_export_may_schedule_the_sync(repo):
    http = {"type": "http", "http": {"url": "https://lookup.example.com"}}
    with pytest.raises(ConfigError, match="or http with export.enabled"):
        store_repo(repo, http, change_spec=_schedule)()
    uc = store_repo(repo, {**http, "export": {"enabled": True, "readers": [f"serviceAccount:{TEAM}"]}},
                    change_spec=_schedule)()
    assert uc.ml["online_features"]["export"]["readers"] == [TEAM]


def test_start_sync_runs_the_export_for_an_http_store(monkeypatch):
    from pipewright.gcp import storage

    monkeypatch.setattr(storage, "gcs_write", lambda *a, **k: 1)
    gcp = Gcp({("POST", ":run"): (200, {"metadata": {"name": "executions/e1"}})})
    http = {"type": "http", "export": {"uri": "gs://art/online-export/churn/churn-x", "keep": 3, "readers": []}}
    out = ol.start_sync(contract(http), {**MANIFEST, "artifacts_bucket": "art"}, "gs://art/m.json", gcp)
    assert out["status"] == "submitted"
    assert ol.start_sync(contract({"type": "http"}), MANIFEST, "m", Gcp({}))["status"] == "refused"


# -- item 7: the export readers the platform grants ------------------------------------------------------------------

def test_validate_warns_about_readers_the_platform_has_not_granted(repo):
    http = {"type": "http", "http": {"url": "https://lookup.example.com"},
            "export": {"enabled": True, "readers": [TEAM, "other-store@acme-team.iam.gserviceaccount.com"]}}
    env_store = {"types": ["bigtable", "http"], "export_readers": {"churn": [f"serviceAccount:{TEAM}"]}}
    uc = store_repo(repo, http, env_store)()
    warned = [w for w in uc.warnings if "not granted by the platform" in w]
    assert len(warned) == 1 and "other-store@acme-team" in warned[0] and TEAM not in warned[0]
    silent = store_repo(repo, http, {"types": ["bigtable", "http"]})()     # a platform that does not say
    assert not any("not granted by the platform" in w for w in silent.warnings)


# -- item 6: the team's own store on Cloud Run lets the lookup identity call it -------------------------------------

def test_the_team_store_on_cloud_run_gets_the_lookup_identity_as_invoker():
    base = "https://run.googleapis.com/v2/projects/acme-ml-dev/locations/europe-west2/services"
    store = "projects/acme-ml-dev/locations/europe-west2/services/team-store"
    gcp = Gcp({("GET", "/services"): (200, {"services": [
                   {"name": store, "uri": "https://team-store-abc.a.run.app", "urls": []}]}),
               ("POST", ":getIamPolicy"): (200, {"bindings": [{"role": "roles/run.invoker",
                                                                "members": ["user:a@example.com"]}]})})
    assert ol.grant_store_invoker(gcp, base, "https://team-store-abc.a.run.app/lookup", "pwl@x.iam") == "team-store"
    policy = next(c for c in gcp.calls if c[1].endswith(":setIamPolicy"))[2]["policy"]
    assert policy["bindings"] == [{"role": "roles/run.invoker",
                                   "members": ["serviceAccount:pwl@x.iam", "user:a@example.com"]}]
    elsewhere = Gcp({("GET", "/services"): (200, {"services": []})})
    assert ol.grant_store_invoker(elsewhere, base, "https://lookup.example.com", "pwl@x.iam") is None
    assert not any(c[1].endswith(":setIamPolicy") for c in elsewhere.calls)


# -- item 8: undeploy removes the lookup service ---------------------------------------------------------------------

class DeletingGcp(Gcp):
    def delete(self, url, **p):
        return self._do("DELETE", url, **p)


def test_the_launcher_removes_the_lookup_service():
    gcp = DeletingGcp({("DELETE", "/services/pwl-churn-churn-x"): (200, {})})
    assert ol.remove_lookup_service(contract(), MANIFEST, gcp)["status"] == "removed"
    gone = DeletingGcp({("DELETE", "/services/"): (404, {})})
    assert ol.remove_lookup_service(contract(), MANIFEST, gone)["status"] == "absent"
    assert ol.remove_lookup_service(contract({"type": "bigquery"}), MANIFEST, gcp)["status"] == "none"
    with pytest.raises(ConfigError, match="not under the tenant's prefix"):
        ol.remove_lookup_service(contract({**ONLINE, "service": "pwl-other-x"}), MANIFEST, gcp)


def test_undeploy_asks_the_launcher_directly_or_over_the_topic(monkeypatch):
    from pipewright.runs import launch

    uc = SimpleNamespace(name="churn-x", ml={"online_features": ONLINE})
    calls = []
    monkeypatch.setattr(launch, "call", lambda env, payload, path="/run": calls.append((path, payload)) or {
        "status": "removed"})
    monkeypatch.setattr(launch, "request", lambda env, topic, payload, gcp=None: calls.append((topic, payload)))
    env = SimpleNamespace(launcher={"url": "https://l"})
    assert ol.remove_from_cli(uc, env, log=lambda m: None)["status"] == "removed"
    assert calls[-1] == ("/online/lookup-service/delete", {"usecase": "churn-x"})
    env = SimpleNamespace(launcher={"url": "https://l", "run_requests_topic": "projects/p/topics/t"})
    assert ol.remove_from_cli(uc, env, log=lambda m: None)["status"] == "requested"
    assert calls[-1] == ("projects/p/topics/t", {"usecase": "churn-x", "kind": "online.lookup_service.delete"})
    assert ol.remove_from_cli(SimpleNamespace(name="x", ml={}), env) is None
