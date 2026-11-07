"""Pipewright 1.4.1 (FR-103): feature definitions, fingerprinted, versioned and checked (CONTEXT section 8.17)."""

from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import definitions as d

VIEW_SQL = ("SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d\nFROM `p-test.feat.transactions`\n"
            "WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07', INTERVAL 90 DAY)\nGROUP BY customer_id")
SCHEMA = [{"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
          {"name": "avg_spend_90d", "type": "FLOAT", "description": "mean spend"},
          {"name": "feature_timestamp", "type": "TIMESTAMP"}]


class R:
    def __init__(self, status, body=None):
        self.status, self.body = status, body or {}

    @property
    def ok(self):
        return 200 <= self.status < 300


class FakeGcp:
    """tables.get, the feature registry, transfer configs, dry runs and feature views, from dicts."""

    def __init__(self, tables, groups=None, transfers=None, stores=None):
        self.tables, self.groups, self.transfers = tables, groups or {}, transfers or {}
        self.stores = stores or {}
        self.posted = []

    def get(self, url, **params):
        if "/tables/" in url:
            p, ds, t = url.split("/projects/")[1].split("/")[0], url.split("/datasets/")[1].split("/")[0], \
                url.rsplit("/", 1)[1]
            body = self.tables.get(f"{p}.{ds}.{t}")
            return R(200, body) if body else R(404)
        if "/featureGroups/" in url:
            g = url.rsplit("/", 1)[1]
            return R(200, self.groups[g]) if g in self.groups else R(404)
        if "transferConfigs" in url:
            name = url.split("/v1/", 1)[1]
            return R(200, self.transfers[name]) if name in self.transfers else R(404)
        if url.endswith("/featureOnlineStores"):
            return R(200, {"featureOnlineStores": [{"name": f"projects/1/locations/r/featureOnlineStores/{s}"}
                                                   for s in self.stores]})
        if url.endswith("/featureViews"):
            store = url.split("/featureOnlineStores/")[1].split("/")[0]
            return R(200, {"featureViews": [
                {"name": f"projects/1/locations/r/featureOnlineStores/{store}/featureViews/{v}",
                 "featureRegistrySource": {"featureGroups": [{"featureGroupId": g} for g in groups]}}
                for v, (groups, _) in self.stores[store].items()]})
        if url.endswith("/featureViewSyncs"):
            store = url.split("/featureOnlineStores/")[1].split("/")[0]
            view = url.split("/featureViews/")[1].split("/")[0]
            return R(200, {"featureViewSyncs": self.stores[store][view][1]})
        return R(404)

    def post(self, url, body, **params):
        self.posted.append((url, body))
        ref = body["configuration"]["query"]["query"].split("`")[1]
        base = (self.tables.get(ref) or {}).get("_base") or [ref]
        return R(200, {"statistics": {"query": {"referencedTables": [
            dict(zip(("projectId", "datasetId", "tableId"), b.split("."), strict=True)) for b in base]}}})


def view(sql=VIEW_SQL, schema=SCHEMA, base=None):
    return {"type": "VIEW", "view": {"query": sql}, "schema": {"fields": schema}, "location": "europe-west2",
            "lastModifiedTime": "1", "numRows": "0", **({"_base": base} if base else {})}


FROM = {"entity": {"columns": ["customer_id"]},
        "sources": [{"table": "bq://p-test.feat.spend", "features": ["avg_spend_90d"]}]}


# -- fingerprints ----------------------------------------------------------------------------------------------------

def test_the_fingerprint_ignores_whitespace_comments_ctas_and_the_own_project():
    a = d.normalise_sql(VIEW_SQL, "p-test")
    assert d.normalise_sql("  " + VIEW_SQL.replace(" ", "   ") + " ;", "p-test") == a
    assert d.normalise_sql("/* {\"app\": \"dbt\"} */\n" + VIEW_SQL, "p-test") == a
    assert d.normalise_sql("CREATE OR REPLACE TABLE `p-test.feat.spend` AS " + VIEW_SQL, "p-test") == a
    # the same definition in prod (found by design: promotion compares across projects)
    assert d.normalise_sql(VIEW_SQL.replace("p-test", "p-prod"), "p-prod") == a
    assert d.normalise_sql(VIEW_SQL.replace("90 DAY", "60 DAY"), "p-test") != a


def test_the_schema_is_names_types_and_modes_never_descriptions():
    assert d.schema_of(SCHEMA) == ["customer_id STRING REQUIRED", "avg_spend_90d FLOAT NULLABLE",
                                   "feature_timestamp TIMESTAMP NULLABLE"]
    other = [{**f, "description": "changed"} for f in SCHEMA]
    assert d.schema_of(other) == d.schema_of(SCHEMA)
    nested = [{"name": "a", "type": "RECORD", "fields": [{"name": "b", "type": "INT64"}]}]
    assert d.schema_of(nested) == ["a RECORD NULLABLE", "a.b INT64 NULLABLE"]


# -- the contract ----------------------------------------------------------------------------------------------------

def test_a_contract_is_built_from_the_live_view_and_versioned_by_what_changed():
    gcp = FakeGcp({"p-test.feat.spend": view()})
    first = d.build(gcp, FROM, "p-test", "europe-west2", None, None)
    sec = first["section"]
    assert first["level"] == "new" and sec["version"] == "1.0.0"
    src = sec["sources"][0]
    assert src["name"] == "spend" and src["kind"] == "view" and src["sql_sha256"] and src["schema_sha256"]
    assert src["features"] == {"avg_spend_90d": {"type": "FLOAT"}}
    src["features"]["avg_spend_90d"]["description"] = "Mean spend over 90 days"
    # the same: nothing to change, descriptions kept
    same = d.build(gcp, FROM, "p-test", "europe-west2", sec, None)
    assert same["level"] is None and same["section"]["version"] == "1.0.0"
    assert same["section"]["sources"][0]["features"]["avg_spend_90d"]["description"] == "Mean spend over 90 days"
    # SQL changed: minor
    gcp.tables["p-test.feat.spend"] = view(VIEW_SQL.replace("90 DAY", "60 DAY"))
    minor = d.build(gcp, FROM, "p-test", "europe-west2", sec, None)
    assert minor["level"] == "minor" and minor["section"]["version"] == "1.1.0"
    assert any("its SQL changed" in c for c in minor["changes"])
    # a column retyped: major
    gcp.tables["p-test.feat.spend"] = view(schema=[SCHEMA[0], {**SCHEMA[1], "type": "STRING"}, SCHEMA[2]])
    major = d.build(gcp, FROM, "p-test", "europe-west2", sec, None)
    assert major["level"] == "major" and major["section"]["version"] == "2.0.0"
    assert "spend: column avg_spend_90d is now STRING NULLABLE (was FLOAT NULLABLE)" in major["changes"]


def test_a_feature_group_is_followed_to_its_bigquery_source():
    gcp = FakeGcp({"p-test.feat.spend_v": view()},
                  groups={"spend": {"bigQuery": {"bigQuerySource": {"inputUri": "bq://p-test.feat.spend_v"}}}})
    src = {"feature_group": "spend", "features": ["avg_spend_90d"]}
    assert d.source_name(src) == "spend"
    assert d.resolve(gcp, src, "p-test", "europe-west2") == "p-test.feat.spend_v"
    with pytest.raises(ConfigError, match="feature group missing cannot be read"):
        d.resolve(gcp, {"feature_group": "missing"}, "p-test", "europe-west2")


def test_a_built_table_takes_its_sql_from_the_declared_file(tmp_path):
    table = {"type": "TABLE", "schema": {"fields": SCHEMA}, "location": "EU"}
    gcp = FakeGcp({"p-test.feat.spend": table})
    first = d.build(gcp, FROM, "p-test", "EU", None, tmp_path)["section"]
    assert first["sources"][0]["kind"] == "table" and first["sources"][0]["sql_sha256"] is None
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "spend.sql").write_text(VIEW_SQL)
    first["sources"][0]["sql_file"] = "sql/spend.sql"
    second = d.build(gcp, FROM, "p-test", "EU", first, tmp_path)
    assert second["section"]["sources"][0]["sql_sha256"] == d.sha256(d.normalise_sql(VIEW_SQL, "p-test"))
    assert second["level"] == "minor"
    gcp.transfers["projects/1/locations/eu/transferConfigs/9"] = {"params": {"query": VIEW_SQL}}
    first["sources"][0]["scheduled_query"] = "projects/1/locations/eu/transferConfigs/9"
    third = d.build(gcp, FROM, "p-test", "EU", first, tmp_path)
    assert third["section"]["sources"][0]["sql_sha256"] == second["section"]["sources"][0]["sql_sha256"]


# -- the check -------------------------------------------------------------------------------------------------------

def test_the_check_names_what_changed_never_a_value():
    gcp = FakeGcp({"p-test.feat.spend": view()})
    sec = d.build(gcp, FROM, "p-test", "europe-west2", None, None)["section"]
    assert d.check(gcp, sec["sources"], FROM, "p-test", "europe-west2")[0] == []
    gcp.tables["p-test.feat.spend"] = view(VIEW_SQL.replace("90 DAY", "60 DAY"))
    problems, _, _ = d.check(gcp, sec["sources"], FROM, "p-test", "europe-west2")
    assert len(problems) == 1 and problems[0].startswith("feature source spend: its SQL changed (sha256 ")
    gcp.tables["p-test.feat.spend"] = view(schema=SCHEMA[:1] + SCHEMA[2:])
    problems, _, _ = d.check(gcp, sec["sources"], FROM, "p-test", "europe-west2")
    assert "column avg_spend_90d removed" in problems[0]
    del gcp.tables["p-test.feat.spend"]
    problems, _, _ = d.check(gcp, sec["sources"], FROM, "p-test", "europe-west2")
    assert "cannot be read (HTTP 404)" in problems[0]


def test_an_approval_records_the_objects_and_is_checked_against_them():
    gcp = FakeGcp({"p-test.feat.spend": view()})
    sec = d.build(gcp, FROM, "p-test", "europe-west2", None, None)["section"]
    _, _, now = d.check(gcp, sec["sources"], FROM, "p-test", "europe-west2")
    rec = d.approval_record(sec, now)
    assert rec["contract_version"] == "1.0.0" and rec["contract_sha256"] == d.contract_digest(sec)
    assert rec["sources"][0]["object"] == "p-test.feat.spend"
    # descriptions are not part of the digest
    described = {**sec, "sources": [{**sec["sources"][0], "features": {"avg_spend_90d": {"description": "x"}}}]}
    assert d.contract_digest(described) == rec["contract_sha256"]
    assert d.check(gcp, rec["sources"], None, "p-test", "europe-west2")[0] == []
    gcp.tables["p-test.feat.spend"] = view(VIEW_SQL + " HAVING COUNT(*) > 1")
    assert "its SQL changed" in d.check(gcp, rec["sources"], None, "p-test", "europe-west2")[0][0]


def test_a_built_table_is_checked_by_its_last_job_only_with_built_tables(monkeypatch):
    table = {"type": "TABLE", "schema": {"fields": SCHEMA}, "location": "EU"}
    gcp = FakeGcp({"p-test.feat.spend": table})
    declared = d.sha256(d.normalise_sql(VIEW_SQL, "p-test"))
    expected = [{"name": "spend", "object": "p-test.feat.spend", "kind": "table", "sql_sha256": declared,
                 "schema_sha256": d.sha256(d.schema_of(SCHEMA))}]
    problems, notes, _ = d.check(gcp, expected, None, "p-test", "EU", built_tables=False)
    assert problems == [] and "checked by schema only" in notes[0]
    monkeypatch.setattr(d, "built_sql", lambda gcp, ref, loc, kms=None: {"sql_sha256": declared, "job": "j1"})
    assert d.check(gcp, expected, None, "p-test", "EU", built_tables=True)[0] == []
    monkeypatch.setattr(d, "built_sql", lambda gcp, ref, loc, kms=None: {"sql_sha256": "f" * 64, "job": "j2"})
    problems, _, _ = d.check(gcp, expected, None, "p-test", "EU", built_tables=True)
    assert problems == [f"feature source spend: last built by SQL sha256 ffffffffffff (j2), approved {declared[:12]}"]
    monkeypatch.setattr(d, "built_sql", lambda gcp, ref, loc, kms=None: {})
    assert "no job that wrote it" in d.check(gcp, expected, None, "p-test", "EU", built_tables=True)[1][0]


# -- the policy, the approval check, the launcher, the watch, rollback ----------------------------------------------

def test_the_features_policy_is_off_by_default():
    from pipewright.models import integrity

    assert integrity.features_policy(None) == {"check": False, "built_tables": False, "record_reads": False,
                                               "fingerprint_rows": False, "snapshot_days": 0}
    on = integrity.features_policy({"check": "on", "snapshot_days": 3})
    assert on["check"] and on["record_reads"] and on["snapshot_days"] == 3
    with pytest.raises(ConfigError):
        integrity.features_policy({"snapshot_days": -1})


def test_verify_compares_the_approved_features_only_when_the_check_is_on():
    from pipewright.models import integrity

    gcp = FakeGcp({"p-test.feat.spend": view(VIEW_SQL + " LIMIT 5")})
    approved = {"sources": [{"name": "spend", "object": "p-test.feat.spend", "kind": "view",
                             "sql_sha256": d.sha256(d.normalise_sql(VIEW_SQL, "p-test")),
                             "schema_sha256": d.sha256(d.schema_of(SCHEMA))}]}
    off = SimpleNamespace(project="p-test", region="r", policies={}, security={})
    assert integrity.compare_features(gcp, off, approved) == ([], [])
    on = SimpleNamespace(project="p-test", region="r", policies={"integrity": {"features": {"check": "on"}}},
                         security={})
    problems, _ = integrity.compare_features(gcp, on, approved)
    assert "its SQL changed" in problems[0]
    assert integrity.compare_features(gcp, on, None) == ([], [])


def _contract(section, features_from=FROM, metadata=None):
    import json

    return SimpleNamespace(usecase="churn", environment="test", project="p-test", region="r", release_id="r1",
                           tenant="t", metadata=metadata or {},
                           pipeline_names=["training"],
                           pipeline=lambda n: SimpleNamespace(static_parameters={"ml_config": json.dumps(
                               {"feature_contract": section, "data": {"features_from": features_from}})}))


def test_the_launcher_refuses_training_on_a_changed_definition_and_records_the_contract():
    from pipewright.models import integrity

    gcp = FakeGcp({"p-test.feat.spend": view()})
    sec = d.build(gcp, FROM, "p-test", "r", None, None)["section"]
    manifest = {"policies": {"integrity": {"features": {"check": "on"}}}}
    ok = integrity.feature_gate(gcp, manifest, _contract(sec), "training")
    assert ok["problem"] is None and ok["record"]["feature_contract"] == {"version": "1.0.0",
                                                                          "sha256": d.contract_digest(sec)}
    gcp.tables["p-test.feat.spend"] = view(VIEW_SQL.replace("90 DAY", "30 DAY"))
    bad = integrity.feature_gate(gcp, manifest, _contract(sec), "training")
    assert bad["mode"] == "refuse" and "not the ones in feature contract 1.0.0" in bad["problem"]
    # check off: recorded, never refused
    off = integrity.feature_gate(gcp, {}, _contract(sec), "training")
    assert off["problem"] is None and off["record"]["feature_contract"]["version"] == "1.0.0"
    # no contract: nothing at all
    assert integrity.feature_gate(gcp, manifest, _contract(None), "training") == {"mode": "off", "problem": None,
                                                                                    "record": {}}


def test_a_scoring_run_records_the_base_tables_it_reads(monkeypatch):
    import datetime

    from pipewright.models import integrity

    base = {"type": "TABLE", "lastModifiedTime": "1794038186457", "numRows": "20000", "schema": {"fields": []}}
    gcp = FakeGcp({"p-test.feat.spend": view(base=["p-test.feat.transactions"]),
                   "p-test.feat.transactions": base})
    sec = d.build(gcp, FROM, "p-test", "r", None, None)["section"]
    queries = []
    monkeypatch.setattr("pipewright.gcp.bigquery.query", lambda gcp, p, sql, *a, **k: queries.append(sql) or {
        "schema": {"fields": [{"name": "n", "type": "INTEGER"}, {"name": "fp", "type": "INTEGER"}]},
        "rows": [{"f": [{"v": "200"}, {"v": "42"}]}]})
    at = datetime.datetime(2026, 11, 7, 9, 0, tzinfo=datetime.timezone.utc)
    manifest = {"policies": {"integrity": {"features": {"record_reads": True, "fingerprint_rows": True,
                                                        "snapshot_days": 3}}}}
    rec = integrity.feature_gate(gcp, manifest, _contract(sec, metadata={"dataset": "uc"}), "inference",
                                 run_id="r9", now=at)["record"]["feature_reads"]
    assert rec["read_at"] == "2026-11-07T09:00:00Z"
    src = rec["sources"][0]
    assert src["object"] == "p-test.feat.spend" and src["rows"] == 200 and src["row_fingerprint"] == "42"
    assert src["base_tables"] == [{"table": "p-test.feat.transactions", "last_modified": "1794038186457",
                                   "rows": "20000", "snapshot": "p-test.uc.transactions_snap_r9"}]
    assert any("CREATE SNAPSHOT TABLE `p-test.uc.transactions_snap_r9` CLONE `p-test.feat.transactions`" in q
               and "2026-11-10 09:00:00" in q for q in queries)
    assert gcp.posted[0][1]["configuration"]["dryRun"] is True


def test_the_watch_reports_a_feature_view_whose_newest_sync_failed():
    from pipewright.models import watch

    failed = [{"finalStatus": {"code": 3, "message": "Unrecognized name: avg_spend_90d"}, "runTime": {}}]
    gcp = FakeGcp({}, stores={"store": {"spend_view": (["spend"], failed), "other": (["unrelated"], [])}})
    env = SimpleNamespace(project="p", region="r")
    uses, notes = watch.feature_views(gcp, env, {"spend": "churn"})
    assert [(u.where, u.status, u.severity) for u in uses] == [("feature_view:store/spend_view", "changed",
                                                               "incident")]
    assert "serves the last good values" in uses[0].problems[0] and uses[0].usecase == "churn"
    gcp.stores["store"]["spend_view"] = (["spend"], [{"finalStatus": {}, "runTime": {"endTime": "t"}}])
    uses, _ = watch.feature_views(gcp, env, {"spend": "churn"})
    assert uses[0].status == "unchanged"
    assert watch.feature_views(gcp, env, {}) == ([], [])


def test_rolling_back_checks_the_old_releases_feature_contract(monkeypatch):
    import json

    from pipewright.models import integrity

    gcp = FakeGcp({"p-test.feat.spend": view()})
    sec = d.build(gcp, FROM, "p-test", "r", None, None)["section"]
    gcp.tables["p-test.feat.spend"] = view(VIEW_SQL.replace("90 DAY", "7 DAY"))
    monkeypatch.setattr("pipewright.release.contract.ReleaseContract.from_dict",
                        classmethod(lambda cls, data: _contract(sec)))
    env = SimpleNamespace(name="test", project="p-test", region="r", security={},
                          policies={"integrity": {"features": {"check": "on"}}})
    check = integrity.Check()
    integrity.rollback_features(gcp, env, "r1", json.dumps({}), check)
    assert check.problems and check.problems[0].startswith("release r1: feature source spend: its SQL changed")
    off = integrity.Check()
    integrity.rollback_features(gcp, SimpleNamespace(**{**vars(env), "policies": {}}), "r1", "{}", off)
    assert off.problems == []


def test_the_contract_section_must_match_the_features_from_sources():
    from pipewright.config.ml import _feature_contract

    problems = []
    _feature_contract({"version": "1.0.0", "sources": [{"name": "spend", "kind": "view"}]}, FROM, problems)
    assert problems == []
    _feature_contract({"version": "1.0", "sources": [{"name": "other", "kind": "cube"}]}, FROM, problems)
    assert any("MAJOR.MINOR.PATCH" in p for p in problems)
    assert any("'other' is not a data.features_from source (spend)" in p for p in problems)
    assert any("kind must be one of" in p for p in problems)
    assert any("no entry for spend" in p for p in problems)


def test_a_use_case_without_a_contract_resolves_without_the_section():
    from pipewright.config.ml import resolve_ml

    ml = resolve_ml({}, "churn", True, set(), [])
    assert "feature_contract" not in ml
    timed = {**FROM, "entity": {"columns": ["customer_id"], "time_column": "snapshot_date"}}
    with_it = resolve_ml({"data": {"features_from": timed}, "feature_contract": {
        "version": "1.0.0", "sources": [{"name": "spend", "kind": "view"}]}}, "churn", True, set(), [])
    assert with_it["feature_contract"]["version"] == "1.0.0"


def test_a_source_table_without_a_project_is_in_the_environments_project():
    """Found live: features_from tables are usually bq://${resources.bigquery_dataset}.<table>."""
    gcp = FakeGcp({"p-test.feat.spend": view()})
    src = {"table": "bq://feat.spend", "features": ["avg_spend_90d"]}
    assert d.source_name(src) == "spend"
    assert d.resolve(gcp, src, "p-test", "r") == "p-test.feat.spend"
    sec = d.build(gcp, {"sources": [src]}, "p-test", "r", None, None)["section"]
    assert sec["sources"][0]["kind"] == "view"



def test_a_table_built_from_a_commented_sql_file_matches_the_file():
    """Found live: built from the declared file, the job's statement is CREATE ... AS -- comment ... SELECT."""
    body = "-- Support activity per customer\nSELECT customer_id, n\nFROM `p-test.ds.events`\n"
    job = "CREATE OR REPLACE TABLE `p-test.ds.support` AS " + body
    assert d.normalise_sql(job, "p-test") == d.normalise_sql(body, "p-test")
    assert d.normalise_sql(job, "p-test") == "SELECT customer_id, n FROM `{project}.ds.events`"
    assert d.normalise_sql(body.replace("p-test", "{project}"), "p-test") == d.normalise_sql(job, "p-test")
