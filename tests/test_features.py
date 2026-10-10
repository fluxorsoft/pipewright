"""Features from feature tables: data.features_from (config checks, the point-in-time query, the event, doctor)."""
import copy
import importlib.util
import json

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.scaffold.new_usecase import list_examples, new_usecase

FEATURES_FROM = {
    "entity": {"columns": ["customer_id"], "time_column": "snapshot_date"},
    "sources": [
        {"table": "bq://feats.customer_profile", "features": ["tenure_months", "monthly_charges"]},
        {"table": "bq://other-project.feats.support", "entity_columns": ["cust"], "time_column": "as_of",
         "features": ["num_support_tickets"]},
    ],
}


def scaffold(repo):
    repo.write()
    dest = new_usecase(repo.root, "churn-feats", "churn", "team-feats", example="features")
    return dest, load_environment(repo.root, "dev")


def resolve_with(repo, change):
    dest, env = scaffold(repo)
    path = dest / "config" / "data.yaml"
    import yaml

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    change(doc)
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return lambda: resolve_usecase(dest / "usecase.yaml", env)


def test_the_example_resolves_with_its_feature_tables(repo):
    assert "features" in list_examples()
    dest, env = scaffold(repo)
    uc = resolve_usecase(dest / "usecase.yaml", env)
    spec = uc.ml["data"]["features_from"]
    assert [s["table"] for s in spec["sources"]] == ["bq://churn.churn_feats_customer_profile",
                                                     "bq://churn.churn_feats_support_activity"]
    assert uc.ml_config()["data"]["features_from"] == spec               # what read_source receives
    assert "tenure_months" in uc.ml["features"]["include"] and "churned" not in uc.ml["features"]["include"]
    assert (dest / "data" / "entities.csv").is_file() and (dest / "data" / "load_tables.sh").is_file()


@pytest.mark.parametrize("change, message", [
    (lambda d: d["data"]["features_from"]["sources"][0]["features"].append("not_in_contract"),
     "not_in_contract' is not in data.contract.columns"),
    (lambda d: d["data"]["features_from"]["sources"][1]["features"].append("tenure_months"),
     "also comes from sources\\[0\\]; set a prefix"),
    (lambda d: d["data"]["features_from"]["sources"][0]["features"].append("churned"), "is the label"),
    (lambda d: d["data"]["features_from"]["entity"].update(time_column="num_products"),
     "must be a date or timestamp column"),
    (lambda d: d["data"]["features_from"]["sources"][0].update(entity_columns=["a", "b"]),
     "one column for each of data.features_from.entity.columns"),
    (lambda d: d["data"]["features_from"]["sources"][0].update(table="bq://x"), "is not bq://project.dataset.table"),
    (lambda d: d["data"]["features_from"]["sources"][0]["features"].append("bad-name"), "is not a column name"),
])
def test_config_problems_are_reported(repo, change, message):
    resolve = resolve_with(repo, change)
    with pytest.raises(ConfigError, match=message):
        resolve()


def test_a_landing_trigger_cannot_deliver_an_entity_table(repo):
    dest, env = scaffold(repo)
    path = dest / "usecase.yaml"
    path.write_text(path.read_text(encoding="utf-8").replace(
        "    api: true", "    landing: { suffixes: [.csv] }\n    api: true"), encoding="utf-8")
    with pytest.raises(ConfigError, match="triggers.landing delivers gs:// files"):
        resolve_usecase(path, env)


# ------------------------------------------------------------------- policy --

def with_manifest(repo, required=None, datasets=None):
    if required is not None:
        repo.manifests["dev"]["features"] = {"required": required}
    if datasets is not None:
        repo.manifests["dev"]["tenants"]["churn"]["features"] = {"datasets": datasets}
    return scaffold(repo)


def test_feature_tables_must_be_in_the_tenants_datasets(repo):
    project = repo.manifests["dev"]["project"]
    dest, env = with_manifest(repo, datasets=[f"{project}.features_core"])
    with pytest.raises(ConfigError, match=f"{project}.churn is not a feature dataset of tenant 'churn'"):
        resolve_usecase(dest / "usecase.yaml", env)


def test_listed_datasets_and_required_pass_when_every_feature_is_a_feature(repo):
    project = repo.manifests["dev"]["project"]
    dest, env = with_manifest(repo, required=True, datasets=[f"{project}.churn"])
    uc = resolve_usecase(dest / "usecase.yaml", env)
    assert env.features == {"required": True} and len(uc.ml["features"]["include"]) == 8


def test_required_refuses_a_feature_from_the_entity_table(repo):
    def entity_feature(d):
        d["data"]["contract"]["columns"].append({"name": "region", "type": "string"})

    repo.manifests["dev"]["features"] = {"required": True}
    resolve = resolve_with(repo, entity_feature)
    with pytest.raises(ConfigError, match="not from a feature table: region"):
        resolve()


def test_required_refuses_a_use_case_without_feature_tables(repo):
    repo.manifests["dev"]["features"] = {"required": True}
    repo.write()
    with pytest.raises(ConfigError, match="add data.features_from"):
        resolve_usecase(new_usecase(repo.root, "churn-min", "churn", "team", example="minimal") / "usecase.yaml",
                        load_environment(repo.root, "dev"))


# ------------------------------------------------------------------ runtime --

def runtime():
    from pipewright.build.templates import pipewright_runtime as rt

    return rt


def test_the_query_is_point_in_time_per_entity_and_cut_off():
    sql = runtime().features_query("p.d.entities", ["customer_id", "snapshot_date", "churned"], FEATURES_FROM, "p")
    assert "FROM `p.d.entities`" in sql and "SELECT DISTINCT `customer_id`, CAST(`snapshot_date` AS TIMESTAMP)" in sql
    # each source: its own keys and time column, values at or before the cut-off, the newest one
    assert "JOIN `p.feats.customer_profile` AS f ON f.`customer_id` = k.`customer_id` AND " \
           "CAST(f.`feature_timestamp` AS TIMESTAMP) <= k.__pw_cutoff" in sql
    assert "JOIN `other-project.feats.support` AS f ON f.`cust` = k.`customer_id` AND " \
           "CAST(f.`as_of` AS TIMESTAMP) <= k.__pw_cutoff" in sql
    assert sql.count("QUALIFY ROW_NUMBER() OVER (PARTITION BY k.`customer_id`, k.__pw_cutoff ORDER BY") == 2
    # every entity row kept, joined back on (entity, cut-off)
    assert "LEFT JOIN f0 ON f0.`customer_id` = entity.`customer_id` AND f0.__pw_cutoff = " \
           "CAST(entity.`snapshot_date` AS TIMESTAMP)" in sql
    assert "f1.`num_support_tickets`" in sql and "__pw_hit_1" in sql


def test_scoring_without_the_cut_off_column_uses_the_latest_values():
    sql = runtime().features_query("p.d.to_score", ["customer_id"], FEATURES_FROM, "p")
    assert "CURRENT_TIMESTAMP() AS __pw_cutoff" in sql and "f0.__pw_cutoff = CURRENT_TIMESTAMP()" in sql


def test_the_query_refuses_a_missing_key_or_a_clash():
    rt = runtime()
    with pytest.raises(ValueError, match="has no column customer_id"):
        rt.features_query("p.d.e", ["id", "snapshot_date"], FEATURES_FROM, "p")
    with pytest.raises(ValueError, match="already has tenure_months"):
        rt.features_query("p.d.e", ["customer_id", "snapshot_date", "tenure_months"], FEATURES_FROM, "p")
    spec = copy.deepcopy(FEATURES_FROM)
    spec["sources"][0]["prefix"] = "profile_"
    sql = rt.features_query("p.d.e", ["customer_id", "snapshot_date", "tenure_months"], spec, "p")
    assert "f.`tenure_months` AS `profile_tenure_months`" in sql


def test_read_source_records_what_it_read(capsys, monkeypatch):
    pd = pytest.importorskip("pandas")
    rt = runtime()

    class Job:
        job_id, started = "job-1", None

        def __init__(self, sql):
            self.sql = sql

        def to_dataframe(self, **_):
            return pd.DataFrame({"customer_id": ["a", "b"], "snapshot_date": ["2026-01-31", "2026-02-28"],
                                 "tenure_months": [3, None], "monthly_charges": [9.5, None],
                                 "num_support_tickets": [1, 2], "__pw_hit_0": [True, False],
                                 "__pw_hit_1": [True, True]})

    class Client:
        def get_table(self, ref):
            cols = ["customer_id", "snapshot_date", "churned"]
            return type("T", (), {"schema": [type("F", (), {"name": c})() for c in cols]})()

        def query(self, sql):
            self.sql = sql
            return Job(sql)

    client = Client()
    monkeypatch.setattr(rt, "bigquery_client", lambda project, ml_config=None: client)
    cfg = {"usecase": "churn", "environment": "dev", "platform": {"tenant": "churn"},
           "data": {"features_from": FEATURES_FROM}}
    df = rt.read_source("bq://d.entities", "p", cfg, "run-1")
    assert list(df.columns) == ["customer_id", "snapshot_date", "tenure_months", "monthly_charges",
                                "num_support_tickets"]
    lines = [json.loads(x) for x in capsys.readouterr().out.strip().splitlines()]
    assert "1 of 2 entity rows have no value in p.feats.customer_profile" in lines[0]["message"]
    event = lines[-1]["pipewright_event"]
    assert event["event_type"] == "data.features.resolved" and event["run_id"] == "run-1"
    assert event["data"]["cutoff"] == "snapshot_date" and event["data"]["query_job"] == "job-1"
    assert event["data"]["sources"][0] == {"table": "p.feats.customer_profile", "time_column": "feature_timestamp",
                                           "features": ["tenure_months", "monthly_charges"],
                                           "rows_matched": 1, "rows": 2}
    with pytest.raises(ValueError, match="the entity table, a bq:// table"):
        rt.read_source("gs://b/x.csv", "p", cfg)


# ------------------------------------------------------------------- doctor --

def test_doctor_checks_each_feature_table(repo):
    from pipewright.checks import doctor

    dest, env = scaffold(repo)
    uc = resolve_usecase(dest / "usecase.yaml", env)
    schemas = {
        "churn_feats_customer_profile": [("customer_id", "STRING"), ("feature_timestamp", "TIMESTAMP"),
                                         ("tenure_months", "INT64"), ("monthly_charges", "FLOAT64"),
                                         ("total_charges", "STRING"), ("contract_length_months", "INT64"),
                                         ("num_products", "INT64"), ("has_paperless_billing", "INT64")],
        "churn_feats_support_activity": [("customer_id", "STRING"), ("feature_timestamp", "TIMESTAMP")],
    }

    class Gcp:
        def get(self, url, **_):
            fields = [{"name": n, "type": t} for n, t in schemas[url.rsplit("/", 1)[-1]]]
            return type("R", (), {"ok": True, "status": 200, "body": {"type": "TABLE", "schema": {"fields": fields}}})()

    report = doctor.Report()
    doctor._check_feature_tables(uc, env, Gcp(), report)
    text = report.render()
    assert "total_charges is STRING, the contract says float" in text
    assert "no column num_support_tickets, avg_monthly_usage_gb" in text
    assert report.failed == 1 and report.warnings == 1


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_the_example_compiles(repo):
    from pipewright.build.compile import check_parameters, compile_all

    dest, env = scaffold(repo)
    uc = resolve_usecase(dest / "usecase.yaml", env)
    infos = compile_all(uc)
    assert check_parameters(uc, infos) == ([], [])
    assert "run_id" in infos["inference"].parameters
