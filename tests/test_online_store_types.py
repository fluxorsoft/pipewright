"""FR-104: model.serving.online_store resolves to a lookup of the chosen type; Bigtable use cases are unchanged."""
import shutil

import pytest
import yaml

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.online import config as online_config
from pipewright.scaffold.new_usecase import new_usecase

INSTANCE = "acme-ml-dev:europe-west2:features"


def store_repo(repo, online_store, env_store=None, change_spec=None):
    repo.manifests["dev"].setdefault("features", {})["online_store"] = env_store if env_store is not None else {
        "types": ["bigtable", "bigquery", "cloudsql", "http"],
        "bigquery": {"bi_engine_gib": 1},
        "cloudsql": {"instance": INSTANCE, "private_ip": "10.41.0.3", "database": "features",
                     "settings": {"tier": "db-custom-1-3840"}},
        "lookup": {"defaults": {"min_instances": 1}, "limits": {"max_instances": 10}}}
    repo.write()
    shutil.rmtree(repo.root / "usecases" / "churn-feats", ignore_errors=True)
    dest = new_usecase(repo.root, "churn-feats", "churn", "team-feats", example="features")
    uc = dest / "usecase.yaml"
    spec = yaml.safe_load(uc.read_text(encoding="utf-8"))
    serving = spec["spec"].setdefault("model", {}).setdefault("serving", {})
    serving["features"] = "online"
    serving["online_store"] = online_store
    if change_spec:
        change_spec(spec)
    uc.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    return lambda: resolve_usecase(uc, load_environment(repo.root, "dev"))


def test_cloudsql_with_pipewrights_lookup_service(repo):
    uc = store_repo(repo, {"type": "cloudsql", "cloudsql": {"schema": "team_churn"}})()
    o = uc.ml["online_features"]
    assert o["type"] == "cloudsql" and o["key"] == "customer_id" and o["provider"] == "pipewright"
    assert o["instance"] == INSTANCE and o["schema"] == "team_churn" and o["table"] == "pw_online_churn_feats"
    assert o["service"] == "pwl-churn-churn-feats"
    assert o["lookup"]["min_instances"] == 1                       # the environment's default
    assert o["lookup"]["cpu_boost"] is True and o["lookup"]["billing"] == "request"
    assert set(uc.ml["features"]["include"]) == set(o["features"])
    assert "export" not in o                                       # off by default
    assert uc.ml_config()["online_features"] == o


def test_cloudsql_sizes_are_checked(repo):
    with pytest.raises(ConfigError, match=r"lookup.cpu 3: Cloud Run takes 1, 2, 4, 6 or 8"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"cpu": 3}})()


def test_the_platform_limits_and_floor_apply(repo):
    env_store = {"types": ["cloudsql"], "cloudsql": {"instance": INSTANCE},
                 "lookup": {"limits": {"min_min_instances": 1, "max_instances": 3}}}
    with pytest.raises(ConfigError, match="keeps at least 1 warm"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"min_instances": 0}}, env_store)()
    with pytest.raises(ConfigError, match="max_instances 5: this environment allows at most 3"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"min_instances": 1}}, env_store)()


def test_provider_own_needs_a_reader_and_no_sizes(repo):
    with pytest.raises(ConfigError, match="name the service account"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"provider": "own"}})()
    with pytest.raises(ConfigError, match="cpu size Pipewright's lookup service"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"provider": "own", "cpu": 2,
                                                          "reader": "reader-a1@acme-ml-dev.iam.gserviceaccount.com"}})()
    uc = store_repo(repo, {"type": "cloudsql", "lookup": {"provider": "own",
                                                         "reader": "reader-a1@acme-ml-dev.iam.gserviceaccount.com"}})()
    o = uc.ml["online_features"]
    assert o["provider"] == "own" and "service" not in o and o["reader"].startswith("reader-a1@")


def test_export_is_a_toggle_with_its_place(repo):
    uc = store_repo(repo, {"type": "cloudsql", "export": {"enabled": True}})()
    exp = uc.ml["online_features"]["export"]
    assert exp["uri"] == "gs://acme-ml-dev-artifacts/online-export/churn/churn-feats" and exp["keep"] == 3
    with pytest.raises(ConfigError, match="a tenant writes only under"):
        store_repo(repo, {"type": "cloudsql", "export": {
            "enabled": True, "uri": "gs://acme-ml-dev-artifacts/online-export/other/x"}})()


def test_bigquery_needs_bi_engine(repo):
    uc = store_repo(repo, {"type": "bigquery"})()
    o = uc.ml["online_features"]
    assert o["type"] == "bigquery" and o["table"].endswith(".pw_online_churn_feats")
    with pytest.raises(ConfigError, match="has no BI Engine reservation"):
        store_repo(repo, {"type": "bigquery"}, {"types": ["bigquery"]})()


def test_a_type_the_environment_does_not_offer(repo):
    with pytest.raises(ConfigError, match="dev offers bigtable"):
        store_repo(repo, {"type": "cloudsql"}, {})()


def test_http(repo):
    uc = store_repo(repo, {"type": "http", "http": {"url": "https://lookup.example.com"}})()
    o = uc.ml["online_features"]
    assert o == {**o, "type": "http", "url": "https://lookup.example.com", "audience": "https://lookup.example.com"}


def test_online_store_needs_online_features(repo):
    def request(spec):
        spec["spec"]["model"]["serving"]["features"] = "request"
    with pytest.raises(ConfigError, match="needs model.serving.features online"):
        store_repo(repo, {"type": "cloudsql"}, change_spec=request)()


def test_service_names_fit_cloud_run():
    assert online_config.service_name("churn", "churn-feats") == "pwl-churn-churn-feats"
    long = online_config.service_name("t" * 30, "u" * 40)
    assert len(long) <= 49 and long.startswith("pwl-")
