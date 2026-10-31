"""FR-68: the entity's cut-off and key columns are never a feature by default in BigQuery ML; the container path
keeps 1.x's default and validate warns (Pipewright 1.2)."""
from tests.test_bqml_config import bqml_repo, resolve
from tests.test_config_ml import ml_repo
from tests.test_config_ml import resolve as resolve_container

FEATURES_FROM = {"entity": {"columns": ["customer_id"], "time_column": "snapshot_date"},
                 "sources": [{"table": "bq://acme-ml-dev.features.activity",
                              "features": ["tenure_months", "monthly_charges"]}]}


def with_entity(repo, split=None):
    spec = repo.usecase["spec"]
    spec["data"]["features_from"] = FEATURES_FROM
    spec["triggers"].pop("landing", None)                     # an entity table is read with a query, not delivered
    spec["data"]["split"] = split or {"strategy": "random", "test_fraction": 0.2}
    spec.setdefault("data_protection", {})["personal_data_columns"] = []
    return repo


def test_bigquery_ml_leaves_the_cut_off_out_and_warns_about_the_key(repo):
    # The ebook's Chapter 8: customer-segments (k-means, random split) got snapshot_date as a feature, and the
    # evaluation failed: `Invalid cast from DATE to FLOAT64`. A key column trained in 1.1, so it stays (1.x
    # stability) and validate warns.
    uc = resolve(with_entity(bqml_repo(repo)))
    include = uc.ml["features"]["include"]
    assert "snapshot_date" not in include and "customer_id" in include and "tenure_months" in include
    assert any("customer_id" in w and "snapshot_date" not in w and "FR-68" in w for w in uc.warnings)


def test_the_cut_off_leaves_the_default_drift_features_too(repo):
    # Found live: drift on, the features left to the default; evaluation profiled snapshot_date for the drift
    # baseline and failed the same way.
    bqml_repo(repo)
    spec = with_entity(repo).usecase["spec"]
    spec["monitoring"] = {"drift": {"enabled": True}, "output_table": "monitoring"}
    spec["outputs"]["bigquery"]["tables"]["monitoring"] = "schemas/scores.json"
    drift = resolve(repo).ml["monitoring"]["drift"]["features"]
    assert "snapshot_date" not in drift and "tenure_months" in drift
    repo.usecase["spec"]["monitoring"]["drift"]["features"] = ["tenure_months"]     # listed: left alone
    assert resolve(repo).ml["monitoring"]["drift"]["features"] == ["tenure_months"]


def test_the_container_path_keeps_the_default_and_warns(repo):
    ml_repo(repo)
    uc = resolve_container(with_entity(repo))
    assert "snapshot_date" in uc.ml["features"]["include"]                       # 1.x's default, unchanged
    assert any("snapshot_date" in w and "features.exclude" in w and "FR-68" in w for w in uc.warnings)


def test_a_team_that_lists_its_features_is_left_alone(repo):
    ml_repo(repo)
    with_entity(repo).usecase["spec"]["features"] = {"include": ["tenure_months", "snapshot_date"]}
    uc = resolve_container(repo)
    assert uc.ml["features"]["include"] == ["tenure_months", "snapshot_date"] and not uc.warnings
    bqml_repo(repo)
    with_entity(repo).usecase["spec"]["features"] = {"include": ["tenure_months", "snapshot_date"]}
    assert resolve(repo).ml["features"]["include"] == ["tenure_months", "snapshot_date"]


def test_a_time_split_on_the_cut_off_already_leaves_it_out(repo):
    ml_repo(repo)
    uc = resolve_container(with_entity(repo, {"strategy": "time", "time_column": "snapshot_date"}))
    assert "snapshot_date" not in uc.ml["features"]["include"]
    assert not any("snapshot_date" in w for w in uc.warnings if "FR-68" in w)
