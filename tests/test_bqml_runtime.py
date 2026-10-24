"""The BigQuery ML steps' SQL and helpers (src/pipewright/bqml/steps), without BigQuery."""
import sys

import pytest

from pipewright import bqml
from tests.test_bqml_config import bqml_repo, resolve

sys.path.insert(0, str(bqml.STEPS_DIR))
import bqml_runtime as rt  # noqa: E402


@pytest.fixture
def cfg(repo):
    return resolve(bqml_repo(repo, options={"max_iterations": 30}, explain=True)).ml_config()


def test_templates_fill_every_placeholder_once():
    sql = rt.render("register.sql", model="p.d.m_r1", registry_id="churn-classifier")
    assert "ALTER MODEL `p.d.m_r1` SET OPTIONS (vertex_ai_model_id = 'churn-classifier')" in sql
    with pytest.raises(ValueError, match="no value for registry_id"):
        rt.render("register.sql", model="p.d.m")
    # a value holding braces (a team's SQL) is not read as a placeholder
    sql = rt.render("rows.sql", rows_table="p.d.t", labels="", run_id="r1", test_flag="FALSE",
                    prepared="SELECT JSON_VALUE(j, '$.a') AS a, '{model}' AS b FROM x")
    assert "'{model}' AS b" in sql


def test_names_per_run(cfg):
    assert rt.model_ref("p", cfg, "r1F4") == "p.churn.churn_bqml_r1f4"
    assert rt.table_ref("p", cfg, "r1", "test") == "p.churn.churn_bqml_test_r1"
    assert rt.record_uri(cfg, "r1") == "gs://acme-ml-dev-artifacts/pipeline-root/churn/churn/bqml/r1/"


def test_create_model_on_the_training_rows_only(cfg):
    columns = ["customer_id", "snapshot_date", "tenure_months", "monthly_charges", "contract_length_months",
               "churned", "_pw_test"]
    sql = rt.create_model_sql(cfg, "p.d.m", "p.d.rows", columns)
    assert "CREATE OR REPLACE MODEL `p.d.m`" in sql and "WHERE NOT _pw_test" in sql
    assert ("OPTIONS (model_type = 'boosted_tree_classifier', input_label_cols = ['churned'], "
            "data_split_method = 'NO_SPLIT', enable_global_explain = TRUE, max_iterations = 30)") in sql
    # the features (customer_id is personal data, snapshot_date the split's time column) and the label
    assert "SELECT `tenure_months`, `monthly_charges`, `contract_length_months`, `churned`" in sql
    assert "TRANSFORM" not in sql


def test_transform_and_prepare_shape_the_sql(cfg):
    cfg["bqml"]["transform"] = {"sql": "ML.STANDARD_SCALER(tenure_months) OVER () AS tenure_months, churned"}
    sql = rt.create_model_sql(cfg, "p.d.m", "p.d.rows", ["tenure_months", "churned", "_pw_test"])
    assert "TRANSFORM (\nML.STANDARD_SCALER(tenure_months) OVER () AS tenure_months, churned\n)" in sql
    assert "SELECT * EXCEPT (_pw_test)" in sql
    cfg["bqml"]["prepare"] = {"sql": "SELECT *, tenure_months * 2 AS tenure2 FROM {rows}"}
    assert rt.prepared_sql(cfg, "SELECT * FROM `p.d.e`") == (
        "SELECT *, tenure_months * 2 AS tenure2 FROM (SELECT * FROM `p.d.e`)")
    # with prepare.sql the features are the prepared columns, less the label, keys, cut-off and personal data
    assert rt.feature_columns(cfg, ["customer_id", "snapshot_date", "tenure_months", "tenure2", "churned",
                                    "_pw_test"]) == ["tenure_months", "tenure2"]


def test_split_is_held_out_by_key_or_by_time(cfg):
    flag = rt.split_flag(cfg, rt.split_keys(cfg))
    assert "CAST(prepared.`snapshot_date` AS TIMESTAMP) >=" in flag and "[OFFSET(800)]" in flag
    cfg["data"]["split"] = {"strategy": "random", "test_fraction": 0.25, "seed": 7}
    flag = rt.split_flag(cfg, rt.split_keys(cfg))
    assert flag == ("MOD(ABS(FARM_FINGERPRINT(CONCAT('7', TO_JSON_STRING(STRUCT(`customer_id`))))), 1000000) "
                    "< 250000")


def test_predictions_per_task(cfg):
    sql = rt.predict_sql(cfg, "p.d.m", "p.d.rows", "p.d.test", "_pw_test", {"run-id": "r1"}, True)
    assert "SELECT * EXCEPT (`predicted_churned`, `predicted_churned_probs`)" in sql
    assert "WHERE CAST(p.label AS STRING) = '1') AS _pw_score" in sql
    assert "FROM ML.PREDICT(MODEL `p.d.m`, (SELECT * EXCEPT (_pw_test) FROM `p.d.rows` WHERE _pw_test), " \
           "STRUCT(0.5 AS threshold))" in sql
    assert "OPTIONS (labels = [('run-id', 'r1')]" in sql
    cfg["data"]["label"]["task"] = "regression"
    sql = rt.predict_sql(cfg, "p.d.m", "p.d.rows", "p.d.s", "TRUE", {}, False)
    assert "CAST(`predicted_churned` AS FLOAT64) AS _pw_prediction" in sql and "threshold" not in sql
    cfg["data"]["label"]["task"] = "multiclass"
    assert "_pw_true_prob" in rt.predict_sql(cfg, "p.d.m", "p.d.rows", "p.d.t", "_pw_test", {}, True)
    assert "_pw_true_prob" not in rt.predict_sql(cfg, "p.d.m", "p.d.rows", "p.d.t", "TRUE", {}, False)


def test_metrics_sql_per_task_and_slice(cfg):
    sql = rt.metrics_sql(cfg, "p.d.test")
    assert "'overall' AS g" in sql and "COUNTIF(p >= 0.5 AND y = 1) AS tp" in sql and "AS calibration_error" in sql
    assert "CAST(`contract_length_months` AS STRING) AS g" in rt.metrics_sql(cfg, "p.d.test", "contract_length_months")
    for task, metric in (("multiclass", "f1_macro"), ("regression", "median_absolute_error")):
        cfg["data"]["label"]["task"] = task
        assert f"AS {metric}" in rt.metrics_sql(cfg, "p.d.test")
    assert rt.shape_metrics({"auc": 0.81234567, "f1": None, "brier": float("nan")}, ["auc", "f1", "brier"]) == {
        "auc": 0.812346, "f1": None, "brier": None}


def test_contract_checks_count_what_the_container_path_checks(cfg):
    columns = {"customer_id", "snapshot_date", "tenure_months", "monthly_charges", "contract_length_months",
               "churned"}
    checks, rules = rt.contract_checks(cfg, columns, "training")
    assert "COUNTIF(`tenure_months` < 0) AS _min_2" in checks
    assert "CAST(`contract_length_months` AS STRING) NOT IN ('1', '12', '24')" in checks
    assert "COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT(`customer_id`))) AS _duplicates" in checks
    row = {"_rows": 400, "_min_2": 3, "_duplicates": 0, "_nulls_0": 0}
    errors = rt.contract_errors(row, rules, cfg["data"]["contract"], "training")
    assert errors == ["only 400 rows, need at least 500", "tenure_months: values below 0 (3 rows)"]
    _, rules = rt.contract_checks(cfg, columns - {"churned"}, "inference")      # scoring rows have no label
    assert rt.contract_errors({"_rows": 10}, rules, cfg["data"]["contract"], "inference") == []
    _, rules = rt.contract_checks(cfg, columns - {"tenure_months"}, "training")
    assert "missing column tenure_months" in rt.contract_errors({"_rows": 600}, rules, cfg["data"]["contract"],
                                                                "training")


def test_psi_matches_the_container_path():
    pytest.importorskip("pandas")
    sys.path.insert(0, str(bqml.HERE.parents[2] / "examples" / "churn" / "components"))
    import mlkit
    import numpy as np

    rng = np.random.default_rng(3)
    train, now = rng.integers(1, 5, 4000).astype(float), rng.integers(1, 6, 1000).astype(float)
    edges = rt.baseline({"distinct_values": 4, "smallest": sorted(set(train)), "quantiles": []})
    assert edges == mlkit.psi_baseline(train)["edges"]

    def counts(values):
        buckets = np.searchsorted(np.asarray(edges), values, side="right")
        return [{"bucket": int(b), "n": int((buckets == b).sum())} for b in set(buckets.tolist())]

    expected = rt.fractions(counts(train), edges)
    assert expected == mlkit.psi_baseline(train)["fractions"]
    assert rt.psi(expected, rt.fractions(counts(now), edges)) == pytest.approx(
        mlkit.psi(mlkit.psi_baseline(train), now), abs=1e-6)
    assert rt.class_psi({"a": 0.5, "b": 0.5}, {"a": 0.5, "b": 0.5}) < 1e-9
    assert rt.class_psi({"a": 0.5, "b": 0.5}, {"a": 0.4, "b": 0.4, "c": 0.2}) > 0.2


def test_option_values_and_labels_as_sql():
    assert rt.literal("it's") == "'it\\'s'" and rt.literal(True) == "TRUE" and rt.literal([1, "a"]) == "[1, 'a']"
    assert rt.label_pairs({"pipewright-usecase": "Churn Feats", "x": None}) == "('pipewright-usecase', 'churn-feats')"
    assert rt.is_query("-- a comment\nWITH t AS (SELECT 1) SELECT * FROM t") and not rt.is_query("CREATE MODEL m")
    with pytest.raises(ValueError):
        rt.q("bad name")


def test_both_pipelines_compile_on_one_image(repo, tmp_path):
    pytest.importorskip("kfp")
    from pipewright.build.compile import compile_all

    uc = resolve(bqml_repo(repo))
    image = "europe-west2-docker.pkg.dev/acme/img/churn@sha256:" + "0" * 64
    infos = compile_all(uc, image, tmp_path)
    assert {n: i.name for n, i in infos.items()} == {"training": "bqml-training", "inference": "bqml-scoring"}
    for info in infos.values():
        assert info.images == {image}
        assert set(info.parameters) == {"project", "region", "source_uri", "run_id", "ml_config"}


def test_performance_joins_this_versions_latest_scores_with_outcomes(cfg):
    cfg["monitoring"]["performance"] = {"enabled": True, "metric": "auc", "min": 0.75,
                                        "labels": {"table": "outcomes", "key": "customer_id", "label": "churned_30d"}}
    cfg["inference"]["output_table"] = "scores"
    sql = rt.performance_sql(cfg, "p", "7", "p.churn.perf", {"run-id": "r1"})
    assert "SELECT lb.`churned_30d` AS `churned`, pr.predicted_label AS _pw_label, pr.score AS _pw_score" in sql
    assert "FROM `p.churn.scores` AS pr\nJOIN `p.churn.outcomes` AS lb USING (`customer_id`)" in sql
    assert "WHERE pr.model_version_id = '7'" in sql and "ORDER BY pr.scored_at DESC) = 1" in sql
    with pytest.raises(ValueError):
        rt.performance_sql(cfg, "p", "7' OR '1'='1", "p.churn.perf", {})        # versions are numbers
    assert rt.performance_trigger(0.70, "auc", 0.75) and not rt.performance_trigger(0.80, "auc", 0.75)
    assert rt.performance_trigger(3.0, "rmse", 2.0) and not rt.performance_trigger(None, "auc", 0.75)


def test_lower_is_better_matches_the_core():
    from pipewright.config.ml import LOWER_IS_BETTER

    assert rt.LOWER_IS_BETTER == LOWER_IS_BETTER


def test_retrain_proposal_has_what_the_container_path_sends(cfg):
    summary = {"rows": 10, "feature_psi": {"a": 0.3, "b": 0.1}, "drifted_features": ["a"], "prediction_psi": 0.05,
               "performance": {"auc": 0.7}, "labelled_rows": 80}
    body = rt.proposal(cfg, "r2", ["drift", "performance"], summary, {"rows": 10}, "3", "projects/p/models/m", [])
    assert body["kind"] == "retrain.proposal" and body["usecase"] == "churn" and body["run_id"] == "r2"
    obs = body["observation"]
    assert obs["triggers"] == ["drift", "performance"] and obs["max_feature_psi"] == 0.3
    assert obs["performance"] == {"auc": 0.7} and obs["engine"] == "bqml"
    assert body["champion"] == {"version": "3", "resource_name": "projects/p/models/m"}


def test_scoring_proposes_and_records(repo, tmp_path):
    pytest.importorskip("kfp")
    from pipewright.build.compile import compile_all

    uc = resolve(bqml_repo(repo))
    infos = compile_all(uc, "europe-west2-docker.pkg.dev/acme/img/churn@sha256:" + "0" * 64, tmp_path)
    assert {"check-drift-and-performance", "propose-retraining", "record-monitoring"} <= set(infos["inference"].tasks)
