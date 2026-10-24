"""BigQuery ML tier 1c: k-means (clustering) and PCA (decomposition), no label (Pipewright 1.1)."""
from types import SimpleNamespace

from pipewright.bqml.steps import bqml_runtime as rt
from tests.test_bqml_config import bqml_repo, problems, resolve


def unsupervised(repo, task, model_type, **extra):
    bqml_repo(repo, model_type=model_type, options=extra.pop("options", {}))
    spec = repo.usecase["spec"]
    spec["data"]["label"] = {"task": task}
    spec["data"]["split"] = {"strategy": "random", "test_fraction": 0.2}
    spec.update(extra)
    return repo


def test_kmeans_and_pca_resolve_without_a_label(repo):
    uc = resolve(unsupervised(repo, "clustering", "kmeans", options={"num_clusters": 4}))
    ev = uc.ml["evaluation"]
    assert ev["primary_metric"] == "davies_bouldin_index" and "davies_bouldin_index" in ev["lower_is_better"]
    assert uc.ml["bqml"]["model_type"] == "kmeans" and not uc.ml["bqml"]["explain"]
    uc = resolve(unsupervised(repo, "decomposition", "pca", options={"num_principal_components": 3}))
    assert uc.ml["evaluation"]["primary_metric"] == "total_explained_variance_ratio"


def test_what_needs_a_label_is_refused(repo):
    r = unsupervised(repo, "decomposition", "pca", monitoring={"prediction_drift": {"enabled": True},
                                                               "output_table": "monitoring_out"})
    assert "PCA has no prediction" in problems(r)
    r = unsupervised(repo, "clustering", "kmeans")
    r.usecase["spec"]["data"]["label"]["column"] = "churned"
    assert "a clustering model has no label" in problems(r)
    r = unsupervised(repo, "clustering", "logistic_reg")
    assert "does not fit data.label.task clustering" in problems(r)


ML = {"usecase": "segments", "platform": {"bigquery_dataset": "pw"},
      "data": {"label": {"task": "clustering"}, "split": {"strategy": "random"}},
      "features": {"include": ["tenure", "spend"], "exclude": []},
      "bqml": {"model_type": "kmeans", "options": {"num_clusters": 4}}, "inference": {},
      "evaluation": {"metrics": ["davies_bouldin_index", "mean_squared_distance"]}}


def test_kmeans_trains_without_a_label_and_scores_clusters():
    sql = rt.create_model_sql(ML, "p.d.m", "p.d.rows", ["tenure", "spend", "_pw_test"])
    assert "input_label_cols" not in sql and "model_type = 'kmeans'" in sql and "num_clusters = 4" in sql
    assert "SELECT `tenure`, `spend`" in sql and "WHERE NOT _pw_test" in sql
    pred = rt.predict_sql(ML, "p.d.m", "p.d.rows", "p.d.out", "TRUE", {}, False)
    assert "EXCEPT (CENTROID_ID, NEAREST_CENTROIDS_DISTANCE)" in pred
    assert "CAST(CENTROID_ID AS STRING) AS _pw_label" in pred
    pca = {**ML, "data": {**ML["data"], "label": {"task": "decomposition"}}}
    assert "SELECT *\nFROM ML.PREDICT(MODEL `p.d.m`" in rt.predict_sql(pca, "p.d.m", "p.d.rows", "p.d.out", "TRUE",
                                                                       {}, False)


def test_unsupervised_evaluation_uses_ml_evaluate():
    calls = []

    class Jobs:
        def run(self, sql, what):
            calls.append(sql)
            return [{"n": 200}] if "COUNTIF" in sql else [{"davies_bouldin_index": 1.234567891,
                                                          "mean_squared_distance": 2.5}]

    got = rt.evaluate_unsupervised(Jobs(), ML, "p.d.m", "p.d.rows")
    assert got == {"metrics": {"davies_bouldin_index": 1.234568, "mean_squared_distance": 2.5}, "rows": 200,
                   "slices": {}}
    assert "ML.EVALUATE(MODEL `p.d.m`, (SELECT * EXCEPT (_pw_test) FROM `p.d.rows` WHERE _pw_test))" in calls[0]
    pca = {**ML, "data": {**ML["data"], "label": {"task": "decomposition"}},
           "evaluation": {"metrics": ["total_explained_variance_ratio"]}}
    calls.clear()
    rt.evaluate_unsupervised(SimpleNamespace(run=Jobs().run), pca, "p.d.m", "p.d.rows")
    assert calls[0].strip().endswith("ML.EVALUATE(MODEL `p.d.m`)")


def test_unsupervised_tasks_are_bigquery_ml_only(repo):
    from tests.test_config_ml import ml_repo
    from tests.test_config_ml import problems as container_problems

    r = ml_repo(repo)
    r.usecase["spec"]["data"]["label"] = {"task": "clustering"}
    assert "BigQuery ML only" in container_problems(r)
