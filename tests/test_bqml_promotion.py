"""BigQuery ML promotion by files (phase 2): parity, the imported model's SQL, scoring an imported model."""
from types import SimpleNamespace

from pipewright.bqml import promotion
from pipewright.bqml.steps import bqml_runtime as rt

ML = {"usecase": "churn-bqml", "platform": {"bigquery_dataset": "pw_churn"},
      "data": {"label": {"column": "churned", "task": "binary", "positive": 1}},
      "inference": {"decision_threshold": 0.5}}
UC = SimpleNamespace(ml_config=lambda: ML)


def doc(task="binary"):
    return {"task": task, "expected": promotion._SCORE[task], "columns": {"tenure": "INTEGER", "plan": "STRING"},
            "rows": [{"instance": {"tenure": 1, "plan": "a"}, "expected": 0.62759, "has_null": False},
                     {"instance": {"tenure": None, "plan": "b"}, "expected": 0.1, "has_null": True}]}


def test_parity_compares_every_row_within_the_tolerance():
    ok = promotion.compare(doc(), [{"i": 0, "_pw_score": 0.62759 + 1e-9}, {"i": 1, "_pw_score": 0.1}])
    assert ok == {"rows": 2, "max_difference": ok["max_difference"], "failed": 0, "failed_with_missing": 0}
    bad = promotion.compare(doc(), [{"i": 0, "_pw_score": 0.62759}, {"i": 1, "_pw_score": 0.17}])
    assert bad["failed"] == 1 and bad["failed_with_missing"] == 1          # the row with a missing input
    assert promotion.compare(doc(), [{"i": 0, "_pw_score": 0.62759}])["failed"] == 1   # a row not scored fails
    labels = doc("multiclass")
    labels["rows"][0]["expected"], labels["rows"][1]["expected"] = "gold", "silver"
    assert promotion.compare(labels, [{"i": 0, "_pw_label": "gold"}, {"i": 1, "_pw_label": "gold"}])["failed"] == 1


def test_the_parity_query_scores_the_given_rows_through_the_transform_model():
    sql = promotion.parity_sql(UC, doc(), "p.d.churn_bqml_r1", "p.d.churn_bqml_r1_transform")
    assert "SAFE_CAST(JSON_VALUE(j, '$.tenure') AS INT64) AS `tenure`" in sql
    assert "ML.PREDICT(MODEL `p.d.churn_bqml_r1_transform`, (SELECT r AS _pw_raw, r.* FROM (SELECT * FROM given)" in sql
    assert sql.index("p.d.churn_bqml_r1_transform") > sql.index("MODEL `p.d.churn_bqml_r1`")   # transform inside
    assert "UNNEST(p.`churned_probs`)" in sql and ">= 0.5" in sql


def test_an_imported_model_scores_with_the_rows_own_columns():
    sql = rt.predict_sql(ML, "p.d.m", "p.d.rows", "p.d.out", "TRUE", {"run-id": "r1"}, False,
                         parts={"transform": None})
    assert "SELECT p._pw_raw.*," in sql and "ML.PREDICT(MODEL `p.d.m`, (SELECT r AS _pw_raw, r.* FROM (" in sql
    assert "SELECT * EXCEPT (_pw_test) FROM `p.d.rows` WHERE TRUE" in sql
    native = rt.predict_sql(ML, "p.d.m", "p.d.rows", "p.d.out", "TRUE", {"run-id": "r1"}, False)
    assert "_pw_raw" not in native and "predicted_churned_probs" in native


def test_model_parts_tells_imported_from_trained():
    class Client:
        def __init__(self, kind, transform):
            self.kind, self.transform = kind, transform

        def get_model(self, ref):
            if ref.endswith("_transform") and not self.transform:
                raise LookupError(ref)
            return SimpleNamespace(model_type=self.kind)

    assert rt.model_parts(SimpleNamespace(client=Client("LOGISTIC_REGRESSION", False)), "p.d.m") is None
    assert rt.model_parts(SimpleNamespace(client=Client("TENSORFLOW", True)), "p.d.m") == {
        "transform": "p.d.m_transform"}
    assert rt.model_parts(SimpleNamespace(client=Client("TENSORFLOW", False)), "p.d.m") == {"transform": None}


def test_tree_models_are_not_promoted_by_files():
    tree = SimpleNamespace(ml={"bqml": {"model_type": "boosted_tree_classifier"}})
    assert "promoted by retraining" in promotion.check_exportable(tree)
    assert promotion.check_exportable(SimpleNamespace(ml={"bqml": {"model_type": "logistic_reg"}})) is None
