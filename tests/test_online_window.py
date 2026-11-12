"""The online-window reader (FR-98 stage 2, 1.4.3): both log shapes, the caller contract's prediction ID,
de-duplication, and insufficient_data."""

import json
from types import SimpleNamespace

from pipewright.build.templates import pipewright_runtime as rt

FEATURES = ["a", "b"]
ML = {"usecase": "churn", "environment": "dev", "monitoring": {"online": {"window_hours": 24, "min_samples": 3}},
      "platform": {"project": "p", "region": "r"}}


class Client:
    def __init__(self, columns, rows):
        self.columns, self.rows, self.sql = columns, rows, None

    def get_table(self, table):
        return SimpleNamespace(schema=[SimpleNamespace(name=c) for c in self.columns])

    def query(self, sql, job_config=None):
        self.sql = sql
        rows = [SimpleNamespace(items=lambda r=r: r.items()) for r in self.rows]
        return SimpleNamespace(result=lambda: rows)


def test_parse_the_caller_contract():
    assert rt.parse_instance([1, 2]) == ([1, 2], None)
    assert rt.parse_instance({"_pw_prediction_id": "p1", "_pw_unit": "h", "row": [1, 2]}) == ([1, 2], "p1")
    assert rt.parse_instance(json.dumps({"instances": [[3, 4]], "_pw_prediction_id": "p2"})) == ([3, 4], "p2")
    assert rt.parse_prediction("[0.7]") == 0.7 and rt.parse_prediction({"error": "x"}) is None


def test_agent_platform_rows_deduplicated_and_enough():
    rows = [{"instance": json.dumps({"_pw_prediction_id": "p1", "row": [1, 2]}), "answer": "0.1"},
            {"instance": json.dumps({"_pw_prediction_id": "p1", "row": [1, 2]}), "answer": "0.1"},   # a retry
            {"instance": "[3, 4]", "answer": "0.2"}, {"instance": "[5, 6]", "answer": "0.3"},
            {"instance": "[7]", "answer": "0.4"}]                                                      # wrong shape
    c = Client(["request_payload", "response_payload", "deployed_model_id"], rows)
    found, summary = rt.online_window(ML, "p", "bq://acme-ml-dev.d.churn_online_log", FEATURES, "3", client=c,
                                      deployed_model="dm1")
    assert [r["prediction"] for r in found] == [0.1, 0.2, 0.3] and found[0]["_pw_prediction_id"] == "p1"
    assert summary["source"] == "agent_platform" and summary["skipped"] == 1 and not summary["insufficient_data"]
    assert "UNNEST(request_payload) AS req WITH OFFSET" in c.sql


def test_kserve_records_pair_request_and_answer_and_insufficient_data():
    rows = [{"request": json.dumps({"instances": [[1, 2], [3, 4]]}),
             "response": json.dumps({"predictions": [0.1, 0.2]})}]
    c = Client(["jsonPayload", "timestamp"], rows)
    found, summary = rt.online_window(ML, "p", "bq://acme-ml-dev.d.stdout", FEATURES, "3", client=c, isvc="churn")
    assert len(found) == 2 and summary["source"] == "kserve"
    assert summary["insufficient_data"] is True                   # 2 rows, min_samples 3: never "no drift"


def test_no_capture_table_yet_is_insufficient_data_not_a_crash():
    # 1.4.3 GA QA (live): the scheduled online monitoring of a use case not serving yet failed with a 404
    from google.api_core.exceptions import NotFound

    class Missing(Client):
        def get_table(self, table):
            raise NotFound("Not found: Table pw-ebook:pw_retail.churn_ga_online_log")

    rows, summary = rt.online_window(ML, "p", "bq://acme-ml-dev.d.churn_online_log", FEATURES, "3",
                                     client=Missing([], []))
    assert rows == [] and summary["insufficient_data"] and summary["missing_table"]
