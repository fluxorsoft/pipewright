"""Registration through the launcher for BigQuery ML (FR-100, 1.4.3): the model in the release's dataset, the record
in the release's records folder, ALTER MODEL run by the launcher, labels and the default as the step set them."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from pipewright.gcp.session import Response
from pipewright.models import registration as reg

RUNNER = "retail-runner@acme-ml-dev.iam.gserviceaccount.com"
RECORDS = "gs://acme-artifacts/pipeline-root/retail/bqml/records/"
RECORD_TEXT = json.dumps({"metrics": {"roc_auc": 0.8}}, indent=2)


def contract(set_default="on_approval"):
    ml = {"registry": {"set_default": set_default}, "platform": {"bigquery_dataset": "retail", "kms_key": None},
          "bqml": {"registry_id": "lapse-risk", "records": RECORDS},
          "evaluation": {"primary_metric": "roc_auc", "gates": [{"metric": "roc_auc", "min": 0.7}]}}
    return SimpleNamespace(usecase="lapse-risk", tenant="retail", project="acme-ml-dev", region="europe-west2",
                           environment="dev", runner_service_account=RUNNER, pipeline_root="gs://x/pipeline-root/retail",
                           release_id="r1", static_parameters={"ml_config": ml})


class FakeGcp:
    def __init__(self, existed=True):
        self.existed, self.calls = existed, []

    def get(self, url, **params):
        self.calls.append(("GET", url))
        if url.endswith("/models/lapse-risk"):
            return Response(200, {"name": "m"}) if self.existed else Response(404, {})
        if "/datasets/retail/models/" in url:
            return Response(200, {"trainingRuns": [{"vertexAiModelVersion": "7"}]})
        if "@7" in url:
            return Response(200, {"labels": {"a": "b"}})
        raise AssertionError(url)

    def post(self, url, body, **params):
        self.calls.append(("POST", url, body))
        if url.endswith("/queries"):
            return Response(200, {"jobComplete": True, "jobReference": {"jobId": "j"}})
        if url.endswith(":mergeVersionAliases"):
            return Response(200, {})
        raise AssertionError(url)

    def patch(self, url, body, **params):
        self.calls.append(("PATCH", url, body))
        return Response(200, {})


@pytest.fixture
def store(monkeypatch):
    objects = {("acme-artifacts", "pipeline-root/retail/bqml/records/abc/evaluation.json"): RECORD_TEXT}
    monkeypatch.setattr(reg, "gcs_read", lambda gcp, b, n: (objects.get((b, n)), 1))
    monkeypatch.setattr(reg, "gcs_write", lambda gcp, b, n, text, **kw: objects.__setitem__((b, n), text) or 1)
    return objects


def running(contract, run_id):
    return {"state": "PIPELINE_STATE_RUNNING", "service_account": RUNNER, "usecase": "lapse-risk"}


def body(**extra):
    return {"engine": "bqml", "run_id": "abc", "registry_id": "lapse-risk",
            "bigquery_model": "acme-ml-dev.retail.lapse_risk_abc", "labels": {"status": "candidate"},
            "evaluation": {"uri": f"{RECORDS}abc/evaluation.json",
                           "sha256": hashlib.sha256(RECORD_TEXT.encode()).hexdigest()}, **extra}


def test_the_launcher_runs_alter_model_labels_and_moves_the_default(store):
    gcp = FakeGcp()
    out = reg.Registrations(gcp, "acme-artifacts", dict, running).register(contract(), body(), RUNNER)
    query = next(c for c in gcp.calls if c[0] == "POST" and c[1].endswith("/queries"))[2]["query"]
    assert query == ("ALTER MODEL `acme-ml-dev.retail.lapse_risk_abc` SET OPTIONS "
                     "(vertex_ai_model_id = 'lapse-risk')")
    assert any(c[1].endswith("@7:mergeVersionAliases") for c in gcp.calls if c[0] == "POST")
    labels = [c[2]["labels"] for c in gcp.calls if c[0] == "PATCH"]
    assert labels[0]["registered-by"] == "launcher" and labels[-1]["status"] == "champion"
    assert out["version"] == "7" and out["default"] is True


def test_manual_never_moves_the_default(store):
    gcp = FakeGcp()
    out = reg.Registrations(gcp, "acme-artifacts", dict, running).register(contract("manual"), body(), RUNNER)
    assert not any(c[1].endswith(":mergeVersionAliases") for c in gcp.calls if c[0] == "POST")
    assert out["default"] is False


@pytest.mark.parametrize("extra,expect", [
    ({"bigquery_model": "acme-ml-dev.other.lapse_risk_abc"}, "is not in lapse-risk's dataset"),
    ({"registry_id": "someone-else"}, "is not lapse-risk's"),
    ({"evaluation": {"uri": "gs://acme-artifacts/elsewhere/evaluation.json", "sha256": "x"}}, "is not under"),
])
def test_refusals(store, extra, expect):
    gcp = FakeGcp()
    with pytest.raises(reg.RegistrationRefused) as e:
        reg.Registrations(gcp, "acme-artifacts", dict, running).register(contract(), body(**extra), RUNNER)
    assert expect in e.value.reason
    assert not any(c[0] == "POST" for c in gcp.calls)
