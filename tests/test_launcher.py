import base64
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from pipewright.launcher import routing
from pipewright.launcher.routing import BadRequest, FileEvent, Ignored
from tests.test_release_parts import CONTRACT, V3

pytest.importorskip("flask")

CHURN = replace(CONTRACT, triggers={"landing_prefix": "incoming/", "landing_suffixes": [".csv"], "api": False,
                                    "schedules": {"nightly": {"cron": "0 2 * * *", "timezone": "Europe/London",
                                                              "source_uri": "gs://l/incoming/latest.csv"}}})
NIMBUS = replace(CONTRACT, usecase="churn-nimbus", tenant="nimbus",
                 triggers={"landing_prefix": "incoming/nimbus/", "landing_suffixes": [".csv"], "api": True,
                           "schedules": {}})


def envelope(name="incoming/customers.csv", event="OBJECT_FINALIZE", generation="7"):
    data = {"bucket": "landing", "name": name, "generation": generation}
    return {"message": {"messageId": "m1",
                        "attributes": {"eventType": event, "bucketId": "landing", "objectId": name,
                                       "objectGeneration": generation},
                        "data": base64.b64encode(json.dumps(data).encode()).decode()}}


# ---------------------------------------------------------------- routing --

def test_parse_and_ignore():
    ev = routing.parse_pubsub(envelope())
    assert ev == FileEvent("landing", "incoming/customers.csv", "7", "m1")
    with pytest.raises(Ignored):
        routing.parse_pubsub(envelope(event="OBJECT_DELETE"))
    with pytest.raises(BadRequest):
        routing.parse_pubsub({"nope": 1})


def test_longest_prefix_wins():
    live = [CHURN, NIMBUS]
    assert routing.route_file(live, FileEvent("l", "incoming/nimbus/a.csv", "1")) == [NIMBUS]
    assert routing.route_file(live, FileEvent("l", "incoming/a.csv", "1")) == [CHURN]
    with pytest.raises(Ignored):
        routing.route_file(live, FileEvent("l", "incoming/a.parquet", "1"))
    with pytest.raises(Ignored):
        routing.route_file(live, FileEvent("l", "archive/a.csv", "1"))


def test_run_ids_are_deterministic():
    a = routing.run_id("churn", "l", "incoming/a.csv", "1")
    assert a == routing.run_id("churn", "l", "incoming/a.csv", "1") != routing.run_id("churn", "l", "incoming/a.csv",
                                                                                      "2")
    jid = routing.job_id("churn-nimbus", a, "Customers 2026")
    assert jid == f"churn-nimbus-{a}-customers-2026" and len(jid) <= 128


def test_resolve_run_schedule_and_api():
    t = datetime(2026, 10, 5, 2, 0, 30, tzinfo=timezone.utc)
    r = routing.resolve_run(CHURN, {"usecase": "churn", "schedule": "nightly"}, now=t)
    assert (r.source_uri, r.trigger, r.pipeline) == ("gs://l/incoming/latest.csv", "schedule", "training")
    assert r.run_id == routing.resolve_run(CHURN, {"schedule": "nightly"}, now=t.replace(second=59)).run_id
    with pytest.raises(BadRequest, match="does not accept API runs"):
        routing.resolve_run(CHURN, {"source_uri": "gs://x/y.csv"})
    with pytest.raises(BadRequest, match="no schedule"):
        routing.resolve_run(CHURN, {"schedule": "weekly"})
    r = routing.resolve_run(NIMBUS, {"source_uri": "gs://x/y.csv"})
    assert (r.source_uri, r.trigger) == ("gs://x/y.csv", "api")


def test_resolve_run_selects_pipeline():
    t = datetime(2026, 10, 5, 2, 0, 30, tzinfo=timezone.utc)
    train = routing.resolve_run(V3, {"source_uri": "gs://x/y.csv"}, now=t)
    infer = routing.resolve_run(V3, {"source_uri": "gs://x/y.csv", "pipeline": "inference"}, now=t)
    assert (train.pipeline, infer.pipeline) == ("training", "inference") and train.run_id != infer.run_id
    with pytest.raises(BadRequest, match="no scoring pipeline"):
        routing.resolve_run(V3, {"source_uri": "gs://x/y.csv", "pipeline": "scoring"})
    with pytest.raises(BadRequest, match="runs the training pipeline"):
        routing.resolve_run(V3, {"schedule": "weekly", "source_uri": "gs://x", "pipeline": "inference"})
    assert routing.landing_pipeline(V3) == "inference" and routing.landing_pipeline(CHURN) == "training"
    params = routing.parameters_for(V3, V3.pipeline("inference"), "gs://l/a.csv", "r1")
    assert params["ml_config"] == {"usecase": "churn"} and "auc_threshold" not in params


def test_parameters_merge_static_and_launcher():
    p = routing.parameters_for(CHURN, CHURN.pipeline(), "gs://l/a.csv", "r1")
    assert p == {"auc_threshold": 0.75, "project": "p", "region": "europe-west2", "source_uri": "gs://l/a.csv",
                 "run_id": "r1"}


# -------------------------------------------------------------------- app --

KEY = "projects/p/locations/r/keyRings/k/cryptoKeys/aiplatform"
NETWORK = "projects/123/global/networks/vpc"


class FakeRegistry:
    def __init__(self, live, errors=None):
        self._live, self._errors = live, errors or {}
        self.manifest = {"environment": "dev", "security": {"kms_key": KEY, "network": NETWORK}}

    def live(self, force=False):
        return self._live

    def get(self, usecase):
        return next((c for c in self._live if c.usecase == usecase), None)

    @property
    def errors(self):
        return self._errors


@pytest.fixture
def client(monkeypatch):
    from pipewright import adapters
    from pipewright.adapters.vertex import AlreadySubmitted
    from pipewright.launcher.app import create_app

    calls = []

    def fake_submit(contract, target, params, job_id, labels, security=None):
        if job_id in [c[1] for c in calls]:
            raise AlreadySubmitted(job_id)
        calls.append((contract.usecase, job_id, params, labels, security))
        return f"projects/p/locations/r/pipelineJobs/{job_id}"

    monkeypatch.setattr(adapters, "submit", fake_submit)
    app = create_app(FakeRegistry([CHURN, NIMBUS], {"broken": "bad contract"}))
    c = app.test_client()
    c.calls = calls
    return c


def test_pubsub_routes_to_tenant_and_dedupes(client):
    r = client.post("/pubsub", json=envelope("incoming/nimbus/jan.csv"))
    assert r.status_code == 200 and r.json["results"][0]["usecase"] == "churn-nimbus"
    assert client.calls[0][3]["pipewright-tenant"] == "nimbus" and client.calls[0][3]["trigger"] == "landing"
    again = client.post("/pubsub", json=envelope("incoming/nimbus/jan.csv"))
    assert again.json["results"][0]["status"] == "duplicate" and len(client.calls) == 1


def test_pubsub_ignored_and_malformed_are_acked(client):
    assert client.post("/pubsub", json=envelope("archive/x.csv")).json["status"] == "ignored"
    assert client.post("/pubsub", json={"x": 1}).json["status"] == "dropped"


def test_run_endpoint(client):
    r = client.post("/run", json={"usecase": "churn", "schedule": "nightly"})
    assert r.status_code == 200 and r.json["status"] == "submitted"
    assert client.calls[-1][2]["source_uri"] == "gs://l/incoming/latest.csv"
    assert client.post("/run", json={"usecase": "churn", "source_uri": "gs://x"}).status_code == 400
    r = client.post("/run", json={"usecase": "broken"})
    assert r.status_code == 404 and "bad contract" in r.json["reason"]


def test_jobs_get_the_manifest_security_settings(client):
    client.post("/run", json={"usecase": "churn", "schedule": "nightly"})
    assert client.calls[-1][4] == {"kms_key": KEY, "network": NETWORK}


def run_envelope(body) -> dict:
    data = base64.b64encode(json.dumps(body).encode()).decode()
    return {"message": {"data": data, "messageId": "m1"}, "subscription": "projects/p/subscriptions/s"}


def test_pubsub_run_requests(client):
    r = client.post("/pubsub/run", json=run_envelope({"usecase": "churn", "schedule": "nightly"}))
    assert r.status_code == 200 and r.json["status"] == "submitted"
    # Bad requests are acknowledged (redelivery cannot fix them); a missing live release is retried.
    assert client.post("/pubsub/run", json=run_envelope({"pipeline": "training"})).json["status"] == "dropped"
    assert client.post("/pubsub/run", json={"x": 1}).json["status"] == "dropped"
    assert client.post("/pubsub/run", json=run_envelope({"usecase": "broken"})).status_code == 503
    bad = {"message": {"data": base64.b64encode(b"[1, 2]").decode()}}
    assert client.post("/pubsub/run", json=bad).json["status"] == "dropped"


def test_health(client):
    r = client.get("/health")
    assert r.json["usecases"]["churn"] == {"release": "abc123", "pipelines": ["training"]} and r.json["errors"]
