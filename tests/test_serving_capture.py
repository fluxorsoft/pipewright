"""Capture (FR-98 stage 2, 1.4.3): Agent Platform endpoint logging, the KServe logger and receiver, TorchServe
metrics settings."""

import json
from types import SimpleNamespace

import pytest
import yaml

from pipewright.errors import ConfigError
from pipewright.gcp.session import Response
from pipewright.serving import capture, receiver, torchserve_metrics

CAP = {"enabled": True, "sample_rate": 0.05, "max_per_day": 100000, "retention_days": 30}


def uc(cap=CAP):
    return SimpleNamespace(name="churn", platform={"bigquery_dataset": "pw_churn"},
                           model={"serving": {"capture": cap}}, ml={"monitoring": {}})


ENV = SimpleNamespace(name="dev", project="acme", region="europe-west2")


class G:
    def __init__(self, endpoint, table=None):
        self.endpoint, self.table, self.patched = endpoint, table, []

    def get(self, url, **params):
        if "bigquery" in url:
            return Response(200, self.table) if self.table else Response(404, {})
        return Response(200, self.endpoint)

    def patch(self, url, body, **params):
        self.patched.append((url, body, params))
        return Response(200, body)


def test_endpoint_logging_is_set_to_the_tenant_dataset():
    g = G({}, table={"timePartitioning": {"type": "DAY", "field": "logging_time"}})
    assert capture.sync_endpoint(uc(), ENV, g, "projects/acme/locations/r/endpoints/1", log=lambda m: None) == "on"
    _, body, params = g.patched[0]
    cfg = body["predictRequestResponseLoggingConfig"]
    assert cfg == {"enabled": True, "samplingRate": 0.05,
                   "bigqueryDestination": {"outputUri": "bq://acme.pw_churn.churn_online_log"}}
    assert params["updateMask"] == "predict_request_response_logging_config"
    assert g.patched[1][1]["timePartitioning"]["expirationMs"] == str(30 * 86400 * 1000)


def test_unchanged_and_off():
    same = {"predictRequestResponseLoggingConfig": capture.wanted_logging(uc(), ENV)}
    assert capture.sync_endpoint(uc(), ENV, G(same), "e", log=lambda m: None) == "unchanged"
    g = G(same)
    assert capture.sync_endpoint(uc({**CAP, "enabled": False}), ENV, g, "e", log=lambda m: None) == "off"
    assert capture.sync_endpoint(SimpleNamespace(name="x", model={"serving": {}}), ENV, g, "e") == "not configured"


def test_a_model_monitoring_v1_job_is_refused():
    with pytest.raises(ConfigError, match="Model Monitoring v1"):
        capture.sync_endpoint(uc(), ENV, G({"modelDeploymentMonitoringJob": "projects/p/jobs/1"}), "e")


def test_kserve_logger_carries_the_rate_and_cap():
    assert capture.kserve_logger(uc(), "serving-churn") == {
        "mode": "all", "url": "http://pipewright-capture.serving-churn.svc.cluster.local/?rate=0.05&max=100000"}
    assert capture.kserve_logger(uc({**CAP, "enabled": False}), "ns") is None


def test_receiver_samples_by_request_caps_per_service_and_never_blocks():
    lines = []
    r = receiver.Receiver(size=2, out=lines.append)
    h = {"ce-id": "a", "ce-type": "org.kubeflow.serving.inference.request", "ce-inferenceservicename": "churn"}
    assert r.take(h, b'{"instances": [[1]]}', {"rate": "1", "max": "2"}) == "kept"
    assert r.take({**h, "ce-type": "...response"}, b"{}", {"rate": "1", "max": "2"}) == "kept"
    assert r.take(h, b"{}", {"rate": "1", "max": "2"}) == "capped"              # the day's cap for churn
    assert r.take({**h, "ce-inferenceservicename": "other"}, b"{}", {"max": "5"}) == "dropped"   # queue of 2 full
    assert r.drain_once(0) == 1
    record = json.loads(lines[0])[receiver.MARKER]
    assert record["isvc"] == "churn" and json.loads(record["body"]) == {"instances": [[1]]}
    kept = [i for i in range(400) if receiver.Receiver.sampled(f"id-{i}", 0.05)]
    assert 5 <= len(kept) <= 40                                                # about 5%
    assert receiver.Receiver.sampled("id-7", 0.05) == receiver.Receiver.sampled("id-7", 0.05)   # request = answer
    assert r.stats["received"] == 4 and r.stats["capped"] == 1 and r.stats["dropped"] == 1


def test_torchserve_settings_and_metrics_config():
    c = torchserve_metrics.container_settings({"name": "kserve-container", "env": [{"name": "A", "value": "1"}],
                                               "ports": [{"containerPort": 8080}]})
    names = {e["name"]: e["value"] for e in c["env"]}
    assert names["TS_METRICS_MODE"] == "prometheus" and names["TS_METRICS_CONFIG"] == torchserve_metrics.METRICS_FILE
    assert yaml.safe_load(names["PIPEWRIGHT_TS_METRICS"])["model_metrics"]["histogram"][0]["name"] == \
        "PwPredictLatencySeconds"
    assert c["ports"][-1] == {"name": "ts-metrics", "containerPort": 8082, "protocol": "TCP"}
    assert "exec python entrypoint.py" in c["command"][-1]
    assert torchserve_metrics.is_torchserve("europe-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest")
    pm = torchserve_metrics.pod_monitoring("churn", "serving-churn", "churn")
    assert pm["spec"]["endpoints"][0]["port"] == "ts-metrics"
