"""Serving health (FR-98 stage 1, 1.4.3): policies compiled per deployed version, the incident route, rollback
proposals."""

import base64
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pipewright.gcp.session import Response
from pipewright.serving import health, incidents, rollback

HEALTH = {"enabled": True, "error_rate": {"warning": 0.01, "critical": 0.05},
          "latency_p95_ms": {"warning": 500, "critical": 1000}, "min_ready_replicas": 1, "scaling_minutes": 5,
          "propose_rollback_hours": 0}
VERTEX = {"target": "vertex", "endpoint_id": "123", "deployed_model_id": "456", "version": "3"}
KSERVE = {"target": "kserve", "namespace": "serving-churn", "isvc": "churn", "version": "3"}


def uc(health_cfg=HEALTH):
    return SimpleNamespace(name="churn", environment="prod", release="r1", ml={"monitoring": {"health": health_cfg}})


class FakeGcp:
    def __init__(self, live=(), refuse=()):
        self.live, self.refuse, self.posted, self.deleted = list(live), refuse, [], []

    def get(self, url, **params):
        return Response(200, {"alertPolicies": self.live})

    def post(self, url, body, **params):
        q = json.dumps(body["conditions"])
        if any(r in q for r in self.refuse):
            return Response(400, {"error": {"message": "The following PromQL metric(s) are invalid:  Requests5XX"}})
        self.posted.append(body)
        return Response(200, body)

    def patch(self, url, body, **params):
        return self.post(url, body)

    def delete(self, url, **params):
        self.deleted.append(url)
        return Response(200, {})


CHANNEL = "projects/p/notificationChannels/9"
ENV = SimpleNamespace(name="prod", project="p", observability={"incidents_channel": CHANNEL})


def test_agent_platform_policies_per_version_and_severity():
    g = FakeGcp()
    plan = health.sync(uc(), ENV, [VERTEX], g)
    assert len(plan.create) == 6 and not plan.errors          # errors and latency x 2 severities, replicas, scaling
    errors = next(b for b in g.posted if b["userLabels"]["pipewright-signal"] == "errors"
                  and b["userLabels"]["pipewright-severity"] == "critical")
    c = errors["conditions"][0]["conditionThreshold"]
    assert 'response_code=monitoring.regex.full_match("5..")' in c["filter"]
    assert c["denominatorFilter"].endswith('metric.label.deployed_model_id="456"') and c["thresholdValue"] == 0.05
    assert errors["userLabels"]["pipewright-threshold"] == "0_05" and errors["severity"] == "CRITICAL"
    assert errors["notificationChannels"] == ["projects/p/notificationChannels/9"]
    scaling = next(b for b in g.posted if b["userLabels"]["pipewright-signal"] == "scaling")
    assert "prediction_online_target_replicas" in scaling["conditions"][0]["conditionPrometheusQueryLanguage"]["query"]


def test_kserve_p95_and_the_mean_when_the_handler_has_no_histogram():
    g = FakeGcp(refuse=("PwPredictLatencySeconds", "Requests5XX"))
    plan = health.sync(uc(), ENV, [KSERVE], g)
    latency = [b for b in g.posted if b["userLabels"]["pipewright-signal"] == "latency"]
    assert {b["userLabels"]["pipewright-latency"] for b in latency} == {"mean"} and len(latency) == 2
    assert len(plan.mean_latency) == 2
    assert len(plan.pending) == 2                       # the 5xx ratio waits for TorchServe's first 5xx
    g = FakeGcp()
    health.sync(uc(), ENV, [KSERVE], g)
    q = [b["conditions"][0]["conditionPrometheusQueryLanguage"]["query"] for b in g.posted
         if b["userLabels"]["pipewright-signal"] == "latency"]
    assert all("histogram_quantile(0.95" in x and "* 1000 >" in x for x in q)


def test_versions_no_longer_deployed_lose_their_policies_and_off_removes_all():
    old = {"displayName": "pipewright churn prod: serving errors warning (v2, 111)",
           "name": "projects/p/alertPolicies/1", "userLabels": {"pipewright-health": "true"}}
    g = FakeGcp(live=[old])
    plan = health.sync(uc(), ENV, [VERTEX], g)
    assert plan.delete == [old["displayName"]]
    g = FakeGcp(live=[old])
    plan = health.sync(uc({**HEALTH, "enabled": False}), ENV, [VERTEX], g)
    assert plan.delete == [old["displayName"]] and not g.posted


def test_no_channel_is_an_error():
    plan = health.sync(uc(), SimpleNamespace(name="prod", project="p", observability={}), [VERTEX], FakeGcp())
    assert "no incident channel" in plan.errors[0]


def incident(state="open", severity="critical", **labels):
    body = {"incident": {"state": state, "incident_id": "0.abc", "observed_value": "0.2",
                         "policy_user_labels": {"pipewright-health": "true", "pipewright-usecase": "churn",
                                                "pipewright-signal": "errors", "pipewright-severity": severity,
                                                "pipewright-threshold": "0_05", "pipewright-model-version": "3",
                                                "pipewright-endpoint": "123", "pipewright-deployed-model": "456",
                                                **labels}}}
    return {"message": {"data": base64.b64encode(json.dumps(body).encode()).decode()}}


def test_incidents_become_events():
    i = incidents.parse(incident())
    assert incidents.event_type(i) == "serving.degraded" and i["threshold"] == 0.05 and i["model_version"] == "3"
    assert incidents.event_type(incidents.parse(incident("closed"))) == "serving.recovered"
    other = {"message": {"data": base64.b64encode(json.dumps({"incident": {"state": "open"}}).encode()).decode()}}
    try:
        incidents.parse(other)
        raise AssertionError("not ours")
    except incidents.NotOurs:
        pass


def test_the_launcher_route(monkeypatch):
    from pipewright.launcher import app as launcher_app

    contract = SimpleNamespace(usecase="churn", tenant="churn", environment="prod", release_id="r1", region="r",
                               project="p", created_at="", static_parameters={"ml_config": {}}, metadata={})

    class Reg:
        manifest, bucket, gcp, errors = {}, "b", None, {}

        def live(self):
            return [contract]

        def get(self, usecase, release=None):
            return contract

    sent = []
    monkeypatch.setattr(launcher_app.events, "emit", lambda e, *a, **k: sent.append(e))
    client = launcher_app.create_app(Reg()).test_client()
    r = client.post("/incidents", json=incident())
    assert r.status_code == 200 and sent[0]["event_type"] == "serving.degraded"
    assert sent[0]["data"]["signal"] == "errors" and sent[0]["data"]["threshold"] == 0.05


def test_rollback_proposal_only_when_on_critical_and_recent(monkeypatch):
    store = {}
    monkeypatch.setattr(rollback, "gcs_read", lambda gcp, b, n: (store.get(n), 1 if n in store else 0))
    monkeypatch.setattr(rollback, "gcs_write", lambda gcp, b, n, t, **k: store.__setitem__(n, t) or 1)
    now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)

    class G:
        def get(self, url, **params):
            if url.endswith("/endpoints/123"):
                return Response(200, {"deployedModels": [{"id": "456", "createTime": (now - timedelta(hours=1))
                                                          .isoformat()}]})
            if url.endswith("/models"):
                return Response(200, {"models": [{"name": "m", "createTime": "1"}]})
            return Response(200, {"models": [{"versionId": "1"}, {"versionId": "2"}, {"versionId": "3"}]})

    def contract(hours):
        return SimpleNamespace(usecase="churn", environment="prod", release_id="r1", region="r", project="p",
                               created_at="", metadata={"model": "churn"},
                               static_parameters={"ml_config": {"monitoring": {"health": {
                                   "propose_rollback_hours": hours}}}})

    i = incidents.parse(incident())
    assert rollback.maybe_propose(contract(0), i, G(), "b", now) is None              # off by default
    assert rollback.maybe_propose(contract(24), incidents.parse(incident(severity="warning")), G(), "b", now) is None
    p = rollback.maybe_propose(contract(24), i, G(), "b", now)
    assert p["previous_version"] == "2" and p["hours_since_deploy"] == 1.0
    assert rollback.maybe_propose(contract(24), i, G(), "b", now) is None              # one case per incident
    assert rollback.maybe_propose(contract(0.5 and 0), i, G(), "b", now) is None
    # GA QA (found live): capacity signals and a first version propose nothing
    store.clear()
    capacity = incidents.parse(incident(**{"pipewright-signal": "replicas"}))
    assert rollback.maybe_propose(contract(24), capacity, G(), "b", now) is None
    first = incidents.parse(incident(**{"pipewright-model-version": "1"}))
    assert rollback.maybe_propose(contract(24), first, G(), "b", now) is None


def test_an_ab_treatment_on_agent_platform_is_watched_as_its_own_arm():
    # GA QA (found live): the treatment's endpoint had no health policies
    from types import SimpleNamespace

    from pipewright.serving import health

    eps = {"churn-endpoint": "projects/p/locations/r/endpoints/1",
           "churn-ab-endpoint": "projects/p/locations/r/endpoints/2"}
    bodies = {"endpoints/1": [{"id": "11", "modelVersionId": "1", "dedicatedResources": {"minReplicaCount": 1}}],
              "endpoints/2": [{"id": "22", "modelVersionId": "4", "dedicatedResources": {"minReplicaCount": 1}}]}

    def get(url, **k):
        if "filter" in k:
            name = k["filter"].split('"')[1]
            return SimpleNamespace(ok=True, body={"endpoints": [{"name": eps[name]}] if name in eps else []})
        return SimpleNamespace(ok=True, body={"deployedModels": bodies[url.split("/v1/")[1].split("/", 4)[-1]]})

    uc = SimpleNamespace(name="churn", environment="test", model={"serving": {"enabled": True}})
    env = SimpleNamespace(region="r", project="p", name="test")
    import pipewright.models.deploy as deploy

    real = deploy.find_endpoint
    deploy.find_endpoint = lambda u, e, g: eps["churn-endpoint"]
    try:
        targets = health.deployed_targets(uc, env, SimpleNamespace(get=get))
    finally:
        deploy.find_endpoint = real
    assert [(t["version"], t.get("arm")) for t in targets] == [("1", "control"), ("4", "treatment")]
    name = health.policy_name(uc, targets[1], {"signal": "errors", "severity": "critical"})
    assert "(v4, treatment, 22)" in name
    del eps["churn-ab-endpoint"]
    deploy.find_endpoint = lambda u, e, g: eps["churn-endpoint"]
    try:
        alone = health.deployed_targets(uc, env, SimpleNamespace(get=get))
    finally:
        deploy.find_endpoint = real
    assert [(t["version"], t.get("arm")) for t in alone] == [("1", None)]           # no test: no arm, as before
