"""FR-104: the launcher runs the tenant's sync job and creates the lookup services."""
import json
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.online import launcher as ol

ONLINE = {"type": "cloudsql", "provider": "pipewright", "service": "pwl-churn-churn-x", "schema": "team",
          "table": "pw_online_churn_x", "key": "customer_id", "features": ["tenure"],
          "instance": "acme-ml-dev:europe-west2:features", "database": "features",
          "lookup": {"cpu": 1, "memory": "512Mi", "min_instances": 1, "max_instances": 5, "concurrency": 80,
                     "pool_size": 8, "timeout_ms": 1000, "cpu_boost": True, "billing": "request"}}
MANIFEST = {"project": "acme-ml-dev", "region": "europe-west2",
            "features": {"online_store": {"cloudsql": {"private_ip": "10.41.0.3"}, "lookup": {
                "image": "europe-west2-docker.pkg.dev/acme/pw/launcher@sha256:abc", "network": "net",
                "subnetwork": "sub"}}},
            "tenants": {"churn": {"features": {
                "lookup_service_account": "pwl-churn@acme-ml-dev.iam.gserviceaccount.com",
                "sync_job": "projects/acme-ml-dev/locations/europe-west2/jobs/pw-sync-churn"}}}}


def contract(online=ONLINE):
    target = SimpleNamespace(static_parameters={"ml_config": {"online_features": online}})
    return SimpleNamespace(usecase="churn-x", tenant="churn", release_id="R1", pipeline=lambda name: target,
                           runner_service_account="runner-churn@acme-ml-dev.iam.gserviceaccount.com")


class Gcp:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def _do(self, method, url, body=None, **params):
        self.calls.append((method, url, body, params))
        for (m, part), resp in self.answers.items():
            if m == method and part in url:
                status, out = resp() if callable(resp) else resp
                return SimpleNamespace(ok=200 <= status < 300, status=status, body=out)
        return SimpleNamespace(ok=True, status=200, body={})

    def get(self, url, **p):
        return self._do("GET", url, **p)

    def post(self, url, body, **p):
        return self._do("POST", url, body, **p)

    def patch(self, url, body, **p):
        return self._do("PATCH", url, body, **p)


def test_start_sync_runs_the_tenants_job_with_the_release_usecase():
    gcp = Gcp({("POST", ":run"): (200, {"metadata": {"name": "executions/e1"}})})
    out = ol.start_sync(contract(), MANIFEST, "gs://art/pipewright/environment.json", gcp, "nightly")
    assert out["status"] == "submitted" and out["execution"] == "executions/e1"
    method, url, body, _ = gcp.calls[0]
    assert url.endswith("/jobs/pw-sync-churn:run")
    assert body["overrides"]["containerOverrides"][0]["args"] == [
        "features", "sync", "--manifest", "gs://art/pipewright/environment.json", "--usecase", "churn-x"]


def test_start_sync_refuses_without_a_synced_store_or_a_job():
    assert ol.start_sync(contract({"type": "http"}), MANIFEST, "m", Gcp({}))["status"] == "refused"
    bare = {**MANIFEST, "tenants": {"churn": {}}}
    assert "no sync job" in ol.start_sync(contract(), bare, "m", Gcp({}))["reason"]


def test_the_service_spec_follows_the_use_cases_sizes_and_the_platform():
    spec = ol.service_spec(contract(), MANIFEST, ONLINE, "CA-PEM")
    t = spec["template"]
    assert spec["ingress"] == "INGRESS_TRAFFIC_INTERNAL_ONLY" and spec["scaling"]["minInstanceCount"] == 1
    assert t["serviceAccount"].startswith("pwl-churn@") and t["scaling"]["maxInstanceCount"] == 5
    c = t["containers"][0]
    assert c["command"] == ["python", "-m", "pipewright.online.server"] and "@sha256:" in c["image"]
    assert c["resources"]["startupCpuBoost"] is True and c["resources"]["cpuIdle"] is True
    cfg = json.loads(c["env"][0]["value"])
    assert cfg["host"] == "10.41.0.3" and cfg["server_ca"] == "CA-PEM" and cfg["pool_size"] == 8
    assert t["vpcAccess"]["egress"] == "PRIVATE_RANGES_ONLY"


def test_a_service_outside_the_tenants_prefix_is_refused():
    with pytest.raises(ConfigError, match="not under the tenant's prefix"):
        ol.service_spec(contract(), MANIFEST, {**ONLINE, "service": "pwl-other-x"}, None)


def test_ensure_creates_waits_and_lets_the_endpoint_call_it():
    states = iter([(404, {}), (200, {"reconciling": True}),
                   (200, {"reconciling": False, "terminalCondition": {"state": "CONDITION_SUCCEEDED"},
                          "uri": "https://pwl-churn-churn-x.run.app"})])
    gcp = Gcp({("GET", "sqladmin"): (200, {"serverCaCert": {"cert": "CA"}}),
               ("GET", "/services/pwl-churn-churn-x"): lambda: next(states),
               ("POST", ":getIamPolicy"): (200, {"bindings": [{"role": "roles/run.invoker", "members": ["x"]}]}),
               ("POST", "/services"): (200, {})})
    out = ol.ensure_lookup_service(contract(), MANIFEST, gcp, sleep=lambda s: None)
    assert out["status"] == "created" and out["url"] == "https://pwl-churn-churn-x.run.app"
    create = next(c for c in gcp.calls if c[0] == "POST" and c[1].endswith("/services"))
    assert create[3] == {"serviceId": "pwl-churn-churn-x"}
    policy = next(c for c in gcp.calls if c[1].endswith(":setIamPolicy"))[2]["policy"]
    assert policy["bindings"] == [{"role": "roles/run.invoker",
                                   "members": ["serviceAccount:runner-churn@acme-ml-dev.iam.gserviceaccount.com"]}]


def test_ensure_refuses_a_release_without_pipewrights_lookup():
    with pytest.raises(ConfigError, match="has no Pipewright lookup service"):
        ol.ensure_lookup_service(contract({**ONLINE, "provider": "own"}), MANIFEST, Gcp({}))
