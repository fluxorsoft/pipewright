"""FR-92 (1.4.2): endpoints the platform's Terraform creates, public or private; without them nothing changes."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import deploy
from pipewright.serving import endpoints

RES = "projects/123/locations/europe-west2/endpoints/987"


def env(entries=None):
    return SimpleNamespace(name="dev", region="europe-west2", project="acme-ml-dev", security={},
                           serving={"endpoints": entries} if entries is not None else {})


def test_without_declarations_the_cli_creates_the_endpoint_as_before():
    e = env()
    assert endpoints.declared(e, "churn") is None and endpoints.network_type(e, "churn") == "standard"
    assert endpoints.predict_url(e, "churn", RES) == f"https://europe-west2-aiplatform.googleapis.com/v1/{RES}:predict"


def test_a_private_endpoint_is_called_at_its_name():
    e = env({"churn": {"name": "churn-endpoint", "private": True}})
    assert endpoints.network_type(e, "churn") == "psc"
    assert endpoints.predict_url(e, "churn", RES) == (
        f"https://987-europe-west2-123.prediction.p.vertexai.goog/v1/{RES}:predict")
    e = env({"churn": {"private": True, "dns": "x.prediction.p.vertexai.goog"}})
    assert endpoints.predict_url(e, "churn", RES).startswith("https://x.prediction.p.vertexai.goog/v1/")


class Ai:
    def __init__(self, found):
        self.created = []
        ai = self

        class Endpoint:
            @staticmethod
            def list(filter):
                return found

            @staticmethod
            def create(**kw):
                ai.created.append(kw)
                return "new"

        self.Endpoint = Endpoint


UC = SimpleNamespace(name="churn", tenant="churn")


def test_deploy_never_creates_an_endpoint_terraform_owns():
    ai = Ai([])
    with pytest.raises(ConfigError, match="created by the platform's Terraform.*does not create it itself, because a "
                                          "public endpoint would stand in for a private one"):
        deploy._endpoint_for(ai, UC, env({"churn": {"name": "churn-endpoint", "private": True}}), create=True)
    assert ai.created == []
    assert deploy._endpoint_for(Ai(["tf"]), UC, env({"churn": {"private": True}}), create=True) == "tf"
    ai = Ai([])
    assert deploy._endpoint_for(ai, UC, env(), create=True) == "new"
    assert ai.created[0]["display_name"] == "churn-endpoint"


def _uc(online=None):
    return SimpleNamespace(name="churn", tenant="churn", ml={"online_features": online} if online else {
        "data": {"contract": {"columns": [{"name": "tenure", "type": "float"}]}}, "features": {"include": ["tenure"]}})


def test_how_to_call_a_private_endpoint():
    text = deploy.how_to_call(_uc(), env({"churn": {"private": True}}), RES)
    assert "from inside the network only" in text and "prediction.p.vertexai.goog" in text
    assert "openssl s_client" in text


def test_how_to_call_through_the_gateway():
    online = {"mode": "gateway", "service": "pwl-churn-churn", "key": "customer_id",
              "gateway": {"callers": ["serviceAccount:app@acme.example"]}}
    text = deploy.how_to_call(_uc(online), env(), RES)
    assert "through the lookup gateway" in text and "<service URL>/predict" in text
    assert "serviceAccount:app@acme.example" in text and "called by the gateway only" in text


def test_how_to_call_a_public_endpoint_is_unchanged():
    text = deploy.how_to_call(_uc(), env(), RES)
    assert text.startswith("How to call it:") and "aiplatform.googleapis.com" in text


class Dns:
    def __init__(self, ip, existing=None, write_status=200):
        self.ip, self.existing, self.write_status, self.writes = ip, existing, write_status, []

    def get(self, url, **p):
        if "aiplatform" in url:
            return SimpleNamespace(ok=True, status=200, body={"privateServiceConnectConfig": {
                "pscAutomationConfigs": [{"ipAddress": self.ip}] if self.ip else [{}]}})
        if self.existing is None:
            return SimpleNamespace(ok=False, status=404, body={})
        return SimpleNamespace(ok=True, status=200, body={"rrdatas": [self.existing]})

    def post(self, url, body, **p):
        self.writes.append(("POST", url, body))
        return SimpleNamespace(ok=self.write_status < 300, status=self.write_status, body={})

    def patch(self, url, body, **p):
        self.writes.append(("PATCH", url, body))
        return SimpleNamespace(ok=self.write_status < 300, status=self.write_status, body={})


def test_deploy_writes_a_private_endpoints_name_once_it_has_an_address():
    e = env({"churn": {"private": True, "dns_zone": "pw-vertexai-prediction"}})
    d = Dns("10.10.9.2")
    assert endpoints.publish_dns(d, e, "churn", RES, log=lambda m: None) == "10.10.9.2"
    method, url, body = d.writes[0]
    assert method == "POST" and url.endswith("/managedZones/pw-vertexai-prediction/rrsets")
    assert body["name"] == "987-europe-west2-123.prediction.p.vertexai.goog." and body["rrdatas"] == ["10.10.9.2"]
    d = Dns("10.10.9.2", existing="10.10.9.2")
    endpoints.publish_dns(d, e, "churn", RES, log=lambda m: None)
    assert d.writes == []                                  # already right: nothing written
    said = []
    assert endpoints.publish_dns(Dns(None), e, "churn", RES, log=said.append) is None and "no address yet" in said[0]
    said = []
    assert endpoints.publish_dns(Dns("10.1.1.1", write_status=403), e, "churn", RES, log=said.append) is None
    assert "dns.resourceRecordSets" in said[0]
    assert endpoints.publish_dns(Dns("10.1.1.1"), env({"churn": {"private": True}}), "churn", RES) is None


def test_private_endpoints_are_found_where_the_sdk_lists_them():
    """Found live: the SDK's Endpoint.list leaves out Private Service Connect endpoints."""
    ai = SimpleNamespace(Endpoint=SimpleNamespace(list=lambda filter: []),
                         PrivateEndpoint=SimpleNamespace(list=lambda filter: ["psc"]))
    assert deploy.list_endpoints(ai, "churn-endpoint") == ["psc"]
    ai = SimpleNamespace(Endpoint=SimpleNamespace(list=lambda filter: ["public"]),
                         PrivateEndpoint=SimpleNamespace(list=lambda filter: ["psc"]))
    assert deploy.list_endpoints(ai, "churn-endpoint") == ["public"]
