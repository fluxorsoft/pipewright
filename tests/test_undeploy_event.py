"""FR-112: a version leaving an endpoint emits model.undeployed, which lineage and the fleet report read."""
from types import SimpleNamespace

from pipewright.models import deploy
from pipewright.observability import events


def test_undeploy_emits_one_event_per_version(monkeypatch):
    sent = []
    monkeypatch.setattr(events, "emit", lambda ev, *a, **k: sent.append(ev))
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda creds=None: SimpleNamespace(identity=lambda: "user:a@example.com"))

    class Endpoint:
        def list_models(self):
            return [SimpleNamespace(model_version_id="10"), SimpleNamespace(model_version_id="11")]

        def undeploy_all(self):
            self.done = True

    ep = Endpoint()
    monkeypatch.setattr(deploy, "_aiplatform", lambda env, creds: None)
    monkeypatch.setattr(deploy, "list_endpoints", lambda ai, name: [ep])
    monkeypatch.setattr("pipewright.serving.endpoints.is_private", lambda env, name: False)
    uc = SimpleNamespace(name="lapse-risk", tenant="retail", model={"name": "lapse-risk-classifier", "serving": {}})
    env = SimpleNamespace(name="test", region="europe-west2", observability={"events_topic": "t"})
    monkeypatch.setattr(deploy, "endpoint_name", lambda u: "lapse-risk-endpoint")
    deploy.undeploy(uc, env, None, log=lambda *_: None)
    assert ep.done
    got = [(e["event_type"], e["data"]["version"]) for e in sent]
    assert got == [("model.undeployed", "10"), ("model.undeployed", "11")]
    assert all("approval" in e["data"] for e in sent)          # a decision: how it was approved is recorded
