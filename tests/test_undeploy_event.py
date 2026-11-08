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


def test_a_release_records_the_person_behind_ci(monkeypatch):
    """FR-113: release.live carries the approval (GitHub actor and run) like every other decision."""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_ACTOR", "someone")
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    monkeypatch.delenv("PIPEWRIGHT_APPROVAL", raising=False)
    e = events.event("release.live", "uc", "test", "t", "r1", None, by="ci@example.com")
    assert e["data"]["approval"]["via"] == "github" and e["data"]["approval"]["actor"] == "someone"


def test_a_model_trained_online_is_refused_where_callers_send_features():
    """FR-114: model.serving.features request, but the model carries an online lookup: refused, not served wrongly."""
    from pipewright.models import online

    lookup = {"store": "projects/p/locations/r/featureOnlineStores/s", "views": []}
    msg = online.check(None, {online.ENV_VAR: online.env_value(lookup)}, None, "test")
    assert msg and "features up in an online store by ID" in msg and "features request" in msg
    assert online.check(None, {}, {"online_features": lookup}, "test")              # from serving.json too
    assert online.check(None, {}, None, "test") is None                              # a request-mode model
    assert online.check(None, {}, {"online_features": {**lookup, "mode": "gateway"}}, "test") is None


def test_lineage_reads_how_approved_files_are_protected_now():
    """FR-116: protection the renewer or a deploy added after the approval shows in lineage."""
    from datetime import datetime, timedelta, timezone

    from pipewright.models import protection

    later = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    metas = {"m/model.pt": {"retention": {"retainUntilTime": later}}, "m/serving.json": {"temporaryHold": True}}

    class Gcp:
        def get(self, url, **_):
            name = url.split("/o/")[1].replace("%2F", "/")
            return SimpleNamespace(ok=True, body=metas.get(name, {}))

    files = {"folder": "gs://b/m", "files": [{"file": "model.pt", "generation": 1},
                                             {"file": "serving.json", "generation": 2},
                                             {"file": "card.md", "generation": 3}]}
    got = protection.current(Gcp(), files)
    assert got["files"] == 3 and got["protected"] == 2 and got["holds"] == 1 and got["until"] == later[:16].replace("T", " ")
    assert protection.current(Gcp(), None) is None
