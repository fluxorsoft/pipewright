"""GA QA (found live): the closed loop validates its candidate in test before anyone approves it. Test's
policies.integrity.inference refuse refused that scoring run, so a loop could never pass validation."""
from types import SimpleNamespace

from pipewright.models import integrity


class Gcp:
    def get(self, url, **k):
        if url.endswith("/models"):
            return SimpleNamespace(ok=True, status=200, body={"models": [{"name": "projects/p/locations/r/models/m",
                                                                          "createTime": "2026-01-01T00:00:00Z"}]})
        return SimpleNamespace(ok=True, status=200, body={"versionId": "5", "artifactUri": "gs://b/m",
                                                           "name": "projects/p/locations/r/models/m"})


MANIFEST = {"policies": {"integrity": {"inference": "refuse"}}}
CONTRACT = SimpleNamespace(metadata={"model": "m"}, region="r", project="p", environment="test", usecase="churn")


def test_an_unapproved_candidate_is_scored_for_validation_only(monkeypatch):
    monkeypatch.setattr(integrity, "verify", lambda *a, **k: integrity.Check(
        problems=["v5 has no model.approved event: nobody approved it here."]))
    plain = integrity.inference_gate(Gcp(), MANIFEST, CONTRACT, "5")
    assert plain["problem"] and plain["mode"] == "refuse"                      # a normal run: refused, as before
    loop = integrity.inference_gate(Gcp(), MANIFEST, CONTRACT, "5", candidate=True)
    assert loop["problem"] is None and loop["candidate"] and loop["approved_files"] is None


def test_an_approved_candidate_is_checked_as_any_version(monkeypatch):
    monkeypatch.setattr(integrity, "verify", lambda *a, **k: integrity.Check(
        problems=["model.mar changed"], approval={"files": {"files": []}}))
    out = integrity.inference_gate(Gcp(), MANIFEST, CONTRACT, "5", candidate=True)
    assert out["problem"] == "model.mar changed" and not out.get("candidate")
