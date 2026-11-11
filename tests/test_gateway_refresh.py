"""GA QA (found live): after a launcher upgrade, the lookup services and A/B routers it had made kept the old image,
so fixes never reached them. The tick refreshes one whose image is not the environment's."""
from types import SimpleNamespace

from pipewright.online import launcher


def contract():
    online = {"mode": "gateway", "type": "cloudsql", "service": "pwl-retail-churn"}
    return SimpleNamespace(usecase="churn", tenant="retail",
                           static_parameters={"ml_config": {"serving": {"online": online}}})


def gcp(image):
    return SimpleNamespace(get=lambda url, **k: SimpleNamespace(
        ok=True, status=200, body={"template": {"containers": [{"image": image}]}}))


MANIFEST = {"project": "p", "region": "r", "launcher": {"image": "repo/launcher@sha256:new"}}


def test_a_gateway_on_an_older_image_is_refreshed(monkeypatch):
    monkeypatch.setattr(launcher, "online_of", lambda c: c.static_parameters["ml_config"]["serving"]["online"])
    monkeypatch.setattr(launcher, "has_service", lambda online: True)
    done = []
    was = launcher.refresh_if_stale(contract(), MANIFEST, gcp("repo/launcher@sha256:old"), refresh=done.append)
    assert was == "repo/launcher@sha256:old" and len(done) == 1
    assert launcher.refresh_if_stale(contract(), MANIFEST, gcp("repo/launcher@sha256:new"), refresh=done.append) is None
    assert len(done) == 1


def test_no_service_no_test_nothing(monkeypatch):
    monkeypatch.setattr(launcher, "online_of", lambda c: {})
    monkeypatch.setattr(launcher, "has_service", lambda online: False)
    monkeypatch.setattr(launcher, "running_ab", lambda c, m, g: None)
    assert launcher.refresh_if_stale(contract(), MANIFEST, gcp("x"), refresh=lambda c: 1 / 0) is None
