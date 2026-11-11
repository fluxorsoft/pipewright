"""GA QA (found live): online monitoring judges the version that serves, not the registry's default. Under
promote_model auto a new default appears without a deploy; judging it refused the monitoring run (never approved)
and would have read traffic it never received."""
from types import SimpleNamespace

from pipewright.models import watch


def use(where, version, traffic=None, kind="vertex"):
    return watch.Use("churn-ga", "churn-ga-classifier", version, where, kind, traffic=traffic)


def test_the_version_with_most_traffic_on_its_own_endpoint(monkeypatch):
    uses = [use("default", "2", kind="default"), use("endpoint:churn-ga-endpoint", "1", 90),
            use("endpoint:churn-ga-endpoint", "3", 10), use("endpoint:churn-ga-ab-endpoint", "3", 100)]
    monkeypatch.setattr(watch, "in_use", lambda *a, **k: (uses, []))
    env = SimpleNamespace(kserve=None)
    assert watch.served_version(None, env, "churn-ga", "churn-ga-classifier") == "1"


def test_kserve_and_nothing_deployed(monkeypatch):
    env = SimpleNamespace(kserve={"namespaces": {"retail": "retail-serving"}})
    monkeypatch.setattr(watch, "in_use", lambda *a, **k: ([use("kserve:retail-serving/churn-ga", "10", None, "kserve"),
                                                           use("kserve:retail-serving/churn-ga-ab", "11", None,
                                                               "kserve")], []))
    assert watch.served_version(None, env, "churn-ga", "m") == "10"
    monkeypatch.setattr(watch, "in_use", lambda *a, **k: ([use("default", "2", kind="default")], []))
    assert watch.served_version(None, env, "churn-ga", "m") is None


def test_the_launcher_passes_it_to_a_monitoring_run():
    import inspect

    from pipewright.launcher import app

    src = inspect.getsource(app)
    assert 'if pipeline == "monitoring" and not (extra or {}).get("model_version"):' in src
    assert "served_watch.served_version(" in src
