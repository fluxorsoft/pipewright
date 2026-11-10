"""FR-124: a registration lost when two training runs ran together. Two runs registering a first version at once
each found no model and each made one; every lookup took whichever the API listed first. Now a first version is made
under a fixed model ID (the second run's upload is refused and becomes a version), the oldest model with the name is
the use case's model everywhere, and the launcher's tick says when an approved run registered nothing."""
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from google.api_core.exceptions import AlreadyExists

from pipewright.bqml.steps import bqml_runtime
from pipewright.build.templates import pipewright_runtime as rt
from pipewright.gcp import PreconditionFailed
from pipewright.launcher import unregistered
from pipewright.models import names

ROOT = Path(__file__).resolve().parents[1]


def test_the_model_id_rule_is_the_same_in_the_runtime():
    for name in ("churn-gke-classifier", "Churn_Classifier v2", "7-day-demand", "-x-", "a" * 80):
        assert rt.registry_model_id(name) == names.model_id(name)
    assert names.model_id("churn-gke-classifier") == "churn-gke-classifier"
    assert names.model_id("7-day-demand") == "m-7-day-demand"
    assert len(names.model_id("a" * 80)) == 63


def test_the_oldest_model_is_the_use_cases():
    rest = [{"name": "m/2", "createTime": "2026-11-09T17:53:09Z"},
            {"name": "m/1", "createTime": "2026-11-09T17:53:08Z"}]
    assert [m["name"] for m in names.oldest(rest)] == ["m/1", "m/2"]
    sdk = [SimpleNamespace(resource_name="m/b", create_time=datetime(2026, 11, 9, 18, tzinfo=timezone.utc)),
           SimpleNamespace(resource_name="m/a", create_time=datetime(2026, 11, 9, 17, tzinfo=timezone.utc))]
    assert [m.resource_name for m in names.oldest(sdk)] == ["m/a", "m/b"]
    assert [m.resource_name for m in rt.registry_models(SimpleNamespace(Model=SimpleNamespace(
        list=lambda filter: sdk)), "x")] == ["m/a", "m/b"]


# --------------------------------------------------------------------------------- the register step --
class Registry:
    """Model.upload as Agent Platform does it: a model ID already taken is refused (ALREADY_EXISTS)."""

    def __init__(self):
        self.models, self.uploads = {}, []
        registry = self

        class Model:
            def __init__(self, name=None, version="1", model_name=None):
                if model_name is not None:                 # a lookup by ID, as aiplatform.Model(model_name=...)
                    name = next((k for k in registry.models if k.endswith(f"/{model_name}")), None)
                    if name is None:
                        raise LookupError(model_name)
                self.resource_name, self.version_id = name, version
                self.versioned_resource_name = f"{name}@{version}"
                self.create_time = registry.models[name]["created"]
                self.display_name = registry.models[name]["display"]

            @staticmethod
            def list(filter):
                name = filter.split('"')[1]
                return [Model(k, "1") for k, v in registry.models.items() if v["display"] == name]

            @staticmethod
            def upload(**k):
                registry.uploads.append(k)
                if registry.before_upload:
                    hook, registry.before_upload = registry.before_upload, None
                    hook()
                if k["parent_model"]:
                    m = registry.models[k["parent_model"]]
                    m["versions"].append(k)
                    return Model(k["parent_model"], str(len(m["versions"])))
                rid = k["model_id"] or f"{len(registry.models) + 1000}"
                name = f"projects/p/locations/r/models/{rid}"
                if name in registry.models:
                    raise AlreadyExists("model id taken")
                registry.models[name] = {"display": k["display_name"], "versions": [k],
                                         "created": len(registry.models)}
                return Model(name, "1")

        self.Model, self.before_upload = Model, None

    def init(self, **k):
        pass


def _register(monkeypatch, tmp_path, registry, set_default="manual"):
    spec = importlib.util.spec_from_file_location("register_fr124", ROOT / "examples/churn/components/register.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    fake_ai = ModuleType("google.cloud.aiplatform")
    fake_ai.init, fake_ai.Model = registry.init, registry.Model

    class Blob:
        def upload_from_string(self, *a, **k):
            pass

    fake_storage = ModuleType("google.cloud.storage")
    fake_storage.Client = lambda project=None: SimpleNamespace(bucket=lambda b: SimpleNamespace(blob=lambda n: Blob()))
    import google.cloud

    monkeypatch.setattr(google.cloud, "aiplatform", fake_ai, raising=False)
    monkeypatch.setattr(google.cloud, "storage", fake_storage, raising=False)
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform", fake_ai)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", fake_storage)
    runtime = ModuleType("pipewright_runtime")
    events = []
    runtime.folder_files = lambda uri, project: {"count": 0}
    runtime.registry_model_id, runtime.registry_models = rt.registry_model_id, rt.registry_models
    runtime.emit_event = lambda event_type, cfg, run_id, **d: events.append((event_type, run_id, d))
    runtime.lineage_record = lambda *a, **k: {}
    runtime.write_lineage = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "pipewright_runtime", runtime)
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"metrics": {"auc": 0.809}, "threshold": 0.5, "calibration": {"ece": 0.01},
                                  "decision": "approve", "reasons": []}))
    cfg = {"usecase": "churn-gke", "environment": "ent", "model_name": "churn-gke-classifier", "owner": "x",
           "features": {"include": ["a"]}, "data": {"split": {"strategy": "random"}},
           "data_protection": {"personal_data_columns": []}, "evaluation": {"primary_metric": "auc"},
           "registry": {"set_default": set_default}, "platform": {}}

    def run(run_id, source_uri="bq://p.d.t"):
        out = SimpleNamespace(uri="", metadata={})
        mod.register_model.python_func(
            project="p", region="r", model=SimpleNamespace(uri="gs://b/models/m"),
            report=SimpleNamespace(path=str(report)), model_display_name="churn-gke-classifier", run_id=run_id,
            source_uri=source_uri, ml_config=cfg, vertex_model=out,
            card=SimpleNamespace(path=str(tmp_path / f"card-{run_id}.md")))
        return out
    return run, events


def test_two_first_versions_at_once_make_one_model(monkeypatch, tmp_path):
    """The battle test's race: run B looked for the model before run A made it, so both upload a first version."""
    registry = Registry()
    run, events = _register(monkeypatch, tmp_path, registry)
    # run A's upload lands between run B's look and run B's upload
    registry.before_upload = lambda: registry.Model.upload(
        display_name="churn-gke-classifier", parent_model=None, model_id="churn-gke-classifier")
    out = run("r4eca1d257914")
    assert list(registry.models) == ["projects/p/locations/r/models/churn-gke-classifier"]
    (model,) = registry.models.values()
    assert len(model["versions"]) == 2                          # A's first version, B's as the second
    b = registry.uploads[-1]
    assert b["parent_model"].endswith("/churn-gke-classifier") and b["model_id"] is None
    assert b["is_default_version"] is False and b["labels"]["status"] == "candidate"     # manual: not the default
    assert "approval" not in b["labels"]
    assert out.metadata["versionId"] == "2" and events[-1][0] == "model.registered"


def test_an_id_held_by_another_display_name_falls_back_to_a_generated_id(monkeypatch, tmp_path):
    """Edge case: Churn-GKE_Classifier and churn-gke-classifier map to one ID; the second use case's first version
    must not wait for a model of its own name that never comes."""
    registry = Registry()
    run, _ = _register(monkeypatch, tmp_path, registry)
    registry.Model.upload(display_name="Churn-GKE_Classifier", parent_model=None, model_id="churn-gke-classifier")
    out = run("r1")
    assert len(registry.models) == 2 and registry.uploads[-1]["model_id"] is None
    assert out.metadata["resourceName"] != "projects/p/locations/r/models/churn-gke-classifier"
    assert out.metadata["versionId"] == "1"


def test_a_first_version_is_made_under_the_fixed_id(monkeypatch, tmp_path):
    registry = Registry()
    run, _ = _register(monkeypatch, tmp_path, registry)
    run("r1")
    first = registry.uploads[0]
    assert first["model_id"] == "churn-gke-classifier" and first["parent_model"] is None
    assert first["is_default_version"] is True and first["labels"]["approval"] == "pending"
    run("r2")
    assert registry.uploads[1]["parent_model"].endswith("/churn-gke-classifier")


def test_a_replay_says_it_registered_nothing(monkeypatch, tmp_path):
    registry = Registry()
    run, events = _register(monkeypatch, tmp_path, registry)
    report = json.loads((tmp_path / "report.json").read_text())
    report["champion"] = {"auc": 0.809, "_version": "3"}
    (tmp_path / "report.json").write_text(json.dumps(report))
    run("r9", source_uri="snapshot://churn/2026-11-01")
    assert registry.uploads == []
    ((kind, rid, data),) = events
    assert (kind, rid, data["reason"]) == ("model.registration_skipped", "r9", "replay")


def test_bigquery_ml_first_version_race_becomes_a_version():
    posts = []

    class Session:
        def get(self, url, timeout=None):
            if url.endswith("/models/demand"):
                return SimpleNamespace(status_code=404)
            return SimpleNamespace(json=lambda: {"done": True, "response": {"modelVersionId": "2"}})

        def post(self, url, timeout=None, json=None):
            posts.append(json)
            if "modelId" in json:
                return SimpleNamespace(status_code=409, text="already exists")
            return SimpleNamespace(status_code=200, json=lambda: {"name": "op/1"})

    version = bqml_runtime.upload_version("p", "r", "demand", {}, "run r2", session=Session())
    assert version == "2" and "modelId" in posts[0] and posts[1]["parentModel"].endswith("/models/demand")


def test_bigquery_ml_conflict_in_the_operation_is_handled_too():
    posts = []

    class Session:
        def get(self, url, timeout=None):
            if url.endswith("/models/demand"):
                return SimpleNamespace(status_code=404)
            done = {"done": True, "error": {"code": 6, "message": "exists"}} if url.endswith("op/1") else \
                {"done": True, "response": {"modelVersionId": "2"}}
            return SimpleNamespace(json=lambda: done)

        def post(self, url, timeout=None, json=None):
            posts.append(json)
            return SimpleNamespace(status_code=200, json=lambda: {"name": f"op/{len(posts)}"})

    assert bqml_runtime.upload_version("p", "r", "demand", {}, "run r2", session=Session()) == "2"
    assert "parentModel" in posts[1]


# ------------------------------------------------------------------------------------ the launcher's tick --
NOW = datetime(2026, 11, 9, 19, 0, tzinfo=timezone.utc)


def _job(rid, ended, usecase="churn-gke", pipeline="training"):
    return {"name": f"projects/p/locations/r/pipelineJobs/churn-training-{rid}", "state": "PIPELINE_STATE_SUCCEEDED",
            "endTime": ended, "labels": {"pipewright-usecase": usecase, "pipewright-pipeline": pipeline,
                                         "run-id": rid}}


def _event(kind, rid, **data):
    return {"jsonPayload": {"pipewright_event": {"event_type": kind, "run_id": rid, "data": data}}}


class Gcp:
    def __init__(self, jobs, events, logs_ok=True):
        self.jobs, self.events, self.logs_ok, self.filters, self.markers = jobs, events, logs_ok, [], {}

    def get(self, url, **params):
        return SimpleNamespace(ok=True, status=200, body={"pipelineJobs": self.jobs})

    def post(self, url, body):
        self.filters.append(body)
        if not self.logs_ok:
            return SimpleNamespace(ok=False, status=403, body={})
        rid = body["filter"].split('run_id="')[1].split('"')[0]
        return SimpleNamespace(ok=True, status=200, body={"entries": self.events.get(rid, [])})


def _patch_markers(monkeypatch, gcp):
    def write(g, bucket, name, text, if_generation_match=None):
        if name in gcp.markers:
            raise PreconditionFailed(name)
        gcp.markers[name] = json.loads(text)
        return 1

    monkeypatch.setattr(unregistered, "gcs_write", write)
    monkeypatch.setattr(unregistered, "gcs_read", lambda g, b, name: (json.dumps(gcp.markers[name]), 1)
                        if name in gcp.markers else (None, 0))


def test_an_approved_run_without_a_registration_is_reported_once(monkeypatch):
    ended = "2026-11-09T18:16:00Z"
    gcp = Gcp([_job("r4eca", ended), _job("r9e5", ended), _job("rrej", ended), _job("rrep", ended),
               _job("rold", ended), _job("rnew", "2026-11-09T18:55:00Z"),
               _job("rinf", ended, pipeline="inference"), _job("rother", ended, usecase="not-live")],
              {"r4eca": [_event("training.evaluated", "r4eca", decision="approve", primary_metric="auc",
                                primary_value=0.809)],
               "r9e5": [_event("training.evaluated", "r9e5", decision="approve"), _event("model.registered", "r9e5")],
               "rrej": [_event("training.evaluated", "rrej", decision="reject")],
               "rrep": [_event("training.evaluated", "rrep", decision="approve"),
                        _event("model.registration_skipped", "rrep", reason="replay")],
               "rnew": [_event("training.evaluated", "rnew", decision="approve")]})
    _patch_markers(monkeypatch, gcp)
    emitted = []
    contract = SimpleNamespace(usecase="churn-gke")
    first = unregistered.report(gcp, "b", "p", "r", [contract], lambda c, rid, d: emitted.append((rid, d)), NOW)
    again = unregistered.report(gcp, "b", "p", "r", [contract], lambda c, rid, d: emitted.append((rid, d)), NOW)
    assert [o["run_id"] for o in first] == ["r4eca"] and again == []
    ((rid, detail),) = emitted
    assert rid == "r4eca" and detail["status"] == "unregistered" and detail["primary_value"] == 0.809
    statuses = {k.rsplit("-", 1)[-1]: v["status"] for k, v in gcp.markers.items()}
    assert statuses == {"r4eca": "unregistered", "r9e5": "registered", "rrej": "rejected", "rrep": "skipped",
                        "rold": "not_checkable"}           # rnew ended 5 minutes ago: the next tick


def test_the_log_read_uses_the_platforms_bucket(monkeypatch):
    gcp = Gcp([_job("r1", "2026-11-09T18:00:00Z")], {})
    _patch_markers(monkeypatch, gcp)
    unregistered.report(gcp, "b", "p", "r", [SimpleNamespace(usecase="churn-gke")], lambda *a: None, NOW,
                        log_bucket="projects/p/locations/r/buckets/pipewright")
    assert gcp.filters[0]["resourceNames"] == ["projects/p/locations/r/buckets/pipewright/views/_AllLogs"]


def test_without_log_access_it_says_so_once(monkeypatch):
    gcp = Gcp([_job("r1", "2026-11-09T18:00:00Z"), _job("r2", "2026-11-09T18:00:00Z")], {}, logs_ok=False)
    _patch_markers(monkeypatch, gcp)
    out = unregistered.report(gcp, "b", "p", "r", [SimpleNamespace(usecase="churn-gke")], lambda *a: None, NOW)
    assert len(out) == 1 and "logging.viewer" in out[0]["error"] and gcp.markers == {}


def test_kubeflow_runs_are_looked_at_too(monkeypatch):
    from pipewright.adapters import kfp

    run = {"display_name": "churn-gke-r4eca", "state": "SUCCEEDED", "finished_at": "2026-11-09T18:16:00Z",
           "created_at": "2026-11-09T17:48:00Z",
           "description": kfp.run_description({"pipewright-usecase": "churn-gke", "run-id": "r4eca",
                                                "pipewright-pipeline": "training"})}

    class Kube:
        def get(self, path, **params):
            return SimpleNamespace(ok=True, status=200, body={"runs": [run, {**run, "state": "FAILED"}]})

    monkeypatch.setattr("pipewright.kube.KubeClient.from_manifest", lambda manifest, gcp: Kube())
    jobs = unregistered.kfp_succeeded_jobs({"kubernetes": {}}, None, NOW - timedelta(hours=6))
    assert [j["job_id"] for j in jobs] == ["churn-gke-r4eca"] and jobs[0]["labels"]["run-id"] == "r4eca"


def test_run_unregistered_may_send_an_email(repo):
    from pipewright.alerts import sync as alerts
    from pipewright.config import load_environment
    from tests.test_alerts import FakeMonitoring, uc_for

    uc = uc_for(repo, notify=["ml-team@example.com"], events=["run.unregistered"])
    mon = FakeMonitoring()
    alerts.sync(uc, load_environment(repo.root, "dev"), mon)
    (policy,) = mon.policies.values()
    assert 'event_type="run.unregistered"' in str(policy) and "approved model was not registered" in str(policy)


@pytest.mark.parametrize("path", ["src/pipewright/launcher/unregistered.py", "src/pipewright/models/names.py"])
def test_no_em_dashes(path):
    assert "—" not in (ROOT / path).read_text(encoding="utf-8")


# ------------------------------------------------------------------ the champion rule under parallel runs --
def _gcp_for(versions, jobs):
    class G:
        def __init__(self, creds=None):
            pass

        def get(self, url, **params):
            if url.endswith(":listVersions"):
                return SimpleNamespace(ok=True, body={"models": versions})
            return SimpleNamespace(ok=True, body={"pipelineJobs": [j for j in jobs if params["filter"].endswith(
                f'"{j["run"]}"')]})
    return G


def test_a_version_trained_beside_another_needed_no_champion(monkeypatch):
    from pipewright.models import registry

    env = SimpleNamespace(region="r", project="p")
    versions = [{"versionId": "1", "versionCreateTime": "2026-11-10T10:52:35Z", "labels": {"pipewright-run": "ra"}},
                {"versionId": "2", "versionCreateTime": "2026-11-10T10:52:50Z", "labels": {"run_id": "run-rb"}}]
    # both runs started at 10:42, before v1 was registered: v2 had no champion to compare with
    monkeypatch.setattr("pipewright.gcp.Gcp", _gcp_for(versions, [{"run": "rb", "createTime": "2026-11-10T10:42:24Z"}]))
    assert registry.champion_expected(env, None, "m", "2", {"1": 1, "2": 2}) is False
    # a run that started after v1 was registered had one
    monkeypatch.setattr("pipewright.gcp.Gcp", _gcp_for(versions, [{"run": "rb", "createTime": "2026-11-10T11:00:00Z"}]))
    assert registry.champion_expected(env, None, "m", "2", {"1": 1, "2": 2}) is True
    # the run cannot be found (Kubeflow): the stricter rule, as before
    monkeypatch.setattr("pipewright.gcp.Gcp", _gcp_for(versions, []))
    assert registry.champion_expected(env, None, "m", "2", {"1": 1, "2": 2}) is True
    assert registry.champion_expected(env, None, "m", "1", {"1": 1, "2": 2}) is False


def test_a_second_model_with_the_name_is_said_once(monkeypatch):
    """QA edge case: an old register step made a duplicate; both runs sent model.registered, so only the registry
    shows it. The tick says so once, naming the model it keeps (the oldest)."""
    models = [{"name": "projects/p/locations/r/models/222", "createTime": "2026-11-10T10:56:00Z"},
              {"name": "projects/p/locations/r/models/111", "createTime": "2026-11-10T10:55:00Z"}]
    gcp = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok=True, body={"models": models}), markers={})
    _patch_markers(monkeypatch, gcp)
    emitted = []
    contract = SimpleNamespace(usecase="churn-gke", metadata={"model": "churn-gke-classifier"})
    first = unregistered.duplicates(gcp, "b", "p", "r", [contract], lambda c, rid, d: emitted.append(d))
    again = unregistered.duplicates(gcp, "b", "p", "r", [contract], lambda c, rid, d: emitted.append(d))
    assert len(first) == 1 and again == [] and len(emitted) == 1
    assert emitted[0]["kept"].endswith("/111") and emitted[0]["duplicate"].endswith("/222")
    one = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok=True, body={"models": models[:1]}), markers={})
    _patch_markers(monkeypatch, one)
    assert unregistered.duplicates(one, "b", "p", "r", [contract], lambda *a: None) == []
