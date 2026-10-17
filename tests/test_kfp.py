"""Orchestrator kfp (the private-gke blueprint): runs through the Kubeflow Pipelines API behind the cluster's Kubernetes
API, and Ray clusters as KubeRay RayClusters (pipewright.adapters.kfp, pipewright.ray_clusters.service, the runtime)."""

import copy
import importlib.util
import json
from dataclasses import replace
from pathlib import Path

import pytest

from pipewright import kube
from pipewright.adapters import kfp
from pipewright.adapters.vertex import AlreadySubmitted
from pipewright.config.schema import validate
from pipewright.kube import Response
from pipewright.ray_clusters import settings
from pipewright.ray_clusters.service import RayClusters, RayRequestError, pod_resources, raycluster_body
from tests.conftest import MANIFEST as BASE_MANIFEST
from tests.test_ray import RAY_CONTRACT, RUNNER

MANIFEST = {"orchestrator": "kfp",
            "kubernetes": {"endpoint": "https://gke.example", "ca_certificate": None, "namespace": "kubeflow"},
            "tenants": {"nimbus": {"runner_service_account": RUNNER, "bigquery_dataset": "d",
                                   "kubernetes_service_account": "runner-nimbus"}}}


class FakeKube:
    """The Kubernetes API with the Kubeflow Pipelines API behind its service proxy, in memory."""

    def __init__(self):
        self.runs, self.clusters, self.calls = [], {}, []

    def get(self, path, **params):
        self.calls.append(("GET", path, params))
        if path.endswith("/runs"):
            predicate = json.loads(params["filter"])["predicates"][0]
            value = predicate["string_value"]
            match = (lambda n: n == value) if predicate["operation"] == "EQUALS" else (lambda n: value in n)
            return Response(200, {"runs": [r for r in self.runs if match(r["display_name"])]})
        return Response(200, {"items": list(self.clusters.values())})

    def post(self, path, body):
        self.calls.append(("POST", path, body))
        if path.endswith("/runs"):
            self.runs.append({**body, "run_id": f"id-{len(self.runs)}", "state": "PENDING"})
            return Response(200, self.runs[-1])
        name = body["metadata"]["name"]
        if name in self.clusters:
            return Response(409, {"message": "exists"})
        self.clusters[name] = {**body, "metadata": {**body["metadata"], "creationTimestamp": "1970-01-01T00:00:00Z"}}
        return Response(201, body)

    def delete(self, path):
        self.calls.append(("DELETE", path, None))
        return Response(200, {}) if self.clusters.pop(path.rsplit("/", 1)[-1], None) else Response(404, {})


class FakeGcp:
    class session:  # noqa: N801 - stands in for an AuthorizedSession
        @staticmethod
        def get(url, timeout):
            class R:
                status_code = 200
                text = "pipelineInfo: {name: train}\n---\nplatforms: {kubernetes: {}}\n"
            return R()


@pytest.fixture
def fake(monkeypatch):
    k = FakeKube()
    monkeypatch.setattr(kube.KubeClient, "from_manifest", classmethod(lambda cls, manifest, gcp: k))
    return k


def contract():
    return replace(RAY_CONTRACT, tenant="nimbus")


# ---------------------------------------------------------------- runs

def test_submit_creates_a_run_as_the_tenants_kubernetes_service_account(fake):
    c = contract()
    name = kfp.submit(c, c.pipeline("training"), {"run_id": "r1"}, "churn-r1", {"run-id": "r1"}, MANIFEST, FakeGcp())
    assert name == "kubeflow:kubeflow/runs/id-0"
    method, path, body = fake.calls[-1]
    assert path == "/api/v1/namespaces/kubeflow/services/ml-pipeline:8888/proxy/apis/v2beta1/runs"
    assert body["service_account"] == "runner-nimbus" and body["display_name"] == "churn-r1"
    assert body["runtime_config"] == {"parameters": {"run_id": "r1"}, "pipeline_root": c.pipeline_root}
    assert body["pipeline_spec"] == {"pipeline_spec": {"pipelineInfo": {"name": "train"}},
                                     "platform_spec": {"platforms": {"kubernetes": {}}}}
    with pytest.raises(AlreadySubmitted):                                   # the same job ID again: a duplicate
        kfp.submit(c, c.pipeline("training"), {}, "churn-r1", {}, MANIFEST, FakeGcp())
    assert kfp.run_state(MANIFEST, FakeGcp(), "churn-r1") == "PENDING"


def test_submit_needs_the_tenants_kubernetes_service_account(fake):
    c = replace(contract(), tenant="other")
    with pytest.raises(RuntimeError, match="no kubernetes_service_account"):
        kfp.submit(c, c.pipeline("training"), {}, "j", {}, MANIFEST, FakeGcp())


def test_manifest_schema_takes_the_kubernetes_block():
    manifest = copy.deepcopy(BASE_MANIFEST)
    manifest.update(orchestrator="kfp", kubernetes=MANIFEST["kubernetes"])
    for tenant in manifest["tenants"].values():
        tenant["kubernetes_service_account"] = "runner-nimbus"
    validate(manifest, "manifest", "m.json")


# ---------------------------------------------------------------- Ray on GKE (KubeRay)

def test_head_size_minimum_is_agent_platforms_only():
    s = settings.platform_settings({"limits": {"machines": ["e2-standard-8", "e2-standard-16"]}}, None)
    _, problems = settings.resolve_steps({"tune": {"ray": {"head_machine": "e2-standard-8"}}}, s)
    assert problems and "a Ray head node needs at least" in problems[0]
    clusters, problems = settings.resolve_steps({"tune": {"ray": {}}}, s, orchestrator="kfp")
    assert problems == [] and clusters["tune"]["head_machine"] == "e2-standard-8"   # the first allowed machine


def test_raycluster_runs_on_the_ray_pool_as_the_tenant():
    body = raycluster_body(contract(), "tune-on-ray", "run-1", {"ray_version": "2.47"}, "kubeflow", "runner-nimbus",
                           now=1000)
    assert body["metadata"]["name"] == settings.kube_cluster_name("churn", "run-1", "tune-on-ray")
    assert len(body["metadata"]["name"] + "-head-svc") <= 50       # KubeRay keeps the head's Service name as is
    assert body["spec"]["rayVersion"] == "2.47.1"
    head = body["spec"]["headGroupSpec"]["template"]["spec"]
    assert head["serviceAccountName"] == "runner-nimbus" and head["nodeSelector"] == {"pipewright.dev/pool": "ray"}
    assert head["tolerations"][0]["key"] == "pipewright.dev/ray"
    assert head["containers"][0]["image"] == RAY_CONTRACT.ray["image"]
    workers = body["spec"]["workerGroupSpecs"][0]
    assert workers["replicas"] == workers["minReplicas"] == workers["maxReplicas"] == 2
    assert body["metadata"]["labels"]["pipewright-deadline"] == str(1000 + 60 * (60 + 30))
    assert pod_resources("e2-standard-8")["requests"] == {"cpu": "6800m", "memory": "25Gi"}
    with pytest.raises(RayRequestError, match="not known"):
        pod_resources("custom-4-16384")


def test_kuberay_create_delete_and_cleanup(fake):
    rays = RayClusters(None, lambda: MANIFEST)
    c = contract()
    first = rays.create(c, "tune-on-ray", "run-1", RUNNER)
    assert first["status"] == "creating" and first["record"]["kubernetes_service_account"] == "runner-nimbus"
    assert first["record"]["pools"] == [{"machine": "n1-standard-4", "nodes": 1}, {"machine": "n1-standard-4",
                                                                                   "nodes": 2}]
    assert rays.create(c, "tune-on-ray", "run-1", RUNNER)["status"] == "exists"
    with pytest.raises(RayRequestError, match="only its runner identity may"):
        rays.create(c, "tune-on-ray", "run-1", "someone@else.com")
    assert rays.delete(c, "tune-on-ray", "run-1", RUNNER)["status"] == "deleting"
    assert rays.delete(c, "tune-on-ray", "run-1", RUNNER)["status"] == "gone"

    for run in ("running", "done", "orphan"):
        rays.create(c, "tune-on-ray", run, RUNNER)
    fake.runs += [{"display_name": "churn-running-train", "state": "RUNNING"},
                  {"display_name": "churn-done-train", "state": "SUCCEEDED"}]
    reaped = {r["cluster"]: r["reason"] for r in rays.reap("p", "r", now=2000)}
    assert reaped == {settings.kube_cluster_name("churn", "done", "tune-on-ray"): "its run ended (succeeded)",
                      settings.kube_cluster_name("churn", "orphan", "tune-on-ray"): "no pipeline run has its run ID"}
    assert list(fake.clusters) == [settings.kube_cluster_name("churn", "running", "tune-on-ray")]


def runtime_module():
    spec = importlib.util.spec_from_file_location(
        "runtime_kfp", Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_names_match_and_the_step_waits_for_the_head(monkeypatch):
    import time as real_time

    runtime = runtime_module()
    for args in [("churn", "r1", "tune-on-ray"), ("a-very-long-use-case-name-indeed", "r2", "Tune_On Ray")]:
        assert runtime.ray_kube_name(*args) == settings.kube_cluster_name(*args)
    clock = {"t": 1000.0}
    monkeypatch.setattr(real_time, "time", lambda: clock["t"])
    monkeypatch.setattr(real_time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    sent = []
    monkeypatch.setattr(runtime, "_ray_request", lambda cfg, action, step, run: sent.append(action))
    monkeypatch.setattr(runtime, "log", lambda *a, **k: None)
    states = iter([{}, {"state": "PENDING"}, {"state": "RUNNING"}])
    monkeypatch.setattr(runtime._RayCluster, "_state", lambda self: next(states))
    ml = {"usecase": "churn", "platform": {"project": "p", "region": "r", "orchestrator": "kfp",
                                           "kubernetes_namespace": "kubeflow",
                                           "ray": {"steps": {"tune-on-ray": {"max_minutes": 60}}}}}
    with runtime.ray_cluster(ml, "tune-on-ray", "run-1") as cluster:
        name = runtime.ray_kube_name("churn", "run-1", "tune-on-ray")
        assert cluster == f"http://{name}-head-svc.kubeflow.svc:8265"
    assert sent == ["create", "delete"]


def test_run_description_fits_kubeflows_column_and_round_trips():
    labels = {"pipewright-usecase": "churn-feats-ray", "run-id": "rd7ff22995721", "pipewright-pipeline": "training",
              "pipewright-release": "7bb52f1bcc79-dirty-1792226245", "pipewright-tenant": "churn", "trigger": "api"}
    text = kfp.run_description(labels)
    assert len(text) <= kfp.DESCRIPTION_MAX and kfp.run_labels(text) == labels
    long = {k: "x" * 63 for k in labels}                       # every label at its maximum length
    text = kfp.run_description(long)
    kept = kfp.run_labels(text)
    assert len(text) <= kfp.DESCRIPTION_MAX and kept["pipewright-usecase"] == "x" * 63 and "trigger" not in kept
    assert kfp.run_labels("not json") == {} and kfp.run_labels(None) == {}
    assert kfp.run_labels('{"labels": {"pipewright-usecase": "churn"}}') == {"pipewright-usecase": "churn"}


def test_oneof_branch_tasks_run_uncached_on_kubeflow():
    spec = {"pipeline_spec": {"root": {"dag": {"tasks": {
                "prepare": {"cachingOptions": {"enableCache": True}},
                "condition-branches-1": {"componentRef": {"name": "comp-condition-branches-1"}},
                "train": {"cachingOptions": {"enableCache": True}}}}},
            "components": {
                "comp-condition-branches-1": {"dag": {"tasks": {
                    "condition-3": {"componentRef": {"name": "comp-condition-3"}},
                    "condition-4": {"componentRef": {"name": "comp-condition-4"}}}}},
                "comp-condition-3": {"dag": {"tasks": {
                    "for-loop-2": {"componentRef": {"name": "comp-for-loop-2"}},
                    "select-best": {"cachingOptions": {"enableCache": True}}}}},
                "comp-for-loop-2": {"dag": {"tasks": {"tune-trial": {"cachingOptions": {"enableCache": True}}}}},
                "comp-condition-4": {"dag": {"tasks": {"use-defaults": {"cachingOptions": {"enableCache": True}}}}},
                "comp-train": {"executorLabel": "exec-train"}}}}
    assert sorted(kfp.uncache_branches(spec)) == ["select-best", "tune-trial", "use-defaults"]
    tasks = spec["pipeline_spec"]["root"]["dag"]["tasks"]
    assert tasks["prepare"]["cachingOptions"] == {"enableCache": True}            # outside the branches: unchanged
    defaults = spec["pipeline_spec"]["components"]["comp-condition-4"]["dag"]["tasks"]["use-defaults"]
    assert defaults["cachingOptions"] == {}
