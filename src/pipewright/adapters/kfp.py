"""Kubeflow Pipelines adapter (orchestrator kfp, the private-gke blueprint).

A run is created through the Kubeflow Pipelines v2beta1 API, reached through the cluster's Kubernetes API (a service
proxy): the caller authenticates with its own Google identity and RBAC lets it call only that API, so the Kubeflow
Pipelines endpoint is never exposed. The run uses the same compiled template, parameters and pipeline root as on
Agent Platform, and runs as the tenant's Kubernetes service account, bound to its runner (Workload Identity).

Kubeflow Pipelines has no client-chosen run ID, so the deterministic job ID is the run's display name, and a run
with that name already present is a duplicate delivery (AlreadySubmitted).
"""

from __future__ import annotations

import json
import logging

import yaml

from pipewright.adapters.vertex import AlreadySubmitted
from pipewright.kube import KubeClient, message
from pipewright.release.contract import PipelineTarget, ReleaseContract

log = logging.getLogger("pipewright.launcher")


DESCRIPTION_MAX = 255     # Kubeflow Pipelines stores a run's description in a VARCHAR(255) column
# The labels a run keeps (Kubeflow Pipelines v2 runs have no labels of their own), most important first: what does not
# fit in the description is left out.
_DESCRIBED = ("pipewright-usecase", "run-id", "pipewright-pipeline", "pipewright-release", "pipewright-tenant",
              "trigger")


def run_description(labels: dict) -> str:
    """The run's labels as compact JSON within DESCRIPTION_MAX: {"pw": {"usecase": ..., "run-id": ...}}."""
    kept: dict = {}
    for key in _DESCRIBED:
        if labels.get(key) is None:
            continue
        trial = {**kept, key.removeprefix("pipewright-"): labels[key]}
        if len(json.dumps({"pw": trial}, separators=(",", ":"))) > DESCRIPTION_MAX:
            break
        kept = trial
    return json.dumps({"pw": kept}, separators=(",", ":"))


def run_labels(description: str | None) -> dict:
    """The labels run_description kept, under their label names (pipewright-usecase, run-id, ...); {} otherwise."""
    try:
        doc = json.loads(description or "{}") or {}
        if isinstance(doc.get("labels"), dict):      # runs submitted before the compact form
            return doc["labels"]
        kept = doc.get("pw") or {}
    except (ValueError, AttributeError):
        return {}
    names = {k.removeprefix("pipewright-"): k for k in _DESCRIBED}
    return {names.get(k, k): v for k, v in kept.items()} if isinstance(kept, dict) else {}


def api_path(namespace: str) -> str:
    return f"/api/v1/namespaces/{namespace}/services/ml-pipeline:8888/proxy/apis/v2beta1"


def template(gcp, template_uri: str) -> dict:
    """The compiled pipeline from the Artifact Registry KFP repository: the pipeline spec, and its platform spec
    when the template has one (a second YAML document)."""
    r = gcp.session.get(template_uri, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"reading {template_uri} failed: {r.status_code} {r.text[:300]}")
    docs = [d for d in yaml.safe_load_all(r.text) if d]
    if not docs:
        raise RuntimeError(f"{template_uri} is empty")
    if len(docs) == 1:
        return docs[0]
    return {"pipeline_spec": docs[0], "platform_spec": docs[1]}


def uncache_branches(spec: dict) -> list[str]:
    """Turns caching off for the tasks inside dsl.OneOf branches (components `comp-condition-branches-*` and
    everything under them), in place; returns their names. Kubeflow Pipelines resolves a OneOf output only from a
    branch task that ran: when the chosen branch's task was a cache hit, the next step fails ("No successful task
    found for output key", found 2026-10-17 on 2.17.2). Agent Platform resolves cached branches, so templates keep
    their caching and only runs on Kubeflow change."""
    pipeline = spec.get("pipeline_spec", spec)
    components = pipeline.get("components") or {}
    changed: list[str] = []

    def walk(component: str, seen: set) -> None:
        if component in seen:
            return
        seen.add(component)
        for name, task in ((components.get(component) or {}).get("dag") or {}).get("tasks", {}).items():
            if (task.get("cachingOptions") or {}).get("enableCache"):
                task["cachingOptions"] = {}
                changed.append(name)
            child = (task.get("componentRef") or {}).get("name")
            if child:
                walk(child, seen)

    seen: set = set()
    for component in components:
        if component.startswith("comp-condition-branches-"):
            walk(component, seen)
    return changed


def find(kube: KubeClient, namespace: str, display_name: str) -> dict | None:
    flt = json.dumps({"predicates": [{"key": "display_name", "operation": "EQUALS", "string_value": display_name}]})
    r = kube.get(f"{api_path(namespace)}/runs", filter=flt, page_size=5)
    if not r.ok:
        raise RuntimeError(f"listing Kubeflow runs failed: {r.status} {message(r)}")
    runs = (r.body or {}).get("runs") or []
    return runs[0] if runs else None


def submit(contract: ReleaseContract, target: PipelineTarget, parameters: dict, job_id: str, labels: dict,
           manifest: dict, gcp) -> str:
    k8s = manifest.get("kubernetes") or {}
    namespace = k8s.get("namespace") or "kubeflow"
    tenant = (manifest.get("tenants") or {}).get(contract.tenant) or {}
    service_account = tenant.get("kubernetes_service_account")
    if not service_account:
        raise RuntimeError(f"tenant {contract.tenant} has no kubernetes_service_account in the environment manifest")
    kube = KubeClient.from_manifest(manifest, gcp)
    if find(kube, namespace, job_id):
        raise AlreadySubmitted(job_id)
    spec = template(gcp, target.template_uri)
    uncached = uncache_branches(spec)
    if uncached:
        log.info("kubeflow run %s: no caching for OneOf branch tasks %s", job_id, ", ".join(sorted(uncached)))
    body = {
        "display_name": job_id,
        "description": run_description(labels),
        "pipeline_spec": spec,
        "runtime_config": {"parameters": parameters, "pipeline_root": contract.pipeline_root},
        "service_account": service_account,
    }
    r = kube.post(f"{api_path(namespace)}/runs", body)
    if not r.ok:
        raise RuntimeError(f"creating Kubeflow run {job_id} failed: {r.status} {message(r)}")
    run_id = (r.body or {}).get("run_id")
    log.info("kubeflow run %s (%s) as %s", run_id, job_id, service_account)
    return f"kubeflow:{namespace}/runs/{run_id}"


def run_state(manifest: dict, gcp, job_id: str) -> str | None:
    """The run's state (PENDING, RUNNING, SUCCEEDED, FAILED, ...) by its job ID, or None when there is no such run."""
    k8s = manifest.get("kubernetes") or {}
    run = find(KubeClient.from_manifest(manifest, gcp), k8s.get("namespace") or "kubeflow", job_id)
    return run.get("state") if run else None
