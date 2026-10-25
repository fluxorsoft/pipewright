"""Orchestrator adapters: `vertex` (Agent Platform Pipelines) and `kfp` (Kubeflow Pipelines, private-gke)."""

from __future__ import annotations

from pipewright.release.contract import PipelineTarget, ReleaseContract


def submit(contract: ReleaseContract, target: PipelineTarget, parameters: dict, job_id: str, labels: dict,
           security: dict | None = None, manifest: dict | None = None, gcp=None) -> str:
    """`security` is the manifest's security block (CMEK key, network): it is read at submit time, so the platform
    can change it without app teams re-releasing. `manifest` and `gcp` are needed by kfp (the cluster and the
    tenant's Kubernetes service account)."""
    if contract.orchestrator == "vertex":
        from pipewright.adapters.vertex import submit as vertex_submit

        return vertex_submit(contract, target, parameters, job_id, labels, security)
    if contract.orchestrator == "kfp":
        from pipewright.adapters.kfp import submit as kfp_submit

        if manifest is None or gcp is None:
            raise ValueError("orchestrator kfp needs the environment manifest and a Google session")
        return kfp_submit(contract, target, parameters, job_id, labels, manifest, gcp)
    raise NotImplementedError(f"orchestrator {contract.orchestrator!r} is not supported yet")


def job_state(contract: ReleaseContract, job_id: str, manifest: dict | None = None, gcp=None) -> tuple:
    """(state, error message) of a submitted run, by its job ID; state None when there is no such run (yet)."""
    if contract.orchestrator == "kfp":
        from pipewright.adapters.kfp import find
        from pipewright.kube import KubeClient

        k8s = (manifest or {}).get("kubernetes") or {}
        run = find(KubeClient.from_manifest(manifest or {}, gcp), k8s.get("namespace") or "kubeflow", job_id)
        if not run:
            return None, ""
        return run.get("state"), str((run.get("error") or {}).get("message") or "")
    from pipewright.gcp import Gcp

    gcp = gcp or Gcp()
    r = gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/projects/{contract.project}/locations/"
                f"{contract.region}/pipelineJobs/{job_id}")
    if r.status == 404:
        return None, ""
    if not r.ok:
        raise RuntimeError(f"pipeline job {job_id}: HTTP {r.status}")
    return (r.body or {}).get("state"), str(((r.body or {}).get("error") or {}).get("message") or "")
