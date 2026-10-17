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
