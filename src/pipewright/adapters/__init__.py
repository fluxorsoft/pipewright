"""Orchestrator adapters. `vertex` today; `kfp` arrives with the private-gke blueprint."""

from __future__ import annotations

from pipewright.release.contract import PipelineTarget, ReleaseContract


def submit(contract: ReleaseContract, target: PipelineTarget, parameters: dict, job_id: str, labels: dict,
           security: dict | None = None) -> str:
    """`security` is the manifest's security block (CMEK key, network): it is read at submit time, so the platform
    can change it without app teams re-releasing."""
    if contract.orchestrator == "vertex":
        from pipewright.adapters.vertex import submit as vertex_submit

        return vertex_submit(contract, target, parameters, job_id, labels, security)
    raise NotImplementedError(f"orchestrator {contract.orchestrator!r} is not supported yet")
