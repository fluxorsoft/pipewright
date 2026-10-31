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


def job_reasons(contract: ReleaseContract, job_id: str, gcp=None) -> list[str]:
    """Why a finished training run's evaluation refused its model: the evaluate step's `reasons` output (Pipewright
    1.2, FR-79). Agent Platform only; [] when the run has none or cannot be read."""
    import json

    if contract.orchestrator == "kfp":
        return []
    from pipewright.gcp import Gcp

    gcp = gcp or Gcp()
    r = gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/projects/{contract.project}/locations/"
                f"{contract.region}/pipelineJobs/{job_id}")
    if not r.ok:
        return []
    for task in ((r.body or {}).get("jobDetail") or {}).get("taskDetails") or []:
        for key, value in ((task.get("execution") or {}).get("metadata") or {}).items():
            if key.rsplit(":", 1)[-1] not in ("reasons", "decision_reasons"):
                continue
            try:
                found = json.loads(value) if isinstance(value, str) else value
            except ValueError:
                found = [value]
            if found:
                return [str(x) for x in (found if isinstance(found, list) else [found])][:20]
    return []


def job_state(contract: ReleaseContract, job_id: str, manifest: dict | None = None, gcp=None) -> tuple:
    """(state, error message) of a submitted run, by its job ID; state None when there is no such run (yet)."""
    if contract.orchestrator == "kfp":
        from pipewright.adapters.kfp import find
        from pipewright.kube import KubeClient

        k8s = (manifest or {}).get("kubernetes") or {}
        kube = KubeClient.from_manifest(manifest or {}, gcp)
        run = find(kube, k8s.get("namespace") or "kubeflow", job_id)
        if not run:
            return None, ""
        message = str((run.get("error") or {}).get("message") or "")
        if not message and str(run.get("state")).upper() == "FAILED":
            message = _failed_steps(kube, k8s.get("namespace") or "kubeflow", run)
        return run.get("state"), message
    from pipewright.gcp import Gcp

    gcp = gcp or Gcp()
    r = gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/projects/{contract.project}/locations/"
                f"{contract.region}/pipelineJobs/{job_id}")
    if r.status == 404:
        return None, ""
    if not r.ok:
        raise RuntimeError(f"pipeline job {job_id}: HTTP {r.status}")
    return (r.body or {}).get("state"), str(((r.body or {}).get("error") or {}).get("message") or "")



def _failed_steps(kube, namespace: str, run: dict) -> str:
    """Kubeflow keeps a failed run's error in its step's log, not on the run: name the steps that failed."""
    from pipewright.adapters.kfp import api_path

    r = kube.get(f"{api_path(namespace)}/runs/{run.get('run_id')}")
    tasks = ((r.body or {}).get("run_details") or {}).get("task_details") or [] if r.ok else []
    noise = ("driver", "executor", "root", "condition")
    steps = sorted({t.get("display_name", "").split("(")[0] for t in tasks if t.get("state") == "FAILED"
                    and not any(w in t.get("display_name", "") for w in noise)} - {""})
    return (f"step {', '.join(steps)} failed (Kubeflow keeps the error in the step's log)" if steps
            else "the run failed (Kubeflow keeps the error in the step's log)")
