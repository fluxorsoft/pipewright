"""Failed runs (Pipewright 1.2, FR-67): the launcher's hourly tick (/retraining/tick) finds the pipeline runs it
started that ended failed or cancelled, and records one `run.failed` event for each: the failed steps and their
error lines, which Pipewright's steps keep free of data values. `monitoring.alerts.events: [run.failed]` emails it,
so a data contract that stops a run, or a broken release, reaches the team without a log-based alert of its own.

A marker in the artifacts bucket (runs/failed/<job>) makes each run reported once, whatever the retries and
overlapping ticks. Agent Platform runs are listed in Agent Platform; Kubeflow runs (private-gke) in the cluster, through
the same Kubernetes API proxy the launcher submits them with.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone

from pipewright.gcp import PreconditionFailed, gcs_write

log = logging.getLogger(__name__)

FAILED = ("PIPELINE_STATE_FAILED", "PIPELINE_STATE_CANCELLED")
MARKERS = "runs/failed"
LOOKBACK = timedelta(hours=6)          # the tick is hourly; a missed tick or two is still caught
MAX_ERROR = 300


def _label(v: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", v.lower())[:63]


def failed_jobs(gcp, project: str, region: str, since: datetime) -> list[dict]:
    """Pipeline jobs Pipewright started (labelled pipewright-usecase) that ended failed or cancelled after since."""
    api = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/pipelineJobs"
    states = " OR ".join(f'state="{s}"' for s in FAILED)
    params = {"filter": f'({states}) AND end_time>"{since.strftime("%Y-%m-%dT%H:%M:%SZ")}"', "pageSize": 100}
    jobs, token = [], None
    while True:
        r = gcp.get(api, **params, **({"pageToken": token} if token else {}))
        if not r.ok:
            raise RuntimeError(f"cannot list pipeline jobs: HTTP {r.status}")
        jobs += [j for j in (r.body or {}).get("pipelineJobs", []) if (j.get("labels") or {}).get("pipewright-usecase")]
        token = (r.body or {}).get("nextPageToken")
        if not token:
            return jobs


def full_job(gcp, job: dict, region: str) -> dict:
    """The job with its steps: a list leaves out jobDetail.taskDetails (found live), a get has them."""
    r = gcp.get(f"https://{region}-aiplatform.googleapis.com/v1/{job['name']}")
    return r.body if r.ok and r.body else job


def failure(job: dict) -> dict:
    """What a run.failed event says about a job: its state, the failed steps and their first error line."""
    tasks = ((job.get("jobDetail") or {}).get("taskDetails")) or []
    root = ("root", job["name"].rsplit("/", 1)[-1])            # the DAG's own task is named after the job
    steps = [t for t in tasks if t.get("state") == "FAILED" and t.get("taskName") not in (None, "", *root)
             and not str(t.get("taskName")).startswith("exit-handler")]
    errors = {t["taskName"]: str((t.get("error") or {}).get("message") or "").strip().splitlines()[:1]
              for t in steps}
    labels = job.get("labels") or {}
    message = str((job.get("error") or {}).get("message") or "").strip().splitlines()
    return {"pipeline": labels.get("pipewright-pipeline"), "job_id": job["name"].rsplit("/", 1)[-1],
            "state": job.get("state", "").removeprefix("PIPELINE_STATE_").lower(),
            "trigger": labels.get("trigger"), "failed_steps": sorted(errors),
            "error": "; ".join(f"{k}: {v[0]}" for k, v in sorted(errors.items()) if v)[:MAX_ERROR]
            or (message[0][:MAX_ERROR] if message else ""),
            "ended": job.get("endTime")}


KFP_FAILED = ("FAILED", "CANCELED")


def kfp_failed_jobs(manifest: dict, gcp, since: datetime) -> list[dict]:
    """Kubeflow runs (orchestrator kfp, private-gke) that ended failed or cancelled after since, in the shape
    failure() reads (found in 1.2 QA: the watch saw Agent Platform runs only). Newest first; a run created more than a
    day before since cannot have ended after it."""
    from pipewright.adapters.kfp import api_path, run_labels
    from pipewright.kube import KubeClient, message

    namespace = (manifest.get("kubernetes") or {}).get("namespace") or "kubeflow"
    kube = KubeClient.from_manifest(manifest, gcp)
    stamp = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    oldest = (since - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    jobs, token = [], None
    while True:
        r = kube.get(f"{api_path(namespace)}/runs", page_size=100, sort_by="created_at desc",
                     **({"page_token": token} if token else {}))
        if not r.ok:
            raise RuntimeError(f"cannot list Kubeflow runs: {r.status} {message(r)}")
        runs = (r.body or {}).get("runs") or []
        for run in runs:
            labels = run_labels(run.get("description"))
            if (run.get("state") in KFP_FAILED and labels.get("pipewright-usecase")
                    and (run.get("finished_at") or "") > stamp):
                # Found live: Kubeflow also lists the DAG, its drivers, executors and "(0)" attempts as failed, and
                # keeps no error text: only the pipeline's own steps count, and the error is in their pods' logs.
                steps = _kfp_steps(run.get("pipeline_spec") or {})
                tasks = [t for t in ((run.get("run_details") or {}).get("task_details")) or []
                         if t.get("display_name") in steps]
                jobs.append({
                    "name": f"kubeflow/{namespace}/runs/{run.get('display_name')}",
                    "state": "PIPELINE_STATE_" + ("CANCELLED" if run["state"] == "CANCELED" else "FAILED"),
                    "labels": labels, "endTime": run.get("finished_at"), "error": run.get("error") or {},
                    "jobDetail": {"taskDetails": [
                        {"taskName": t["display_name"], "state": t.get("state"),
                         "error": t.get("error") or {"message": "failed: Kubeflow keeps the error in the step's pod "
                                                                "logs"}} for t in tasks]}})
        token = (r.body or {}).get("next_page_token")
        if not token or not runs or (runs[-1].get("created_at") or "") < oldest:
            return jobs


def _kfp_steps(spec: dict) -> set[str]:
    """The pipeline's own steps: the task names of its DAGs, without the compiler's condition-* wrappers."""
    spec = spec.get("pipeline_spec") or spec
    dags = [spec.get("root") or {}, *(spec.get("components") or {}).values()]
    return {name for d in dags for name in ((d.get("dag") or {}).get("tasks") or {})
            if not name.startswith("condition-")}


def report(gcp, bucket: str, project: str, region: str, contracts: list, emit, now: datetime | None = None,
           manifest: dict | None = None) -> list:
    """Records run.failed once for each failed run of a live use case; returns what it recorded. With a manifest,
    the use cases on Kubeflow (orchestrator kfp) are looked up in the cluster instead of Agent Platform."""
    now = now or datetime.now(timezone.utc)
    by_label = {_label(c.usecase): c for c in contracts}
    kubeflow = manifest is not None
    jobs = (kfp_failed_jobs(manifest, gcp, now - LOOKBACK) if kubeflow
            else failed_jobs(gcp, project, region, now - LOOKBACK))
    out = []
    for job in jobs:
        contract = by_label.get(_label(job["labels"]["pipewright-usecase"]))
        if contract is None:
            continue
        detail = failure(job if kubeflow else full_job(gcp, job, region))
        try:
            gcs_write(gcp, bucket, f"{MARKERS}/{detail['job_id']}", json.dumps(detail), if_generation_match=0)
        except PreconditionFailed:
            continue                                            # reported already
        emit(contract, job["labels"].get("run-id"), detail)
        out.append({"usecase": contract.usecase, **detail})
        log.info("run.failed %s %s (%s)", contract.usecase, detail["job_id"], detail["error"])
    return out
