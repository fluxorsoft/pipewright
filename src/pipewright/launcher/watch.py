"""Failed runs (Pipewright 1.2.0, FR-67): the launcher's hourly tick (/retraining/tick) finds the pipeline runs it
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


ENDED = ("PIPELINE_STATE_SUCCEEDED", "PIPELINE_STATE_FAILED", "PIPELINE_STATE_CANCELLED")


def _when(value):
    from datetime import datetime as _dt

    return _dt.fromisoformat(value.replace("Z", "+00:00")) if value else None


def trace_runs(gcp, project: str, region: str, since: datetime) -> int:
    """FR-127: for each run that carries its trigger's trace (label pipewright-trace) and ended after `since`, a run
    span and one span per step from Agent Platform's own task times. Telemetry only: nothing else changes."""
    from pipewright import telemetry

    if not telemetry.is_on():
        return 0
    api = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/pipelineJobs"
    states = " OR ".join(f'state="{s}"' for s in ENDED)
    r = gcp.get(api, filter=f'({states}) AND end_time>"{since.strftime("%Y-%m-%dT%H:%M:%SZ")}"', pageSize=100)
    written = 0
    for j in (r.body or {}).get("pipelineJobs", []) if r.ok else []:
        labels = j.get("labels") or {}
        tp = labels.get("pipewright-trace")
        if not tp or not j.get("startTime") or not j.get("endTime"):
            continue
        full = full_job(gcp, j, region)
        steps = [{"name": t.get("taskName"), "start": _when(t.get("startTime")), "end": _when(t.get("endTime")),
                  "state": t.get("state")} for t in ((full.get("jobDetail") or {}).get("taskDetails") or [])
                 if t.get("taskName") and not str(t.get("taskName")).endswith("-driver")]
        written += telemetry.run_spans(tp, {"start": _when(j["startTime"]), "end": _when(j["endTime"]),
                                            "state": j.get("state"), "usecase": labels.get("pipewright-usecase"),
                                            "pipeline": labels.get("pipewright-pipeline"),
                                            "run_id": labels.get("run-id"), "job_id": j["name"].rsplit("/", 1)[-1]},
                                       steps)
    return written


KFP_ENDED = ("SUCCEEDED", "FAILED", "CANCELED")


def trace_kfp_runs(manifest: dict, gcp, since: datetime) -> int:
    """FR-127 on Kubeflow: the run and step spans for runs that carry their trigger's trace and ended after `since`,
    from Kubeflow's own task times (the pipeline's steps only, as kfp_job). Telemetry only."""
    from pipewright import telemetry
    from pipewright.adapters.kfp import api_path, run_labels
    from pipewright.kube import KubeClient

    if not telemetry.is_on():
        return 0
    namespace = (manifest.get("kubernetes") or {}).get("namespace") or "kubeflow"
    kube = KubeClient.from_manifest(manifest, gcp)
    stamp = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    r = kube.get(f"{api_path(namespace)}/runs", page_size=100, sort_by="created_at desc")
    written = 0
    for run in (r.body or {}).get("runs") or [] if r.ok else []:
        labels = run_labels(run.get("description"))
        tp = labels.get("pipewright-trace")
        if not tp or run.get("state") not in KFP_ENDED or (run.get("finished_at") or "") <= stamp:
            continue
        steps = _kfp_steps(run.get("pipeline_spec") or {})
        tasks = [{"name": t.get("display_name"), "start": _when(t.get("start_time")), "end": _when(t.get("end_time")),
                  "state": t.get("state")} for t in ((run.get("run_details") or {}).get("task_details")) or []
                 if t.get("display_name") in steps]
        written += telemetry.run_spans(tp, {"start": _when(run.get("created_at")), "end": _when(run.get("finished_at")),
                                            "state": run.get("state"), "usecase": labels.get("pipewright-usecase"),
                                            "pipeline": labels.get("pipewright-pipeline"),
                                            "run_id": labels.get("run-id"), "job_id": run.get("display_name")}, tasks)
    return written


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
    failure() reads (found in 1.2.0 QA: the watch saw Agent Platform runs only). Newest first; a run created more than a
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
                jobs.append(kfp_job(run, namespace, labels))
        token = (r.body or {}).get("next_page_token")
        if not token or not runs or (runs[-1].get("created_at") or "") < oldest:
            return jobs


def kfp_job(run: dict, namespace: str, labels: dict) -> dict:
    """A failed Kubeflow run in the shape failure() reads. Found live: Kubeflow also lists the DAG, its drivers,
    executors and "(0)" attempts as failed, and keeps no error text: only the pipeline's own steps count, and the
    error is in their pods' logs."""
    steps = _kfp_steps(run.get("pipeline_spec") or {})
    tasks = [t for t in ((run.get("run_details") or {}).get("task_details")) or []
             if t.get("display_name") in steps]
    return {
        "kfp_run_id": run.get("run_id"),
        "name": f"kubeflow/{namespace}/runs/{run.get('display_name')}",
        "state": "PIPELINE_STATE_" + ("CANCELLED" if run.get("state") == "CANCELED" else "FAILED"),
        "labels": labels, "endTime": run.get("finished_at"), "error": run.get("error") or {},
        "jobDetail": {"taskDetails": [
            {"taskName": t["display_name"], "state": t.get("state"),
             "error": t.get("error") or {"message": "failed: Kubeflow keeps the error in the step's pod logs"}}
            for t in tasks]}}


def _kfp_steps(spec: dict) -> set[str]:
    """The pipeline's own steps: the task names of its DAGs, without the compiler's condition-* wrappers."""
    spec = spec.get("pipeline_spec") or spec
    dags = [spec.get("root") or {}, *(spec.get("components") or {}).values()]
    return {name for d in dags for name in ((d.get("dag") or {}).get("tasks") or {})
            if not name.startswith("condition-")}


POD_NOISE = ("Completed", "PodInitializing", "ContainerCreating")


def kfp_failed_pods(manifest: dict, gcp, run_id: str) -> list[dict]:
    """1.4.4: the status of a Kubeflow run's failed pods (why a container stopped: OOMKilled, an image that cannot be
    pulled), never their logs. Needs a role to read pods in the Kubeflow namespace; without it, an empty list."""
    from pipewright.kube import KubeClient

    namespace = (manifest.get("kubernetes") or {}).get("namespace") or "kubeflow"
    r = KubeClient.from_manifest(manifest, gcp).get(f"/api/v1/namespaces/{namespace}/pods",
                                                    labelSelector=f"pipeline/runid={run_id}")
    if not r.ok:
        log.info("pod status of run %s not readable (%s): the cause comes from the failure record only", run_id,
                 r.status)
        return []
    out = []
    for pod in (r.body or {}).get("items") or []:
        meta, status = pod.get("metadata") or {}, pod.get("status") or {}
        args = [a for c in ((pod.get("spec") or {}).get("containers") or [])
                for a in (c.get("command") or []) + (c.get("args") or [])]
        # the step: Kubeflow's pods carry the component's function, not the task's name (seen live in the 1.4.4 QA)
        step = next((args[i + 1] for i, a in enumerate(args[:-1]) if a == "--task_name"), None) or next(
            (args[i + 1].replace("_", "-") for i, a in enumerate(args[:-1]) if a == "--function_to_execute"), None)
        for cs in (status.get("containerStatuses") or []) + (status.get("initContainerStatuses") or []):
            state = cs.get("state") or {}
            ended, waiting = state.get("terminated") or {}, state.get("waiting") or {}
            last = (cs.get("lastState") or {}).get("terminated") or {}
            reason = ended.get("reason") or waiting.get("reason") or last.get("reason")
            code = ended.get("exitCode", last.get("exitCode"))
            if reason and reason not in POD_NOISE:
                out.append({"pod": meta.get("name"), "step": step, "reason": str(reason)[:80], "exit_code": code,
                            "workflow": (meta.get("labels") or {}).get("workflows.argoproj.io/workflow")})
    # the status that names a cause first (OOMKilled, ImagePullBackOff), then plain errors
    return sorted(out, key=lambda p: p["reason"] == "Error")


def _cause(gcp, contract, job: dict, detail: dict, since: datetime, manifest: dict | None,
           now: datetime) -> dict | None:
    """1.4.4, with failure records on for the use case: why the run failed and whose it is (launcher.cause)."""
    from pipewright.launcher import cause

    where = cause.folder(contract)
    if not where:
        return None
    pods = []
    if manifest is not None and job.get("kfp_run_id"):
        try:
            pods = kfp_failed_pods(manifest, gcp, job["kfp_run_id"])
        except Exception:  # noqa: BLE001 - pod status is a help, never a need
            log.exception("pod status of %s not read", detail["job_id"])
    extra = tuple(x for x in (job.get("kfp_run_id"), *sorted({p["workflow"] for p in pods if p.get("workflow")}))
                  if x)
    try:
        found = cause.records(gcp, where, contract.usecase, (job.get("labels") or {}).get("run-id"),
                              detail["job_id"], since, extra, now)
    except Exception:  # noqa: BLE001 - the folder cannot be read: the cause comes from what Google says
        log.exception("failure records of %s not read", detail["job_id"])
        found = []
    return cause.cause(detail, found, pods[0] if pods else None)


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
            why = _cause(gcp, contract, job, detail, now - LOOKBACK - timedelta(days=1), manifest, now)
        except Exception:  # noqa: BLE001 - run.failed is recorded with or without its cause
            log.exception("cause of %s not worked out", detail["job_id"])
            why = None
        if why:
            # owner, why and next are plain fields so the email's short text carries them; cause has the detail
            detail.update({k: v for k, v in (("cause", why), ("owner", why["owner"]), ("why", why["summary"]),
                                             ("next", why.get("next"))) if v is not None})
            if not detail["failed_steps"] and why.get("step"):       # Kubeflow can list no failed step (found live)
                detail["failed_steps"] = [why["step"]]
        try:
            gcs_write(gcp, bucket, f"{MARKERS}/{detail['job_id']}", json.dumps(detail), if_generation_match=0)
        except PreconditionFailed:
            continue                                            # reported already
        emit(contract, job["labels"].get("run-id"), detail)
        out.append({"usecase": contract.usecase, **detail})
        log.info("run.failed %s %s (%s)", contract.usecase, detail["job_id"], detail["error"])
    return out
