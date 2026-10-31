"""Failed runs (Pipewright 1.2, FR-67): the launcher's hourly tick (/retraining/tick) finds the pipeline runs it
started that ended failed or cancelled, and records one `run.failed` event for each: the failed steps and their
error lines, which Pipewright's steps keep free of data values. `monitoring.alerts.events: [run.failed]` emails it,
so a data contract that stops a run, or a broken release, reaches the team without a log-based alert of its own.

A marker in the artifacts bucket (runs/failed/<job>) makes each run reported once, whatever the retries and
overlapping ticks. Agent Platform runs only: a Kubeflow run keeps its errors in its steps' logs.
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


def failure(job: dict) -> dict:
    """What a run.failed event says about a job: its state, the failed steps and their first error line."""
    tasks = ((job.get("jobDetail") or {}).get("taskDetails")) or []
    steps = [t for t in tasks if t.get("state") == "FAILED" and t.get("taskName") not in (None, "", "root")
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


def report(gcp, bucket: str, project: str, region: str, contracts: list, emit, now: datetime | None = None) -> list:
    """Records run.failed once for each failed run of a live use case; returns what it recorded."""
    now = now or datetime.now(timezone.utc)
    by_label = {_label(c.usecase): c for c in contracts}
    out = []
    for job in failed_jobs(gcp, project, region, now - LOOKBACK):
        contract = by_label.get(job["labels"]["pipewright-usecase"])
        if contract is None:
            continue
        detail = failure(job)
        try:
            gcs_write(gcp, bucket, f"{MARKERS}/{detail['job_id']}", json.dumps(detail), if_generation_match=0)
        except PreconditionFailed:
            continue                                            # reported already
        emit(contract, job["labels"].get("run-id"), detail)
        out.append({"usecase": contract.usecase, **detail})
        log.info("run.failed %s %s (%s)", contract.usecase, detail["job_id"], detail["error"])
    return out
