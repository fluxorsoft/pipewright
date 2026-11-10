"""Approved runs that registered nothing (Pipewright FR-124): the launcher's hourly tick looks at the training runs it
started that ended succeeded, and records one `run.unregistered` event for a run whose evaluation approved the model
(its `training.evaluated` event says decision approve) but which has no `model.registered` event, and no
`model.registration_skipped` (a replay, which rightly registers nothing). `monitoring.alerts.events:
[run.unregistered]` emails it.

Found in the FR-57 battle test: of two training runs that ran together on Kubeflow, both approved, one registered
nothing and the run still said succeeded. Whatever the cause, a model the gates approved that never reached the
registry is now said, not silent.

The events are read from Cloud Logging (the launcher needs logging.viewer; with observability.log_bucket, the platform's
bucket view). A run is looked at once it ended SETTLE ago, so its events are in; a marker in the artifacts bucket
(runs/registration/<job>) makes each run looked at once. A run without a training.evaluated event (a release from
before the event, or a pipeline of the team's own that does not send it) is recorded as not checkable, never alerted.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from pipewright.gcp import PreconditionFailed, gcs_read, gcs_write

log = logging.getLogger(__name__)

MARKERS = "runs/registration"
LOOKBACK = timedelta(hours=6)
SETTLE = timedelta(minutes=10)            # events reach Cloud Logging within a minute or two of the step
LOG_API = "https://logging.googleapis.com/v2/entries:list"
EVENTS = ("training.evaluated", "model.registered", "model.registration_skipped")
KFP_SUCCEEDED = "SUCCEEDED"


def _z(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def succeeded_jobs(gcp, project: str, region: str, since: datetime) -> list[dict]:
    """Agent Platform training runs Pipewright started that ended succeeded after since: {job_id, labels, ended}."""
    api = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/pipelineJobs"
    params = {"filter": f'state="PIPELINE_STATE_SUCCEEDED" AND end_time>"{_z(since)}"', "pageSize": 100}
    out, token = [], None
    while True:
        r = gcp.get(api, **params, **({"pageToken": token} if token else {}))
        if not r.ok:
            raise RuntimeError(f"cannot list pipeline jobs: HTTP {r.status}")
        for j in (r.body or {}).get("pipelineJobs", []):
            labels = j.get("labels") or {}
            if labels.get("pipewright-usecase"):
                out.append({"job_id": j["name"].rsplit("/", 1)[-1], "labels": labels, "ended": j.get("endTime")})
        token = (r.body or {}).get("nextPageToken")
        if not token:
            return out


def kfp_succeeded_jobs(manifest: dict, gcp, since: datetime) -> list[dict]:
    """Kubeflow runs (orchestrator kfp) that ended succeeded after since, in the same shape."""
    from pipewright.adapters.kfp import api_path, run_labels
    from pipewright.kube import KubeClient, message

    namespace = (manifest.get("kubernetes") or {}).get("namespace") or "kubeflow"
    kube = KubeClient.from_manifest(manifest, gcp)
    stamp, oldest = _z(since), _z(since - timedelta(days=1))
    out, token = [], None
    while True:
        r = kube.get(f"{api_path(namespace)}/runs", page_size=100, sort_by="created_at desc",
                     **({"page_token": token} if token else {}))
        if not r.ok:
            raise RuntimeError(f"cannot list Kubeflow runs: {r.status} {message(r)}")
        runs = (r.body or {}).get("runs") or []
        for run in runs:
            labels = run_labels(run.get("description"))
            if (run.get("state") == KFP_SUCCEEDED and labels.get("pipewright-usecase")
                    and (run.get("finished_at") or "") > stamp):
                out.append({"job_id": str(run.get("display_name") or run.get("run_id")), "labels": labels,
                            "ended": run.get("finished_at")})
        token = (r.body or {}).get("next_page_token")
        if not token or not runs or (runs[-1].get("created_at") or "") < oldest:
            return out


def run_events(gcp, project: str, run_id: str, since: datetime, bucket: str | None = None) -> list[dict]:
    """The run's evaluation and registration events, from Cloud Logging."""
    types = " OR ".join(f"jsonPayload.pipewright_event.event_type={json.dumps(e)}" for e in EVENTS)
    flt = (f'jsonPayload.pipewright_event.run_id={json.dumps(run_id)} AND ({types}) AND '
           f'timestamp>="{_z(since)}"')
    r = gcp.post(LOG_API, {"resourceNames": [f"{bucket}/views/_AllLogs" if bucket else f"projects/{project}"],
                           "filter": flt, "orderBy": "timestamp asc", "pageSize": 100})
    if not r.ok:
        raise PermissionError(f"Cloud Logging: HTTP {r.status} (the launcher needs logging.viewer to see whether "
                              "approved runs registered)")
    return [(e.get("jsonPayload") or {}).get("pipewright_event") or {} for e in (r.body or {}).get("entries") or []]


def judge(events: list[dict]) -> dict:
    """What the events say about a run: {status: registered | skipped | rejected | unregistered | not_checkable}."""
    kinds = {e.get("event_type") for e in events}
    evaluated = [e for e in events if e.get("event_type") == "training.evaluated"]
    decision = ((evaluated[-1].get("data") or {}).get("decision")) if evaluated else None
    data = (evaluated[-1].get("data") or {}) if evaluated else {}
    if "model.registered" in kinds:
        return {"status": "registered"}
    if "model.registration_skipped" in kinds:
        return {"status": "skipped"}
    if not evaluated:
        return {"status": "not_checkable", "why": "the run sent no training.evaluated event"}
    if decision != "approve":
        return {"status": "rejected", "decision": decision}
    return {"status": "unregistered", "decision": decision, "primary_metric": data.get("primary_metric"),
            "primary_value": data.get("primary_value")}


def report(gcp, bucket: str, project: str, region: str, contracts: list, emit, now: datetime | None = None,
           manifest: dict | None = None, log_bucket: str | None = None) -> list:
    """run.unregistered once for each approved training run of a live use case that registered nothing; returns what it
    recorded (and an error entry when the logs cannot be read)."""
    from pipewright.launcher.watch import _label

    now = now or datetime.now(timezone.utc)
    by_label = {_label(c.usecase): c for c in contracts}
    since = now - LOOKBACK
    jobs = (kfp_succeeded_jobs(manifest, gcp, since) if manifest is not None
            else succeeded_jobs(gcp, project, region, since))
    out = []
    for job in jobs:
        labels = job["labels"]
        contract = by_label.get(_label(labels.get("pipewright-usecase", "")))
        run_id = labels.get("run-id")
        if contract is None or not run_id or (labels.get("pipewright-pipeline") or "training") != "training":
            continue
        ended = datetime.fromisoformat(str(job.get("ended") or _z(now)).replace("Z", "+00:00"))
        if now - ended < SETTLE:
            continue                                            # its events may not be in yet: the next tick
        marker = f"{MARKERS}/{job['job_id']}"
        if gcs_read(gcp, bucket, marker)[0]:
            continue
        try:
            verdict = judge(run_events(gcp, project, run_id, ended - timedelta(days=2), log_bucket))
        except PermissionError as e:
            out.append({"error": str(e)})
            return out                                          # the same for every run: said once
        detail = {"pipeline": "training", "job_id": job["job_id"], "run_id": run_id, "ended": job.get("ended"),
                  **verdict}
        try:
            gcs_write(gcp, bucket, marker, json.dumps(detail), if_generation_match=0)
        except PreconditionFailed:
            continue                                            # another tick looked at it
        if verdict["status"] == "unregistered":
            emit(contract, run_id, {**detail, "why": "the evaluation approved the model, and the run ended "
                                                       "succeeded without registering it"})
            out.append({"usecase": contract.usecase, **detail})
            log.warning("run.unregistered %s %s (run %s)", contract.usecase, job["job_id"], run_id)
    return out


def duplicates(gcp, bucket: str, project: str, region: str, contracts: list, emit) -> list:
    """model.duplicated once for each second model with a use case's model name (FR-124, found in the QA edge-case
    review): a team's register step from before FR-124 can still make one when two runs register a first version at
    once. Each registration sends model.registered, so report() sees nothing wrong, and the oldest-model rule hides
    the newer model's versions. The event names both models; the team moves or retrains the hidden version."""
    from pipewright.models.names import oldest

    api = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/models"
    out = []
    for contract in contracts:
        name = (getattr(contract, "metadata", None) or {}).get("model")
        if not name:
            continue
        r = gcp.get(api, filter=f'display_name="{name}"', pageSize=20)
        models = oldest((r.body or {}).get("models") or []) if r.ok else []
        for extra in models[1:]:
            mid = extra["name"].rsplit("/", 1)[-1]
            marker = f"{MARKERS}/duplicate-{contract.usecase}-{mid}"
            detail = {"model": name, "kept": models[0]["name"], "duplicate": extra["name"],
                      "created": extra.get("createTime"),
                      "why": "two models have the use case's model name; Pipewright uses the oldest, so the other's "
                             "versions are not seen (a register step from before Pipewright FR-124)"}
            try:
                gcs_write(gcp, bucket, marker, json.dumps(detail), if_generation_match=0)
            except PreconditionFailed:
                continue                                        # said already
            emit(contract, None, detail)
            out.append({"usecase": contract.usecase, **detail})
            log.warning("model.duplicated %s: %s besides %s", contract.usecase, extra["name"], models[0]["name"])
    return out
