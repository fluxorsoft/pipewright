"""pipewright schedules sync: a use case's triggers.schedules as Cloud Scheduler jobs or Kubernetes CronJobs.

The environment manifest's `schedules.backend` chooses (default cloud_scheduler); the use case is the same for both.

cloud_scheduler: each schedule becomes job `pw-<usecase>-<schedule>`, which POSTs {"usecase", "schedule"} to the
launcher's /run with an OIDC token for the launcher service account. kubernetes (private-gke): each schedule becomes
a CronJob that publishes the same request to the run requests topic (schedules/kubernetes.py). Jobs for schedules
removed from the config are deleted. Only jobs this use case owns are touched.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, error_text

SCHEDULER = "https://cloudscheduler.googleapis.com/v1"


def job_prefix(usecase: str) -> str:
    return f"pw-{usecase}-"


def desired_jobs(uc: ResolvedUseCase, env: Environment) -> dict[str, dict]:
    url = (env.launcher or {}).get("url")
    sa = (env.launcher or {}).get("service_account")
    if uc.triggers["schedules"] and not (url and sa):
        raise ConfigError("the environment manifest has no launcher URL or service account: ask the platform team")
    jobs = {}
    for s in uc.triggers["schedules"]:
        name = f"{job_prefix(uc.name)}{s['name']}"
        body = json.dumps({"usecase": uc.name, "schedule": s["name"]}).encode()
        jobs[name] = {
            "description": f"Pipewright: {uc.name} schedule {s['name']} (managed; edit usecase.yaml instead)",
            "schedule": s["cron"],
            "timeZone": s["timezone"],
            "httpTarget": {
                "uri": f"{url.rstrip('/')}/run",
                "httpMethod": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": base64.b64encode(body).decode(),
                "oidcToken": {"serviceAccountEmail": sa, "audience": url.rstrip("/")},
            },
            "retryConfig": {"retryCount": 3, "minBackoffDuration": "60s"},
            "attemptDeadline": "120s",
        }
    return jobs


def _comparable(job: dict) -> tuple:
    t = job.get("httpTarget", {})
    return (job.get("schedule"), job.get("timeZone"), t.get("uri"), t.get("body"),
            t.get("oidcToken", {}).get("serviceAccountEmail"))


@dataclass
class Change:
    action: str          # create | update | delete | none
    job: str
    detail: str = ""


def backend(env: Environment) -> str:
    return (env.schedules or {}).get("backend") or "cloud_scheduler"


def _diff(want: dict, have: dict, comparable, describe) -> list[Change]:
    changes = []
    for name, job in want.items():
        if name not in have:
            changes.append(Change("create", name, describe(job)))
        elif comparable(have[name]) != comparable(job):
            changes.append(Change("update", name, describe(job)))
        else:
            changes.append(Change("none", name, describe(job)))
    for name in have:
        if name not in want:
            changes.append(Change("delete", name, "no longer in usecase.yaml"))
    return changes


def _sync_kubernetes(uc: ResolvedUseCase, env: Environment, gcp, dry_run: bool) -> list[Change]:
    from pipewright.schedules import kubernetes

    want = kubernetes.desired(uc, env)
    k8s = kubernetes.Backend(env, gcp)
    changes = _diff(want, k8s.existing(uc.name), kubernetes.comparable,
                    lambda j: f"{j['spec']['schedule']} ({j['spec']['timeZone']}), CronJob in {k8s.namespace}")
    if not dry_run:
        for c in changes:
            if c.action != "none":
                k8s.apply(c.action, c.job, want.get(c.job))
    return changes


def plan(uc: ResolvedUseCase, env: Environment, gcp: Gcp) -> tuple[list[Change], dict, str]:
    parent = f"projects/{env.project}/locations/{env.region}"
    want = desired_jobs(uc, env)
    resp = gcp.get(f"{SCHEDULER}/{parent}/jobs", pageSize=500)
    if resp.status == 403 and "SERVICE_DISABLED" in json.dumps(resp.body):
        raise ConfigError(f"the Cloud Scheduler API is not enabled in {env.project}: ask the platform team "
                          "(schedules need cloudscheduler.googleapis.com)")
    if not resp.ok:
        raise ConfigError(f"cannot list Cloud Scheduler jobs: {error_text(resp)}")
    have = {j["name"].rsplit("/", 1)[-1]: j for j in resp.body.get("jobs", [])
            if j["name"].rsplit("/", 1)[-1].startswith(job_prefix(uc.name))}
    return _diff(want, have, _comparable, lambda j: f"{j['schedule']} ({j['timeZone']})"), want, parent


def sync(uc: ResolvedUseCase, env: Environment, gcp: Gcp | None = None, dry_run: bool = False) -> list[Change]:
    gcp = gcp or Gcp()
    if backend(env) == "kubernetes":
        return _sync_kubernetes(uc, env, gcp, dry_run)
    changes, want, parent = plan(uc, env, gcp)
    if dry_run:
        return changes
    for c in changes:
        url = f"{SCHEDULER}/{parent}/jobs"
        if c.action == "create":
            r = gcp.post(url, {"name": f"{parent}/jobs/{c.job}", **want[c.job]})
        elif c.action == "update":
            r = gcp.patch(f"{url}/{c.job}", {"name": f"{parent}/jobs/{c.job}", **want[c.job]})
        elif c.action == "delete":
            r = gcp.delete(f"{url}/{c.job}")
        else:
            continue
        if not r.ok:
            raise ConfigError(f"{c.action} {c.job} failed: {error_text(r)}")
    return changes


def render(changes: list[Change], uc: ResolvedUseCase, dry_run: bool) -> str:
    lines = [f"\nSchedules for {uc.name} ({uc.environment})" + ("  [dry run]" if dry_run else "")]
    if not changes:
        lines.append("  OK    no schedules configured, none present")
    for c in changes:
        tag = "OK  " if c.action == "none" else ("PLAN" if dry_run else "DONE")
        verb = "up to date" if c.action == "none" else (("would " if dry_run else "") + c.action)
        lines.append(f"  {tag}  {c.job}: {verb} {c.detail}".rstrip())
    return "\n".join(lines)
