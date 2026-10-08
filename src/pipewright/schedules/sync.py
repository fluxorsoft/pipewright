"""pipewright schedules sync: Cloud Scheduler jobs from a use case's triggers.schedules.

Each schedule becomes job `pw-<usecase>-<schedule>`, which POSTs {"usecase", "schedule"} to the launcher's /run
with an OIDC token for the launcher service account. Jobs for schedules removed from the config are deleted.
Only jobs with this naming prefix are touched.
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
    changes = []
    for name, job in want.items():
        if name not in have:
            changes.append(Change("create", name, f"{job['schedule']} ({job['timeZone']})"))
        elif _comparable(have[name]) != _comparable(job):
            changes.append(Change("update", name, f"{job['schedule']} ({job['timeZone']})"))
        else:
            changes.append(Change("none", name, f"{job['schedule']} ({job['timeZone']})"))
    for name in have:
        if name not in want:
            changes.append(Change("delete", name, "no longer in usecase.yaml"))
    return changes, want, parent


def sync(uc: ResolvedUseCase, env: Environment, gcp: Gcp | None = None, dry_run: bool = False) -> list[Change]:
    gcp = gcp or Gcp()
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
