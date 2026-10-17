"""Schedules as Kubernetes CronJobs (manifest `schedules.backend: kubernetes`, the private-gke blueprint).

Each schedule in triggers.schedules becomes CronJob `pw-<usecase>-<schedule>` in the schedules namespace. Its pod runs
`python -m pipewright.schedules.fire`, which publishes {"usecase", "schedule"} to the environment's run requests topic:
the same request a Cloud Scheduler job makes, so the launcher resolves the schedule (pipeline, source, run ID) from
the live release in both cases. The pod runs as the namespace's scheduler service account, whose identity may only
publish run requests; tenant runners live in another namespace, so whoever manages CronJobs cannot run as them.
CronJobs for schedules removed from the config are deleted; only CronJobs labelled for this use case are touched.
"""

from __future__ import annotations

import hashlib
import re

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError

MANAGED = {"app.kubernetes.io/managed-by": "pipewright"}
NAME_MAX = 52                 # a CronJob's name: its Jobs add an 11-character suffix within 63
_NAME = re.compile(r"[^a-z0-9-]+")


def cronjob_name(usecase: str, schedule: str) -> str:
    name = _NAME.sub("-", f"pw-{usecase}-{schedule}".lower()).strip("-")
    if len(name) <= NAME_MAX:
        return name
    digest = hashlib.sha256(f"{usecase}|{schedule}".encode()).hexdigest()[:8]
    return f"{name[:NAME_MAX - 9].rstrip('-')}-{digest}"


def _label(value: str) -> str:
    return _NAME.sub("-", str(value).lower()).strip("-")[:63]


def settings(env: Environment) -> dict:
    cfg = env.schedules or {}
    topic = (env.launcher or {}).get("run_requests_topic")
    missing = [k for k, v in (("schedules.image", cfg.get("image")), ("launcher.run_requests_topic", topic),
                              ("kubernetes.endpoint", (env.kubernetes or {}).get("endpoint"))) if not v]
    if missing:
        raise ConfigError(f"schedules on Kubernetes need {', '.join(missing)} in the environment manifest: ask the "
                          "platform team")
    return {"namespace": cfg.get("namespace") or "pipewright-schedules",
            "service_account": cfg.get("service_account") or "pipewright-scheduler",
            "image": cfg["image"], "topic": topic, "region": env.region,
            "run_as_user": int(cfg.get("run_as_user") or 10001)}   # the launcher image's user


def desired(uc: ResolvedUseCase, env: Environment) -> dict[str, dict]:
    if not uc.triggers["schedules"]:
        return {}
    s = settings(env)
    jobs = {}
    for sched in uc.triggers["schedules"]:
        name = cronjob_name(uc.name, sched["name"])
        labels = {**MANAGED, "pipewright.dev/usecase": _label(uc.name),
                  "pipewright.dev/schedule": _label(sched["name"])}
        env_vars = [{"name": "PIPEWRIGHT_TOPIC", "value": s["topic"]},
                    {"name": "PIPEWRIGHT_REGION", "value": s["region"]},
                    {"name": "PIPEWRIGHT_USECASE", "value": uc.name},
                    {"name": "PIPEWRIGHT_SCHEDULE", "value": sched["name"]}]
        jobs[name] = {
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": name, "namespace": s["namespace"], "labels": labels, "annotations": {
                "pipewright.dev/description": f"Pipewright: {uc.name} schedule {sched['name']} (managed; edit "
                                              "usecase.yaml instead)"}},
            "spec": {
                "schedule": sched["cron"], "timeZone": sched["timezone"],
                "concurrencyPolicy": "Forbid",        # a run request in flight is not sent twice
                "startingDeadlineSeconds": 300,       # a missed tick (cluster down) is skipped after 5 minutes
                "successfulJobsHistoryLimit": 1, "failedJobsHistoryLimit": 3,
                "jobTemplate": {"metadata": {"labels": labels}, "spec": {
                    "backoffLimit": 3, "activeDeadlineSeconds": 600, "ttlSecondsAfterFinished": 86400,
                    "template": {"metadata": {"labels": labels}, "spec": {
                        "serviceAccountName": s["service_account"], "restartPolicy": "Never",
                        "securityContext": {"runAsNonRoot": True, "runAsUser": s["run_as_user"],
                                            "runAsGroup": s["run_as_user"],
                                            "seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [{
                            "name": "fire", "image": s["image"],
                            "command": ["python", "-m", "pipewright.schedules.fire"], "env": env_vars,
                            "resources": {"requests": {"cpu": "50m", "memory": "128Mi"},
                                          "limits": {"memory": "256Mi"}},
                            "securityContext": {"allowPrivilegeEscalation": False,
                                                "capabilities": {"drop": ["ALL"]}}}]}}}},
            },
        }
    return jobs


def comparable(job: dict) -> tuple:
    spec = job.get("spec") or {}
    pod = (((spec.get("jobTemplate") or {}).get("spec") or {}).get("template") or {}).get("spec") or {}
    container = (pod.get("containers") or [{}])[0]
    return (spec.get("schedule"), spec.get("timeZone"), pod.get("serviceAccountName"), container.get("image"),
            (pod.get("securityContext") or {}).get("runAsUser"),
            tuple((e.get("name"), e.get("value")) for e in container.get("env") or []))


class Backend:
    """CronJobs through the cluster's Kubernetes API, as the caller (CI or a platform admin) from outside the VPC:
    the DNS endpoint when the manifest has it."""

    def __init__(self, env: Environment, gcp):
        from pipewright.kube import KubeClient

        self.env = env
        self.kube = KubeClient.from_manifest({"kubernetes": env.kubernetes}, gcp, dns=True)
        self.namespace = (env.schedules or {}).get("namespace") or "pipewright-schedules"

    def _path(self, name: str = "") -> str:
        return f"/apis/batch/v1/namespaces/{self.namespace}/cronjobs" + (f"/{name}" if name else "")

    def existing(self, usecase: str) -> dict[str, dict]:
        from pipewright.kube import message

        selector = f"app.kubernetes.io/managed-by=pipewright,pipewright.dev/usecase={_label(usecase)}"
        r = self.kube.get(self._path(), labelSelector=selector)
        if r.status == 403:
            raise ConfigError(f"cannot list CronJobs in {self.namespace}: {message(r)} (schedules need the "
                              "pipewright-schedules role there: ask the platform team)")
        if not r.ok:
            raise ConfigError(f"cannot list CronJobs in {self.namespace}: {r.status} {message(r)}")
        return {j["metadata"]["name"]: j for j in (r.body or {}).get("items") or []}

    def apply(self, action: str, name: str, job: dict | None) -> None:
        from pipewright.kube import message

        if action == "create":
            r = self.kube.post(self._path(), job)
        elif action == "update":
            current = self.kube.get(self._path(name))
            body = {**job, "metadata": {**job["metadata"],
                                        "resourceVersion": (current.body or {}).get("metadata", {}).get(
                                            "resourceVersion")}}
            r = self.kube.request("PUT", self._path(name), body)
        else:
            r = self.kube.delete(self._path(name))
            if r.status == 404:
                return
        if not r.ok:
            raise ConfigError(f"{action} CronJob {name} failed: {r.status} {message(r)}")
