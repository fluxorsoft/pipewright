"""Schedules as Kubernetes CronJobs (manifest schedules.backend: kubernetes): the same triggers.schedules, a CronJob
per schedule that publishes the run request, and `schedules sync` creating, updating and deleting them."""

import base64
import copy
import json

import pytest

from pipewright import kube
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.kube import Response
from pipewright.schedules import fire, kubernetes, sync
from tests.conftest import MANIFEST

IMAGE = "r-docker.pkg.dev/p/images/pipewright-launcher@sha256:" + "a" * 64
TOPIC = "projects/p/topics/pw-run-requests"


def k8s_env(repo, **schedules):
    launcher = {**copy.deepcopy(MANIFEST["launcher"]), "run_requests_topic": TOPIC}
    repo.add_env("gke", launcher=launcher, orchestrator="kfp",
                 kubernetes={"endpoint": "https://10.0.0.2", "dns_endpoint": "gke.example", "namespace": "kubeflow"},
                 schedules={"backend": "kubernetes", "namespace": "pipewright-schedules",
                            "service_account": "pipewright-scheduler", "image": IMAGE, **schedules})
    root = repo.write()
    env = load_environment(repo.root, "gke")
    return resolve_usecase(root, env), env


class FakeKube:
    def __init__(self):
        self.jobs, self.calls = {}, []

    def get(self, path, **params):
        self.calls.append(("GET", path, params))
        name = path.rsplit("/cronjobs", 1)[1].strip("/")
        if name:
            return Response(200, self.jobs[name])
        return Response(200, {"items": list(self.jobs.values())})

    def post(self, path, body):
        self.calls.append(("POST", path, None))
        self.jobs[body["metadata"]["name"]] = {**body, "metadata": {**body["metadata"], "resourceVersion": "1"}}
        return Response(201, body)

    def request(self, method, path, body=None, **params):
        self.calls.append((method, path, None))
        self.jobs[body["metadata"]["name"]] = body
        return Response(200, body)

    def delete(self, path):
        self.calls.append(("DELETE", path, None))
        return Response(200, {}) if self.jobs.pop(path.rsplit("/", 1)[-1], None) else Response(404, {})


@pytest.fixture
def fake(monkeypatch):
    k = FakeKube()
    monkeypatch.setattr(kube.KubeClient, "from_manifest", classmethod(lambda cls, m, gcp, dns=False: k))
    return k


def test_a_schedule_becomes_a_cronjob_that_publishes_its_run_request(repo):
    uc, env = k8s_env(repo)
    job = kubernetes.desired(uc, env)["pw-churn-nightly"]
    assert job["metadata"]["namespace"] == "pipewright-schedules"
    assert job["spec"]["schedule"] == "0 2 * * *" and job["spec"]["timeZone"] == "Europe/London"
    assert job["spec"]["concurrencyPolicy"] == "Forbid"
    pod = job["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "pipewright-scheduler" and pod["securityContext"]["runAsNonRoot"]
    assert pod["securityContext"]["runAsUser"] == 10001                 # numeric: Kubernetes verifies it
    container = pod["containers"][0]
    assert container["image"] == IMAGE and container["command"][-1] == "pipewright.schedules.fire"
    assert {e["name"]: e["value"] for e in container["env"]} == {
        "PIPEWRIGHT_TOPIC": TOPIC, "PIPEWRIGHT_REGION": env.region, "PIPEWRIGHT_USECASE": "churn",
        "PIPEWRIGHT_SCHEDULE": "nightly"}
    # the message is the one Cloud Scheduler sends to /run
    assert json.loads(base64.b64decode(fire.message("churn", "nightly")["data"])) == {"usecase": "churn",
                                                                                     "schedule": "nightly"}


def test_cronjob_names_fit_kubernetes():
    assert kubernetes.cronjob_name("churn", "nightly") == "pw-churn-nightly"
    long = kubernetes.cronjob_name("a-very-long-use-case-name-for-retention", "every-weekday-morning")
    assert len(long) <= kubernetes.NAME_MAX and long != kubernetes.cronjob_name("a-very-long-use-case-name-for-x",
                                                                                "every-weekday-morning")


def test_sync_creates_updates_and_deletes_cronjobs(repo, fake):
    uc, env = k8s_env(repo)
    assert sync.backend(env) == "kubernetes"
    changes = sync.sync(uc, env, gcp=object(), dry_run=True)
    assert [(c.action, c.job) for c in changes] == [("create", "pw-churn-nightly")] and not fake.jobs
    sync.sync(uc, env, gcp=object())
    assert list(fake.jobs) == ["pw-churn-nightly"]
    assert [c.action for c in sync.sync(uc, env, gcp=object())] == ["none"]

    uc.triggers["schedules"][0]["cron"] = "0 3 * * *"                     # changed in usecase.yaml
    assert [c.action for c in sync.sync(uc, env, gcp=object())] == ["update"]
    assert fake.jobs["pw-churn-nightly"]["spec"]["schedule"] == "0 3 * * *"
    assert fake.jobs["pw-churn-nightly"]["metadata"]["resourceVersion"] == "1"

    uc.triggers["schedules"] = []                                         # removed
    assert [(c.action, c.job) for c in sync.sync(uc, env, gcp=object())] == [("delete", "pw-churn-nightly")]
    assert not fake.jobs
    listed = [p for m, _, p in fake.calls if m == "GET" and p]
    assert listed[0]["labelSelector"] == "app.kubernetes.io/managed-by=pipewright,pipewright.dev/usecase=churn"


def test_kubernetes_schedules_need_the_platforms_settings(repo):
    uc, env = k8s_env(repo, image=None)
    with pytest.raises(ConfigError, match="schedules.image"):
        kubernetes.desired(uc, env)


def test_without_a_schedules_block_cloud_scheduler_is_used(repo):
    repo.add_env("prod")
    repo.write()
    assert sync.backend(load_environment(repo.root, "prod")) == "cloud_scheduler"
