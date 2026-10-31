"""FR-67: a failed run is an event and an alert (Pipewright 1.2.0). The launcher's tick records run.failed once per
failed or cancelled run of a live use case; monitoring.alerts.events may name it."""
from datetime import datetime, timezone
from types import SimpleNamespace

from pipewright.alerts import sync as alerts
from pipewright.config import load_environment
from pipewright.gcp import PreconditionFailed
from pipewright.launcher import watch
from tests.test_alerts import FakeMonitoring, uc_for

JOB = {
    "name": "projects/p/locations/europe-west2/pipelineJobs/churn-training-r7",
    "state": "PIPELINE_STATE_FAILED", "endTime": "2026-10-31T09:12:00Z",
    "labels": {"pipewright-usecase": "churn", "pipewright-pipeline": "training", "trigger": "schedule",
               "run-id": "r7"},
    "error": {"message": "The DAG failed because some tasks failed. The failed tasks are: [validate-data]."},
    "jobDetail": {"taskDetails": [
        {"taskName": "churn-training-r7", "state": "FAILED"},     # live, the DAG's task is named after the job
        {"taskName": "ingest-data", "state": "SUCCEEDED"},
        {"taskName": "validate-data", "state": "FAILED",
         "error": {"message": "data contract failed:\n  orders: values below 1 (12 rows)"}}]},
}


class Fake:
    def __init__(self, jobs):
        self.jobs, self.params, self.markers = jobs, [], set()

    def get(self, url, **params):
        """Like the API: a list leaves out each job's taskDetails, a get of one job has them."""
        for job in self.jobs:
            if url.endswith(job["name"]):
                return SimpleNamespace(ok=True, status=200, body=job)
        self.params.append(params)
        listed = [{**j, "jobDetail": {"pipelineContext": {}}} for j in self.jobs]
        return SimpleNamespace(ok=True, status=200, body={"pipelineJobs": listed})


def test_a_failed_run_is_reported_once(monkeypatch):
    gcp = Fake([JOB, {**JOB, "name": "x/other-r1", "labels": {"pipewright-usecase": "not-live"}}])

    def write(gcp_, bucket, name, text, if_generation_match=None):
        assert if_generation_match == 0
        if name in gcp.markers:
            raise PreconditionFailed(name)
        gcp.markers.add(name)
        return 1

    monkeypatch.setattr(watch, "gcs_write", write)
    emitted = []
    contract = SimpleNamespace(usecase="churn")
    now = datetime(2026, 10, 31, 10, 17, tzinfo=timezone.utc)
    first = watch.report(gcp, "b", "p", "europe-west2", [contract],
                         lambda c, rid, d: emitted.append((c.usecase, rid, d)), now)
    again = watch.report(gcp, "b", "p", "europe-west2", [contract], lambda *a: emitted.append(a), now)
    assert len(first) == 1 and again == [] and len(emitted) == 1
    usecase, rid, detail = emitted[0]
    assert (usecase, rid) == ("churn", "r7") and gcp.markers == {"runs/failed/churn-training-r7"}
    assert detail["state"] == "failed" and detail["pipeline"] == "training" and detail["trigger"] == "schedule"
    assert detail["failed_steps"] == ["validate-data"] and detail["error"] == "validate-data: data contract failed:"
    assert 'end_time>"2026-10-31T04:17:00Z"' in gcp.params[0]["filter"]
    assert 'state="PIPELINE_STATE_CANCELLED"' in gcp.params[0]["filter"]


def test_a_cancelled_run_without_step_errors_keeps_the_run_message():
    job = {**JOB, "state": "PIPELINE_STATE_CANCELLED", "jobDetail": {}, "error": {"message": "cancelled by x\nmore"}}
    detail = watch.failure(job)
    assert detail["state"] == "cancelled" and detail["failed_steps"] == [] and detail["error"] == "cancelled by x"


def test_run_failed_may_send_an_email(repo):
    uc = uc_for(repo, notify=["ml-team@example.com"], events=["run.failed"])
    mon = FakeMonitoring()
    alerts.sync(uc, load_environment(repo.root, "dev"), mon)
    (policy,) = mon.policies.values()
    assert 'event_type="run.failed"' in str(policy) and "pipeline run failed" in str(policy)


def test_kubeflow_runs_are_watched_too(monkeypatch):
    """Found in 1.2.0 QA: the watch saw Agent Platform runs only; a private-gke environment's runs are Kubeflow runs."""
    from pipewright.adapters.kfp import run_description

    labels = {"pipewright-usecase": "churn", "run-id": "r9", "pipewright-pipeline": "training", "trigger": "api"}
    runs = [
        {"display_name": "churn-training-r9", "state": "FAILED", "description": run_description(labels),
         "created_at": "2026-10-31T09:50:00Z", "finished_at": "2026-10-31T09:58:00Z",
         "error": {"message": "1 task failed"},
         "pipeline_spec": {"root": {"dag": {"tasks": {"ingest-data": {}, "validate-data": {}, "condition-1": {}}}},
                           "components": {"comp-condition-1": {"dag": {"tasks": {"train-model": {}}}}}},
         # As live (1.2.0 QA): the DAG, executors, drivers and "(0)" attempts are listed as failed too, without text.
         "run_details": {"task_details": [
             {"display_name": "root", "state": "FAILED"}, {"display_name": "root(0)", "state": "FAILED"},
             {"display_name": "churn-training-x7k2q", "state": "FAILED"},
             {"display_name": "executor", "state": "FAILED"},
             {"display_name": "validate-data(0)", "state": "FAILED"},
             {"display_name": "condition-1", "state": "FAILED"},
             {"display_name": "validate-data", "state": "FAILED", "error": {"message": "data contract failed:\n x"}},
             {"display_name": "train-model", "state": "FAILED"},
             {"display_name": "ingest-data", "state": "SUCCEEDED"}]}},
        {"display_name": "churn-training-r8", "state": "SUCCEEDED", "description": run_description(labels),
         "created_at": "2026-10-31T09:00:00Z", "finished_at": "2026-10-31T09:10:00Z"},
        {"display_name": "someone-elses", "state": "FAILED", "description": "",
         "created_at": "2026-10-31T09:00:00Z", "finished_at": "2026-10-31T09:10:00Z"},
    ]
    seen = {}

    class Kube:
        def get(self, path, **params):
            seen.update(path=path, params=params)
            return SimpleNamespace(ok=True, status=200, body={"runs": runs})

    monkeypatch.setattr("pipewright.kube.KubeClient.from_manifest", classmethod(lambda cls, m, gcp: Kube()))
    markers = set()

    def write(gcp_, bucket, name, text, if_generation_match=None):
        if name in markers:
            raise PreconditionFailed(name)
        markers.add(name)

    monkeypatch.setattr(watch, "gcs_write", write)
    emitted = []
    now = datetime(2026, 10, 31, 10, 17, tzinfo=timezone.utc)
    manifest = {"kubernetes": {"namespace": "kubeflow"}}
    out = watch.report(None, "b", "p", "us-central1", [SimpleNamespace(usecase="churn")],
                       lambda c, rid, d: emitted.append((rid, d)), now, manifest=manifest)
    again = watch.report(None, "b", "p", "us-central1", [SimpleNamespace(usecase="churn")],
                         lambda c, rid, d: emitted.append((rid, d)), now, manifest=manifest)
    assert len(out) == 1 and again == [] and markers == {"runs/failed/churn-training-r9"}
    rid, detail = emitted[0]
    assert rid == "r9" and detail["failed_steps"] == ["train-model", "validate-data"] and detail["state"] == "failed"
    assert detail["error"] == ("train-model: failed: Kubeflow keeps the error in the step's pod logs; "
                               "validate-data: data contract failed:") and detail["pipeline"] == "training"
    assert seen["path"].endswith("/namespaces/kubeflow/services/ml-pipeline:8888/proxy/apis/v2beta1/runs")
