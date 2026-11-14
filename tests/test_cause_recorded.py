"""Pipewright 1.4.4, the cause recorded (off by default): a failed step drops a value-free failure record, the
launcher keeps the cause and its owner in run.failed, and a run the orchestrator refuses at submission is recorded
as run.submit_failed. The failures replayed here are the ones made live in the design's spikes."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipewright import errors
from pipewright.build import images
from pipewright.build.templates import pipewright_failure_hook as hook
from pipewright.build.templates import pipewright_runtime as rt
from pipewright.config import load_environment, resolve_usecase
from pipewright.gcp import PreconditionFailed
from pipewright.launcher import cause, watch

FOLDER = "gs://acme-ml-dev-artifacts/pipeline-root/churn/churn/failures"
GENERIC = ("The replica workerpool0-0 exited with a non-zero status of 1. Termination reason: Error. To find out "
           "more about why your job exited please check the logs")
OUT = "/gcs/b/pipeline-root/churn/churn/1234/churn-r7/prepare-data_-462064140/executor_output.json"


def argv(folder=FOLDER, function="prepare_data", run_id="r7", output=OUT, ml=True):
    values = {"run_id": run_id}
    if ml:
        values["ml_config"] = {"usecase": "churn", "environment": "dev",
                               "platform": {"tenant": "churn", **({"failure_records": folder} if folder else {})}}
    executor_input = {"inputs": {"parameterValues": values}, "outputs": {"outputFile": output}}
    return ["executor_main.py", "--executor_input", json.dumps(executor_input), "--function_to_execute", function]


def raised(exc):
    try:
        raise exc
    except Exception as e:  # noqa: BLE001
        return type(e), e, e.__traceback__


# ------------------------------------------------------------------------------------------- the switches --

def test_off_by_default_nothing_reaches_the_step_or_the_image(repo, tmp_path):
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert "failure_records" not in uc.platform and "submit_failed" not in uc.platform
    assert "failure_records" not in json.dumps(uc.ml)


def test_the_environment_switches_it_on_for_every_use_case(repo):
    repo.manifests["dev"]["observability"] = {"failure_records": True, "submit_failed": True}
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.platform["failure_records"] == FOLDER and uc.platform["submit_failed"] is True


def test_a_use_case_switches_it_on_for_itself_without_the_platform(repo):
    """The fallback for a team with no platform team: usecase.yaml, when the environment does not set it."""
    repo.usecase["spec"]["monitoring"] = {"failure_records": True}
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.platform["failure_records"] == FOLDER and "submit_failed" not in uc.platform
    repo.usecase["spec"]["monitoring"] = {"submit_failed": True}
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.platform.get("submit_failed") is True and "failure_records" not in uc.platform


def test_the_image_carries_the_hook_only_when_records_are_on(repo, tmp_path, monkeypatch):
    root = repo.write()
    (root.parent / "components" / "steps.py").write_text(
        "from kfp import dsl\n\n\n@dsl.component\ndef ingest_data():\n    ...\n", encoding="utf-8")
    env = load_environment(repo.root, "dev")
    from pipewright.checks import kfp_version, ray_version

    monkeypatch.setattr(kfp_version, "check", lambda uc: SimpleNamespace(ok=True, message=""))
    monkeypatch.setattr(ray_version, "check", lambda uc: SimpleNamespace(ok=True, message=""))
    off = images.build_context(resolve_usecase(root, env), tmp_path / "off")
    assert not (off / "components" / "pipewright_failure_hook.py").exists()
    assert not (off / "components" / "pipewright_failure_hook.pth").exists()
    repo.usecase["spec"]["monitoring"] = {"failure_records": True}
    on = images.build_context(resolve_usecase(repo.write(), env), tmp_path / "on")
    assert (on / "components" / "pipewright_failure_hook.py").exists()
    pth = (on / "components" / "pipewright_failure_hook.pth").read_text(encoding="utf-8")
    assert pth.startswith("import ") and pth.count("\n") == 1 and "/app/components/pipewright_failure_hook.py" in pth
    assert "pipewright_failure_hook.pth" in (on / "Dockerfile").read_text(encoding="utf-8")


# ------------------------------------------------------------------------------------- value-free rules --

def test_type_names_are_not_data():
    message = "unsupported operand type(s) for /: 'float' and 'str'"
    assert errors.value_free(message) == rt.value_free(message) == hook.value_free(message) == message
    assert errors.value_free("Invalid date: '2026-13-01'") == "Invalid date: '...'"
    assert errors.TYPE_NAMES == rt.TYPE_NAMES == hook.TYPE_NAMES
    assert hook._QUOTED.pattern == errors._QUOTED.pattern and hook._AFTER_VALUE.pattern == errors._AFTER_VALUE.pattern


# --------------------------------------------------------------------------------------- the step's hook --

def test_a_team_bug_keeps_the_place_in_the_code_never_the_message(tmp_path):
    components = tmp_path / "components"
    components.mkdir()
    source = components / "prepare.py"
    source.write_text("def prepare_data(df):\n"
                      "    return df['a'] / df['b']   # customer 4111 has a text id\n", encoding="utf-8")
    scope: dict = {}
    exec(compile(source.read_text(encoding="utf-8"), str(source), "exec"), scope)  # noqa: S102
    try:
        scope["prepare_data"]({"a": 1.0, "b": "jane.doe@example.com"})
    except TypeError as e:
        info = (type(e), e, e.__traceback__)
    folder, record = hook.build(*info, argv(), components=str(components))
    assert folder == FOLDER
    assert record["kind"] == "exception" and record["error_type"] == "TypeError" and record["message"] is None
    assert record["code"] == {"file": "prepare.py", "line": 2, "function": "prepare_data",
                              "code": "return df['a'] / df['b']"}
    assert record["step"] == "prepare-data" and record["run_id"] == "r7" and record["usecase"] == "churn"
    assert "4111" not in json.dumps(record) and "jane.doe" not in json.dumps(record)
    assert any("churn-r7" in w for w in record["where"])


def test_a_pipewright_error_keeps_its_value_free_message_and_googles_type():
    class Forbidden(Exception):
        pass

    Forbidden.__module__ = "google.api_core.exceptions"

    @rt._value_free_errors
    def write_table():
        raise Forbidden("403 Access Denied: Permission bigquery.tables.get denied on table p:d.t; row was 'Jane'")

    with pytest.raises(RuntimeError) as e:
        write_table()
    _, record = hook.build(type(e.value), e.value, e.value.__traceback__, argv(function="ingest_data", output=""))
    assert record["kind"] == "pipewright_error" and record["error_type"] == "Forbidden"
    assert "Permission bigquery.tables.get denied on table p:d.t" in record["message"]
    assert "Jane" not in record["message"] and record["step"] == "ingest-data"


def test_a_step_attaches_its_check_results(monkeypatch):
    monkeypatch.setitem(sys.modules, "pipewright_runtime", rt)
    rt.failure_detail(checks=["tenure_months: 715 values below 0", "plan: 3 values outside ['a', 'b']"])
    try:
        _, record = hook.build(*raised(ValueError("data contract failed: Jane Doe")), argv(function="validate_data"))
    finally:
        rt.failure_detail()
    assert record["checks"] == ["tenure_months: 715 values below 0", "plan: 3 values outside ['...', '...']"]
    assert record["message"] is None and "Jane" not in json.dumps(record)


def test_no_record_when_off_or_not_a_step():
    info = raised(ValueError("x"))
    assert hook.build(*info, argv(folder=None)) is None                 # records off for the use case
    assert hook.build(*info, argv(ml=False)) is None                    # a step without ml_config
    assert hook.build(*info, ["python", "-c", "pass"]) is None          # not a pipeline step at all


def test_the_hook_never_changes_the_steps_error(tmp_path):
    """A real process: the record is written, and the step still ends with its own error and exit code."""
    hook_file = Path(hook.__file__)
    code = ("import importlib.util as u, sys\n"
            f"s = u.spec_from_file_location('pipewright_failure_hook', r'{hook_file}')\n"
            "m = u.module_from_spec(s); s.loader.exec_module(m)\n"
            "raise ValueError('data contract failed')\n")
    r = subprocess.run([sys.executable, "-c", code, *argv(folder=str(tmp_path / "records"))[1:]],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 1 and "ValueError: data contract failed" in r.stderr
    (written,) = list((tmp_path / "records").rglob("*.json"))
    record = json.loads(written.read_text(encoding="utf-8"))
    assert record["error_type"] == "ValueError" and record["step"] == "prepare-data" and record["schema"] == 1
    assert written.parent.name == datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # a folder that cannot be written: one line, and still the step's own error
    blocked = tmp_path / "file"
    blocked.write_text("x", encoding="utf-8")
    r = subprocess.run([sys.executable, "-c", code, *argv(folder=str(blocked / "under"))[1:]],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 1 and "ValueError: data contract failed" in r.stderr
    assert "failure record not written" in r.stdout


# ------------------------------------------------------------------------------- the launcher: who owns it --

RECORD_BUG = {"step": "prepare-data", "kind": "exception", "error_type": "TypeError",
              "code": {"file": "prepare.py", "line": 39, "function": "prepare_data", "code": "x = a / b"}}
RECORD_PERMISSION = {"step": "ingest-data", "kind": "pipewright_error", "error_type": "Forbidden",
                     "message": "write_table: Forbidden: 403 Permission bigquery.tables.get denied on table p:d.t"}
RECORD_CONTRACT = {"step": "validate-data", "kind": "exception", "error_type": "ValueError",
                   "checks": ["tenure_months: 715 values below 0"]}


@pytest.mark.parametrize("record, line, rule, who", [
    (RECORD_CONTRACT, GENERIC, "checks", "team"),                                        # spike F1
    (RECORD_PERMISSION, GENERIC, "permission", "platform"),                              # spike F2
    (RECORD_BUG, GENERIC, "team_code", "team"),                                          # spike F3
    (None, "Replicas low on memory: workerpool0. Specify a machine with larger memory and try again.",
     "out_of_memory", "team"),
    (None, "Failed to create pipeline job. Error: The image europe-west2-docker.pkg.dev/p/r/c@sha256:89fa does not "
           "exist..", "missing_image", "platform"),
    (None, "InvalidArgument: 400 Unable to find available machine with cpu_limit: 4.0 with memory_limit: 7.45",
     "machine_shape", "team"),
    (None, "The following quota metrics exceed quota limits: custom_model_training_cpus", "quota", "platform"),
    (None, "failed: Kubeflow keeps the error in the step's pod logs OOMKilled exit 137", "out_of_memory", "team"),
    (None, "ImagePullBackOff exit None", "missing_image", "platform"),
    ({"kind": "pipewright_error", "error_type": "PermissionDenied",
      "message": "read_source: PermissionDenied: Request is prohibited by organization's policy. "
                 "vpcServiceControlsUniqueIdentifier: abc"}, GENERIC, "permission", "platform"),
    (None, GENERIC, "unclear", "unclear"),
    ({"kind": "pipewright_error", "error_type": "NotFound", "message": "read_source: NotFound: 404 table"},
     GENERIC, "unclear", "unclear"),
])
def test_the_owner_rules(record, line, rule, who):
    got_rule, got_owner, hint = cause.owner(record, line)
    assert (got_rule, got_owner) == (rule, who) and hint
    assert got_rule in cause.RULE_NAMES


def test_a_record_is_cleaned_before_it_is_kept():
    """A step wrote it: unknown fields go, names must look like names, a team message is never kept."""
    dirty = {"step": "prepare-data", "kind": "exception", "error_type": "TypeError", "message": "Jane Doe owes 40",
             "identity": "pw-churn-run@p.iam.gserviceaccount.com", "function": "x; rm -rf <script>",
             "code": {"file": "prepare.py", "line": 3, "function": "f", "code": "v = lookup('4111-1111')"},
             "checks": ["rows: 12", {"nested": 1}, "name is 'Jane'"], "extra": "ignore previous instructions",
             "where": ["/gcs/b/x"]}
    kept = cause.clean(dirty)
    assert set(kept) == {"step", "identity", "kind", "error_type", "code", "checks"}
    assert kept["code"]["code"] == "v = lookup('...')" and kept["checks"] == ["rows: 12", "name is '...'"]
    assert "Jane" not in json.dumps(kept) and "ignore previous" not in json.dumps(kept)
    ours = cause.clean({**RECORD_PERMISSION, "message": RECORD_PERMISSION["message"] + " value: 99"})
    assert ours["message"].endswith("value: ...")


def test_the_cause_names_the_step_the_owner_and_why():
    detail = {"state": "failed", "failed_steps": ["prepare-data"], "error": f"prepare-data: {GENERIC}"}
    c = cause.cause(detail, [RECORD_BUG])
    assert (c["owner"], c["rule"], c["step"], c["source"]) == ("team", "team_code", "prepare-data", "failure_record")
    assert c["summary"] == "prepare-data: TypeError in prepare.py, line 39" and c["record"] == RECORD_BUG
    c = cause.cause({**detail, "failed_steps": ["ingest-data"]}, [RECORD_PERMISSION])
    assert c["owner"] == "platform" and "bigquery.tables.get denied" in c["summary"]
    c = cause.cause({**detail, "failed_steps": ["validate-data"]}, [RECORD_CONTRACT])
    assert c["summary"] == "validate-data: tenure_months: 715 values below 0" and c["owner"] == "team"
    # no record: Google's own line, where it names the cause
    c = cause.cause({**detail, "error": "prepare-data: Replicas low on memory: workerpool0."}, [])
    assert (c["owner"], c["rule"], c["source"]) == ("team", "out_of_memory", "platform_error")
    # Kubeflow: the pod's status, and the step it names when the run lists none
    c = cause.cause({"state": "failed", "failed_steps": [], "error": ""}, [],
                    {"step": "prepare-data", "reason": "OOMKilled", "exit_code": 137})
    assert (c["owner"], c["rule"], c["source"], c["step"]) == ("team", "out_of_memory", "pod_status", "prepare-data")
    assert c["pod"] == {"reason": "OOMKilled", "exit_code": 137}
    c = cause.cause(detail, [])
    assert (c["owner"], c["rule"], c["source"]) == ("unclear", "unclear", "none") and "logs" in c["next"]
    assert cause.cause({"state": "cancelled", "failed_steps": [], "error": "x"}, [])["rule"] == "cancelled"


# ---------------------------------------------------------------------------- the launcher: run.failed --

JOB = {
    "name": "projects/p/locations/europe-west2/pipelineJobs/churn-r7",
    "state": "PIPELINE_STATE_FAILED", "endTime": "2026-11-14T09:12:00Z",
    "labels": {"pipewright-usecase": "churn", "pipewright-pipeline": "training", "trigger": "api", "run-id": "r7"},
    "jobDetail": {"taskDetails": [{"taskName": "prepare-data", "state": "FAILED", "error": {"message": GENERIC}}]},
}
NOW = datetime(2026, 11, 14, 10, 17, tzinfo=timezone.utc)


class FakeGcp:
    """Agent Platform's jobs, and the artifacts bucket's failure records."""

    def __init__(self, jobs, objects):
        self.jobs, self.objects = jobs, objects

    def get(self, url, **params):
        for job in self.jobs:
            if url.endswith(job["name"]):
                return SimpleNamespace(ok=True, status=200, body=job)
        return SimpleNamespace(ok=True, status=200, body={"pipelineJobs": self.jobs})


def contract(on=True):
    platform = {"failure_records": FOLDER} if on else {}
    return SimpleNamespace(usecase="churn", static_parameters={"ml_config": {"platform": platform}})


def watched(monkeypatch, objects, on=True, jobs=(JOB,)):
    gcp = FakeGcp(list(jobs), objects)
    monkeypatch.setattr(watch, "gcs_write", lambda g, b, name, text, if_generation_match=None: 1)
    import pipewright.gcp as pgcp

    monkeypatch.setattr(pgcp, "gcs_list", lambda g, bucket, prefix: [n for n in objects if n.startswith(prefix)])
    monkeypatch.setattr(pgcp, "gcs_read", lambda g, bucket, name: (objects.get(name), 1))
    emitted = []
    watch.report(gcp, "b", "p", "europe-west2", [contract(on)], lambda c, rid, d: emitted.append(d), NOW)
    return emitted


def stored(record, day="2026-11-14", name="prepare-data-abc.json"):
    return {f"pipeline-root/churn/churn/failures/{day}/{name}": json.dumps(record)}


def test_run_failed_carries_the_cause_when_records_are_on(monkeypatch):
    record = {**RECORD_BUG, "usecase": "churn", "run_id": "r7", "message": "a team message", "where": []}
    other = {**RECORD_PERMISSION, "usecase": "churn", "run_id": "another-run", "where": []}
    objects = {**stored(record), **stored(other, name="ingest-data-zzz.json"),
               **stored({"x": 1}, name="junk.json"), "pipeline-root/churn/churn/failures/2026-11-14/bad.json": "{"}
    (detail,) = watched(monkeypatch, objects)
    assert detail["owner"] == "team" and detail["why"] == "prepare-data: TypeError in prepare.py, line 39"
    assert detail["cause"]["rule"] == "team_code" and "message" not in detail["cause"]["record"]
    assert detail["error"].startswith("prepare-data: The replica")           # what run.failed said before, kept
    # the email's short text carries the owner and why
    from pipewright.observability import events

    brief = events.brief(detail)
    assert "owner: team" in brief and "why: prepare-data: TypeError in prepare.py, line 39" in brief


def test_a_record_is_matched_by_the_jobs_id_in_its_paths(monkeypatch):
    """A step without a run_id parameter: Agent Platform puts the job's ID in the step's output paths."""
    record = {**RECORD_BUG, "usecase": "churn", "run_id": None, "where": [OUT]}
    (detail,) = watched(monkeypatch, stored(record, day="2026-11-13"))
    assert detail["cause"]["source"] == "failure_record"
    elsewhere = {**record, "where": [OUT.replace("churn-r7", "churn-r8")]}
    (detail,) = watched(monkeypatch, stored(elsewhere))
    assert detail["cause"]["source"] == "none" and detail["owner"] == "unclear"


def test_run_failed_is_exactly_as_before_when_records_are_off(monkeypatch):
    record = {**RECORD_BUG, "usecase": "churn", "run_id": "r7"}
    (detail,) = watched(monkeypatch, stored(record), on=False)
    assert set(detail) == {"pipeline", "job_id", "state", "trigger", "failed_steps", "error", "ended"}


def test_a_folder_that_cannot_be_read_still_reports_the_run(monkeypatch):
    import pipewright.gcp as pgcp

    gcp = FakeGcp([{**JOB, "jobDetail": {"taskDetails": [
        {"taskName": "prepare-data", "state": "FAILED",
         "error": {"message": "Replicas low on memory: workerpool0."}}]}}], {})
    monkeypatch.setattr(watch, "gcs_write", lambda *a, **k: 1)

    def refused(*a):
        raise RuntimeError("403")

    monkeypatch.setattr(pgcp, "gcs_list", refused)
    emitted = []
    watch.report(gcp, "b", "p", "europe-west2", [contract()], lambda c, rid, d: emitted.append(d), NOW)
    assert emitted[0]["cause"]["rule"] == "out_of_memory" and emitted[0]["owner"] == "team"


def test_kubeflow_falls_back_to_the_failed_pods(monkeypatch):
    """Found live on private GKE: a failed run can list no failed step and no error; the pods say why."""
    from pipewright import kube
    from pipewright.adapters.kfp import run_description

    labels = {"pipewright-usecase": "churn", "run-id": "r9", "pipewright-pipeline": "training", "trigger": "api"}
    run = {"run_id": "96e91452", "display_name": "churn-r9", "state": "FAILED",
           "description": run_description(labels), "created_at": "2026-11-14T09:50:00Z",
           "finished_at": "2026-11-14T09:58:00Z", "pipeline_spec": {"root": {"dag": {"tasks": {"prepare-data": {}}}}},
           "run_details": {"task_details": [{"display_name": "prepare-data", "state": "RUNNING"}]}}
    pod = {"metadata": {"name": "churn-training-bzdf2-system-container-impl-37",
                        "labels": {"workflows.argoproj.io/workflow": "churn-training-bzdf2"}},
           "spec": {"containers": [{"name": "main", "args": ["--task_name", "prepare-data", "--x", "y"]}]},
           "status": {"containerStatuses": [
               {"name": "wait", "state": {"terminated": {"reason": "Completed", "exitCode": 0}}},
               {"name": "main", "state": {"terminated": {"reason": "OOMKilled", "exitCode": 137}}}]}}
    asked = []

    class Kube:
        def get(self, path, **params):
            asked.append((path, params))
            if path.endswith("/pods"):
                return SimpleNamespace(ok=True, status=200, body={"items": [pod]})
            return SimpleNamespace(ok=True, status=200, body={"runs": [run]})

    monkeypatch.setattr(kube.KubeClient, "from_manifest", classmethod(lambda cls, m, g, dns=False: Kube()))
    monkeypatch.setattr(watch, "gcs_write", lambda *a, **k: 1)
    import pipewright.gcp as pgcp

    monkeypatch.setattr(pgcp, "gcs_list", lambda *a: [])
    emitted = []
    watch.report(None, "b", "p", "europe-west2", [contract()], lambda c, rid, d: emitted.append(d), NOW,
                 manifest={"kubernetes": {"namespace": "kubeflow"}})
    (detail,) = emitted
    assert detail["failed_steps"] == ["prepare-data"] and detail["owner"] == "team"
    assert detail["cause"]["source"] == "pod_status" and detail["cause"]["pod"]["reason"] == "OOMKilled"
    assert ("/api/v1/namespaces/kubeflow/pods", {"labelSelector": "pipeline/runid=96e91452"}) in asked
    # pods not readable (no role): the run is still reported, the cause unclear
    Kube.get = lambda self, path, **params: (SimpleNamespace(ok=False, status=403, body={}) if path.endswith("/pods")
                                             else SimpleNamespace(ok=True, status=200, body={"runs": [run]}))
    emitted.clear()
    watch.report(None, "b", "p", "europe-west2", [contract()], lambda c, rid, d: emitted.append(d), NOW,
                 manifest={"kubernetes": {"namespace": "kubeflow"}})
    assert emitted[0]["owner"] == "unclear"


def test_a_marker_is_still_written_once(monkeypatch):
    seen = set()

    def write(g, b, name, text, if_generation_match=None):
        if name in seen:
            raise PreconditionFailed(name)
        seen.add(name)
        assert json.loads(text)["cause"]["owner"] == "unclear"       # the marker holds it: the runs tool reads it
        return 1

    gcp = FakeGcp([JOB], {})
    monkeypatch.setattr(watch, "gcs_write", write)
    import pipewright.gcp as pgcp

    monkeypatch.setattr(pgcp, "gcs_list", lambda *a: [])
    emitted = []
    for _ in range(2):
        watch.report(gcp, "b", "p", "europe-west2", [contract()], lambda c, rid, d: emitted.append(d), NOW)
    assert len(emitted) == 1 and seen == {"runs/failed/churn-r7"}


# ------------------------------------------------------------------------------------ run.submit_failed --

def test_what_is_a_refusal_and_what_may_pass():
    class InvalidArgument(Exception):
        pass

    class ServiceUnavailable(Exception):
        pass

    r = cause.submit_refusal(InvalidArgument("400 Unable to find available machine with cpu_limit: 4.0 for 'x'"))
    assert r["error_type"] == "InvalidArgument" and (r["rule"], r["owner"]) == ("machine_shape", "team")
    assert "'x'" not in r["error"]
    assert cause.submit_refusal(InvalidArgument("400 pipeline spec is wrong"))["rule"] == "bad_request"
    assert cause.submit_refusal(ServiceUnavailable("503")) is None
    assert cause.submit_refusal(RuntimeError("creating Kubeflow run churn-r1 failed: 403 forbidden"))["owner"] \
        == "platform"
    assert cause.submit_refusal(RuntimeError("creating Kubeflow run churn-r1 failed: 503 unavailable")) is None
    assert cause.submit_refusal(TimeoutError("timed out")) is None


@pytest.fixture
def launcher(monkeypatch):
    pytest.importorskip("flask")
    from dataclasses import replace

    from pipewright import adapters
    from pipewright.launcher import app as app_module
    from pipewright.observability import events
    from tests.test_launcher import CHURN, FakeRegistry

    class InvalidArgument(Exception):
        pass

    def make(on_manifest=False, on_usecase=False, error=None):
        ml = {"platform": {"submit_failed": True}} if on_usecase else {"platform": {}}
        live = replace(CHURN, parameters=[*CHURN.parameters, "ml_config"],
                       static_parameters={**CHURN.static_parameters, "ml_config": ml})
        registry = FakeRegistry([live])
        if on_manifest:
            registry.manifest["observability"] = {"submit_failed": True}
        failure = error or InvalidArgument("400 Unable to find available machine with cpu_limit: 4.0")

        def refuse(*a, **k):
            raise failure

        monkeypatch.setattr(adapters, "submit", refuse)
        sent = []
        monkeypatch.setattr(events, "emit", lambda event, *a, **k: sent.append(event) or True)
        client = app_module.create_app(registry).test_client()
        client.sent = sent
        return client

    return make


def test_off_a_refused_submission_is_retried_as_before(launcher):
    client = launcher()
    assert client.post("/run", json={"usecase": "churn", "schedule": "nightly"}).status_code == 500
    assert [e["event_type"] for e in client.sent] == []


@pytest.mark.parametrize("where", ["manifest", "usecase"])
def test_on_a_refusal_is_recorded_once_and_answered(launcher, where):
    client = launcher(on_manifest=where == "manifest", on_usecase=where == "usecase")
    r = client.post("/run", json={"usecase": "churn", "schedule": "nightly"})
    assert r.status_code == 200 and r.json["status"] == "submit_failed" and r.json["owner"] == "team"
    (event,) = client.sent
    assert event["event_type"] == "run.submit_failed" and event["data"]["rule"] == "machine_shape"
    assert event["data"]["error_type"] == "InvalidArgument" and event["data"]["pipeline"] == "training"


def test_on_an_error_that_may_pass_is_still_retried(launcher):
    client = launcher(on_manifest=True, error=TimeoutError("deadline exceeded"))
    assert client.post("/run", json={"usecase": "churn", "schedule": "nightly"}).status_code == 500
    assert client.sent == []


# ----------------------------------------------------------------------------- the catalogue and the docs --

def test_the_new_event_is_in_the_catalogue_and_can_be_alerted_on(repo):
    from pipewright.alerts import sync as alerts
    from pipewright.observability import catalogue
    from tests.test_alerts import FakeMonitoring, uc_for

    event = catalogue.BY_TYPE["run.submit_failed"]
    assert event.since == "1.4.4" and set(event.personas) == {"ml", "sre"}
    uc = uc_for(repo, notify=["ml-team@example.com"], events=["run.submit_failed"])
    mon = FakeMonitoring()
    alerts.sync(uc, load_environment(repo.root, "dev"), mon)
    (policy,) = mon.policies.values()
    assert 'event_type="run.submit_failed"' in str(policy) and "could not be submitted" in str(policy)


def test_doctor_says_when_causes_are_not_recorded(repo):
    from pipewright.checks import doctor

    def notes(uc):
        report = doctor.Report()
        doctor._check_failure_records(uc, report, "s")
        return [(r.status, r.check) for results in report.sections.values() for r in results]

    repo.write()
    env = load_environment(repo.root, "dev")
    assert notes(resolve_usecase(repo.write(), env)) == [(doctor.INFO, "failure causes"),
                                                         (doctor.INFO, "refused submissions")]
    repo.usecase["spec"]["monitoring"] = {"failure_records": True, "submit_failed": True}
    assert notes(resolve_usecase(repo.write(), env)) == [(doctor.OK, "failure causes")]
