"""Registration through the launcher (FR-100, 1.4.3): the launcher's four checks, the upload with the default
decided by the release's policy, the answer the step waits for, and nothing changed while the policy is off."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from pipewright.gcp.session import Response
from pipewright.models import registration as reg

RUNNER = "churn-runner@acme-ml-dev.iam.gserviceaccount.com"
ROOT = "gs://acme-artifacts/pipeline-root/churn"
FOLDER = f"{ROOT}/123/train/model"
RECORD = {"metrics": {"auc": 0.81}, "champion": None, "decision": "approve"}
RECORD_TEXT = json.dumps(RECORD, indent=2)


def contract(set_default="auto", gates=True):
    ml = {"registry": {"set_default": set_default}, "platform": {"kms_key": None}, "model_name": "churn",
          "evaluation": {"primary_metric": "auc", "gates": [{"metric": "auc", "min": 0.7}] if gates else []}}
    return SimpleNamespace(usecase="churn", tenant="churn", project="acme-ml-dev", region="europe-west2",
                           environment="dev", runner_service_account=RUNNER, pipeline_root=ROOT, release_id="r1",
                           component_image="europe-west2-docker.pkg.dev/acme-ml-dev/pw-images/churn-components@sha256:"
                                           + "a" * 64,
                           static_parameters={"ml_config": ml})


class FakeGcp:
    def __init__(self, models=None, aliases=None):
        self.models = models or []
        self.aliases = aliases or {}
        self.uploads = []

    def get(self, url, **params):
        if url.endswith("/models") and "filter" in params:
            return Response(200, {"models": self.models})
        if "@" in url:
            found = self.aliases.get(url.rsplit("@", 1)[1])
            return Response(200, found) if found else Response(404, {})
        if "/operations/" in url:
            return Response(200, {"done": True, "response": {"model": "projects/p/locations/r/models/churn",
                                                             "modelVersionId": "3"}})
        raise AssertionError(url)

    def post(self, url, body, **params):
        assert url.endswith("/models:upload")
        self.uploads.append(body)
        return Response(200, {"name": "projects/p/locations/r/models/churn/operations/1"})


@pytest.fixture
def store(monkeypatch):
    objects = {("acme-artifacts", "pipeline-root/churn/123/train/model/evaluation.json"): RECORD_TEXT}
    monkeypatch.setattr(reg, "gcs_read", lambda gcp, b, n: (objects.get((b, n)), 1 if (b, n) in objects else 0))
    monkeypatch.setattr(reg, "gcs_write", lambda gcp, b, n, text, **kw: objects.__setitem__((b, n), text) or 1)
    return objects


def job(state="PIPELINE_STATE_RUNNING", sa=RUNNER, usecase="churn"):
    return lambda contract, run_id: {"state": state, "service_account": sa, "usecase": usecase}


def body(**extra):
    return {"usecase": "churn", "run_id": "abc", "display_name": "churn", "artifact_uri": FOLDER,
            "serving_image": "europe-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest",
            "evaluation": {"uri": f"{FOLDER}/evaluation.json",
                           "sha256": hashlib.sha256(RECORD_TEXT.encode()).hexdigest()}, **extra}


def answer(store):
    return json.loads(store[("acme-artifacts", "runs/registration/churn/run-abc.json")])


def test_registers_a_version_of_the_existing_model_and_moves_the_default_under_auto(store):
    gcp = FakeGcp(models=[{"name": "projects/p/locations/r/models/churn", "createTime": "1"}])
    out = reg.Registrations(gcp, "acme-artifacts", dict, job()).register(contract("auto"), body(), RUNNER)
    up = gcp.uploads[0]
    assert up["parentModel"] == "projects/p/locations/r/models/churn" and "modelId" not in up
    assert up["model"]["versionAliases"] == ["run-abc", "default"]
    assert up["model"]["labels"]["status"] == "champion" and up["model"]["labels"]["registered-by"] == "launcher"
    assert out["status"] == "registered" and out["version"] == "3" and out["default"] is True
    assert answer(store)["status"] == "registered"


def test_manual_never_moves_the_default(store):
    gcp = FakeGcp(models=[{"name": "projects/p/locations/r/models/churn", "createTime": "1"}])
    out = reg.Registrations(gcp, "acme-artifacts", dict, job()).register(contract("manual"), body(), RUNNER)
    assert gcp.uploads[0]["model"]["versionAliases"] == ["run-abc"]
    assert gcp.uploads[0]["model"]["labels"]["status"] == "candidate" and out["default"] is False


def test_a_first_version_gets_the_fixed_id_and_is_pending_under_manual(store):
    gcp = FakeGcp()
    reg.Registrations(gcp, "acme-artifacts", dict, job()).register(contract("manual", gates=False),
                                                                   body(evaluation=None), RUNNER)
    up = gcp.uploads[0]
    assert up["modelId"] == "churn" and "parentModel" not in up
    assert up["model"]["labels"]["approval"] == "pending"


def test_a_repeated_request_answers_the_same_without_uploading(store):
    gcp = FakeGcp(models=[{"name": "projects/p/locations/r/models/churn", "createTime": "1"}],
                  aliases={"run-abc": {"versionId": "3", "versionAliases": ["run-abc", "default"]}})
    out = reg.Registrations(gcp, "acme-artifacts", dict, job()).register(contract(), body(), RUNNER)
    assert not gcp.uploads and out["version"] == "3" and out["repeated"] is True


@pytest.mark.parametrize("case,expect", [
    ({"caller": "someone@example.com"}, "only its runner identity"),
    ({"job": None}, "no pipeline run"),
    ({"job": job(usecase="other")}, "belongs to other"),
    ({"job": job(sa="other@example.com")}, "not churn's runner"),
    ({"job": job(state="PIPELINE_STATE_SUCCEEDED")}, "only a running run"),
    ({"body": {"artifact_uri": "gs://acme-artifacts/pipeline-root/retail/9/model"}}, "not under churn's pipeline root"),
    ({"body": {"evaluation": {"uri": f"{FOLDER}/evaluation.json", "sha256": "0" * 64}}}, "changed after the step"),
    ({"body": {"evaluation": None}}, "names no evaluation record"),
    ({"record": {"metrics": {"auc": 0.6}}}, "does not pass churn's gates"),
    # GA QA: another model's name, an image from anywhere, a record whose own decision is reject
    ({"body": {"display_name": "fraud-classifier"}}, "registers only its own model 'churn'"),
    ({"body": {"serving_image": "docker.io/someone/server:latest"}}, "neither one of Google's prebuilt"),
    ({"body": {"serving_image": "europe-west2-docker.pkg.dev/other-project/pw-images/x:1"}}, "neither one of"),
    ({"record": {"metrics": {"auc": 0.81}, "decision": "reject"}}, "own decision is reject"),
])
def test_refusals_are_answered_and_upload_nothing(store, case, expect):
    if "record" in case:
        text = json.dumps(case["record"], indent=2)
        store[("acme-artifacts", "pipeline-root/churn/123/train/model/evaluation.json")] = text
        b = body(evaluation={"uri": f"{FOLDER}/evaluation.json", "sha256": hashlib.sha256(text.encode()).hexdigest()})
    else:
        b = body(**case.get("body", {}))
    finder = case["job"] if "job" in case and case["job"] is not None else (
        (lambda c, r: None) if "job" in case else job())
    gcp = FakeGcp()
    with pytest.raises(reg.RegistrationRefused) as e:
        reg.Registrations(gcp, "acme-artifacts", dict, finder).register(contract(), b, case.get("caller", RUNNER))
    assert expect in e.value.reason
    assert not gcp.uploads
    assert answer(store)["status"] == "refused" and expect in answer(store)["reason"]


def test_off_by_default_the_release_compiles_as_before(repo):
    from pipewright.config import load_environment, resolve_usecase
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert "registration" not in uc.platform and "artifacts_bucket" not in uc.platform


def test_the_launcher_routes(monkeypatch, store):
    from pipewright.launcher import app as launcher_app

    class Reg:
        manifest = {"artifacts_bucket": "acme-artifacts"}
        bucket = "acme-artifacts"
        gcp = FakeGcp(models=[{"name": "projects/p/locations/r/models/churn", "createTime": "1"}])
        errors = {}

        def get(self, usecase, release=None):
            return contract() if usecase == "churn" else None

        def live(self):
            return [contract()]

    sent = []
    monkeypatch.setattr(launcher_app.events, "emit", lambda e, *a, **k: sent.append(e))
    monkeypatch.setattr(launcher_app.identity, "cloud_run_caller", lambda h: RUNNER)
    monkeypatch.setattr(reg.Registrations, "_agent_platform_job", lambda self, c, r: job()(c, r))
    client = launcher_app.create_app(Reg()).test_client()
    r = client.post("/models/register", json=body())
    assert r.status_code == 200 and r.get_json()["version"] == "3"
    assert [e["event_type"] for e in sent] == ["model.registered"] and sent[0]["data"]["registered_by"] == "launcher"
    sent.clear()
    r = client.post("/models/register", json=body(artifact_uri="gs://elsewhere/model"))
    assert r.status_code == 403 and [e["event_type"] for e in sent] == ["model.registration_refused"]


def test_launcher_policy_reaches_the_steps_and_validate_finds_direct_writes(repo):
    from pipewright.config import load_environment, resolve_usecase
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    repo.manifests["dev"]["policies"] = {"registration": "launcher"}
    path = repo.write()
    components = path.parent / "components"
    components.mkdir(exist_ok=True)
    (components / "my_register.py").write_text("from google.cloud import aiplatform\naiplatform.Model.upload(x)\n",
                                               encoding="utf-8")
    (components / "ok_register.py").write_text("aiplatform.Model.upload(x)\nregister_version(c, r, {})\n",
                                               encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    assert uc.platform["registration"] == "launcher" and uc.platform["artifacts_bucket"]
    found = [w for w in uc.warnings if "FR-100" in w]
    assert len(found) == 1 and "my_register.py" in found[0] and "403" in found[0]


def test_doctor_says_whether_runners_can_still_upload():
    from pipewright.checks import doctor

    env = SimpleNamespace(name="prod", project="p", policies={"registration": "launcher"},
                          tenants={"churn": {"runner_service_account": RUNNER}})

    class G:
        def __init__(self, bindings):
            self.bindings = bindings

        def post(self, url, body, **params):
            return Response(200, {"bindings": self.bindings})

    for bindings, level in (([{"role": "roles/aiplatform.user", "members": [f"serviceAccount:{RUNNER}"]}], "FAIL"),
                            ([{"role": "projects/p/roles/pw_runner", "members": [f"serviceAccount:{RUNNER}"]}], "OK")):
        report = doctor.Report()
        doctor._check_registration(env, G(bindings), report, "s", doctor.WARN)
        result = report.sections["s"][0]
        assert result.status == getattr(doctor, level), (bindings, result)


def test_the_answer_names_its_request_the_same_way_the_step_does():
    # GA QA: a run that sent a refused request and then a good one must take the answer to the good one only
    import hashlib
    import inspect
    import json

    from pipewright.build.templates import pipewright_runtime as rt
    from pipewright.models.registration import request_digest

    body = {"display_name": "m", "artifact_uri": "gs://b/x", "env": None, "metrics": {"auc": 0.8125},
            "evaluation": {"uri": "gs://b/x/evaluation.json", "sha256": "a" * 64}, "usecase": "u", "run_id": "r1"}
    sent = json.loads(json.dumps(body, default=str))
    step = hashlib.sha256(json.dumps(sent, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
    assert request_digest({**sent, "kind": "model.registration", "id_token": "t"}) == step
    assert request_digest({**sent, "artifact_uri": "gs://b/y"}) != step
    src = inspect.getsource(rt.register_version)
    assert 'answer.get("request") not in (None, digest)' in src            # older launchers: no digest, accepted


def test_an_image_in_the_environments_repository_is_accepted(store):
    gcp = FakeGcp(models=[{"name": "projects/p/locations/r/models/churn", "createTime": "2026-01-01T00:00:00Z"}])
    image = "europe-west2-docker.pkg.dev/acme-ml-dev/pw-images/churn-serving@sha256:" + "b" * 64
    out = reg.Registrations(gcp, "acme-artifacts", dict, job()).register(contract(), body(serving_image=image), RUNNER)
    assert out["status"] == "registered" and gcp.uploads[0]["model"]["containerSpec"]["imageUri"] == image


def test_a_bigquery_ml_use_case_is_not_warned_about_pipewrights_own_steps(repo):
    # GA QA: the scan read Pipewright's bqml_runtime.py (it registers through the launcher itself)
    from tests.test_bqml_config import bqml_repo, resolve

    bqml_repo(repo)
    repo.manifests["dev"]["policies"] = {"registration": "launcher"}
    uc = resolve(repo)
    assert not [w for w in uc.warnings if "FR-100" in w]
