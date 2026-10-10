import configparser
import subprocess
from dataclasses import replace

import pytest

from pipewright.build.images import find_components, kfp_config_ini
from pipewright.errors import ConfigError
from pipewright.release.contract import ReleaseContract, new_pointer
from pipewright.release.service import release_id_for

DIGEST = "sha256:" + "a" * 64
# A v2 contract (single pipeline), as the launcher still reads them.
CONTRACT = ReleaseContract(
    contract_version="2", release_id="abc123", usecase="churn", tenant="churn", environment="dev",
    blueprint="poc@0.1.0", orchestrator="vertex", project="p", region="europe-west2",
    pipeline_name="churn-training", template_uri=f"https://europe-west2-kfp.pkg.dev/p/r/churn-training/{DIGEST}",
    template_digest=DIGEST, component_image=f"europe-west2-docker.pkg.dev/p/r/churn-components@{DIGEST}",
    pipeline_root="gs://b/pipeline-root/churn", runner_service_account="runner@p.iam.gserviceaccount.com",
    parameters=["auc_threshold", "project", "region", "run_id", "source_uri"],
    static_parameters={"auc_threshold": 0.75},
    triggers={"landing_prefix": "incoming/churn/", "landing_suffixes": [".csv"], "api": True, "schedules": {}},
)

TRAINING = {"pipeline_name": CONTRACT.pipeline_name, "template_uri": CONTRACT.template_uri,
            "template_digest": DIGEST, "parameters": CONTRACT.parameters,
            "static_parameters": CONTRACT.static_parameters, "experiment": "churn"}
INFERENCE = {"pipeline_name": "churn-inference", "template_uri": f"https://europe-west2-kfp.pkg.dev/p/r/churn-inference/{DIGEST}",
             "template_digest": DIGEST, "parameters": ["ml_config", "project", "region", "run_id", "source_uri"],
             "static_parameters": {"ml_config": {"usecase": "churn"}}, "experiment": None}
V3 = replace(CONTRACT, contract_version="3", pipelines={"training": TRAINING, "inference": INFERENCE},
             triggers={**CONTRACT.triggers, "landing_pipeline": "inference",
                       "schedules": {"weekly": {"cron": "0 3 * * 1", "timezone": "UTC", "pipeline": "training"}}})


def test_contract_roundtrip_and_validate():
    again = ReleaseContract.from_json(CONTRACT.to_json())
    assert again == CONTRACT
    again.validate()
    assert again.pipeline_names == ["training"] and again.pipeline().template_uri == CONTRACT.template_uri


def test_v3_contract_pipelines():
    again = ReleaseContract.from_json(V3.to_json())
    again.validate()
    assert again.pipeline_names == ["training", "inference"]
    assert again.pipeline("training").experiment == "churn"
    assert again.pipeline("inference").static_parameters == {"ml_config": {"usecase": "churn"}}
    with pytest.raises(ConfigError, match="has no scoring pipeline"):
        again.pipeline("scoring")


@pytest.mark.parametrize("change, message", [
    ({"pipelines": {"inference": INFERENCE}}, "pipelines.training is required"),
    ({"pipelines": {"training": {**TRAINING, "pipeline_name": "x"}, "inference": INFERENCE}},
     "must describe pipelines.training"),
    ({"pipelines": {"training": TRAINING}}, "starts pipeline 'inference'"),
    ({"pipelines": {"training": TRAINING, "inference": {**INFERENCE, "parameters": ["project"]}}},
     "inference: template does not accept launcher parameters"),
])
def test_v3_contract_rejects(change, message):
    with pytest.raises(ConfigError, match=message):
        replace(V3, **change).validate()


@pytest.mark.parametrize("change, message", [
    ({"contract_version": "1"}, "not supported"),
    ({"parameters": ["project"]}, "launcher parameters"),
    ({"static_parameters": {"epochs": 3}}, "not accepted by the template"),
    ({"static_parameters": {"run_id": "x"}}, "may not set launcher parameters"),
    ({"component_image": "repo/img:latest"}, "pinned by digest"),
    ({"template_uri": "https://x/churn-training/latest"}, "sha256 digest"),
])
def test_contract_rejects(change, message):
    with pytest.raises(ConfigError, match=message):
        replace(CONTRACT, **change).validate()


def test_pointer_history_newest_first_and_capped():
    p = new_pointer("churn", "r1", "contracts/churn/r1.json", "me")
    for i in range(2, 30):
        p = p.moved_to(f"r{i}", f"contracts/churn/r{i}.json", "me")
    assert p.release_id == "r29"
    assert p.history[0]["release_id"] == "r28" and len(p.history) == 20


def test_find_components_and_kfp_config(tmp_path):
    (tmp_path / "a.py").write_text(
        "from kfp import dsl\n@dsl.component(base_image='x')\ndef ingest(): ...\n\ndef helper(): ...\n",
        encoding="utf-8")
    (tmp_path / "b.py").write_text("from kfp.dsl import component\n@component\ndef train(): ...\n",
                                   encoding="utf-8")
    (tmp_path / "handler.py").write_text("class Handler: ...\n", encoding="utf-8")
    found = find_components(tmp_path)
    assert found == {"ingest": "a.py", "train": "b.py"}
    cp = configparser.ConfigParser()
    cp.optionxform = str
    cp.read_string(kfp_config_ini(found))
    assert dict(cp["Components"]) == {"ingest": "a.py", "train": "b.py"}


def test_duplicate_component_names_rejected(tmp_path):
    for f in ("a.py", "b.py"):
        (tmp_path / f).write_text("from kfp import dsl\n@dsl.component\ndef train(): ...\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="defined in a.py and b.py"):
        find_components(tmp_path)


def test_release_id_clean_dirty_and_no_git(tmp_path):
    rid, commit = release_id_for(tmp_path)
    assert rid.startswith("local-") and commit is None

    def git(*a):
        subprocess.run(["git", "-C", str(tmp_path), *a], check=True, capture_output=True)

    git("init", "-q")
    (tmp_path / "f.txt").write_text("x", encoding="utf-8")
    git("add", ".")
    git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "c")
    rid, commit = release_id_for(tmp_path)
    assert len(rid) == 12 and commit.startswith(rid)
    (tmp_path / "f.txt").write_text("y", encoding="utf-8")
    rid, _ = release_id_for(tmp_path)
    assert "-dirty-" in rid


def test_schedule_jobs(repo):
    import base64
    import json

    from pipewright.config import load_environment, resolve_usecase
    from pipewright.schedules.sync import _comparable, desired_jobs

    repo.add_env("prod")
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "prod"))
    jobs = desired_jobs(uc, load_environment(repo.root, "prod"))
    job = jobs["pw-churn-nightly"]
    assert job["schedule"] == "0 2 * * *" and job["timeZone"] == "Europe/London"
    assert job["httpTarget"]["uri"] == "https://launcher.example/run"
    assert job["httpTarget"]["oidcToken"]["audience"] == "https://launcher.example"
    assert json.loads(base64.b64decode(job["httpTarget"]["body"])) == {"usecase": "churn", "schedule": "nightly"}
    changed = dict(job, schedule="0 3 * * *")
    assert _comparable(changed) != _comparable(job)


def test_deploy_approval(repo):
    from pipewright.config import load_environment, resolve_usecase
    from pipewright.models.deploy import check_approval

    repo.add_env("prod")
    repo.usecase["spec"]["approvals"]["approvers"] = ["user:lead@acme.example", "group:ml-leads@acme.example"]
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "prod"))
    with pytest.raises(ConfigError, match="re-run with --approve"):
        check_approval(uc, "lead@acme.example", approved=False)
    with pytest.raises(ConfigError, match="is not an approver"):
        check_approval(uc, "someone@acme.example", approved=True)
    check_approval(uc, "lead@acme.example", approved=True)


def test_a_changed_template_is_a_new_release_even_on_the_same_commit(repo, monkeypatch, tmp_path):
    """Contracts are write-once: same commit, images and templates reuse the contract; a template that compiles
    differently (a Pipewright upgrade) gets a new release ID instead of the old contract."""
    from types import SimpleNamespace

    from pipewright.config import load_environment, resolve_usecase
    from pipewright.release import service

    path = repo.write()
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    env = load_environment(repo.root, "dev")
    store, spec = {}, tmp_path / "training.yaml"
    spec.write_text("template: 1\n")
    image = f"europe-west2-docker.pkg.dev/acme-ml-dev/ml-images/churn-components@{DIGEST}"

    def compile_pinned(uc, component_image, out_dir):
        return {"training": SimpleNamespace(name="churn-training", spec_path=spec,
                                            parameters={"project", "region", "source_uri", "run_id"})}

    def upload(info, repository, uc, release_id, creds):
        digest = "sha256:" + __import__("hashlib").sha256(info.spec_path.read_bytes()).hexdigest()
        return f"https://europe-west2-kfp.pkg.dev/acme-ml-dev/ml-pipelines/churn-training/{digest}", digest

    class FakeGcp:
        session = SimpleNamespace(credentials=None)

        def identity(self):
            return "ci@acme"

    monkeypatch.setattr(service, "release_id_for", lambda d: ("abc123", "abc123full"))
    monkeypatch.setattr(service, "read_pointer", lambda gcp, env, name: (None, 0))
    monkeypatch.setattr(service, "gcs_read", lambda gcp, bucket, name: (store.get(name), 1))
    monkeypatch.setattr(service, "gcs_write", lambda gcp, bucket, name, text, **k: store.__setitem__(name, text))
    monkeypatch.setattr(service, "_token", lambda gcp: "tok")
    monkeypatch.setattr(service.images, "build_and_push", lambda *a, **k: image)
    monkeypatch.setattr(service.publish, "compile_pinned", compile_pinned)
    monkeypatch.setattr(service.publish, "upload_template", upload)
    monkeypatch.setattr(service, "verify", lambda contract, gcp, creds: [])
    monkeypatch.setattr(service.events, "emit", lambda *a, **k: None)
    logs = []
    first = service.release(uc, env, FakeGcp(), skip_tables=True, log=logs.append)
    again = service.release(uc, env, FakeGcp(), skip_tables=True, log=logs.append)
    assert first.contract.release_id == again.contract.release_id == "abc123"
    assert any("already published with these images and templates" in m for m in logs)
    spec.write_text("template: 2\n")                         # the same commit compiles differently
    changed = service.release(uc, env, FakeGcp(), skip_tables=True, log=logs.append)
    assert changed.contract.release_id.startswith("abc123-") and changed.contract.template_digest != \
        first.contract.template_digest
    # Each tenant's runs write under its own prefix of the artifacts bucket (R-04); a contract published with the
    # older shared root is not reused for the same commit.
    assert changed.contract.pipeline_root == f"gs://{env.artifacts_bucket}/pipeline-root/{uc.tenant}/{uc.name}"
    import json as _json

    legacy = _json.loads(changed.contract.to_json())
    legacy["release_id"], legacy["pipeline_root"] = "abc123", f"gs://{env.artifacts_bucket}/pipeline-root/{uc.name}"
    for key in list(store):
        store.pop(key)
    store[f"contracts/{uc.name}/abc123.json"] = _json.dumps(legacy)
    moved = service.release(uc, env, FakeGcp(), skip_tables=True, log=logs.append)
    assert moved.contract.release_id.startswith("abc123-") and moved.contract.pipeline_root.endswith(
        f"/pipeline-root/{uc.tenant}/{uc.name}")
