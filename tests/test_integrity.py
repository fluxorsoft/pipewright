"""Phase 6 (FR-93, FR-94, FR-96, FR-97): what runs is what was approved.

Every message names records, files, digests, metrics and gates, never a data value."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import integrity


def env(**policies):
    return SimpleNamespace(name="prod", project="p", region="r", policies=policies, security={},
                           observability={"evidence_dataset": "pw_evidence"})


# -- the policy ----------------------------------------------------------------------------------------------------

def test_the_policy_refuses_by_default_and_rejects_unknown_modes():
    assert integrity.policy(env()) == {"mode": "refuse", "lock": {"days": 0, "mode": "unlocked"}}
    assert integrity.policy(env(integrity={"mode": "warn", "lock": {"days": 30, "mode": "locked"}})) == {
        "mode": "warn", "lock": {"days": 30, "mode": "locked"}}
    with pytest.raises(ConfigError, match="refuse, warn"):
        integrity.policy(env(integrity={"mode": "ignore"}))


# -- a fake Cloud Storage: objects by name with generation and bytes ----------------------------------------------

class Store:
    def __init__(self, files):
        self.files = {name: (gen, data) for name, (gen, data) in files.items()}     # name -> (generation, bytes)
        self.patched = []
        self.refuse_patch = False

    def get(self, url, **params):
        prefix = params["prefix"]
        items = [{"name": n, "size": str(len(d)), "generation": str(g)} for n, (g, d) in self.files.items()
                 if n.startswith(prefix)]
        return SimpleNamespace(ok=True, status=200, body={"items": items})

    @property
    def session(self):
        store = self

        class Session:
            def get(self, url, params=None, stream=False, timeout=None):
                name = url.split("/o/", 1)[1].replace("%2F", "/")
                gen, data = store.files[name]
                assert params["generation"] == str(gen)
                return SimpleNamespace(status_code=200, iter_content=lambda n: [data])

            def patch(self, url, params=None, json=None, timeout=None):
                store.patched.append((url, params, json))
                if store.refuse_patch:
                    return SimpleNamespace(status_code=412, text="object retention is not enabled on this bucket")
                return SimpleNamespace(status_code=200, text="")

            def head(self, url, headers=None, timeout=None):
                digest = store.files.get("__tag__", (0, b""))[1].decode() or None
                return SimpleNamespace(status_code=200 if digest else 404,
                                       headers={"Docker-Content-Digest": digest} if digest else {})

        return Session()


FOLDER = "gs://art/pipeline-root/churn/run1/model"


def store(**changes):
    files = {"pipeline-root/churn/run1/model/model.pt": (1, b"weights"),
             "pipeline-root/churn/run1/model/evaluation.json": (2, b'{"auc": 0.81}'),
             "pipeline-root/churn/run1/model/lineage.json": (3, b"{}")}
    files.update(changes)
    return Store({k: v for k, v in files.items() if v is not None})


def test_the_approval_fingerprints_every_model_file():
    s = store()
    files = integrity.model_files(s, FOLDER)
    assert files["count"] == 3 and [f["file"] for f in files["files"]] == ["evaluation.json", "lineage.json",
                                                                          "model.pt"]
    assert files["files"][2] == {"file": "model.pt", "generation": 1, "size": 7,
                                 "sha256": hashlib.sha256(b"weights").hexdigest()}
    assert integrity.model_files(s, None) is None                          # BigQuery ML: no folder
    assert integrity.compare_files(s, files, FOLDER) == []


def test_any_change_to_the_approved_files_is_named_without_its_contents():
    approved = integrity.model_files(store(), FOLDER)
    swapped = store(**{"pipeline-root/churn/run1/model/model.pt": (9, b"other weights")})
    problems = integrity.compare_files(swapped, approved, FOLDER)
    assert problems == [f"model.pt: changed after the approval (sha256 {hashlib.sha256(b'weights').hexdigest()[:16]} "
                        "approved)"]
    assert "other weights" not in json.dumps(problems)
    # rewritten with the same bytes: a new generation, the same content, nothing to report
    same = store(**{"pipeline-root/churn/run1/model/model.pt": (9, b"weights")})
    assert integrity.compare_files(same, approved, FOLDER) == []
    gone = store(**{"pipeline-root/churn/run1/model/model.pt": None,
                    "pipeline-root/churn/run1/model/extra.bin": (5, b"x")})
    assert integrity.compare_files(gone, approved, FOLDER) == ["model.pt: approved, now missing",
                                                               "extra.bin: added after the approval"]


def test_a_large_folder_is_checked_by_its_digest():
    approved = {k: v for k, v in integrity.model_files(store(), FOLDER).items() if k != "files"}
    assert integrity.compare_files(store(), approved, FOLDER) == []
    problems = integrity.compare_files(store(**{"pipeline-root/churn/run1/model/model.pt": (9, b"w2")}), approved,
                                       FOLDER)
    assert len(problems) == 1 and "are not the approved ones" in problems[0]


# -- the serving image ---------------------------------------------------------------------------------------------

def test_a_serving_image_by_tag_is_resolved_and_a_moved_tag_is_caught():
    s = store(__tag__=(0, b"sha256:" + b"a" * 64))
    image = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"
    approved = integrity.serving_image(s, image)
    assert approved == {"image": image, "digest": "sha256:" + "a" * 64, "pinned": False}
    assert integrity.compare_image(s, approved, image) == []
    moved = store(__tag__=(0, b"sha256:" + b"b" * 64))
    assert "its tag moved" in integrity.compare_image(moved, approved, image)[0]
    pinned = "us-docker.pkg.dev/x/y/z@sha256:" + "c" * 64
    assert integrity.serving_image(s, pinned)["pinned"] is True
    assert integrity.compare_image(s, approved, "other/image:1")[0].startswith("the serving image is other/image:1")


# -- the released rules (FR-96) ------------------------------------------------------------------------------------

def test_approval_is_judged_by_the_released_rules(monkeypatch):
    released = {"auc": {"min": 0.8}}
    contract = SimpleNamespace(metadata={"approvals": {"promote_model": "manual", "approvers": ["user:a@x"]}})
    monkeypatch.setattr("pipewright.release.service.read_contract", lambda gcp, env, name: contract)
    monkeypatch.setattr("pipewright.retraining.service.ml_config", lambda c: {"evaluation": released})
    uc = SimpleNamespace(name="churn", ml={"evaluation": {"auc": {"min": 0.5}}},
                         approvals={"promote_model": "manual", "approvers": ["user:a@x", "user:me@x"]})
    rules = integrity.released_rules(uc, env(), None, "rel1")
    assert rules.evaluation == released and rules.source == "release"
    assert rules.differs == ["ml.evaluation", "approvals"]                # a lowered gate, an approver added
    assert rules.record()["release"] == "rel1"
    same = SimpleNamespace(name="churn", ml={"evaluation": released}, approvals=contract.metadata["approvals"])
    assert integrity.released_rules(same, env(), None, "rel1").differs == []
    old = SimpleNamespace(metadata={})                                    # a release from before phase 6
    monkeypatch.setattr("pipewright.release.service.read_contract", lambda gcp, env, name: old)
    rules = integrity.released_rules(same, env(), None, "rel1")
    assert rules.approvals == same.approvals and "predates phase 6" in rules.note
    assert integrity.released_rules(same, env(), None, None).source == "working copy"


# -- verify and enforce (FR-93) ------------------------------------------------------------------------------------

def test_verify_compares_what_is_on_file_with_the_approval(monkeypatch):
    from pipewright.models import gates

    approved_files = integrity.model_files(store(), FOLDER)
    approval = {"at": "2026-11-05T10:00:00", "by": "risk@acme", "files": approved_files,
                "evaluation": {"record": FOLDER + "/evaluation.json", "sha256": "ab" * 32}}
    monkeypatch.setattr(integrity, "approval_events", lambda gcp, env, uc, v: [approval])
    monkeypatch.setattr(gates, "record_digest", lambda gcp, folder: {"sha256": "ab" * 32})
    ok = integrity.verify(store(), env(), "churn", "4", FOLDER, None)
    assert ok.problems == [] and ok.summary() == {"result": "verified", "problems": 0,
                                                  "approved_at": "2026-11-05T10:00:00", "approved_by": "risk@acme"}
    monkeypatch.setattr(gates, "record_digest", lambda gcp, folder: {"sha256": "cd" * 32})
    bad = integrity.verify(store(**{"pipeline-root/churn/run1/model/model.pt": (9, b"x")}), env(), "churn", "4",
                           FOLDER, None)
    assert bad.problems[0].startswith("the evaluation record changed after the approval: sha256 abababababababab")
    assert bad.problems[1].startswith("model.pt: changed after the approval")
    monkeypatch.setattr(gates, "record_digest", lambda gcp, folder: None)
    assert "is missing" in integrity.verify(store(), env(), "churn", "4", FOLDER, None).problems[0]


def test_a_default_nobody_approved_here_is_a_problem(monkeypatch):
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [])
    check = integrity.verify(store(), env(), "churn", "4", FOLDER, None)
    assert "became the default outside Pipewright" in check.problems[0]
    monkeypatch.setattr(integrity, "approval_events", lambda *a: None)            # no event log at all
    check = integrity.verify(store(), env(), "churn", "4", FOLDER, None)
    assert check.problems == [] and "keeps no event log" in check.notes[0]

    def unreadable(*a):
        raise RuntimeError("403 Access Denied: user does not have bigquery.tables.getData")

    monkeypatch.setattr(integrity, "approval_events", unreadable)
    assert "cannot be read" in integrity.verify(store(), env(), "churn", "4", FOLDER, None).problems[0]


def test_an_approval_before_phase_6_still_deploys_with_a_note(monkeypatch):
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [{"at": "2026-10-30T10:00:00", "by": "a@x"}])
    check = integrity.verify(store(), env(), "churn", "4", FOLDER, None)
    assert check.problems == [] and any("predates Pipewright 1.3.1" in n for n in check.notes)
    assert any("approved before phase 6" in n for n in check.notes)


def test_enforce_refuses_by_default_and_warns_when_the_policy_says_so():
    check = integrity.Check(problems=["model.pt: changed after the approval"], notes=["a note"])
    with pytest.raises(ConfigError, match="what is on file is not what was approved"):
        integrity.enforce(check, env(), "deploying v4", log=lambda m: None)
    said = []
    integrity.enforce(check, env(integrity={"mode": "warn"}), "deploying v4", log=said.append)
    assert said == ["integrity: a note", "integrity WARNING (policies.integrity.mode warn): model.pt: changed after "
                                         "the approval"]


# -- the retention lock (FR-97) ------------------------------------------------------------------------------------

def test_approved_files_are_locked_for_the_policy_s_days():
    s = store()
    files = integrity.model_files(s, FOLDER)
    assert integrity.lock(s, env(), files, log=lambda m: None) is None             # no lock policy: nothing
    said = []
    done = integrity.lock(s, env(integrity={"lock": {"days": 30}}), files, log=said.append)
    assert done["locked"] == 3 and done["mode"] == "unlocked" and len(s.patched) == 3
    url, params, body = s.patched[0]
    assert params == {"generation": "2"} and body["retention"]["mode"] == "Unlocked"
    s.refuse_patch = True
    done = integrity.lock(s, env(integrity={"lock": {"days": 30, "mode": "locked"}}), files, log=said.append)
    assert done["locked"] == 0 and "HTTP 412" in done["error"] and "could not be locked" in said[-1]
