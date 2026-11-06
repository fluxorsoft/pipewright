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
                if "/o/" not in url:                                   # the bucket: object retention or not
                    mode = {"mode": "Enabled"} if getattr(store, "retention", False) else {}
                    return SimpleNamespace(status_code=200, json=lambda: {"objectRetention": mode} if mode else {})
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
    approved_files = integrity.model_files(store(), FOLDER)
    approval = {"at": "2026-11-05T10:00:00", "by": "risk@acme", "files": approved_files,
                "evaluation": {"record": FOLDER + "/evaluation.json", "sha256": "ab" * 32}}
    monkeypatch.setattr(integrity, "approval_events", lambda gcp, env, uc, v: [approval])
    monkeypatch.setattr(integrity, "record_sha256", lambda gcp, record: "ab" * 32)
    ok = integrity.verify(store(), env(), "churn", "4", FOLDER, None)
    assert ok.problems == [] and ok.summary() == {"result": "verified", "problems": 0,
                                                  "approved_at": "2026-11-05T10:00:00", "approved_by": "risk@acme"}
    monkeypatch.setattr(integrity, "record_sha256", lambda gcp, record: "cd" * 32)
    bad = integrity.verify(store(**{"pipeline-root/churn/run1/model/model.pt": (9, b"x")}), env(), "churn", "4",
                           FOLDER, None)
    assert bad.problems[0].startswith("the evaluation record changed after the approval: sha256 abababababababab")
    assert bad.problems[1].startswith("model.pt: changed after the approval")
    monkeypatch.setattr(integrity, "record_sha256", lambda gcp, record: None)
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

def test_approved_files_are_locked_with_retention_or_a_hold():
    s = store()
    files = integrity.model_files(s, FOLDER)
    assert integrity.lock(s, env(), files, log=lambda m: None) is None             # no lock policy: nothing
    said = []
    s.retention = True                                     # a bucket created with object retention
    done = integrity.lock(s, env(integrity={"lock": {"days": 30}}), files, log=said.append)
    assert done["locked"] == 3 and done["method"] == "retention" and done["until"]
    url, params, body = s.patched[0]
    assert params == {"generation": "2"} and body["retention"]["mode"] == "Unlocked"
    s.retention, s.patched = False, []                     # an existing bucket: Cloud Storage cannot add retention
    done = integrity.lock(s, env(integrity={"lock": {"days": 30}}), files, log=said.append)
    assert done["method"] == "hold" and s.patched[0][2] == {"temporaryHold": True} and "until" not in done
    assert "temporary hold" in said[-1]
    s.patched = []
    done = integrity.lock(s, env(integrity={"lock": {"days": 30, "mode": "locked"}}), files, log=said.append)
    assert done["locked"] == 0 and done["method"] == "none" and s.patched == []
    assert "only sets when the" in done["error"]
    s.retention, s.refuse_patch = True, True
    done = integrity.lock(s, env(integrity={"lock": {"days": 30}}), files, log=said.append)
    assert done["locked"] == 0 and "HTTP 412" in done["error"]


# -- deploy-model refuses before it touches the endpoint (FR-93) ---------------------------------------------------

def _plan(check):
    from pipewright.models import deploy

    return deploy.DeployPlan("churn-classifier", "4", "churn-endpoint", "n1-standard-2", {"min": 1, "max": 1}, False,
                             [], [], {}, None, check, "rel1")


def test_deploy_model_refuses_what_was_not_approved_before_touching_the_endpoint(monkeypatch):
    from pipewright.models import deploy

    touched = []
    monkeypatch.setattr(deploy, "_aiplatform", lambda env, creds: touched.append("aiplatform") or SimpleNamespace(
        Model=SimpleNamespace(list=lambda **k: touched.append("model") or []),
        Endpoint=SimpleNamespace(list=lambda **k: touched.append("endpoint") or [])))
    check = integrity.Check(problems=["model.pt: changed after the approval (sha256 0123456789abcdef approved)"])
    monkeypatch.setattr(deploy, "plan", lambda uc, env, creds: _plan(check))
    uc = SimpleNamespace(name="churn", model={"name": "churn-classifier"})
    with pytest.raises(ConfigError, match="not what was approved") as e:
        deploy.deploy(uc, env(), None, log=lambda m: None)
    assert "model.pt" in str(e.value) and "endpoint" not in touched and "model" not in touched
    # warn: the deploy goes on (here it stops at the fake registry), and says what it found
    said = []
    with pytest.raises(IndexError):                    # the fake registry has no model: past the check
        deploy.deploy(uc, env(integrity={"mode": "warn"}), None, log=said.append)
    assert said[0].startswith("integrity WARNING (policies.integrity.mode warn): model.pt")


def test_the_deployed_event_records_the_integrity_result(monkeypatch):
    from pipewright.models import deploy

    emitted = []
    monkeypatch.setattr("pipewright.observability.events.emit", lambda event, *a, **k: emitted.append(event))
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda creds=None: SimpleNamespace(identity=lambda: "ops@acme"))
    uc = SimpleNamespace(name="churn", tenant="retail", model={"name": "churn-classifier"})
    check = integrity.Check(approval={"at": "2026-11-05T10:00:00", "by": "risk@acme"})
    deploy._deployed(uc, SimpleNamespace(name="prod", region="r", observability={}), None, "4", 100, check)
    assert emitted[0]["data"]["integrity"] == {"result": "verified", "problems": 0,
                                               "approved_at": "2026-11-05T10:00:00", "approved_by": "risk@acme"}


# -- BigQuery ML: the model in BigQuery, by etag (FR-94) -----------------------------------------------------------

def test_a_bigquery_ml_model_replaced_after_approval_is_caught():
    bodies = {"m": {"etag": "e1", "lastModifiedTime": "1"}}
    gcp = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok="models/" in url and bool(bodies),
                                                               body=dict(bodies.get("m") or {})))
    approved = integrity.bq_model(gcp, "p.ds.churn_model")
    assert approved == {"model": "p.ds.churn_model", "etag": "e1", "modified": "1"}
    assert integrity.compare_bq_model(gcp, approved) == []
    bodies["m"] = {"etag": "e2", "lastModifiedTime": "2"}               # CREATE OR REPLACE MODEL
    assert "changed after the approval" in integrity.compare_bq_model(gcp, approved)[0]
    bodies.clear()
    assert "is missing" in integrity.compare_bq_model(gcp, approved)[0]
    assert integrity.bq_model(gcp, None) is None and integrity.compare_bq_model(gcp, None) == []


# -- promotions check the source before copying (FR-93) ------------------------------------------------------------

def test_a_promotion_checks_the_source_with_the_destination_s_policy(monkeypatch):
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [])            # nobody approved it in the source
    chosen = {"artifactUri": FOLDER, "containerSpec": {"imageUri": None}}
    with pytest.raises(ConfigError, match="is not promoted"):
        integrity.check_source(store(), env(), env(), "churn", "4", chosen, None, log=lambda m: None)
    said = []
    check = integrity.check_source(store(), env(), env(integrity={"mode": "warn"}), "churn", "4", chosen, None,
                                   log=said.append)
    assert check.problems and said[0].startswith("integrity WARNING")
    # a retrain loop's candidate is approved by the loop's production approval instead
    assert integrity.check_source(store(), env(), env(), "churn", "4", chosen, "rc-1", log=said.append) is None
    assert "retrain loop rc-1" in said[-1]


# -- what changed and who (FR-95), who approved (FR-97) ------------------------------------------------------------

def test_the_diff_shows_metrics_and_the_decision_never_slice_values():
    approved = {"decision": "approve", "primary_metric": "auc", "value": 0.81, "metrics": {"auc": 0.81, "f1": 0.6},
                "slices": {"postcode=SW1A 1AA": {"auc": 0.7}}, "reasons": []}
    now = {**approved, "value": 0.92, "metrics": {"auc": 0.92, "f1": 0.6},
           "slices": {"postcode=SW1A 1AA": {"auc": 0.95}}}
    diff = integrity.evaluation_diff(json.dumps(approved), json.dumps(now))
    assert diff == ["value: 0.81 -> 0.92", "metrics.auc: 0.81 -> 0.92",
                    "also changed (not shown: they can hold data values): slices"]
    assert "SW1A" not in json.dumps(diff)
    assert integrity.evaluation_diff("{}", "not json") == ["the record is no longer valid JSON"]
    assert integrity.evaluation_diff('{"a": 1}', '{"a": 1}') == ["the bytes changed, the metrics and decision did not"]


def _logging(entries=None, status=200):
    seen = []

    def post(url, body):
        seen.append(body)
        return SimpleNamespace(ok=status == 200, status=status, body={"entries": entries or []})

    return SimpleNamespace(post=post), seen


def test_each_approval_is_checked_against_the_admin_activity_log():
    entry = {"timestamp": "2026-11-05T13:37:42.1Z", "protoPayload": {
        "methodName": "google.cloud.aiplatform.v1.ModelService.MergeVersionAliases",
        "authenticationInfo": {"principalEmail": "risk@acme"}}}
    gcp, seen = _logging([entry])
    found = integrity.approval_audit(gcp, env(), "projects/p/locations/r/models/123", "2026-11-05 13:37:42",
                                     "risk@acme")
    assert found["state"] == "matched" and found["principal"] == "risk@acme"
    assert 'resourceName:"models/123"' in seen[0]["filter"] and "2026-11-05T13:27:42Z" in seen[0]["filter"]
    assert integrity.approval_audit(gcp, env(), "m/123", "2026-11-05 13:37:42", "intruder@acme")["state"] == \
        "other principal"
    nobody, _ = _logging([])
    assert integrity.approval_audit(nobody, env(), "m/123", "2026-11-05 13:37:42", "a@x")["state"] == "no entry"
    denied, _ = _logging(status=403)
    assert integrity.approval_audit(denied, env(), "m/123", "2026-11-05 13:37:42", "a@x")["state"] == "not checked"


def test_who_wrote_the_record_comes_from_data_access_logs_when_they_are_on():
    entry = {"timestamp": "2026-11-05T14:00:00Z", "protoPayload": {
        "methodName": "storage.objects.create", "authenticationInfo": {"principalEmail": "someone@acme"}}}
    gcp, seen = _logging([entry])
    found = integrity.who_wrote(gcp, env(), "gs://art/m/evaluation.json", "2026-11-05 13:37:42")
    assert found["entries"][0]["principal"] == "someone@acme"
    assert 'objects/m/evaluation.json"' in seen[0]["filter"]
    off, _ = _logging([])
    assert "data access logs off" in integrity.who_wrote(off, env(), "gs://art/m/evaluation.json",
                                                          "2026-11-05 13:37:42")["note"]


def test_lineage_renders_the_integrity_check_and_the_audit():
    from pipewright.models import lineage

    info = {"model": "churn-classifier", "version": "4", "environment": "prod", "lineage": None, "note": "n",
            "decisions": [{"at": "2026-11-05 13:37:42", "event": "model.approved", "version": "4", "by": "a@x",
                           "evaluation": {"sha256": "ab" * 32, "generation": 7}, "evaluation_now": "changed",
                           "evaluation_diff": ["metrics.auc: 0.81 -> 0.92"],
                           "audit": {"state": "no entry", "note": "no Admin Activity entry for this approval"}}],
            "integrity": {"result": "refused", "problems": 1,
                          "problems_found": ["model.pt: changed after the approval"], "notes": [],
                          "lock": {"until": "2026-12-05T00:00:00Z", "mode": "unlocked", "locked": 8,
                                   "method": "retention"}}}
    text = lineage.render(info)
    assert "metrics.auc: 0.81 -> 0.92" in text and "audit log: NO ENTRY" in text
    assert "integrity refused: 1 problem(s)" in text and "8 approved files locked until 2026-12-05T00:00:00Z" in text


# -- scoring with an unapproved default (policies.integrity.inference) ---------------------------------------------

def _registry(labels=None, version="4"):
    def get(url, **params):
        if url.endswith("/models"):
            return SimpleNamespace(ok=True, body={"models": [{"name": "projects/p/locations/r/models/9"}]})
        return SimpleNamespace(ok=True, status=200, body={"versionId": version, "labels": labels or {}})

    return SimpleNamespace(get=get)


CONTRACT = SimpleNamespace(metadata={"model": "churn-classifier"}, region="r", project="p", environment="prod",
                           usecase="churn")


def test_scoring_is_unchanged_unless_the_policy_asks(monkeypatch):
    called = []
    monkeypatch.setattr(integrity, "approval_events", lambda *a: called.append(1) or [])
    assert integrity.inference_check(_registry(), {"policies": {}}, CONTRACT) == ("off", None)
    assert called == []


def test_scoring_with_an_unapproved_default_is_flagged(monkeypatch):
    manifest = {"policies": {"integrity": {"inference": "refuse"}}, "observability": {"evidence_dataset": "ev"}}
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [])
    mode, problem = integrity.inference_check(_registry(), manifest, CONTRACT)
    assert mode == "refuse" and "became the default outside Pipewright" in problem
    mode, problem = integrity.inference_check(_registry({"approval": "pending"}), manifest, CONTRACT)
    assert "nobody has approved it" in problem
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [{"at": "x", "by": "a@x"}])
    assert integrity.inference_check(_registry(), manifest, CONTRACT) == ("refuse", None)


# -- validate: a serving image by tag (FR-94) ----------------------------------------------------------------------

def test_validate_warns_about_a_serving_image_registered_by_tag(tmp_path):
    from pipewright.config import judgement

    tagged = 'SERVING_IMAGE = "europe-docker.pkg.dev/acme-ml/serving/churn-server:1.2"'
    (tmp_path / "register.py").write_text(tagged + "\nm.upload(serving_container_image_uri=SERVING_IMAGE)\n",
                                          encoding="utf-8")
    (tmp_path / "train.py").write_text('IMAGE = "europe-docker.pkg.dev/p/r/trainer:1.0"\n', encoding="utf-8")
    found = judgement.serving_image_by_tag(tmp_path)
    assert len(found) == 1 and "register.py registers the serving image by tag" in found[0]
    (tmp_path / "register.py").write_text('SERVING_IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/x@sha256:'
                                          + "a" * 64 + '"\nm.upload(serving_container_image_uri=SERVING_IMAGE)\n',
                                          encoding="utf-8")
    assert judgement.serving_image_by_tag(tmp_path) == [] and judgement.serving_image_by_tag(None) == []


def test_the_reference_example_registers_its_serving_image_by_digest():
    from pathlib import Path

    from pipewright.config import judgement

    components = Path(__file__).resolve().parents[1] / "examples" / "churn" / "components"
    assert judgement.serving_image_by_tag(components) == []


def test_lineage_says_a_hold_has_no_end_date():
    from pipewright.models import lineage

    lines = []
    lineage._render_integrity({"integrity": {"result": "verified", "lock": {"locked": 8, "method": "hold",
                                                                            "mode": "unlocked"}}}, lines)
    assert lines[-1].strip() == "8 approved files on a temporary hold (no end date)"


def test_agent_platform_prebuilt_images_are_used_by_tag_without_a_warning(tmp_path):
    from pipewright.config import judgement

    # Found live (phase 6): Agent Platform refuses to deploy a prebuilt prediction image given by digest.
    lines = ['IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"',
             "m.upload(serving_container_image_uri=IMAGE)"]
    (tmp_path / "register.py").write_text("\n".join(lines) + "\n", encoding="utf-8")
    assert judgement.serving_image_by_tag(tmp_path) == []


def test_the_remedy_is_said_but_never_counted_as_a_problem():
    """Found live on ent: a refusal with two problems said '3 problems', counting the remedy."""
    from pipewright.errors import ConfigError
    from pipewright.models import integrity

    check = integrity.Check(problems=["model.pt: approved, now missing", "model_card.md: changed after the approval"])
    env = SimpleNamespace(name="ent", policies={})
    with pytest.raises(ConfigError) as e:
        integrity.enforce(check, env, "v1 of m is not served", lambda *_: None)
    text = str(e.value)
    assert "2 problems" in text and "\n  fix: re-approve the version after review" in text
    assert len(e.value.problems) == 2


def test_who_made_an_unapproved_version_the_default():
    """Found live on ent: a version made the default with the API was refused, but nothing said by whom."""
    seen = []

    def post(url, body):
        seen.append(body["filter"])
        entries = [{"timestamp": "2026-11-06T01:54:56.1Z", "protoPayload": {
            "methodName": "google.cloud.aiplatform.v1.ModelService.MergeVersionAliases",
            "authenticationInfo": {"principalEmail": "someone@example.com"}}}] if len(seen) == 1 else []
        return SimpleNamespace(ok=True, body={"entries": entries})

    gcp = SimpleNamespace(post=post)
    env = SimpleNamespace(project="p")
    got = integrity.who_made_default(gcp, env, "projects/p/locations/r/models/115@2", "2", "2026-11-06 01:50:35")
    assert got["principal"] == "someone@example.com" and got["at"] == "2026-11-06T01:54:56"
    assert 'resourceName:"models/115@2"' in seen[0] and 'timestamp>="2026-11-06T01:50:35Z"' in seen[0]
    first = integrity.who_made_default(gcp, env, "projects/p/locations/r/models/115", "1", None)
    assert "made it the default when it was registered" in first["note"]


def test_a_promotion_approval_is_matched_by_its_upload():
    """Found live on ent-prod: a first version approved with its promotion became the default by its upload, which
    the audit check did not look for, so EV-06 said the approval had no entry."""
    seen = []

    def post(url, body):
        seen.append(body["filter"])
        return SimpleNamespace(ok=True, body={"entries": [{"timestamp": "2026-11-06T02:42:46Z", "protoPayload": {
            "methodName": "google.cloud.aiplatform.v1.ModelService.UploadModel",
            "authenticationInfo": {"principalEmail": "lead@example.com"}}}]})

    gcp = SimpleNamespace(post=post)
    got = integrity.approval_audit(gcp, SimpleNamespace(project="p"), "projects/p/locations/r/models/9",
                                   "2026-11-06 02:43:01", "lead@example.com", promoted=True)
    assert got["state"] == "matched" and 'operation.id:"models/9/"' in seen[0]
    integrity.approval_audit(gcp, SimpleNamespace(project="p"), "projects/p/locations/r/models/9",
                             "2026-11-06 02:43:01", "lead@example.com")
    assert "UploadModel" not in seen[1]                     # only a promotion's approval counts its upload


def test_a_release_contract_is_locked_and_checked_against_what_went_live(monkeypatch):
    """QA governance finding G10: a live contract could be changed by anyone who may write the artifacts bucket,
    unseen. With policies.integrity.lock it is locked at release; lineage compares it with release.live's SHA-256."""
    from pipewright.models import lineage

    patched = []

    class Session:
        def get(self, url, params=None, timeout=None):
            if params and params.get("alt") == "media":
                return SimpleNamespace(status_code=200, content=b'{"release_id": "r1"}')
            return SimpleNamespace(status_code=200, json=lambda: {"objectRetention": {"mode": "Enabled"}})

        def patch(self, url, params=None, json=None, timeout=None):
            patched.append((url, params, json))
            return SimpleNamespace(status_code=200, text="")

    gcp = SimpleNamespace(session=Session())
    env = SimpleNamespace(name="prod", policies={"integrity": {"lock": {"days": 30, "mode": "locked"}}})
    got = integrity.lock_contract(gcp, env, "b", "contracts/churn/r1.json", 7, lambda *_: None)
    assert got["locked"] == 1 and got["method"] == "retention" and got["mode"] == "locked"
    (url, params, body) = patched[0]
    assert url.endswith("/o/contracts%2Fchurn%2Fr1.json") and params == {"generation": "7"}
    assert body["retention"]["mode"] == "Locked"
    assert integrity.lock_contract(gcp, SimpleNamespace(policies={}), "b", "c/x.json", 1) is None   # off: as before

    live = integrity.contract_sha256('{"release_id": "r1"}')
    monkeypatch.setattr("pipewright.gcp.gcs_read", lambda gcp, b, n: ('{"release_id": "r1"}', 3))
    e = SimpleNamespace(artifacts_bucket="b")
    assert lineage.contract_now(None, e, "churn", "r1", live) == "unchanged"
    monkeypatch.setattr("pipewright.gcp.gcs_read", lambda gcp, b, n: ('{"release_id": "r1", "x": 1}', 4))
    assert lineage.contract_now(None, e, "churn", "r1", live) == "CHANGED"
    monkeypatch.setattr("pipewright.gcp.gcs_read", lambda gcp, b, n: (None, 0))
    assert lineage.contract_now(None, e, "churn", "r1", live) == "MISSING"


def test_deploy_and_promote_cross_check_the_approval_with_cloud_audit_logs(monkeypatch):
    """QA governance finding G3: a forged model.approved event (no fingerprints, so it looked like an approval from
    before 1.3.1) passed the checks that gate deploy and promote; only lineage flagged it."""
    import datetime

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    approval = {"at": now, "by": "lead@example.com", "version": "2"}
    env = SimpleNamespace(name="prod", project="p", policies={})
    monkeypatch.setattr(integrity, "approval_events", lambda *a: [approval])
    monkeypatch.setattr(integrity, "compare_image", lambda *a: [])
    monkeypatch.setattr(integrity, "compare_bq_model", lambda *a: [])

    def audited(state, **extra):
        monkeypatch.setattr(integrity, "approval_audit", lambda *a, **k: {"state": state, **extra})
        return integrity.verify(None, env, "churn", "2", None, None, model="projects/p/locations/r/models/9")

    check = audited("no entry", note="x")
    assert check.problems == [f"the approval of v2 ({now}, by lead@example.com) has no Admin Activity entry in "
                              "Cloud Audit Logs: the event may not be genuine"]
    assert audited("other principal", note="the audit log names a@x, the event says lead@example.com").problems
    assert not audited("matched", principal="lead@example.com").problems
    unreadable = audited("not checked", note="Cloud Audit Logs: HTTP 403")
    assert not unreadable.problems and "could not be checked" in unreadable.notes[0]          # never a broken deploy
    approval["at"] = "2024-01-01 00:00:00"
    old = audited("no entry", note="x")
    assert not old.problems and "older than Cloud Audit Logs keep" in old.notes[0]
    approval["at"] = now
    assert not integrity.verify(None, env, "churn", "2", None, None).problems     # no model given: as before


def test_lineage_calls_a_candidate_nobody_approved_not_approved_yet(monkeypatch):
    """QA finding: lineage of a candidate said 'it became the default outside Pipewright'."""
    from pipewright.models import lineage

    monkeypatch.setattr(integrity, "verify", lambda *a, **k: integrity.Check(
        problems=["v4 has no model.approved event: it became the default outside Pipewright (the console, gcloud or "
                  "the API), so nobody approved it here"]))
    out = {"version": "4", "default": False, "decisions": []}
    lineage.check_integrity(None, SimpleNamespace(), SimpleNamespace(name="churn"), out, "m", None)
    assert out["integrity"]["result"] == "not approved yet (a candidate)" and not out["integrity"]["problems_found"]
    monkeypatch.setattr(integrity, "who_made_default", lambda *a: {"note": "no alias change"})
    out = {"version": "4", "default": True, "decisions": []}
    lineage.check_integrity(None, SimpleNamespace(), SimpleNamespace(name="churn"), out, "m", None)
    assert out["integrity"]["result"] == "refused"                    # the default: it is a problem
