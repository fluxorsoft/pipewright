"""pipewright promote (FR-29): the acceptance tests of CONTEXT.md, section 8.12, offline."""
import io
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from pipewright.config import load_environment
from pipewright.errors import ConfigError
from pipewright.release import promote
from pipewright.release.contract import ReleaseContract, new_pointer
from tests.test_release_parts import CONTRACT, DIGEST, TRAINING

COMMIT = "c0ffee" + "0" * 34
SRC_IMAGE = f"europe-west2-docker.pkg.dev/acme-ml-dev/ml-images/churn-components@{DIGEST}"
DST_IMAGE = f"europe-west2-docker.pkg.dev/acme-ml-prod/ml-images/churn-components@{DIGEST}"
TEMPLATE = "pipelineInfo: {name: churn-training}\ndeploymentSpec:\n  executors:\n    exec-train:\n      container:\n" \
           "        image: {image}\n        resources: {cpuLimit: 4}\nroot: {inputDefinitions: {parameters: {}}}\n"


def source_contract() -> ReleaseContract:
    return replace(CONTRACT, contract_version="3", release_id="abc123", environment="dev", project="acme-ml-dev",
                   component_image=SRC_IMAGE, source_commit=COMMIT,
                   pipelines={"training": {**TRAINING, "template_uri":
                                           f"https://europe-west2-kfp.pkg.dev/acme-ml-dev/ml-pipelines/churn-training/{DIGEST}"}})


@pytest.fixture
def setup(repo, monkeypatch, tmp_path):
    """dev (the source) and prod (promotion.from: dev), with fakes for every Google Cloud call. `calls` records
    what happened, in order; `store` holds the buckets."""
    repo.add_env("prod", project="acme-ml-prod", artifacts_bucket="acme-ml-prod-artifacts",
                 artifact_registry={"docker": "europe-west2-docker.pkg.dev/acme-ml-prod/ml-images",
                                    "kfp": "https://europe-west2-kfp.pkg.dev/acme-ml-prod/ml-pipelines"})
    repo.environments["prod"]["promotion"] = {"from": "dev"}
    state = SimpleNamespace(calls=[], store={}, dst_template=TEMPLATE.replace("{image}", DST_IMAGE),
                            model_error=None, repo=repo, dst_model={"name": "m"})
    src = source_contract()
    state.store[("acme-ml-dev-artifacts", "contracts/churn/abc123.json")] = (src.to_json(), 1)
    state.store[("acme-ml-dev-artifacts", "contracts/churn/current.json")] = (
        new_pointer("churn", "abc123", "contracts/churn/abc123.json", "ci").to_json(), 1)

    def gcs_read(gcp, bucket, name):
        return state.store.get((bucket, name), (None, 0))

    def gcs_write(gcp, bucket, name, text, if_generation_match=None, **_):
        current = state.store.get((bucket, name))
        if if_generation_match is not None and (current[1] if current else 0) != if_generation_match:
            raise promote.PreconditionFailed(name)
        state.calls.append(f"write {name}")
        state.store[(bucket, name)] = (text, (current[1] if current else 0) + 1)

    def compile_pinned(uc, image, out_dir, synthetic_images=None):
        path = tmp_path / "compiled.yaml"
        path.write_text(state.dst_template, encoding="utf-8")
        return {"training": SimpleNamespace(name="churn-training", spec_path=path, pipeline="training",
                                            parameters={"project", "region", "source_uri", "run_id"})}

    def promote_model(gcp, src_env, dst_env, uc, rid, version, log=print, loop_case=None):
        state.calls.append("copy model" + (f" for {loop_case}" if loop_case else ""))
        if state.model_error:
            raise ConfigError(state.model_error)
        return {"name": "churn-classifier", "source_version": "3", "version": "1", "files": []}

    monkeypatch.setattr(promote, "gcs_read", gcs_read)
    monkeypatch.setattr(promote, "gcs_write", gcs_write)
    monkeypatch.setattr("pipewright.release.service.gcs_read", gcs_read)
    monkeypatch.setattr(promote, "_git", lambda d, *a: COMMIT if a[0] == "rev-parse" else "")
    monkeypatch.setattr(promote, "_download_template", lambda target, creds: TEMPLATE.replace("{image}", SRC_IMAGE))
    monkeypatch.setattr(promote, "copy_image", lambda gcp, s, d, log=print: state.calls.append(f"copy image {d}"))
    monkeypatch.setattr(promote, "promote_model", promote_model)
    monkeypatch.setattr(promote, "_find_model", lambda gcp, env, name: state.dst_model)
    monkeypatch.setattr(promote.publish, "compile_pinned", compile_pinned)
    monkeypatch.setattr(promote.publish, "upload_template", lambda info, repo_, uc, rid, creds: (
        f"{repo_}/churn-training/{DIGEST}", DIGEST))
    monkeypatch.setattr(promote, "verify", lambda contract, gcp, creds: ["contract valid"])
    monkeypatch.setattr(promote.events, "emit",
                        lambda event, *a, **k: state.calls.append(f"event {event['event_type']}"))
    state.gcp = SimpleNamespace(identity=lambda: "promoter@acme.example", session=SimpleNamespace(credentials=None))

    def run(src="dev", dst="prod", **kw):
        path = repo.write()
        return promote.promote(path, load_environment(repo.root, src), load_environment(repo.root, dst),
                               gcp=state.gcp, build_dir=tmp_path / "build", log=lambda m: None, skip_tables=True,
                               **kw)

    state.run = run
    return state


def prod_pointer(state):
    text, _ = state.store.get(("acme-ml-prod-artifacts", "contracts/churn/current.json"), (None, 0))
    return json.loads(text) if text else None


# 1, 2, 3 and 8: nothing rebuilt, digests kept, the model copied, everything recorded; the pointer last.
def test_a_promotion_copies_verifies_records_and_moves_the_pointer_last(setup):
    result = setup.run()
    assert setup.calls == [f"copy image {DST_IMAGE}", "copy model", "write contracts/churn/abc123.json",
                           "write contracts/churn/current.json", "event release.promoted"]
    c = result.contract
    assert c.release_id == "abc123" and c.environment == "prod" and c.component_image == DST_IMAGE
    assert c.source_commit == COMMIT and c.pipeline_root.startswith("gs://acme-ml-prod-artifacts/")
    rec = c.promoted_from
    assert rec["environment"] == "dev" and rec["release_id"] == "abc123" and rec["by"] == "promoter@acme.example"
    assert rec["contract_sha256"].startswith("sha256:") and rec["images"] == {SRC_IMAGE: DST_IMAGE}
    assert rec["templates"]["training"]["differences"] == [] and rec["model"]["source_version"] == "3"
    assert any("templates equal to the source's" in p for p in rec["policy"])
    assert prod_pointer(setup)["release_id"] == "abc123" and result.model_version == "1"


# The closed retrain loop (FR-48): its candidate was trained by the release live in production, so only the model is
# promoted; repeating the step finds the version promoted before.
def test_the_loop_promotes_its_candidate_when_the_release_is_live(setup, monkeypatch):
    setup.run()
    setup.calls.clear()
    assert setup.run(model_version="4").unchanged and setup.calls == []          # by hand: nothing to do, as in 1.0
    monkeypatch.setattr(promote, "_versions", lambda gcp, env, model: [])
    result = setup.run(model_version="4", loop_case="rc-1")
    assert setup.calls == ["copy model for rc-1", "event model.promoted"]       # no contract, no pointer moved
    assert result.model_version == "1" and not result.unchanged
    setup.calls.clear()
    monkeypatch.setattr(promote, "_versions", lambda gcp, env, model: [
        {"versionId": "7", "labels": {"promoted-from": "dev", "source-version": "4"}}])
    again = setup.run(model_version="4", loop_case="rc-1")
    assert again.unchanged and again.model_version == "7" and setup.calls == []


def test_a_loop_candidate_need_not_be_the_source_default(repo, monkeypatch):
    repo.add_env("prod", project="acme-ml-prod")
    repo.environments["prod"]["promotion"] = {"from": "dev"}
    path = repo.write()
    src, dst = load_environment(repo.root, "dev"), load_environment(repo.root, "prod")
    from pipewright.config import resolve_usecase

    uc = resolve_usecase(path, dst)
    versions = [{"versionId": "2", "versionAliases": ["run-x"], "labels": {"release": "other"}}]
    monkeypatch.setattr(promote, "_find_model", lambda gcp, env, name: {"name": "m", "displayName": name})
    monkeypatch.setattr(promote, "_versions", lambda gcp, env, model: versions)
    with pytest.raises(ConfigError, match="was trained by release other"):      # past the default check
        promote.promote_model(None, src, dst, uc, "abc123", "2", log=lambda m: None, loop_case="rc-1")


# 4: retraining promotes the release only; production trains its own candidate.
def test_retrain_promotes_only_the_release(setup):
    setup.repo.usecase["spec"]["model"]["promotion"] = "retrain"
    result = setup.run()
    assert "copy model" not in setup.calls and result.model_version is None


# 5: production's policies refuse before anything is copied.
def test_destination_policies_refuse_first(setup):
    setup.repo.manifests["prod"]["features"] = {"required": True}         # the use case reads no feature tables
    with pytest.raises(ConfigError, match="requires features from feature tables"):
        setup.run()
    assert setup.calls == [] and prod_pointer(setup) is None


def test_only_the_named_source_environment_promotes(setup):
    setup.repo.add_env("qa")
    with pytest.raises(ConfigError, match="prod takes releases from dev"):
        setup.run(src="qa")
    assert setup.calls == []


def test_templates_compiled_differently_are_refused_before_copying(setup):
    setup.dst_template = TEMPLATE.replace("{image}", DST_IMAGE).replace("cpuLimit: 4", "cpuLimit: 8")
    with pytest.raises(ConfigError, match="compile differently from the tested ones") as e:
        setup.run()
    assert any("cpuLimit: 4 != 8" in p for p in e.value.problems)
    assert setup.calls == [] and prod_pointer(setup) is None
    result = setup.run(accept_template_change=True)                       # an explicit, recorded exception
    assert result.contract.promoted_from["templates"]["training"]["differences"]


def test_the_checkout_must_be_the_source_commit(setup, monkeypatch):
    monkeypatch.setattr(promote, "_git", lambda d, *a: "beef" * 10 if a[0] == "rev-parse" else "")
    with pytest.raises(ConfigError, match="check out the source release's commit c0ffee"):
        setup.run()
    assert setup.calls == []


# 6: a failure leaves the live pointer where it was.
def test_a_failed_model_copy_leaves_the_pointer(setup):
    setup.model_error = "checksum mismatch"
    with pytest.raises(ConfigError, match="checksum mismatch"):
        setup.run()
    assert prod_pointer(setup) is None and "write contracts/churn/abc123.json" not in setup.calls


# 7: idempotent; a conflicting request is refused.
def test_a_repeated_promotion_is_a_no_op(setup):
    setup.run()
    setup.calls.clear()
    again = setup.run()
    assert again.unchanged and setup.calls == []


def test_a_conflicting_release_is_refused(setup):
    other = replace(source_contract(), environment="prod", promoted_from={"contract_sha256": "sha256:other"})
    setup.store[("acme-ml-prod-artifacts", "contracts/churn/abc123.json")] = (other.to_json(), 1)
    with pytest.raises(ConfigError, match="already has a different release abc123"):
        setup.run()
    assert setup.calls == [] and prod_pointer(setup) is None


# ------------------------------------------------------------------ parts --

def test_template_differences_ignore_only_the_registry_address():
    src, dst = TEMPLATE.replace("{image}", SRC_IMAGE), TEMPLATE.replace("{image}", DST_IMAGE)
    assert promote.template_differences(src, dst, {SRC_IMAGE: DST_IMAGE}) == []
    assert promote.template_differences(src, dst, {}) != []
    assert promote.destination_image(SRC_IMAGE, "europe-west2-docker.pkg.dev/acme-ml-prod/ml-images") == DST_IMAGE


class Registry:
    """Two registries' manifests and blobs, over the distribution API."""

    def __init__(self):
        manifest = json.dumps({"config": {"digest": "sha256:c"}, "layers": [{"digest": "sha256:l1"}]}).encode()
        self.objects = {("src", "manifests", DIGEST): manifest, ("src", "blobs", "sha256:c"): b"cfg",
                        ("src", "blobs", "sha256:l1"): b"layer"}
        self.digest_after = DIGEST

    @staticmethod
    def _side(url):
        return "src" if "acme-ml-dev" in url else "dst"

    def _resp(self, code, body=b"", headers=None):
        return SimpleNamespace(status_code=code, content=body, text=body.decode(errors="replace"),
                               headers=headers or {}, raw=io.BytesIO(body if isinstance(body, bytes) else b""))

    def get(self, url, **kw):
        kind, ref = url.rsplit("/", 2)[-2:]
        body = self.objects.get((self._side(url), kind, ref))
        return self._resp(200, body, {"Content-Type": "application/vnd.oci.image.manifest.v1+json",
                                      "Content-Length": str(len(body))}) if body else self._resp(404)

    def head(self, url, **kw):
        kind, ref = url.rsplit("/", 2)[-2:]
        found = (self._side(url), kind, ref) in self.objects
        return self._resp(200 if found else 404, headers={"Docker-Content-Digest": self.digest_after})

    def post(self, url, params=None, **kw):
        return self._resp(202, headers={"Location": "/v2/acme-ml-prod/ml-images/churn-components/blobs/uploads/u1"})

    def put(self, url, data=None, **kw):
        if "/blobs/uploads/" in url:
            body = data.read() if hasattr(data, "read") else data
            self.objects[("dst", "blobs", url.split("digest=")[1].replace("%3A", ":"))] = body
        else:
            self.objects[("dst", "manifests", url.rsplit("/", 1)[1])] = data
        return self._resp(201)


def test_images_are_copied_through_the_registry_api():
    reg = Registry()
    gcp = SimpleNamespace(session=reg)
    assert promote.copy_image(gcp, SRC_IMAGE, DST_IMAGE, log=lambda m: None) == DIGEST
    assert reg.objects[("dst", "blobs", "sha256:l1")] == b"layer"
    assert reg.objects[("dst", "manifests", DIGEST)] == reg.objects[("src", "manifests", DIGEST)]
    reg2 = Registry()
    reg2.digest_after = "sha256:" + "b" * 64
    with pytest.raises(ConfigError, match="after copying"):
        promote.copy_image(SimpleNamespace(session=reg2), SRC_IMAGE, DST_IMAGE, log=lambda m: None)


def test_model_files_must_keep_their_checksums():
    listed = {"items": [{"name": "pipeline-root/churn/run/model/model.pt", "crc32c": "AAA=", "size": "3"}]}

    class G:
        def get(self, url, **kw):
            return SimpleNamespace(ok=True, body=listed)

    class S:
        def __init__(self, crc):
            self.crc = crc

        def post(self, url, **kw):
            return SimpleNamespace(status_code=200, json=lambda: {"done": True, "resource": {"crc32c": self.crc}})

    g = G()
    g.session = S("AAA=")
    out = promote.copy_model_files(g, "gs://a/pipeline-root/churn/run/model", "gs://b/x/model", log=lambda m: None)
    assert out == [{"file": "model.pt", "crc32c": "AAA=", "size": "3"}]
    g.session = S("BBB=")
    with pytest.raises(ConfigError, match="checksum BBB= after copying, AAA= before"):
        promote.copy_model_files(g, "gs://a/pipeline-root/churn/run/model", "gs://b/x/model", log=lambda m: None)


def test_only_the_approved_model_of_that_release_is_promoted(repo, monkeypatch):
    repo.add_env("prod", project="acme-ml-prod")
    repo.environments["prod"]["promotion"] = {"from": "dev"}
    path = repo.write()
    src, dst = load_environment(repo.root, "dev"), load_environment(repo.root, "prod")
    from pipewright.config import resolve_usecase

    uc = resolve_usecase(path, dst)
    versions = [{"versionId": "2", "versionAliases": ["run-x"], "labels": {"release": "abc123"}},
                {"versionId": "3", "versionAliases": ["default"], "labels": {"release": "other"}}]
    monkeypatch.setattr(promote, "_find_model", lambda gcp, env, name: {"name": "m", "displayName": name})
    monkeypatch.setattr(promote, "_versions", lambda gcp, env, model: versions)
    with pytest.raises(ConfigError, match="v2 of churn-classifier is not the default"):
        promote.promote_model(None, src, dst, uc, "abc123", "2", log=lambda m: None)
    with pytest.raises(ConfigError, match="was trained by release other, not abc123"):
        promote.promote_model(None, src, dst, uc, "abc123", None, log=lambda m: None)


def test_files_without_a_stored_checksum_are_compared_byte_for_byte():
    listed = {"items": [{"name": "r/model/model.pt", "size": "3"}]}               # no crc32c on the source
    contents = {"a": b"same", "b": b"same"}

    class G:
        def get(self, url, **kw):
            return SimpleNamespace(ok=True, body=listed)

    class S:
        def post(self, url, **kw):
            return SimpleNamespace(status_code=200, json=lambda: {"done": True, "resource": {"crc32c": "DST="}})

        def get(self, url, **kw):
            body = contents["a" if "/b/a/" in url else "b"]
            return SimpleNamespace(status_code=200, iter_content=lambda n: [body])

    g = G()
    g.session = S()
    out = promote.copy_model_files(g, "gs://a/r/model", "gs://b/x/model", log=lambda m: None)
    assert out[0]["crc32c"] == "DST=" and out[0]["sha256"].startswith("sha256:")
    contents["b"] = b"different"
    with pytest.raises(ConfigError, match="SHA-256 .* after copying"):
        promote.copy_model_files(g, "gs://a/r/model", "gs://b/x/model", log=lambda m: None)


# A model's first version becomes the default in Model Registry: under a manual policy it needs --approve.
def test_a_first_model_version_needs_an_explicit_approval(setup):
    setup.dst_model = None
    with pytest.raises(ConfigError, match="has no version in prod yet.*--approve"):
        setup.run()
    assert setup.calls == []
    result = setup.run(approve=True)
    assert "event model.approved" in setup.calls
    assert any("approved by promoter@acme.example with the promotion" in p for p in result.record["policy"])
