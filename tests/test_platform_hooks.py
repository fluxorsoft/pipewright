"""Hooks for private platform profiles and extensions: the manifest's security block, run requests over Pub/Sub,
CMEK and VPC on pipeline jobs, and CLI plugins."""

import sys
import types
from dataclasses import replace

import pytest

from pipewright import plugins
from pipewright.cli import main
from pipewright.cli.main import build_parser
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from tests.test_release_parts import V3

KEY = "projects/acme-ml-dev/locations/europe-west2/keyRings/ml/cryptoKeys/aiplatform"
NETWORK = "projects/123456/global/networks/ml-vpc"
TOPIC = "projects/acme-ml-dev/topics/ml-run-requests"


def private(repo):
    m = repo.manifests["dev"]
    m["launcher"]["run_requests_topic"] = TOPIC
    m["security"] = {"kms_key": KEY, "network": NETWORK, "reserved_ip_ranges": ["ml-psa"], "perimeter": "p"}
    return repo


def test_security_reaches_environment_and_ml_config(repo):
    path = private(repo).write()
    env = load_environment(repo.root, "dev")
    assert env.security["network"] == NETWORK
    uc = resolve_usecase(path, env)
    assert uc.platform["kms_key"] == KEY and uc.platform["run_requests_topic"] == TOPIC
    assert uc.ml_config()["platform"]["kms_key"] == KEY


def test_no_security_block_means_defaults(repo):
    path = repo.write()
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    assert uc.platform["kms_key"] is None and uc.platform["run_requests_topic"] is None


@pytest.mark.parametrize("field,value", [
    ("kms_key", "aiplatform"),                                       # not a key resource name
    ("network", "projects/acme-ml-dev/global/networks/ml-vpc"),      # needs the project number
    ("color", "blue"),                                               # unknown field
])
def test_security_block_is_validated(repo, field, value):
    private(repo).manifests["dev"]["security"][field] = value
    repo.write()
    with pytest.raises(ConfigError):
        load_environment(repo.root, "dev")


def test_vertex_adapter_applies_cmek_and_network(monkeypatch):
    seen = {}

    class FakeJob:
        def __init__(self, **kw):
            seen["init"] = kw
            self.resource_name = "projects/p/locations/r/pipelineJobs/j"

        def submit(self, **kw):
            seen["submit"] = kw

    aiplatform = types.SimpleNamespace(PipelineJob=FakeJob)
    monkeypatch.setitem(sys.modules, "google.cloud.aiplatform", aiplatform)
    from pipewright.adapters import vertex

    target = replace(V3.pipeline("training"), experiment=None)
    vertex.submit(V3, target, {}, "j", {}, {"kms_key": KEY, "network": NETWORK, "reserved_ip_ranges": ["r"]})
    assert seen["init"]["encryption_spec_key_name"] == KEY
    assert seen["submit"]["network"] == NETWORK and seen["submit"]["reserved_ip_ranges"] == ["r"]

    vertex.submit(V3, target, {}, "j2", {})
    assert seen["init"]["encryption_spec_key_name"] is None and seen["submit"]["network"] is None


class FakeEntryPoint:
    def __init__(self, name, fn):
        self.name, self.value, self._fn = name, f"fake:{name}", fn

    def load(self):
        return self._fn


def test_plugins_add_commands_and_a_broken_one_is_skipped(monkeypatch, capsys):
    def register(subparsers, common):
        sp = subparsers.add_parser("evidence", help="from a plugin")
        common(sp)
        sp.set_defaults(func=lambda args: print(f"evidence for {args.env}") or 0)

    def broken(subparsers, common):
        raise RuntimeError("boom")

    monkeypatch.setattr(plugins, "entry_points", lambda group: [FakeEntryPoint("evidence", register),
                                                                FakeEntryPoint("broken", broken)])
    assert build_parser().plugins == {"broken": "RuntimeError: boom", "evidence": "ok"}
    assert main(["evidence", "--env", "prod"]) == 0
    out = capsys.readouterr()
    assert "evidence for prod" in out.out and "plugin 'broken'" in out.err
    assert main(["plugins"]) == 0
    assert "RuntimeError: boom" in capsys.readouterr().out


def test_component_image_tag_follows_the_build_context(tmp_path):
    from pipewright.build import images

    (tmp_path / "a.py").write_text("x = 1\n", encoding="utf-8")
    first = images.context_hash(tmp_path, "r1")
    assert first == images.context_hash(tmp_path, "r1")             # same content: same tag, image reused
    (tmp_path / "a.py").write_text("x = 2\n", encoding="utf-8")
    assert images.context_hash(tmp_path, "r1") != first             # e.g. a Pipewright upgrade: new tag


def test_existing_digest_reads_the_registry_tag():
    from pipewright.build import images

    class FakeGcp:
        def __init__(self, ok, body=None):
            self.ok, self.body, self.urls = ok, body, []

        def get(self, url):
            self.urls.append(url)
            return type("R", (), {"ok": self.ok, "body": self.body})()

    found = FakeGcp(True, {"version": "projects/p/locations/us-central1/repositories/r/packages/churn-components/"
                                      "versions/sha256:" + "a" * 64})
    assert images.existing_digest(found, "us-central1-docker.pkg.dev/p/r", "churn-components", "abc-123") == \
        "sha256:" + "a" * 64
    assert found.urls[0].endswith("/projects/p/locations/us-central1/repositories/r/packages/churn-components/"
                                  "tags/abc-123")
    assert images.existing_digest(FakeGcp(False), "us-central1-docker.pkg.dev/p/r", "x", "t") is None


def test_run_goes_over_pubsub_in_private_environments(repo, monkeypatch):
    import base64
    import json

    from pipewright.runs import launch

    private(repo).write()
    env = load_environment(repo.root, "dev")

    class FakeGcp:
        def __init__(self, status=200):
            self.status, self.calls = status, []

        def post(self, url, body):
            self.calls.append((url, body))
            ok = self.status == 200
            return type("R", (), {"ok": ok, "status": self.status, "body": {"messageIds": ["m1"]} if ok else
                                  {"error": {"message": "denied"}}, "text": "denied"})()

    fake = FakeGcp()
    result = launch.run(env, {"usecase": "churn", "pipeline": "inference", "source_uri": "gs://b/x.csv"}, gcp=fake)
    url, body = fake.calls[0]
    assert url == f"https://europe-west2-pubsub.googleapis.com/v1/{TOPIC}:publish"
    sent = json.loads(base64.b64decode(body["messages"][0]["data"]))
    assert sent["pipeline"] == "inference" and sent["run_key"].startswith("cli-")   # redeliveries are no-ops
    assert result["status"] == "requested" and result["message_id"] == "m1"

    with pytest.raises(ConfigError, match="roles/pubsub.publisher"):
        launch.run(env, {"usecase": "churn", "source_uri": "gs://b/x.csv"}, gcp=FakeGcp(403))

    monkeypatch.setenv("PIPEWRIGHT_RUN_VIA", "http")
    monkeypatch.setattr(launch, "call", lambda env, payload, session=None: {"status": "submitted"})
    assert launch.run(env, {"usecase": "churn", "source_uri": "gs://b/x.csv"}, gcp=fake)["status"] == "submitted"


def test_contract_metadata_is_optional_and_round_trips():
    import json
    from dataclasses import asdict, replace

    from pipewright.release.contract import ReleaseContract

    old = asdict(V3)
    old.pop("metadata")
    assert ReleaseContract.from_dict(old).metadata == {}                 # contracts written before it still load
    meta = {"owner": "team-retention", "model": "churn-classifier", "contacts": ["ml@acme.example"]}
    again = ReleaseContract.from_json(replace(V3, metadata=meta).to_json())
    assert again.metadata == meta
    assert "metadata" in json.loads(again.to_json())


def test_environment_from_the_manifest_alone(repo):
    from pipewright.config import environment_from_manifest

    private(repo).write()
    env = environment_from_manifest("dev.manifest.json", repo.root / "environments")
    assert env.name == "dev" and env.security["kms_key"] == KEY and env.launcher["run_requests_topic"] == TOPIC


def test_bigquery_queries_carry_the_environment_key():
    from pipewright.gcp import bigquery

    class FakeGcp:
        def __init__(self):
            self.bodies = []

        def post(self, url, body):
            self.bodies.append(body)
            return type("R", (), {"ok": True, "body": {"jobComplete": True, "rows": []}})()

    fake = FakeGcp()
    bigquery.query(fake, "p", "SELECT 1", kms_key=KEY)
    bigquery.query(fake, "p", "SELECT 1")
    assert fake.bodies[0]["destinationEncryptionConfiguration"] == {"kmsKeyName": KEY}
    assert "destinationEncryptionConfiguration" not in fake.bodies[1]


def test_runtime_bigquery_client_sets_the_key_for_every_query(monkeypatch):
    pytest.importorskip("google.cloud.bigquery")
    import importlib.util
    from importlib import resources

    spec = importlib.util.spec_from_file_location(
        "pipewright_runtime", resources.files("pipewright.build.templates").joinpath("pipewright_runtime.py"))
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    from google.cloud import bigquery

    seen = {}
    monkeypatch.setattr(bigquery, "Client", lambda project, default_query_job_config=None: seen.update(
        config=default_query_job_config) or "client")
    runtime.bigquery_client("p", {"platform": {"bigquery_kms_key": KEY}})
    assert seen["config"].destination_encryption_configuration.kms_key_name == KEY
    runtime.bigquery_client("p", {"platform": {}})
    assert seen["config"] is None
