import pytest
import yaml

from pipewright.config import load_environment
from pipewright.errors import ConfigError
from pipewright.runs import launch
from pipewright.scaffold import ci


def test_render_orders_release_jobs_by_environment():
    files = ci.render(["dev", "staging", "prod"])
    release = yaml.safe_load(files[".github/workflows/pipewright-release.yml"])
    jobs = release["jobs"]
    assert list(jobs) == ["changes", "release-dev", "release-staging", "release-prod"]
    assert jobs["release-dev"]["needs"] == ["changes"]
    assert jobs["release-prod"]["needs"] == ["changes", "release-staging"]
    assert jobs["release-prod"]["environment"] == "prod"
    steps = [s.get("name") for s in jobs["release-dev"]["steps"]]
    assert steps[-4:] == ["Tables", "Release", "Schedules", "Alerts"]
    pr = yaml.safe_load(files[".github/workflows/pipewright-pr.yml"])
    assert pr["jobs"]["check"]["environment"] == "dev"
    operate = yaml.safe_load(files[".github/workflows/pipewright-operate.yml"])
    trigger = operate[True]["workflow_dispatch"]["inputs"]          # YAML reads `on` as true
    assert trigger["environment"]["options"] == ["dev", "staging", "prod"]
    assert "data-erase" not in trigger["action"]["options"]
    assert "predict" in trigger["action"]["options"] and "instances" in trigger
    step = operate["jobs"]["operate"]["steps"][-1]["run"]
    assert 'p model predict --env "$ENV" --usecase "$UC" --instances "$INSTANCES"' in step
    for text in files.values():
        assert "@@" not in text


def test_a_failing_command_fails_its_step_even_when_piped():
    # GitHub runs `run:` steps as `bash -e` unless the shell is named; only `shell: bash` adds -o pipefail, without
    # which `pipewright release ... | tee` passes when the release fails (found by the first CI run, R-01).
    files = ci.render(["dev"])
    for name in ("pipewright-pr.yml", "pipewright-release.yml", "pipewright-operate.yml",
                 "pipewright-decisions.yml", "pipewright-gate.yml"):
        doc = yaml.safe_load(files[f".github/workflows/{name}"])
        assert doc["defaults"]["run"]["shell"] == "bash", name


def test_decision_workflows_gate_by_github_environment():
    # FR-56: the listing job needs no reviewer; a gate job waits for its environment's reviewers and runs the
    # command of the decision it re-reads, never one from its inputs.
    files = ci.render(["test", "prod"])
    decisions = yaml.safe_load(files[".github/workflows/pipewright-decisions.yml"])
    job = decisions["jobs"]["decisions"]
    assert job["strategy"]["matrix"]["environment"] == ["test", "prod"]
    assert job["environment"] == "${{ matrix.environment }}-decisions"
    assert decisions["permissions"]["actions"] == "write"                 # dispatches the gate runs
    assert "gh workflow run pipewright-gate.yml" in job["steps"][-1]["run"]
    gate = yaml.safe_load(files[".github/workflows/pipewright-gate.yml"])
    assert gate["run-name"] == "gate ${{ inputs.id }}"                   # how pipewright-decisions finds open gates
    assert set(gate[True]["workflow_dispatch"]["inputs"]) == {"environment", "gate", "id"}
    decide = gate["jobs"]["decide"]
    assert decide["environment"] == "${{ inputs.gate }}"
    steps = {s.get("name"): s for s in decide["steps"]}
    assert "actions/runs/$GITHUB_RUN_ID/approvals" in steps["Approval record"]["run"]
    assert "PIPEWRIGHT_APPROVAL=" in steps["Approval record"]["run"]
    assert "item.json" in steps["Decide"]["run"] and "inputs" not in steps["Decide"]["run"]


def test_id_tokens_for_several_launchers(monkeypatch):
    monkeypatch.setenv("PIPEWRIGHT_ID_TOKENS", '{"https://a.example/": "ta", "https://b.example": "tb"}')
    monkeypatch.setenv("PIPEWRIGHT_ID_TOKEN", "single")
    assert launch.id_token("https://a.example") == "ta"
    assert launch.id_token("https://b.example/") == "tb"
    assert launch.id_token("https://c.example") == "single"              # not in the map: the single token
    monkeypatch.setenv("PIPEWRIGHT_ID_TOKENS", "nope")
    with pytest.raises(ConfigError, match="JSON object"):
        launch.id_token("https://a.example")


def test_render_rejects_bad_environments():
    with pytest.raises(ConfigError, match="at least one"):
        ci.render([])
    with pytest.raises(ConfigError, match="unique"):
        ci.render(["dev", "dev"])


def test_init_writes_files_once(repo):
    repo.write()
    written = ci.init(repo.root, ["dev"])
    assert {p.relative_to(repo.root).as_posix() for p in written} == set(ci.FILES.values())
    with pytest.raises(ConfigError, match="already present"):
        ci.init(repo.root, ["dev"])
    ci.init(repo.root, ["dev"], force=True)
    with pytest.raises(ConfigError, match="no environments/"):
        ci.init(repo.root, ["prod"], force=True)


class FakeSession:
    def __init__(self, status=200, body=None):
        self.status, self.body, self.calls = status, body or {}, []

    def post(self, url, json, headers, timeout):
        self.calls.append((url, json, headers))
        status, body = self.status, self.body
        return type("R", (), {"status_code": status, "json": lambda self: body, "text": str(body)})()


def test_run_posts_to_the_launcher_with_an_id_token(repo, monkeypatch):
    repo.write()
    env = load_environment(repo.root, "dev")
    monkeypatch.setenv("PIPEWRIGHT_ID_TOKEN", "tok")
    fake = FakeSession(body={"status": "submitted", "usecase": "churn", "job_id": "churn-r1"})
    payload = launch.body("churn", "inference", "gs://b/x.csv", None, "k1")
    assert launch.run(env, payload, fake)["job_id"] == "churn-r1"
    url, sent, headers = fake.calls[0]
    assert url == "https://launcher.example/run" and headers["Authorization"] == "Bearer tok"
    assert sent == {"usecase": "churn", "pipeline": "inference", "source_uri": "gs://b/x.csv", "run_key": "k1"}
    with pytest.raises(ConfigError, match="launcher returned 400: churn does not accept API runs"):
        launch.run(env, payload, FakeSession(400, {"status": "error", "reason": "churn does not accept API runs"}))
    with pytest.raises(ConfigError, match="give --source-uri"):
        launch.body("churn", None, None, None, None)


def test_self_hosted_runners_and_optional_federation():
    files = ci.render(["dev", "prod"], ["self-hosted", "linux", "mlops"])
    for path in (".github/workflows/pipewright-pr.yml", ".github/workflows/pipewright-release.yml",
                 ".github/workflows/pipewright-operate.yml"):
        doc = yaml.safe_load(files[path])
        assert all(job["runs-on"] == ["self-hosted", "linux", "mlops"] for job in doc["jobs"].values())
    assert files[".github/actionlint.yaml"].endswith("    - linux\n    - mlops\n")
    action = yaml.safe_load(files[".github/actions/pipewright-setup/action.yml"])
    steps = action["runs"]["steps"]
    assert [s.get("if") for s in steps[:3]] == ["inputs.setup_tools != 'off'", "inputs.setup_tools != 'off'",
                                                "inputs.wif_provider != ''"]
    assert "python3 -m venv" in steps[3]["run"]
    assert ".github/actionlint.yaml" not in ci.render(["dev"])            # GitHub-hosted: no label config
    with pytest.raises(ConfigError, match="runner labels"):
        ci.render(["dev"], ["self hosted"])
