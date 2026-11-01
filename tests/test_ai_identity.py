"""AI build assistance: the platform's switch, the agent's identity and labelled calls (Pipewright 1.3.0)."""

from types import SimpleNamespace

import pytest

from pipewright.ai import identity
from pipewright.config import load_environment
from pipewright.errors import ConfigError
from pipewright.gcp import bigquery
from pipewright.gcp.session import Gcp, Response


def env(**ai):
    return SimpleNamespace(name="prod", ai=ai, tenants={"retail": {"agent_service_account": "pw-agent@p.iam"}})


def test_the_platform_can_forbid_the_tools():
    identity.allowed(env())
    identity.allowed(env(local_tools="allowed"))
    with pytest.raises(ConfigError, match="ai.local_tools: forbidden"):
        identity.allowed(env(local_tools="forbidden"))


def test_the_agent_account_comes_from_the_tenant_or_an_override(monkeypatch):
    monkeypatch.delenv(identity.AGENT_ENV, raising=False)
    assert identity.agent_account(env(), "retail") == "pw-agent@p.iam"
    assert identity.agent_account(env(), "other") is None
    monkeypatch.setenv(identity.AGENT_ENV, "x@p.iam")
    assert identity.agent_account(env(), "other") == "x@p.iam"


def test_calls_are_labelled():
    assert identity.user_agent("Claude Code!").startswith("pipewright-mcp/") and "agent=ClaudeCode" in \
        identity.user_agent("Claude Code!")
    labels = identity.job_labels("Gemini CLI", "S-42 abc")
    assert labels == {"pipewright-mcp": "true", "pipewright-agent": "gemini-cli", "pipewright-session": "s-42-abc"}


def test_queries_carry_the_sessions_job_labels(monkeypatch):
    sent = {}

    class Fake(Gcp):
        def post(self, url, body, **params):
            sent.update(body)
            return Response(200, {"jobComplete": True, "rows": []})

    bigquery.query(Fake(object(), job_labels={"pipewright-mcp": "true"}), "p", "SELECT 1")
    assert sent["labels"] == {"pipewright-mcp": "true"}
    sent.clear()
    bigquery.query(Fake(object()), "p", "SELECT 1")
    assert "labels" not in sent


def test_impersonation_targets_the_agent_account(monkeypatch):
    import google.auth.credentials as gac

    class Source(gac.Credentials):
        def refresh(self, request): ...

    monkeypatch.delenv(identity.AGENT_ENV, raising=False)
    monkeypatch.setattr("pipewright.gcp.session.credentials", lambda: Source())
    creds, acting = identity.credentials_for(env(), "retail")
    assert acting == "pw-agent@p.iam" and creds._target_principal == "pw-agent@p.iam"
    creds, acting = identity.credentials_for(env(), "other")
    assert acting is None and isinstance(creds, Source)


def test_the_manifest_carries_ai_and_the_agent_account(repo):
    repo.manifests["dev"]["ai"] = {"local_tools": "forbidden"}
    repo.manifests["dev"]["tenants"]["churn"]["agent_service_account"] = "pw-agent-churn@acme.iam"
    repo.write()
    e = load_environment(repo.root, "dev")
    assert e.ai == {"local_tools": "forbidden"}
    assert identity.agent_account(e, "churn") == "pw-agent-churn@acme.iam"
    assert not e.manifest_ignored


def test_doctor_for_an_agent_wants_no_power_to_change_anything(repo):
    from pipewright.checks import doctor
    from pipewright.config import resolve_usecase
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    env = load_environment(repo.root, "dev")

    class G:
        def __init__(self, held):
            self.held = held

        def post(self, url, body, **params):
            return Response(200, {"permissions": [p for p in body["permissions"] if p in self.held]})

        def get(self, url, **params):
            return Response(200, {"permissions": [p for p in params.get("permissions", []) if p in self.held]})

    def results(held):
        report = doctor.Report()
        doctor._check_permissions(uc, env, G(held), report, as_agent=True)
        return [(r.status, r.check) for rs in report.sections.values() for r in rs]

    assert results(set()) == [(doctor.OK, "agent identity")]
    assert results({"bigquery.tables.getData"}) == [(doctor.WARN, "agent identity can change things")]
    assert results({"storage.objects.create"}) == [(doctor.WARN, "agent identity can change things")]


def test_gcloud_never_inherits_stdin(monkeypatch):
    """Inside `pipewright mcp` stdin is the MCP channel: gcloud inheriting it stalled the server (found live)."""
    import subprocess

    from pipewright.gcp import session

    seen = {}

    def fake_run(cmd, **kw):
        seen.update(kw)
        return subprocess.CompletedProcess(cmd, 0, stdout='{"credential": {"access_token": "t", '
                                                          '"token_expiry": "2030-01-01T00:00:00Z"}}', stderr="")

    monkeypatch.setenv("GCLOUD_CONFIG", "x")
    monkeypatch.delenv("GOOGLE_OAUTH_ACCESS_TOKEN", raising=False)
    monkeypatch.setattr(subprocess, "run", fake_run)
    session._AGENT.clear()
    session.credentials().refresh(None)
    assert seen.get("stdin") is subprocess.DEVNULL
