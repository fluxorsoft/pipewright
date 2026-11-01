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
