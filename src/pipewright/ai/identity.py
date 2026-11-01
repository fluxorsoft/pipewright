"""The agent's identity and labelled calls.

A coding agent should not borrow a person's full access. The platform gives each tenant an agent service account
(`tenants.<t>.agent_service_account` in the manifest) with metadata-only roles, and the team's engineers may
impersonate it. Every call the tools make then runs as that account: Google Cloud's IAM stops it reading rows on any
route, and Cloud Audit Logs record the account and the person it acted for. A user agent and BigQuery job labels mark
the calls as Pipewright's tools, so the platform can tell them from anything else the account does.

Without an agent account the tools run as the person, and say so.
"""

from __future__ import annotations

import os

from pipewright import __version__
from pipewright.errors import ConfigError

AGENT_ENV = "PIPEWRIGHT_AGENT_SERVICE_ACCOUNT"    # overrides the manifest, e.g. for a one-off check
TOKEN_LIFETIME = 3600


def allowed(env) -> None:
    """Refuses when the platform forbids the tools in this environment (manifest ai.local_tools: forbidden)."""
    if (getattr(env, "ai", None) or {}).get("local_tools") == "forbidden":
        raise ConfigError(f"{env.name}: the platform does not allow AI build tools here (ai.local_tools: forbidden); "
                          "use another environment, or ask the platform team")


def agent_account(env, tenant: str | None) -> str | None:
    if os.environ.get(AGENT_ENV):
        return os.environ[AGENT_ENV]
    if not tenant:
        return None
    return ((getattr(env, "tenants", None) or {}).get(tenant) or {}).get("agent_service_account")


def user_agent(agent: str | None = None) -> str:
    name = "".join(c for c in (agent or "unknown") if c.isalnum() or c in "-_.")[:40] or "unknown"
    return f"pipewright-mcp/{__version__} agent={name}"


def job_labels(agent: str | None, session_id: str) -> dict:
    """BigQuery label values: lowercase letters, digits, - and _, at most 63 characters."""
    def clean(v: str) -> str:
        return "".join(c if c.isalnum() or c in "-_" else "-" for c in v.lower())[:63]

    return {"pipewright-mcp": "true", "pipewright-agent": clean(agent or "unknown"),
            "pipewright-session": clean(session_id)}


def credentials_for(env, tenant: str | None):
    """(credentials, acting_as): impersonated credentials of the tenant's agent account when there is one; otherwise
    the person's own credentials, and acting_as is None."""
    from pipewright.gcp import session

    source = session.credentials()
    account = agent_account(env, tenant)
    if not account:
        return source, None
    from google.auth import impersonated_credentials

    creds = impersonated_credentials.Credentials(source_credentials=source, target_principal=account,
                                                 target_scopes=session.SCOPES, lifetime=TOKEN_LIFETIME)
    return creds, account


def gcp_for(env, tenant: str | None, agent: str | None, session_id: str):
    """A Gcp session for the tools: the agent's identity when configured, the user agent and the job labels."""
    from pipewright.gcp.session import Gcp

    creds, acting_as = credentials_for(env, tenant)
    return Gcp(creds, user_agent=user_agent(agent), job_labels=job_labels(agent, session_id)), acting_as
