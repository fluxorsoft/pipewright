"""Asking a hosted tool server (Pipewright Enterprise): the question is answered inside the environment.

Where the environment's manifest names a hosted tool server for the team (tenants.<team>.agent_server), `pipewright
assist ask` does not run the assistant on the person's machine. It sends the question to the server's `assist_ask`
tool with the person's Google ID token; the server answers as its own metadata-only identity, calls the model as the
environment's Assist identity, counts the question against the team's budget and records the answer as evidence.

How the server is reached, as for `pipewright mcp --print-config`:
    a local proxy that signs each request as the person (Cloud Run):  --server http://localhost:8765
    straight to the service, where its ingress lets the caller in:    the manifest's URL, with the ID token
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess

from pipewright.errors import ConfigError

TOOL = "assist_ask"


def server_of(env, tenant: str | None) -> dict | None:
    return ((env.tenants or {}).get(tenant or "") or {}).get("agent_server")


def identity_token() -> str:
    import os

    gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud"
    configuration = os.environ.get("GCLOUD_CONFIG")
    out = subprocess.run([gcloud, "auth", "print-identity-token",
                          *([f"--configuration={configuration}"] if configuration else [])],
                         capture_output=True, text=True, encoding="utf-8", errors="replace", stdin=subprocess.DEVNULL)
    if out.returncode != 0 or not out.stdout.strip():
        raise ConfigError(f"gcloud could not issue an ID token: {out.stderr.strip()[-300:]}")
    return out.stdout.strip().splitlines()[-1]


async def _call(url: str, headers: dict, arguments: dict) -> dict:
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client
    except ImportError:
        raise ConfigError("asking a hosted tool server needs the mcp extra: "
                          "pip install 'pipewright-mlops[gcp,mcp]'") from None
    async with streamablehttp_client(url, headers=headers, timeout=60, sse_read_timeout=300) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(TOOL, arguments)
    if getattr(result, "structuredContent", None):
        return result.structuredContent
    for part in result.content or []:
        text = getattr(part, "text", None)
        if text:
            try:
                return json.loads(text)
            except ValueError:
                return {"ok": False, "error": text[:600]}
    return {"ok": False, "error": "the tool server gave no answer"}


def ask(env, tenant: str, question: str, usecase: str | None, understood: str, server_url: str | None = None,
        call=None) -> dict:
    """The hosted server's answer: {ok, answer, ...} or {ok: False, refused | error}."""
    hosted = server_of(env, tenant)
    if not hosted and not server_url:
        raise ConfigError(f"the environment names no hosted tool server for team {tenant}")
    proxied = bool(server_url)
    url = (server_url or hosted["url"]).rstrip("/")
    url = url if url.endswith("/mcp") else url + "/mcp"
    headers = {"X-Pipewright-Agent": "pipewright-assist"}
    in_cluster = bool(hosted) and hosted.get("runtime") == "gke"
    if not proxied or in_cluster:             # Cloud Run's proxy signs for you; a port-forward into GKE does not
        headers["Authorization"] = f"Bearer {identity_token()}"
    arguments = {"question": question, "usecase": usecase or "", "understood": understood or ""}
    try:
        return (call or (lambda *a: asyncio.run(_call(*a))))(url, headers, arguments)
    except ConfigError:
        raise
    except Exception as e:  # noqa: BLE001 - not reachable, not admitted: said plainly, with how to reach it
        how = ""
        if in_cluster and not proxied:
            how = (" It runs in the cluster: keep a port-forward running and pass --server http://localhost:8765:\n  "
                   f"kubectl port-forward -n {hosted.get('namespace', 'pipewright-mcp')} "
                   f"service/{hosted.get('service', 'pipewright-mcp')} 8765:80")
        elif hosted and not proxied:
            how = (" If its ingress is internal, run the proxy and pass --server http://localhost:8765:\n  "
                   f"gcloud run services proxy {hosted.get('service', 'pipewright-mcp')} --project {env.project} "
                   f"--region {env.region} --port 8765")
        raise ConfigError(f"the hosted tool server at {url} could not be reached "
                          f"({type(e).__name__}: {str(e)[:200]}).{how}") from None
