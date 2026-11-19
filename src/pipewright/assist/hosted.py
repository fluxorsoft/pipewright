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


ANSWER_TIMEOUT = 240          # seconds: the server calls the model, tools and Model Armor before it answers


def _post(url: str, headers: dict, body: str) -> tuple[int, str, str]:
    """POST and return (status, content type, body text). The body is read off the connection as it arrives and
    un-chunked here only if it really is chunked: web shields on engineers' laptops were found to pass a body
    through unchunked while leaving the chunked header on it, which HTTP libraries then refuse (found live, as
    with AI build assistance). One request per connection, closed by the server."""
    import http.client
    import ssl
    from urllib.parse import urlsplit

    u = urlsplit(url)
    if u.scheme == "https":
        conn = http.client.HTTPSConnection(u.hostname, u.port or 443, timeout=ANSWER_TIMEOUT,
                                           context=ssl.create_default_context())
    else:
        conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=ANSWER_TIMEOUT)
    try:
        conn.request("POST", u.path or "/", body=body.encode("utf-8"), headers={**headers, "Connection": "close"})
        resp = conn.getresponse()
        raw = resp.fp.read() if resp.fp is not None else b""        # the bytes as sent, no transfer decoding
        return resp.status, resp.getheader("content-type") or "", _unchunk(raw).decode("utf-8", "replace")
    finally:
        conn.close()


def _unchunk(raw: bytes) -> bytes:
    """A chunked body put together; a body that is not chunked (whatever its header says) returned as it is."""
    out, rest = b"", raw
    while rest:
        head, sep, tail = rest.partition(b"\r\n")
        try:
            size = int(head.split(b";", 1)[0].strip(), 16)
        except ValueError:
            return raw
        if not sep or len(tail) < size:
            return raw
        if size == 0:
            return out
        out, rest = out + tail[:size], tail[size:].removeprefix(b"\r\n")
    return out or raw


def _call(url: str, headers: dict, arguments: dict) -> dict:
    """One tool call as plain JSON-RPC over HTTP. The hosted server is stateless and answers in JSON, so one POST
    is the whole exchange: no session, no event stream to keep open (found live: an MCP client session waited on
    its stream through a port-forward and never sent the call)."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": TOOL, "arguments": arguments}}
    status, kind, text = _post(url, {**headers, "Content-Type": "application/json",
                                     "Accept": "application/json, text/event-stream"}, json.dumps(body))
    if status in (401, 403):
        return {"ok": False, "refused": f"the tool server did not admit you ({status}): only the team's "
                                        "listed engineers may ask, with a Google ID token"}
    if status >= 400:
        # 1.4.5 GA QA: from outside the network Cloud Run answers with an HTML 404 page; quote it only if it is not HTML
        detail = "a web page, not the tool server" if text.lstrip().lower().startswith(("<html", "<!doctype")) else text[:200]
        raise RuntimeError(f"the tool server answered {status}: {detail}")
    if "text/event-stream" in kind:                                    # the same answer, framed as one event
        text = next((line[5:].strip() for line in text.splitlines() if line.startswith("data:")), "")
    reply = json.loads(text)
    if reply.get("error"):
        return {"ok": False, "error": str(reply["error"].get("message") or reply["error"])[:600]}
    result = reply.get("result") or {}
    if isinstance(result.get("structuredContent"), dict):
        content = result["structuredContent"]
        inner = content.get("result")           # the MCP server wraps a tool's answer as {"result": {...}}
        return inner if isinstance(inner, dict) and set(content) == {"result"} else content
    for part in result.get("content") or []:
        if part.get("text"):
            try:
                return json.loads(part["text"])
            except ValueError:
                return {"ok": False, "error": part["text"][:600]}
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
        return (call or _call)(url, headers, arguments)
    except ConfigError:
        raise
    except Exception as e:  # noqa: BLE001 - not reachable, not admitted: said plainly, with how to reach it
        how = ""
        if in_cluster and not proxied:
            how = (" It runs in the cluster: keep a port-forward running and pass --server http://localhost:8765:\n  "
                   f"kubectl port-forward -n {hosted.get('namespace', 'pipewright-mcp')} "
                   f"service/{hosted.get('service', 'pipewright-mcp')} 8765:80")
        elif hosted and not proxied:
            # 1.4.4 GA QA (live): with internal ingress Cloud Run drops a call from outside the network (404), and
            # the local proxy sends the same call from the same machine, so it does not help there
            how = (" If its ingress is internal (the default), Cloud Run only takes calls from inside the "
                   "environment's network: ask from a machine on it (a workstation on the VPC, or over your VPN). "
                   "There the same command works; the proxy is an alternative:\n  "
                   f"gcloud run services proxy {hosted.get('service', 'pipewright-mcp')} --project {env.project} "
                   f"--region {env.region} --port 8765   (then --server http://localhost:8765)")
        raise ConfigError(f"the hosted tool server at {url} could not be reached "
                          f"({type(e).__name__}: {str(e)[:200]}).{how}") from None
