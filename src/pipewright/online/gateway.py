"""The lookup gateway (FR-106, Pipewright 1.4.2): `POST /predict` on a use case's lookup service.

An Agent Platform endpoint cannot reach into the network (spike 2026-11-08: the internal lookup service, Cloud SQL's
private IP and the network's address for Google APIs were all unreachable from inside both a standard and a private
endpoint). So the gateway turns the call around: the caller sends entity IDs to the lookup service inside the network,
the gateway reads their features and calls the endpoint as its own identity.

    POST /predict {"entity_ids": ["c-1", "c-2"]}
      -> {"predictions": {"c-1": ..., "c-2": ...}, "not_found": [], "deployed_model": "...", "model_version": "3"}

Where the features come from:
- Cloud SQL, synced by Pipewright (online_store type cloudsql);
- the team's own lookup service inside the network (type http, the `POST /lookup` contract): the Pipewright custom
  solution, for a team that runs its own store, for example from Pipewright's Parquet export.

A prediction is safe to repeat, so the gateway retries 503, 429 and dropped connections (default 3 more tries, 0.25
to 2 s apart): on a shared pool every change restarts all machines, and the retries took a change from about 6% of
calls failing to 1 in 4,629 (spike). The canary split stays on the endpoint.

Each request is logged with the caller's identity (from the ID token Cloud Run has checked), the use case, the
deployed model and version, and counts; never an ID or a feature value. Standard library only, like the server.
"""

from __future__ import annotations

import base64
import json
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
PSC_SAN = "*.prediction.p.vertexai.goog"
RETRY_STATUS = (429, 503)


def _md(path: str) -> bytes:
    request = urllib.request.Request(MD + path, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=10) as r:
        return r.read()


class IdTokens:
    """ID tokens for the team's lookup service, per audience, reused for 25 minutes."""

    def __init__(self, fetch=None):
        self._fetch = fetch or (lambda aud: _md("/identity?audience=" + urllib.parse.quote(aud, safe="")).decode())
        self._lock, self._cache = threading.Lock(), {}

    def get(self, audience: str) -> str:
        with self._lock:
            hit = self._cache.get(audience)
            if not hit or time.time() - hit[1] > 1500:
                hit = (self._fetch(audience), time.time())
                self._cache[audience] = hit
            return hit[0]


class HttpSource:
    """The team's own lookup service (type http): POST /lookup {"entity_ids"} -> {"features": {id: {...}}}."""

    def __init__(self, cfg: dict, tokens: IdTokens | None = None, opener=None):
        # The `http` contract's URL is the service; the lookup is POST <url>/lookup (as the serving handler calls it).
        self.url, self.audience = cfg["url"].rstrip("/") + "/lookup", cfg.get("audience") or cfg["url"]
        self.timeout = int(cfg.get("timeout_ms", 1000)) / 1000
        self.tokens, self._open = tokens or IdTokens(), opener or urllib.request.urlopen

    def fetch(self, ids: list[str]) -> dict:
        request = urllib.request.Request(self.url, method="POST", data=json.dumps({"entity_ids": ids}).encode(),
                                         headers={"Authorization": "Bearer " + self.tokens.get(self.audience),
                                                  "Content-Type": "application/json"})
        try:
            with self._open(request, timeout=self.timeout) as r:
                return (json.loads(r.read() or b"{}").get("features")) or {}
        except Exception as e:  # noqa: BLE001 - re-raised, marked: the store failed, not the model (1.4.2)
            e.stage = f"lookup at {urllib.parse.urlsplit(self.url).netloc}"
            raise


def caller_of(header: str | None) -> str:
    """Who called: the email in the ID token Cloud Run has already checked (IAM run.invoker). Read, not verified
    again; Cloud Run refuses a request without a valid token before it reaches the gateway."""
    try:
        payload = (header or "").split(" ", 1)[1].split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return claims.get("email") or claims.get("sub") or "unknown"
    except Exception:  # noqa: BLE001 - a malformed header is logged as unknown, never an error
        return "unknown"


def _fetch_pem(host: str, address: str | None = None) -> str:
    """The certificate a private endpoint presents, asking for `host` at `address` (its IP when DNS has no name)."""
    probe = ssl.create_default_context()
    probe.check_hostname, probe.verify_mode = False, ssl.CERT_NONE
    with socket.create_connection((address or host, 443), timeout=10) as raw:
        with probe.wrap_socket(raw, server_hostname=host) as s:
            return ssl.DER_cert_to_PEM_cert(s.getpeercert(binary_form=True))


def pinned_context(host: str, fetch=None, address: str | None = None) -> ssl.SSLContext:
    """A private endpoint's certificate is signed by Google for *.prediction.p.vertexai.goog, not by a public CA.
    Google's guidance is to fetch it and trust it: the gateway fetches it once per instance from the endpoint's
    address inside the network, checks it names that domain, and trusts exactly that certificate."""
    pem = fetch(host) if fetch else _fetch_pem(host, address)
    ctx = ssl.create_default_context(cadata=pem)
    ctx.check_hostname = False               # checked below against the certificate's own name
    der = ssl.PEM_cert_to_DER_cert(pem)
    if b"prediction.p.vertexai.goog" not in der:
        raise ssl.SSLError(f"{host}: the certificate does not name {PSC_SAN}")
    return ctx


def endpoint_address(endpoint: str, token: str, region: str, opener=None) -> str | None:
    """The address Agent Platform made for a private endpoint in the network (its Private Service Connect
    automation), from the endpoint itself. Found live: Agent Platform makes it with the endpoint's first deployed
    model, so Terraform cannot know it when it creates the endpoint, and a DNS name may not exist yet."""
    req = urllib.request.Request(f"https://{region}-aiplatform.googleapis.com/v1/{endpoint}",
                                 headers={"Authorization": "Bearer " + token})
    with (opener or urllib.request.urlopen)(req, timeout=10) as r:
        cfg = (json.loads(r.read() or b"{}").get("privateServiceConnectConfig")) or {}
    for a in cfg.get("pscAutomationConfigs") or []:
        ok = a.get("state", "PSC_AUTOMATION_STATE_SUCCESSFUL") == "PSC_AUTOMATION_STATE_SUCCESSFUL"
        if a.get("ipAddress") and ok:
            return a["ipAddress"]
    return None


def post_private(address: str, host: str, path: str, body: bytes, headers: dict, ctx, timeout: float = 30) -> dict:
    """POST to a private endpoint at its address, presenting its DNS name (TLS and Host), as urllib would."""
    import http.client

    class Conn(http.client.HTTPSConnection):
        def connect(self):
            raw = socket.create_connection((address, 443), timeout)
            self.sock = ctx.wrap_socket(raw, server_hostname=host)

    conn = Conn(host, 443, timeout=timeout, context=ctx)
    try:
        conn.request("POST", path, body=body, headers=headers)
        r = conn.getresponse()
        data = r.read()
        if r.status >= 400:
            raise urllib.error.HTTPError(f"https://{host}{path}", r.status, r.reason, dict(r.getheaders()), None)
        return json.loads(data or b"{}")
    finally:
        conn.close()


class Gateway:
    def __init__(self, cfg: dict, source, tokens=None, opener=None, sleep=time.sleep, context=None,
                 poster=None, resolver=None):
        g = cfg["gateway"]
        self.url, self.private = g["predict_url"], bool(g.get("private"))
        self.endpoint, self.region = g.get("endpoint"), g.get("region")
        self._post, self._resolve = poster or post_private, resolver
        self._address = None
        self.retries = int(g.get("retries", 3))
        self.features, self.usecase = cfg["features"], cfg.get("usecase", "")
        self.source, self._sleep = source, sleep
        self._tokens = tokens                      # access tokens for the endpoint (the gateway's identity)
        self._open = opener or urllib.request.urlopen
        self._context, self._lock = context, threading.Lock()

    def _token(self) -> str:
        if self._tokens:
            return self._tokens.get()
        return json.loads(_md("/token"))["access_token"]

    def _target(self) -> tuple[str, object]:
        """A private endpoint's (address, pinned TLS context): its DNS name when the network resolves it, otherwise
        the address Agent Platform made for it, read from the endpoint."""
        with self._lock:
            if self._address is None:
                host = urllib.parse.urlparse(self.url).hostname
                try:
                    address = socket.getaddrinfo(host, 443)[0][4][0]
                except OSError:
                    resolve = self._resolve or (lambda: endpoint_address(self.endpoint, self._token(), self.region))
                    address = resolve()
                if not address:
                    raise ConnectionError("the private endpoint has no address yet (Agent Platform makes it with "
                                          "the endpoint's first deployed model)")
                if self._context is None:
                    self._context = pinned_context(host, address=address)
                self._address = address
            return self._address, self._context

    def _send(self, body: bytes) -> dict:
        headers = {"Authorization": "Bearer " + self._token(), "Content-Type": "application/json"}
        if self.private:
            address, ctx = self._target()
            parts = urllib.parse.urlparse(self.url)
            return self._post(address, parts.hostname, parts.path, body, headers, ctx)
        request = urllib.request.Request(self.url, data=body, method="POST", headers=headers)
        with self._open(request, timeout=30) as r:
            return json.loads(r.read() or b"{}")

    def _call(self, instances: list) -> tuple[dict, int]:
        body = json.dumps({"instances": instances}).encode()
        for attempt in range(self.retries + 1):
            try:
                return self._send(body), attempt + 1
            except urllib.error.HTTPError as e:
                if e.code not in RETRY_STATUS or attempt >= self.retries:
                    raise
            except (urllib.error.URLError, socket.timeout, ConnectionError, TimeoutError):
                if attempt >= self.retries:
                    raise
            self._sleep(min(0.25 * 2 ** attempt, 2.0))
        raise RuntimeError("unreachable")

    def call(self, instances: list) -> dict:
        """The endpoint's answer to instances as they are (the A/B router-only mode, FR-57), with the retries."""
        return self._call(instances)[0]

    def predict(self, ids: list[str]) -> tuple[dict, dict]:
        """(response body, log fields). Entities without features are listed in not_found, never scored."""
        found = self.source.fetch(ids)
        known = [i for i in ids if i in found]
        out, attempts = {"predictions": [], "deployedModelId": None}, 0
        if known:
            out, attempts = self._call([[found[i].get(f) for f in self.features] for i in known])
        preds = dict(zip(known, out.get("predictions") or [], strict=False))
        body = {"predictions": {i: preds[i] for i in known if i in preds},
                "not_found": [i for i in ids if i not in found],
                "deployed_model": out.get("deployedModelId"), "model_version": out.get("modelVersionId")}
        return body, {"asked": len(ids), "found": len(known), "attempts": attempts,
                      "deployed_model": body["deployed_model"], "model_version": body["model_version"]}
