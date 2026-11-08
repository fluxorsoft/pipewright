"""Pipewright's Cloud SQL lookup service (FR-104): `python -m pipewright.online.server`, in the launcher image.

One service per use case and environment, on Cloud Run, created by the launcher with the tenant's lookup identity.
It answers the `http` contract:

    POST /lookup {"entity_ids": ["c-1", ...]}  ->  {"features": {"c-1": {"tenure": 12.0, ...}, ...}}

reading the use case's table over the Cloud SQL private IP, with IAM database login (the identity's access token is
the password; there is no password user), SSL verified against the instance's server CA, and a bounded connection
pool (spike: one connection per request exhausted the database). Entities the table does not have are left out.
Logs carry counts and timings, never an ID or a value.

Configuration: PIPEWRIGHT_LOOKUP, JSON with host, database, schema, table, key, features, pool_size, timeout_ms and
server_ca (PEM). PORT as Cloud Run sets it.

Pipewright 1.4.2 (FR-106): with `gateway` in the configuration the service also answers `POST /predict` (the lookup
gateway, online/gateway.py). With `source: http` its features come from the team's own lookup service (`url`,
`audience`) instead of Cloud SQL: the Pipewright custom solution.
"""

from __future__ import annotations

import json
import os
import queue
import re
import ssl
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
MAX_IDS = 1000
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _md(path: str) -> bytes:
    request = urllib.request.Request(MD + path, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=10) as r:
        return r.read()


class Tokens:
    """The identity's access token, refreshed before it expires; it is the database password (IAM login)."""

    def __init__(self, fetch=None):
        self._fetch = fetch or (lambda: json.loads(_md("/token"))["access_token"])
        self._lock, self._token, self._at = threading.Lock(), None, 0.0

    def get(self) -> str:
        with self._lock:
            if not self._token or time.time() - self._at > 1500:
                self._token, self._at = self._fetch(), time.time()
            return self._token


def iam_user(email: str) -> str:
    """Cloud SQL's PostgreSQL user for a service account: its email without `.gserviceaccount.com`."""
    return email.removesuffix(".gserviceaccount.com")


def ssl_context(server_ca: str | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if server_ca:
        ctx = ssl.create_default_context(cadata=server_ca)
    else:                                  # tests and local runs only: the launcher always passes the CA
        ctx.verify_mode = ssl.CERT_NONE
    ctx.check_hostname = False             # Cloud SQL's certificate names the instance, not the private IP
    return ctx


class Lookup:
    def __init__(self, cfg: dict, connect=None, tokens: Tokens | None = None):
        for k in ("schema", "table", "key"):
            if not _IDENT.match(cfg[k]):
                raise ValueError(f"{k} {cfg[k]!r} is not a PostgreSQL identifier")
        bad = [f for f in cfg["features"] if not _IDENT.match(f)]
        if bad:
            raise ValueError(f"not PostgreSQL column names: {', '.join(bad)}")
        self.cfg, self.tokens = cfg, tokens or Tokens()
        self.user = cfg.get("user") or iam_user(_md("/email").decode())
        self._connect = connect or self._pg
        self.sql = (f'SELECT "{cfg["key"]}", ' + ", ".join(f'"{f}"' for f in cfg["features"])
                    + f' FROM "{cfg["schema"]}"."{cfg["table"]}" WHERE "{cfg["key"]}" = ANY(:ids)')
        self.pool: queue.Queue = queue.Queue()
        for _ in range(int(cfg.get("pool_size", 8))):
            self.pool.put(None)

    def _pg(self):
        import pg8000.native

        c = pg8000.native.Connection(self.user, host=self.cfg["host"], password=self.tokens.get(),
                                     database=self.cfg.get("database", "features"),
                                     ssl_context=ssl_context(self.cfg.get("server_ca")), timeout=10)
        c.run(f"SET statement_timeout = {int(self.cfg.get('timeout_ms', 1000))}")
        return c

    def fetch(self, ids: list[str]) -> dict:
        c = self.pool.get()                  # waits for a free connection: at most pool_size per instance
        try:
            if c is None:
                c = self._connect()
            rows = c.run(self.sql, ids=ids)
        except Exception:
            c = None                         # a broken connection is replaced on the next request
            raise
        finally:
            self.pool.put(c)
        names = self.cfg["features"]
        return {str(r[0]): {n: (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v)
                            for n, v in zip(names, r[1:], strict=True)} for r in rows}


def handler_for(lookup, gateway=None):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"        # keep-alive: callers reuse the connection (spike lesson)

        def _send(self, code: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._send(200 if self.path == "/healthz" else 404, {"ok": self.path == "/healthz"})

        def do_POST(self):
            t = time.perf_counter()
            if self.path == "/predict" and gateway is not None:
                return self._predict(t)
            if self.path != "/lookup":
                return self._send(404, {"error": "POST /lookup" + (" or POST /predict" if gateway else "")})
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                ids = body.get("entity_ids")
                if not isinstance(ids, list) or not all(isinstance(i, (str, int)) for i in ids):
                    return self._send(400, {"error": "entity_ids: a list of IDs"})
                if len(ids) > MAX_IDS:
                    return self._send(400, {"error": f"entity_ids: at most {MAX_IDS} at once"})
                found = lookup.fetch([str(i) for i in ids])
            except ValueError:
                return self._send(400, {"error": "the body is JSON with entity_ids"})
            except Exception as e:  # noqa: BLE001 - the database's error class only, never the query's values
                print(json.dumps({"severity": "ERROR", "message": "lookup failed", "error": type(e).__name__}),
                      flush=True)
                return self._send(503, {"error": f"the database did not answer ({type(e).__name__})"})
            print(json.dumps({"severity": "INFO", "message": "lookup", "asked": len(ids), "found": len(found),
                              "ms": round((time.perf_counter() - t) * 1000, 1)}), flush=True)
            self._send(200, {"features": found})

        def log_message(self, *args):
            pass

        def _predict(self, t):
            from pipewright.online.gateway import caller_of

            caller = caller_of(self.headers.get("Authorization"))
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
                ids = body.get("entity_ids")
                if not isinstance(ids, list) or not all(isinstance(i, (str, int)) for i in ids) or not ids:
                    return self._send(400, {"error": "entity_ids: a list of IDs"})
                if len(ids) > MAX_IDS:
                    return self._send(400, {"error": f"entity_ids: at most {MAX_IDS} at once"})
                out, fields = gateway.predict([str(i) for i in ids])
            except ValueError:
                return self._send(400, {"error": "the body is JSON with entity_ids"})
            except Exception as e:  # noqa: BLE001 - the error class and HTTP status only, never a value
                status = getattr(e, "code", None)
                print(json.dumps({"severity": "ERROR", "message": "predict failed", "caller": caller,
                                  "usecase": gateway.usecase, "error": type(e).__name__, "status": status}),
                      flush=True)
                return self._send(503 if status in (None, 429, 503) else 502,
                                  {"error": "the prediction failed ("
                                            + type(e).__name__ + (f" {status}" if status else "") + ")"})
            print(json.dumps({"severity": "INFO", "message": "predict", "caller": caller, "usecase": gateway.usecase,
                              **fields, "ms": round((time.perf_counter() - t) * 1000, 1)}), flush=True)
            self._send(200, out)

    return Handler


def build(cfg: dict):
    """(lookup, gateway or None) for a configuration."""
    if cfg.get("source") == "http":
        from pipewright.online.gateway import HttpSource

        lookup = HttpSource(cfg)
    else:
        lookup = Lookup(cfg)
    gateway = None
    if cfg.get("gateway"):
        from pipewright.online.gateway import Gateway

        gateway = Gateway(cfg, lookup, tokens=lookup.tokens if isinstance(lookup, Lookup) else Tokens())
    return lookup, gateway


def main():
    cfg = json.loads(os.environ["PIPEWRIGHT_LOOKUP"])
    lookup, gateway = build(cfg)
    server = ThreadingHTTPServer(("", int(os.environ.get("PORT", "8080"))), handler_for(lookup, gateway))
    server.daemon_threads = True
    where = cfg["url"] if cfg.get("source") == "http" else f"{cfg['schema']}.{cfg['table']}"
    print(json.dumps({"severity": "INFO", "message": "lookup service ready", "source": cfg.get("source", "cloudsql"),
                      "table": where, "features": len(cfg["features"]), "gateway": bool(gateway),
                      "pool_size": cfg.get("pool_size", 8)}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
