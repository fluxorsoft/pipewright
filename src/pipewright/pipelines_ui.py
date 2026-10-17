"""`pipewright pipelines-ui`: the Kubeflow Pipelines UI of a private-gke environment, on localhost.

The UI runs inside the private cluster and has no login of its own, so it is never exposed on a network. This
command serves it on http://localhost:<port> by forwarding each request to the UI's Service through the cluster's
Kubernetes API (its DNS endpoint, IAM authorized), as the caller's Google identity. What the caller may do is decided
there: the platform's `ui_viewers` get a role that allows only GET through the UI's service proxy, so they can see
runs, graphs, logs and artifacts, and Kubernetes refuses anything that would change a run (terminate, retry, delete,
create). Runs are started through the launcher, as everywhere else.

The UI shows every tenant's runs (standalone Kubeflow Pipelines is one namespace): it is a platform-level view.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Headers that belong to one connection, not to the request or response being forwarded.
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers",
       "transfer-encoding", "upgrade", "host", "content-length", "content-encoding", "authorization"}


def upstream(env) -> tuple[str, str | None]:
    """The UI's service proxy base URL and the CA to verify it (None: the DNS endpoint's public certificate)."""
    from pipewright.errors import ConfigError

    k = env.kubernetes or {}
    namespace = k.get("namespace") or "kubeflow"
    if k.get("dns_endpoint"):
        host = f"https://{k['dns_endpoint']}"
    elif k.get("endpoint"):
        host = k["endpoint"]
    else:
        raise ConfigError(f"{env.name} has no Kubeflow Pipelines (orchestrator {env.orchestrator}): the pipelines UI "
                          "is for private-gke environments")
    return f"{host}/api/v1/namespaces/{namespace}/services/ml-pipeline-ui:80/proxy", None


def make_handler(base: str, gcp, log=print):
    import requests

    session = requests.Session()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _forward(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
            with lock:
                token = gcp.access_token()
            headers["Authorization"] = f"Bearer {token}"
            try:
                r = session.request(self.command, base + self.path, headers=headers, data=body, timeout=120,
                                    allow_redirects=False)
            except requests.RequestException as e:
                self.send_error(502, f"cluster not reachable: {e}"[:200])
                return
            if r.status_code == 403 and self.command != "GET":
                log(f"refused: {self.command} {self.path.split('?')[0]} (the pipelines UI is read only; start and "
                    "stop runs through the launcher)")
            content = r.content
            self.send_response(r.status_code)
            for k, v in r.headers.items():
                if k.lower() in HOP:
                    continue
                if k.lower() == "location" and v.startswith(base):
                    v = v[len(base):] or "/"
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = _forward  # noqa: N815 - http.server API

        def log_message(self, *args):
            return

    return Handler


def serve(env, port: int = 8080, gcp=None, log=print) -> None:
    from pipewright.gcp import Gcp

    gcp = gcp or Gcp()
    base, _ = upstream(env)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(base, gcp, log))
    log(f"Kubeflow Pipelines UI of {env.name}: http://localhost:{port}/  (read only, as {gcp.identity() or 'you'}; "
        "Ctrl+C stops it)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
