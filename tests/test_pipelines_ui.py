"""`pipewright pipelines-ui`: the Kubeflow UI on localhost through the cluster's API as the caller."""

import threading
import urllib.request
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from pipewright import pipelines_ui
from pipewright.errors import ConfigError


def test_the_ui_is_reached_through_the_dns_endpoints_service_proxy():
    env = SimpleNamespace(name="ent", orchestrator="kfp",
                          kubernetes={"dns_endpoint": "gke.example", "endpoint": "https://10.0.0.2", "namespace": "kf"})
    base, _ = pipelines_ui.upstream(env)
    assert base == "https://gke.example/api/v1/namespaces/kf/services/ml-pipeline-ui:80/proxy"
    with pytest.raises(ConfigError, match="private-gke"):
        pipelines_ui.upstream(SimpleNamespace(name="dev", orchestrator="vertex", kubernetes={}))


def test_requests_are_forwarded_with_the_callers_token(monkeypatch):
    seen = []

    class Resp:
        status_code, content = 200, b"<html>ok</html>"
        headers = {"Content-Type": "text/html", "Connection": "close"}

    def fake_request(self, method, url, headers=None, data=None, **kw):
        seen.append((method, url, headers))
        return Resp()

    import requests

    monkeypatch.setattr(requests.Session, "request", fake_request)
    gcp = SimpleNamespace(access_token=lambda: "tok-1")
    handler = pipelines_ui.make_handler("https://k/proxy", gcp, log=lambda *_: None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        body = urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/apis/v2beta1/runs?page_size=1").read()
    finally:
        server.shutdown()
    assert body == b"<html>ok</html>"
    method, url, headers = seen[0]
    assert method == "GET" and url == "https://k/proxy/apis/v2beta1/runs?page_size=1"
    assert headers["Authorization"] == "Bearer tok-1" and "Host" not in headers
