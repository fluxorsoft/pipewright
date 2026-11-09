"""The Kubernetes client retries a dropped connection where repeating is safe (found in the A/B battle test: the
private cluster's DNS endpoint dropped connections, and a stop left the A/B router running)."""
from types import SimpleNamespace

import pytest
import requests

from pipewright.kube import KubeClient


def _client(monkeypatch, fail_first):
    calls = []

    def fake(method, url, **kw):
        calls.append(method)
        if len(calls) <= fail_first:
            raise requests.ConnectionError("Remote end closed connection without response")
        return SimpleNamespace(status_code=200, content=b"{}", json=lambda: {}, text="{}")

    monkeypatch.setattr(requests, "request", fake)
    k = KubeClient.__new__(KubeClient)
    k.endpoint, k.verify, k.gcp = "https://cluster", True, SimpleNamespace(access_token=lambda: "tok")
    return k, calls


@pytest.mark.parametrize("method", ["GET", "DELETE", "PUT"])
def test_safe_requests_are_retried(monkeypatch, method):
    k, calls = _client(monkeypatch, 2)
    assert k.request(method, "/x", {} if method == "PUT" else None).status == 200 and calls == [method] * 3


def test_a_create_is_not_repeated(monkeypatch):
    k, calls = _client(monkeypatch, 1)
    with pytest.raises(requests.ConnectionError):
        k.post("/x", {})
    assert calls == ["POST"]
