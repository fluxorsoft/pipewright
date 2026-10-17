"""A small Kubernetes API client for the kfp orchestrator (private GKE): JSON over HTTPS to the control plane, as the
caller's Google identity (GKE accepts its OAuth access token; RBAC in the cluster authorizes it), verifying the
cluster's CA from the environment manifest. No kubeconfig and no extra dependency."""

from __future__ import annotations

import base64
import os
import tempfile
from dataclasses import dataclass


class KubeError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"{status}: {message}")
        self.status = status


@dataclass
class Response:
    status: int
    body: dict | list | None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class KubeClient:
    def __init__(self, endpoint: str, ca_certificate: str | None, gcp):
        """`endpoint` is https://<host>; `ca_certificate` the cluster CA as base64 PEM (None: system roots)."""
        self.endpoint = endpoint.rstrip("/")
        self.gcp = gcp
        self.verify: str | bool = True
        if ca_certificate:
            pem = base64.b64decode(ca_certificate)
            fd, path = tempfile.mkstemp(prefix="pipewright-ca-", suffix=".pem")
            with os.fdopen(fd, "wb") as f:
                f.write(pem)
            self.verify = path

    @classmethod
    def from_manifest(cls, manifest: dict, gcp, dns: bool = False) -> KubeClient:
        """The private endpoint (from the VPC: the launcher), or with `dns` the cluster's DNS endpoint (IAM
        authorized, reachable from outside the VPC: a Fleet View collector), which has a public certificate."""
        k = manifest.get("kubernetes") or {}
        if dns and k.get("dns_endpoint"):
            return cls(f"https://{k['dns_endpoint']}", None, gcp)
        if not k.get("endpoint"):
            raise KubeError(0, "the environment manifest has no kubernetes.endpoint (orchestrator kfp)")
        return cls(k["endpoint"], k.get("ca_certificate"), gcp)

    def request(self, method: str, path: str, body: dict | None = None, **params) -> Response:
        import requests

        headers = {"Authorization": f"Bearer {self.gcp.access_token()}", "Accept": "application/json"}
        attempts = 3 if method == "GET" else 1      # a dropped connection is retried only where repeating is safe
        for attempt in range(attempts):
            try:
                r = requests.request(method, f"{self.endpoint}{path}", json=body, params=params or None,
                                     headers=headers, verify=self.verify, timeout=60)
                break
            except requests.ConnectionError:
                if attempt == attempts - 1:
                    raise
        try:
            data = r.json() if r.content else None
        except ValueError:
            data = {"message": r.text[:500]}
        return Response(r.status_code, data)

    def get(self, path: str, **params) -> Response:
        return self.request("GET", path, **params)

    def post(self, path: str, body: dict, **params) -> Response:
        return self.request("POST", path, body, **params)

    def delete(self, path: str) -> Response:
        return self.request("DELETE", path)


def message(r: Response) -> str:
    if isinstance(r.body, dict):
        return str(r.body.get("message") or r.body.get("error") or r.body)[:400]
    return str(r.body)[:400]
