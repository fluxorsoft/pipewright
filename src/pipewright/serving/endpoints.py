"""Endpoints the platform creates in Terraform, public or private (FR-92).

The starter (optional, off) and the Enterprise blueprint list use cases whose endpoint Terraform creates; the manifest
says so under `serving.endpoints.<use case>`:

    {"name": "churn-endpoint", "resource": "projects/<number>/locations/<region>/endpoints/<id>",
     "private": true, "dns": "<id>-<region>-<number>.prediction.p.vertexai.goog",
     "network": "projects/.../networks/..."}

Terraform owns the endpoint (its CMEK, labels, networking); `deploy-model` deploys onto it exactly as onto an endpoint
it created itself, passing machine type and replicas from CI/CD. A use case listed here is never given an endpoint by
`deploy-model`: a private one cannot be created by the CLI, and creating a public one in its place would expose it.

A private endpoint answers inside the network only, at its DNS name, with a certificate Google signs itself for
`*.prediction.p.vertexai.goog` (Google's documentation: fetch it and trust it, and use the name, not the IP).
"""

from __future__ import annotations

PSC_DOMAIN = "prediction.p.vertexai.goog"


def declared(env, usecase: str) -> dict | None:
    """The manifest's entry for a use case's Terraform endpoint, or None (the CLI creates it, as before)."""
    return (((getattr(env, "serving", None) or {}).get("endpoints")) or {}).get(usecase)


def is_private(env, usecase: str) -> bool:
    return bool((declared(env, usecase) or {}).get("private"))


def network_type(env, usecase: str) -> str:
    """psc for a private endpoint, standard otherwise: a pool serves one kind only (spike: mixing failed)."""
    return "psc" if is_private(env, usecase) else "standard"


def dns_name(endpoint_resource: str, region: str, project_number: str) -> str:
    return f"{endpoint_resource.rsplit('/', 1)[-1]}-{region}-{project_number}.{PSC_DOMAIN}"


def predict_url(env, usecase: str, endpoint_resource: str) -> str:
    """Where to send a prediction: the private endpoint's DNS name inside the network, or the regional API."""
    entry = declared(env, usecase) or {}
    path = f"/v1/{endpoint_resource}:predict"
    if entry.get("private"):
        host = entry.get("dns")
        if not host:
            parts = endpoint_resource.split("/")
            host = dns_name(endpoint_resource, env.region, parts[1] if len(parts) > 1 else "")
        return f"https://{host}{path}"
    return f"https://{env.region}-aiplatform.googleapis.com{path}"


def missing_message(env, usecase: str) -> str:
    entry = declared(env, usecase) or {}
    kind = "private" if entry.get("private") else "public"
    return (f"{entry.get('name') or usecase + '-endpoint'} is created by the platform's Terraform in {env.name} "
            f"({kind}, manifest serving.endpoints.{usecase}) and does not exist yet: ask the platform team to apply "
            "it. Pipewright does not create it itself" + (", because a public endpoint would stand in for a private one"
                                                           if entry.get("private") else ""))


def predict_private(gcp, url: str, instances: list, env, usecase: str) -> dict:
    """A prediction from a private endpoint, trusting the certificate it presents for *.prediction.p.vertexai.goog
    (as the gateway does). From outside the network the name does not resolve: said plainly."""
    import json
    import urllib.error
    import urllib.parse
    import urllib.request

    from google.auth.transport.requests import Request

    from pipewright.errors import ConfigError
    from pipewright.online.gateway import pinned_context

    creds = gcp.session.credentials
    if not creds.valid:
        creds.refresh(Request())
    host = urllib.parse.urlparse(url).hostname
    try:
        ctx = pinned_context(host)
        request = urllib.request.Request(url, data=json.dumps({"instances": instances}).encode(), method="POST",
                                         headers={"Authorization": "Bearer " + creds.token,
                                                  "Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=60, context=ctx) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise ConfigError(f"prediction failed: HTTP {e.code} {e.read()[:300].decode(errors='replace')}") from None
    except OSError as e:
        raise ConfigError(f"{host} could not be reached ({type(e).__name__}): "
                          f"{unreachable_note(env, usecase)}") from None


def unreachable_note(env, usecase: str) -> str:
    return (f"{usecase}'s endpoint is private: it answers only inside {env.name}'s network. Run this from there (a "
            "Cloud Run job, a VM, or CI with a runner in the network), or call it through the lookup gateway")
