"""Thin, read-mostly access to Google Cloud REST APIs with one authorised session.

Credentials, in order:
  GOOGLE_OAUTH_ACCESS_TOKEN   a fixed token (about 1 hour)
  GCLOUD_CONFIG               refreshable: re-runs `gcloud auth print-access-token --configuration=...`
  Application Default Credentials (CI with Workload Identity Federation, Cloud Build, Cloud Run)

Behind TLS-intercepting proxies, set REQUESTS_CA_BUNDLE to a bundle that includes the proxy's root.
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from functools import cached_property
from typing import Any

from pipewright.errors import ConfigError

# userinfo.email: GKE names the caller by its email only when the token has this scope (otherwise by a numeric ID),
# and Kubernetes RBAC for the launcher (orchestrator kfp) is granted to its email.
SCOPES = ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/userinfo.email"]


def _require_gcp():
    try:
        import google.auth  # noqa: F401
        import google.auth.transport.requests  # noqa: F401
    except ImportError:
        raise ConfigError("this command needs the GCP extra: pip install 'pipewright-mlops[gcp]'") from None


def credentials():
    _require_gcp()
    import google.auth
    from google.auth import credentials as ga_credentials
    from google.oauth2.credentials import Credentials

    token = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN")
    if token:
        return Credentials(token)

    configuration = os.environ.get("GCLOUD_CONFIG")
    if configuration:

        class GcloudCredentials(ga_credentials.Credentials):
            def refresh(self, request):
                # config-helper returns gcloud's cached token with its real expiry (a cached token may have
                # only minutes left, so assuming a fixed lifetime lets long releases fail with 401).
                gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud"
                out = subprocess.run(
                    [gcloud, "config", "config-helper", "--format=json", f"--configuration={configuration}",
                     "--min-expiry=15m"],
                    capture_output=True, text=True, encoding="utf-8", errors="replace",
                )
                if out.returncode != 0:
                    raise ConfigError(f"gcloud could not issue a token: {out.stderr.strip()}")
                credential = json.loads(out.stdout)["credential"]
                self.token = credential["access_token"]
                expiry = datetime.datetime.fromisoformat(credential["token_expiry"].replace("Z", "+00:00"))
                self.expiry = expiry.astimezone(datetime.timezone.utc).replace(tzinfo=None)

        return GcloudCredentials()

    creds, _ = google.auth.default(scopes=SCOPES)
    return creds


@dataclass
class Response:
    status: int
    body: Any

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class Gcp:
    """One authorised session; every call returns a Response instead of raising on 4xx."""

    def __init__(self, creds=None):
        self._creds = creds

    @cached_property
    def session(self):
        _require_gcp()
        from google.auth.transport.requests import AuthorizedSession

        return AuthorizedSession(self._creds or credentials())

    def get(self, url: str, **params) -> Response:
        r = self.session.get(url, params=params or None, timeout=30)
        return Response(r.status_code, _json(r))

    def post(self, url: str, body: dict, **params) -> Response:
        r = self.session.post(url, json=body, params=params or None, timeout=60)
        return Response(r.status_code, _json(r))

    def patch(self, url: str, body: dict, **params) -> Response:
        r = self.session.patch(url, json=body, params=params or None, timeout=30)
        return Response(r.status_code, _json(r))

    def put(self, url: str, body: dict, **params) -> Response:
        r = self.session.put(url, json=body, params=params or None, timeout=30)
        return Response(r.status_code, _json(r))

    def delete(self, url: str, **params) -> Response:
        r = self.session.delete(url, params=params or None, timeout=30)
        return Response(r.status_code, _json(r) if r.content else {})

    def access_token(self) -> str:
        """A current OAuth access token of the caller (for APIs called without this session, e.g. a GKE cluster)."""
        creds = self.session.credentials
        if not creds.valid:
            import google.auth.transport.requests

            creds.refresh(google.auth.transport.requests.Request())
        return creds.token

    def identity(self) -> str | None:
        """Email of the caller, if the token says so."""
        creds = self.session.credentials
        if not creds.valid:
            import google.auth.transport.requests

            creds.refresh(google.auth.transport.requests.Request())
        # Service accounts (Cloud Run, GCE, impersonation) know their email; tokeninfo may not be reachable from a
        # private network (restricted Google APIs).
        email = getattr(creds, "service_account_email", None)
        if email and "@" in email:
            return email
        r = self.session.get(
            "https://oauth2.googleapis.com/tokeninfo", params={"access_token": creds.token}, timeout=15
        )
        return _json(r).get("email") if r.ok else None


def _json(r) -> Any:
    try:
        return r.json()
    except ValueError:
        return {"raw": r.text[:300]}


def error_text(resp: Response) -> str:
    if isinstance(resp.body, dict) and "error" in resp.body:
        err = resp.body["error"]
        return err.get("message", str(err)) if isinstance(err, dict) else str(err)
    return f"HTTP {resp.status}"
