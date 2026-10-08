"""Who decided. Email never approves (D-69): a decision carries a Google-signed ID token, and the launcher verifies
it before recording the decision under that identity.

Membership is not checked here. A decision arrives on an approver group's topic, and only the group's members may
publish to it (IAM on the topic, set by the platform), so any Google group, user or service account works without a
directory lookup.

Accepted audiences: the launcher URL (service accounts: `gcloud auth print-identity-token --audiences=<url>`) and
gcloud's own client (people: `gcloud auth print-identity-token`). Tokens last an hour and are refused when older.
"""

from __future__ import annotations

import time

GCLOUD_CLIENT_ID = "32555940559.apps.googleusercontent.com"
ISSUERS = ("accounts.google.com", "https://accounts.google.com")
MAX_AGE_SECONDS = 3600


class NotVerified(Exception):
    pass


def _request():
    import google.auth.transport.requests

    return google.auth.transport.requests.Request()


def verify(token: str, audiences: list[str], request=None, now: float | None = None) -> str:
    """The verified email of an ID token, lowercased. Raises NotVerified."""
    if not token:
        raise NotVerified("no ID token")
    import google.oauth2.id_token

    try:
        claims = google.oauth2.id_token.verify_token(token, request or _request(), audience=[a for a in audiences if a],
                                                     clock_skew_in_seconds=30)
    except Exception as e:  # noqa: BLE001 - signature, expiry, audience: all mean "not verified"
        raise NotVerified(f"ID token not verified: {type(e).__name__}: {e}"[:300]) from None
    if claims.get("iss") not in ISSUERS:
        raise NotVerified(f"ID token issuer {claims.get('iss')!r} is not Google")
    email = (claims.get("email") or "").lower()
    if not email or claims.get("email_verified") in (False, "false"):
        raise NotVerified("ID token has no verified email (service accounts: --include-email)")
    if (now or time.time()) - int(claims.get("iat", 0)) > MAX_AGE_SECONDS:
        raise NotVerified("ID token is too old")
    return email


def bearer(header: str | None) -> str:
    if not header or not header.lower().startswith("bearer "):
        return ""
    return header.split(" ", 1)[1].strip()


def cloud_run_caller(header: str | None) -> str:
    """The caller's email from the Authorization header of a request Cloud Run already authenticated (the service
    requires IAM auth, so only verified callers reach it; Cloud Run removes the token's signature before passing
    it on, so it cannot be verified again here). Empty when there is none."""
    token = bearer(header)
    if token.count(".") != 2:
        return ""
    import base64
    import json

    payload = token.split(".")[1]
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except ValueError:
        return ""
    return (claims.get("email") or "").lower()
