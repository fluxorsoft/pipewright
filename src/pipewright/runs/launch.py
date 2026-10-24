"""pipewright run: start a pipeline through the shared launcher's /run, as a person or from CI.

The launcher needs an ID token for its URL. In order:
  PIPEWRIGHT_ID_TOKENS     set by CI for a job that calls several launchers (the closed retrain loop): a JSON
                           object of launcher URL to ID token
  PIPEWRIGHT_ID_TOKEN      set by CI (google-github-actions/auth with token_format: id_token)
  GCLOUD_CONFIG            `gcloud auth print-identity-token` for that configuration (people)
  service account / ADC    google.oauth2.id_token.fetch_id_token (Cloud Build, GCE, Cloud Run)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess

from pipewright.config import Environment
from pipewright.errors import ConfigError


def launcher_url(env: Environment) -> str:
    url = (env.launcher or {}).get("url")
    if not url:
        raise ConfigError(f"the {env.name} manifest has no launcher.url: ask the platform team")
    return url.rstrip("/")


def id_token(audience: str) -> str:
    try:
        tokens = json.loads(os.environ.get("PIPEWRIGHT_ID_TOKENS") or "{}")
    except ValueError:
        tokens = None
    if not isinstance(tokens, dict):
        raise ConfigError("PIPEWRIGHT_ID_TOKENS is not a JSON object of launcher URL to ID token")
    token = {str(k).rstrip("/"): v for k, v in tokens.items() if v}.get(audience.rstrip("/"))
    if token:
        return str(token).strip()
    token = os.environ.get("PIPEWRIGHT_ID_TOKEN")
    if token:
        return token.strip()
    configuration = os.environ.get("GCLOUD_CONFIG")
    if configuration:
        gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud"
        out = subprocess.run([gcloud, "auth", "print-identity-token", f"--configuration={configuration}"],
                             capture_output=True, text=True, encoding="utf-8", errors="replace")
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
        raise ConfigError(f"gcloud could not issue an ID token: {out.stderr.strip()[:300]}")
    try:
        import google.auth.transport.requests
        import google.oauth2.id_token

        return google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), audience)
    except Exception as e:  # noqa: BLE001
        raise ConfigError(f"no ID token for {audience}: set PIPEWRIGHT_ID_TOKEN, GCLOUD_CONFIG, or run as a "
                          f"service account ({type(e).__name__}: {e})") from None


def body(usecase: str, pipeline: str | None, source_uri: str | None, schedule: str | None,
         run_key: str | None, replay: bool = False, champion_version: str | None = None,
         model_version: str | None = None, loop: str | None = None) -> dict:
    if not schedule and not source_uri:
        raise ConfigError("give --source-uri (or --schedule to run a schedule now)")
    from pipewright.runs import sources

    if source_uri and sources.problem(source_uri, allow_snapshot=replay):
        raise ConfigError(sources.problem(source_uri, allow_snapshot=replay))
    out = {"usecase": usecase}
    for key, value in (("pipeline", pipeline), ("source_uri", source_uri), ("schedule", schedule),
                       ("run_key", run_key), ("champion_version", champion_version),
                       ("model_version", model_version), ("loop", loop)):
        if value:
            out[key] = value
    return out


def live_release(env: Environment, usecase: str, gcp=None) -> str | None:
    """The release live now (its pointer), sent with a run request: a launcher whose cached pointer is older reloads
    it. None when it cannot be read; the launcher then uses what it has."""
    try:
        from pipewright.gcp import Gcp
        from pipewright.release import service

        pointer, _ = service.read_pointer(gcp or Gcp(), env, usecase)
        return pointer.release_id if pointer else None
    except Exception:  # noqa: BLE001 - optional: the request works without it
        return None


def run(env: Environment, payload: dict, session=None, gcp=None) -> dict:
    """Private environments (manifest launcher.run_requests_topic) take run requests over Pub/Sub: their launcher
    accepts no calls from outside the network. PIPEWRIGHT_RUN_VIA=http calls /run directly (from inside it)."""
    topic = (env.launcher or {}).get("run_requests_topic")
    if topic and os.environ.get("PIPEWRIGHT_RUN_VIA", "").lower() != "http":
        return request(env, topic, payload, gcp)
    return call(env, payload, session)


def request(env: Environment, topic: str, payload: dict, gcp=None) -> dict:
    """Publishes a run request. The launcher submits it; its run key makes redeliveries a no-op."""
    import base64
    import json
    import uuid

    from pipewright.gcp import Gcp, error_text
    from pipewright.observability.events import publish_url

    payload = {**payload, "run_key": payload.get("run_key") or f"cli-{uuid.uuid4().hex[:12]}"}
    message = {"data": base64.b64encode(json.dumps(payload).encode()).decode(),
               "attributes": {"usecase": payload["usecase"]}}
    resp = (gcp or Gcp()).post(publish_url(topic, env.region), {"messages": [message]})
    if not resp.ok:
        hint = " (publishing needs roles/pubsub.publisher on it: ask the platform team)" if resp.status == 403 else ""
        raise ConfigError(f"run request not published to {topic}: {error_text(resp)}{hint}")
    return {"status": "requested", "usecase": payload["usecase"], "pipeline": payload.get("pipeline", ""),
            "message_id": resp.body["messageIds"][0], "run_key": payload["run_key"]}


def call(env: Environment, payload: dict, session=None, path: str = "/run") -> dict:
    import requests

    url = launcher_url(env)
    headers = {"Authorization": f"Bearer {id_token(url)}"}
    # Up to 300 s: a cold launcher plus a Agent Platform submit can take over a minute. The run key keeps retries safe.
    r = (session or requests).post(f"{url}{path}", json=payload, headers=headers, timeout=300)
    try:
        result = r.json()
    except ValueError:
        result = {"status": "error", "reason": r.text[:300]}
    if r.status_code != 200:
        raise ConfigError(f"launcher returned {r.status_code}: {result.get('reason', result)}")
    return result
