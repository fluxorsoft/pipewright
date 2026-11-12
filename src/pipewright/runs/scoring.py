"""`pipewright scoring`: held scoring runs and a person's decision on them (FR-130, Pipewright 1.4.3).

The launcher holds each scoring run of a use case with approvals.batch_scoring manual (or the platform's
policies.batch_scoring manual). `scoring approve` submits one as it was asked for; `scoring reject` closes it. The
decision goes to the launcher, which verifies who sent it, as with retrain decisions: directly, or over the run
requests topic with the person's ID token when the launcher is private.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone

from pipewright.config import Environment
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, error_text
from pipewright.runs import held


def store(env: Environment, gcp: Gcp) -> held.HeldStore:
    if not env.artifacts_bucket:
        raise ConfigError(f"{env.name} has no artifacts bucket in its manifest")
    return held.HeldStore(gcp, env.artifacts_bucket)


def render(records: list[dict], now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    if not records:
        return "held scoring runs: none"
    lines = ["hold                                     state      trigger    requested             expires / decided"]
    for r in records:
        st = held.state(r, now)
        when = r.get("decided_at") if st in ("approved", "rejected") else r.get("expires_at")
        who = f" by {r['decided_by']}" if r.get("decided_by") else ""
        lines.append(f"{r['hold_id']:<40} {st:<10} {r.get('trigger', ''):<10} {r.get('requested_at', ''):<21} "
                     f"{when}{who}")
        if st == "held":
            lines.append(f"    source {r.get('source_uri')}")
    return "\n".join(lines)


def send(env: Environment, usecase: str, hold_id: str, action: str, reason: str, gcp: Gcp,
         wait: float = 90) -> dict:
    """The launcher's answer: {status: recorded | refused | sent, ...}."""
    from pipewright.observability import events
    from pipewright.observability.events import publish_url
    from pipewright.retraining.client import run_requests_topic
    from pipewright.runs import launch

    body = {"usecase": usecase, "hold_id": hold_id, "action": action, "reason": reason,
            "approval": events.approval()}
    topic = run_requests_topic(env, None)
    if not topic:
        try:
            return launch.call(env, body, path="/scoring/decide")
        except ConfigError as e:
            if "returned 403" in str(e) or "returned 404" in str(e):
                return {"status": "refused", "reason": str(e).split(": ", 1)[-1]}
            raise
    since = held.iso(datetime.now(timezone.utc))
    payload = {**body, "kind": "scoring.decision", "id_token": launch.id_token(launch.launcher_url(env))}
    r = gcp.post(publish_url(topic, env.region), {"messages": [{
        "data": base64.b64encode(json.dumps(payload).encode()).decode(),
        "attributes": {"usecase": usecase, "hold_id": hold_id, "action": action}}]})
    if not r.ok:
        hint = " (publishing needs roles/pubsub.publisher on it: ask the platform team)" if r.status == 403 else ""
        raise ConfigError(f"decision not sent to {topic}: {error_text(r)}{hint}")
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:          # a private launcher answers on the record itself
        time.sleep(5)
        record = store(env, gcp).get(usecase, hold_id) or {}
        for h in record.get("history") or []:
            if h["at"] >= since and h["action"] in (action, "refused"):
                if h["action"] == "refused":
                    return {"status": "refused", "reason": h.get("reason")}
                return {"status": "recorded", "action": action, "hold_id": hold_id}
    return {"status": "sent", "reason": f"sent over {topic}; not recorded yet: `pipewright scoring list` shows it"}
