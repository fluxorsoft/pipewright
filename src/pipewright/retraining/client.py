"""`pipewright retrain`: list and show cases, and decide (approve, decline, snooze).

Reading needs storage.objects.get on the artifacts bucket's cases/ prefix. Deciding sends a message to the
approver group's topic (manifest approver_groups); publishing needs membership, which is the check. With no
required groups, the decision goes straight to the launcher (who may call it decides).

Approvers do not need the MLOps repo: `--manifest gs://<artifacts>/pipewright/environment.json` is enough.
"""

from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timezone

from pipewright.config import Environment
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, error_text
from pipewright.retraining.cases import ACTIONS, REASONS, approved_groups
from pipewright.retraining.store import CaseStore, case_name

PUBSUB = "https://pubsub.googleapis.com/v1"


def store(env: Environment, gcp: Gcp) -> CaseStore:
    return CaseStore(gcp, env.artifacts_bucket)


def get_case(env: Environment, usecase: str, case_id: str | None, gcp: Gcp) -> dict:
    s = store(env, gcp)
    if case_id:
        case = s.get(usecase, case_id)
        if case is None:
            raise ConfigError(f"no case {case_id} for {usecase} (gs://{env.artifacts_bucket}/"
                              f"{case_name(usecase, case_id)})")
        return case
    case = s.current(usecase)
    if case is None:
        raise ConfigError(f"{usecase} has no open case in {env.name}: `pipewright retrain list` shows past ones")
    return case


def training_outcome(case: dict) -> str:
    """What the case's latest training did (Pipewright 1.2.0, FR-79): registered vN, refused, failed, running, or -."""
    t = case.get("training") or {}
    if not t.get("job_id"):
        return "-"
    return {"registered": f"registered v{t.get('version')}", "rejected": "refused", "failed": "failed",
            "succeeded": "finished"}.get(t.get("outcome") or "", "running")


def render_list(usecase: str, env: Environment, cases: list[dict]) -> str:
    if not cases:
        return f"{usecase} ({env.name}): no retrain cases"
    lines = [f"{usecase} ({env.name}): {len(cases)} case(s), newest first",
             f"  {'case':<20} {'state':<9} {'opened':<20} {'triggers':<18} {'training':<14} approvals / until"]
    for c in cases:
        extra = c.get("until") if c["state"] in ("snoozed", "declined") else ", ".join(approved_groups(c)) or (
            ", ".join(a["by"] for a in c.get("approvals") or []))
        lines.append(f"  {c['case_id']:<20} {c['state']:<9} {c.get('opened_at', '')[:19]:<20} "
                     f"{','.join(c.get('triggers') or []):<18} {training_outcome(c):<14} {extra or ''}")
    return "\n".join(lines)


def can_publish(gcp: Gcp, topic: str) -> bool:
    """Whether the caller may publish to an approver group's topic (testIamPermissions runs as the caller, so
    group membership counts without a directory lookup)."""
    r = gcp.post(f"{PUBSUB}/{topic}:testIamPermissions", {"permissions": ["pubsub.topics.publish"]})
    return r.ok and "pubsub.topics.publish" in (r.body.get("permissions") or [])


def choose_group(env: Environment, case: dict, action: str, group: str | None, gcp: Gcp) -> str | None:
    required = case.get("required") or []
    if not required:
        if group:
            raise ConfigError(f"case {case['case_id']} requires no approver group: drop --group")
        return None
    if group and group not in required:
        raise ConfigError(f"--group {group} is not required by case {case['case_id']} ({', '.join(required)})")
    missing = [g for g in required if g not in env.approver_groups]
    if missing:
        raise ConfigError(f"approver group(s) {', '.join(missing)} are not registered in the {env.name} manifest: "
                          "ask the platform team")
    todo = [g for g in required if action != "approve" or g not in approved_groups(case)]
    candidates = [group] if group else todo
    member = [g for g in candidates if can_publish(gcp, env.approver_groups[g]["topic"])]
    if not member:
        if action == "approve" and not todo:
            raise ConfigError(f"every required group has approved case {case['case_id']}")
        raise ConfigError(f"you are not a member of {'group ' + group if group else 'any approver group'} "
                          f"that can {action} case {case['case_id']} (needed: {', '.join(candidates)}); "
                          "membership is publish access to the group's topic")
    return member[0]


def message(env: Environment, case: dict, action: str, reason: str, because: str | None, days: int | None,
            source_uri: str | None) -> dict:
    if action not in ACTIONS:
        raise ConfigError(f"unknown action {action}")
    if not (reason or "").strip():
        raise ConfigError("give --reason: it is recorded with your name")
    if action not in ("approve", "retry") and because not in REASONS:
        raise ConfigError(f"give --because: one of {', '.join(REASONS)}")
    if source_uri:
        from pipewright.runs import sources

        if sources.problem(source_uri):
            raise ConfigError(sources.problem(source_uri))
    from pipewright.observability.events import approval

    out = {"kind": "retrain.decision", "usecase": case["usecase"], "case_id": case["case_id"], "action": action,
           "reason": reason.strip(), "approval": approval()}       # the GitHub reviewer, or outside CI (D-121)
    for key, value in (("because", because), ("days", days), ("source_uri", source_uri)):
        if value is not None:
            out[key] = value
    return out


def run_requests_topic(env: Environment, group: str | None) -> str | None:
    """The topic a decision no approver group has to make goes over: a private launcher's run requests topic (it
    accepts no direct calls). PIPEWRIGHT_RUN_VIA=http calls the launcher directly (from inside the network)."""
    topic = (env.launcher or {}).get("run_requests_topic")
    if group is None and topic and os.environ.get("PIPEWRIGHT_RUN_VIA", "").lower() != "http":
        return topic
    return None


def send(env: Environment, case: dict, body: dict, group: str | None, gcp: Gcp) -> str:
    """Sends the decision; returns how (for the user)."""
    from pipewright.observability.events import publish_url
    from pipewright.runs import launch

    url = launch.launcher_url(env)
    topic = run_requests_topic(env, group)
    if topic:
        payload = {**body, "kind": "retrain.decision", "id_token": launch.id_token(url)}
        r = gcp.post(publish_url(topic, env.region), {"messages": [{
            "data": base64.b64encode(json.dumps(payload).encode()).decode(),
            "attributes": {"usecase": case["usecase"], "case_id": case["case_id"], "action": body["action"]}}]})
        if not r.ok:
            hint = " (publishing needs roles/pubsub.publisher on it: ask the platform team)" if r.status == 403 else ""
            raise ConfigError(f"decision not sent to {topic}: {error_text(r)}{hint}")
        return f"sent over the run requests topic (message {r.body['messageIds'][0]})"
    if group is None:
        result = launch.call(env, body, path="/retraining/decide")
        if result.get("status") != "recorded":
            raise ConfigError(f"decision refused: {result.get('reason', result)}")
        return "sent to the launcher"
    payload = {**body, "id_token": launch.id_token(url)}
    topic = env.approver_groups[group]["topic"]
    r = gcp.post(publish_url(topic, env.region), {"messages": [{
        "data": base64.b64encode(json.dumps(payload).encode()).decode(),
        "attributes": {"usecase": case["usecase"], "case_id": case["case_id"], "action": body["action"]}}]})
    if not r.ok:
        raise ConfigError(f"decision not sent to {topic}: {error_text(r)}")
    return f"sent as approver group {group} (message {r.body['messageIds'][0]})"


def wait_for(env: Environment, case: dict, since: str, gcp: Gcp, timeout: float = 90,
             interval: float = 5) -> dict | None:
    """The first history entry recorded after `since` (the launcher's answer), or None on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(interval)
        latest = store(env, gcp).get(case["usecase"], case["case_id"]) or {}
        for h in latest.get("history") or []:
            if h["at"] >= since and h["action"] in (*ACTIONS, "refused"):
                return {"entry": h, "case": latest}
    return None


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
