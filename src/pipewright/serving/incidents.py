"""Cloud Monitoring incidents to Pipewright events (Pipewright 1.4.3, FR-98 stage 1).

The environment's incident channel publishes each incident of a health policy (pipewright.serving.health) to a
Pub/Sub topic; the launcher's /incidents route reads them (push, or pull on GKE). An incident that opens becomes
serving.degraded, one that closes serving.recovered, with the policy's Pipewright labels: the deployed version, the
signal, the severity, the threshold in force, and the observed value where Cloud Monitoring gives one (threshold
conditions; PromQL incidents carry the condition, not a value: spike 4). A policy without pipewright-health is not
Pipewright's: ignored.
"""

from __future__ import annotations

import base64
import json


class NotOurs(Exception):
    pass


def _threshold(label: str | None):
    if not label:
        return None
    try:
        return float(label.replace("_", "."))
    except ValueError:
        return label


def parse(envelope: dict | None) -> dict:
    """The incident of a Pub/Sub push envelope (or a pulled message's data), as an event's data:
    {state, usecase_label, environment_label, signal, severity, threshold, ...}."""
    message = (envelope or {}).get("message") or envelope or {}
    raw = message.get("data")
    try:
        body = json.loads(base64.b64decode(raw)) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError) as e:
        raise NotOurs(f"not an incident message: {e}") from None
    incident = body.get("incident") or {}
    labels = incident.get("policy_user_labels") or {}
    if labels.get("pipewright-health") != "true":
        raise NotOurs("not a Pipewright health policy")
    state = str(incident.get("state") or "").lower()
    if state not in ("open", "closed"):
        raise NotOurs(f"incident state {state!r}")
    return {"state": state, "usecase_label": labels.get("pipewright-usecase"),
            "environment_label": labels.get("pipewright-environment"),
            "signal": labels.get("pipewright-signal"), "severity": labels.get("pipewright-severity"),
            "threshold": _threshold(labels.get("pipewright-threshold")),
            "latency": labels.get("pipewright-latency"),
            "model_version": labels.get("pipewright-model-version"), "target": labels.get("pipewright-target"),
            "deployed_model": labels.get("pipewright-deployed-model"), "endpoint": labels.get("pipewright-endpoint"),
            "isvc": labels.get("pipewright-isvc"), "arm": labels.get("pipewright-arm"),
            "release": labels.get("pipewright-release"),
            "observed_value": incident.get("observed_value"), "incident_id": incident.get("incident_id"),
            "started_at": incident.get("started_at"), "ended_at": incident.get("ended_at"),
            "policy": incident.get("policy_name"), "condition": incident.get("condition_name"),
            "summary": incident.get("summary")}


def event_type(incident: dict) -> str:
    return "serving.degraded" if incident["state"] == "open" else "serving.recovered"


def event_data(incident: dict) -> dict:
    return {k: v for k, v in incident.items() if k not in ("state", "usecase_label", "environment_label")
            and v is not None}
