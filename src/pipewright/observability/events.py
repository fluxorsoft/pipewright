"""Lifecycle events from the launcher and the CLI.

An event is one structured log line (Cloud Logging reads JSON on stdout) and, when the environment manifest sets
observability.events_topic, a Pub/Sub message. The platform routes both to its tools (Dynatrace, Datadog, Splunk,
PagerDuty, Slack, BigQuery); Pipewright has no vendor code. Components emit the same shape through
pipewright_runtime.emit_event.

Event types: run.submitted, run.duplicate, release.live, release.rolled_back, model.registered, model.approved,
model.deployed, training.evaluated, monitoring.recorded, monitoring.alert, retrain.requested, data.erased, run.failed
(Pipewright 1.2: a run that ended failed or cancelled, from the launcher's tick); retrain
decisions (pipewright.retraining): retrain.proposed, retrain.approval, retrain.approved, retrain.declined,
retrain.snoozed, retrain.reopened, retrain.escalated, retrain.digest.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
from datetime import datetime, timezone

log = logging.getLogger("pipewright.events")

# Events this process could not publish (Pipewright 1.2, FR-80): the CLI ends with a warning naming them, since a
# command still succeeds when its event never reaches the event log.
LOST: list[tuple[str, str]] = []
MAX_LOST = 20                      # the launcher runs for days: keep the first few, never a growing list


def _lost(event_type: str, why: str) -> None:
    if len(LOST) < MAX_LOST:
        LOST.append((event_type, why))


# Human decisions (FR-56, D-121, D-122): each records how it was approved next to the identity that acted.
DECISIONS = ("model.approved", "model.deployed", "model.promoted", "release.promoted", "release.rolled_back",
             "retrain.approval", "retrain.approved", "retrain.declined", "retrain.snoozed")


def approval() -> dict:
    """How the decision being made now was approved. A gate job of the generated workflows sets
    PIPEWRIGHT_APPROVAL from GitHub's record of its environment approval (the reviewers, their comment, the run);
    any other GitHub Actions job is recorded with its run and actor; anything else as approved outside CI."""
    given = os.environ.get("PIPEWRIGHT_APPROVAL")
    if given:
        try:
            record = json.loads(given)
        except ValueError:
            record = {"note": "PIPEWRIGHT_APPROVAL is not JSON"}
        return {"via": "github", **(record if isinstance(record, dict) else {})}
    if os.environ.get("GITHUB_ACTIONS") == "true":
        run = (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY', '')}"
               f"/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}")
        return {"via": "github", "run_url": run, "actor": os.environ.get("GITHUB_ACTOR"), "reviewers": []}
    return {"via": "outside-ci"}


def event(event_type: str, usecase: str | None, environment: str | None, tenant: str | None = None,
          release: str | None = None, run_id: str | None = None, **data) -> dict:
    if (event_type in DECISIONS or event_type.startswith("loop.")) and "approval" not in data:
        data["approval"] = approval()
    return {"event_type": event_type, "time": datetime.now(timezone.utc).isoformat(), "usecase": usecase,
            "environment": environment, "tenant": tenant, "release": release, "run_id": run_id, "data": data}


def _labels(e: dict) -> dict:
    keys = {"usecase": "pipewright-usecase", "tenant": "pipewright-tenant", "environment": "pipewright-environment",
            "release": "pipewright-release", "run_id": "run-id"}
    return {label: str(e[k]) for k, label in keys.items() if e.get(k)}


def publish_url(topic: str, region: str | None = None) -> str:
    """The environment's regional Pub/Sub endpoint: topics that enforce message storage in transit (private
    profiles) accept publishes only there, and it works for every other topic too."""
    host = f"{region}-pubsub.googleapis.com" if region else "pubsub.googleapis.com"
    return f"https://{host}/v1/{topic}:publish"


def emit(e: dict, topic: str | None = None, gcp=None, stream=None, region: str | None = None,
         attributes: dict | None = None) -> bool:
    """Writes the log line; publishes when a topic is set. Returns whether it was published. Never raises.
    attributes: extra Pub/Sub attributes for subscription filters (e.g. decision_feed)."""
    entry = {"severity": "NOTICE", "message": f"pipewright event {e['event_type']}",
             "logging.googleapis.com/labels": _labels(e), "pipewright_event": e}
    print(json.dumps(entry, default=str), file=stream or sys.stdout, flush=True)
    topic = topic or os.environ.get("PIPEWRIGHT_EVENTS_TOPIC")
    if not topic:
        return False
    try:
        if gcp is None:
            from pipewright.gcp import Gcp

            gcp = Gcp()
        attributes = {**{k: str(v) for k, v in (attributes or {}).items()},
                      **{k: str(e[k]) for k in ("event_type", "usecase", "environment", "tenant") if e.get(k)}}
        message = {"data": base64.b64encode(json.dumps(e, default=str).encode()).decode(), "attributes": attributes}
        r = gcp.post(publish_url(topic, region), {"messages": [message]})
        if not r.ok:
            log.warning("event %s not published to %s: %s", e["event_type"], topic, r.status)
            _lost(e["event_type"], f"HTTP {r.status}")
        return r.ok
    except Exception as ex:  # noqa: BLE001 - events must never break a release or a launch
        log.warning("event %s not published: %s", e["event_type"], ex)
        _lost(e["event_type"], str(ex)[:120])
        return False


def lost_summary() -> str | None:
    """One warning line for the events this process could not publish, or None."""
    if not LOST:
        return None
    what = ", ".join(f"{t} ({why})" for t, why in LOST)
    hint = ("; the identity needs roles/pubsub.publisher on the events topic"
            if any("403" in why for _, why in LOST) else "")
    return f"WARN  {len(LOST)} event(s) did not reach the event log: {what}{hint}"
