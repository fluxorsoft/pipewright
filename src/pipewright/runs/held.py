"""Batch scoring held for a person (FR-130, Pipewright 1.4.3). Off by default.

With `approvals.batch_scoring: manual` in a use case, or `policies.batch_scoring: manual` on the platform (which turns
it on for every use case there), the launcher does not submit a scoring run: it holds it, whatever started it (a
schedule, a landing file, the API, the CLI, a workflow) and whatever runs it (Vertex AI Pipelines or Kubeflow). A
person approves or rejects each held run; an approval submits it exactly as it was asked for. A hold nobody decides
expires after `approvals.batch_scoring_hold_hours` (default 24), so a forgotten run never scores old data late.

gs://<artifacts>/cases/_held/<usecase>/<hold_id>.json   one held run (the launcher is the only writer)

Readers are `pipewright scoring` and `pipewright decisions pending`, with the same read access as retrain cases.
Retrain loops are not held: their scoring runs belong to a case a person already approved.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from pipewright.gcp import Gcp, PreconditionFailed, gcs_list, gcs_read, gcs_write

PREFIX = "cases/_held"
DEFAULT_HOLD_HOURS = 24
MAX_HOLD_HOURS = 168
ACTIONS = ("approve", "reject")
ATTEMPTS = 5


class Refused(Exception):
    """The decision cannot be recorded: the run is not held any more, has expired, or the decider may not decide."""


def mode(approvals: dict | None, policies: dict | None) -> str:
    """manual when the use case or the platform says so; the platform's manual cannot be turned off by a use case."""
    own = (approvals or {}).get("batch_scoring")
    return "manual" if "manual" in (own, (policies or {}).get("batch_scoring")) else "auto"


def hold_hours(approvals: dict | None) -> int:
    value = (approvals or {}).get("batch_scoring_hold_hours")
    return int(value) if value else DEFAULT_HOLD_HOURS


def name(usecase: str, hold_id: str) -> str:
    return f"{PREFIX}/{usecase}/{hold_id}.json"


def iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def state(record: dict, now: datetime) -> str:
    """held, approved, rejected or expired (a held run past its expiry, whether or not anyone has looked)."""
    if record.get("state") == "held" and now >= datetime.fromisoformat(record["expires_at"]):
        return "expired"
    return record.get("state", "held")


def new_record(usecase: str, hold_id: str, request: dict, trigger: str, hours: int, now: datetime,
               version: str | None = None) -> dict:
    return {"hold_id": hold_id, "usecase": usecase, "pipeline": "inference", "state": "held", "trigger": trigger,
            "source_uri": request.get("source_uri"), "requested_at": iso(now),
            "expires_at": iso(now + timedelta(hours=hours)), "default_version": version, "request": request,
            "history": [{"at": iso(now), "action": "held", "trigger": trigger}]}


def decide(record: dict, action: str, by: str, now: datetime, reason: str = "", approval: dict | None = None,
           not_allowed: set[str] | None = None) -> dict:
    """The record after a person's decision. Refused (recorded by the caller) when it cannot be made."""
    if action not in ACTIONS:
        raise Refused(f"action must be one of {', '.join(ACTIONS)}")
    if not by:
        raise Refused("no verified identity")
    if by.lower() in (not_allowed or set()):
        raise Refused(f"{by} runs pipelines or the launcher; a held scoring run is decided by a person or CI")
    current = state(record, now)
    if current != "held":
        raise Refused(f"run {record['hold_id']} is {current}, not held")
    if action == "reject" and not reason:
        raise Refused("a rejection needs a reason")
    entry = {"at": iso(now), "action": action, "by": by, **({"reason": reason} if reason else {}),
             **({"approval": approval} if approval else {})}
    return {**record, "state": "approved" if action == "approve" else "rejected", "decided_by": by,
            "decided_at": iso(now), "history": [*record.get("history", []), entry]}


class HeldStore:
    def __init__(self, gcp: Gcp, bucket: str):
        self.gcp, self.bucket = gcp, bucket

    def _read(self, path: str) -> tuple[dict | None, int]:
        text, generation = gcs_read(self.gcp, self.bucket, path)
        return (json.loads(text) if text else None), generation

    def get(self, usecase: str, hold_id: str) -> dict | None:
        return self._read(name(usecase, hold_id))[0]

    def list(self, usecase: str) -> list[dict]:
        found = [r for r in (self._read(n)[0] for n in gcs_list(self.gcp, self.bucket, f"{PREFIX}/{usecase}/")
                             if n.endswith(".json")) if r]
        return sorted(found, key=lambda r: r.get("requested_at") or "", reverse=True)

    def create(self, record: dict) -> tuple[dict, bool]:
        """(record, created). A run asked for twice (a redelivered message, a retried schedule) is held once."""
        try:
            gcs_write(self.gcp, self.bucket, name(record["usecase"], record["hold_id"]),
                      json.dumps(record, indent=2, sort_keys=True), if_generation_match=0)
            return record, True
        except PreconditionFailed:
            return self.get(record["usecase"], record["hold_id"]) or record, False

    def update(self, usecase: str, hold_id: str, change) -> dict | None:
        """change(record) -> record to write; re-read and re-applied on a lost race. None when there is no record."""
        last: Exception | None = None
        for _ in range(ATTEMPTS):
            record, generation = self._read(name(usecase, hold_id))
            if record is None:
                return None
            new = change(record)
            try:
                gcs_write(self.gcp, self.bucket, name(usecase, hold_id), json.dumps(new, indent=2, sort_keys=True),
                          if_generation_match=generation)
                return new
            except PreconditionFailed as e:
                last = e
        raise last or RuntimeError("held run update failed")
