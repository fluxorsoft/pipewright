"""A/B test records in the artifacts bucket. The CLI writes start, set, pause, resume, stop and conclude; the
launcher writes the analysis, the breaker's trips and decisions. Generation preconditions keep them from overwriting
each other; `update` re-reads and re-applies on a lost race.

gs://<artifacts>/ab-tests/<usecase>/<name>.json     one test (rewritten as it moves)
gs://<artifacts>/ab-tests/<usecase>/current.json    {"running": <name> | null, "last": <name>}

The record (written once at start, then updated):
    name, usecase, tenant, environment, mode, unit, digest (pre-registration), release (the live release at start),
    state: running | paused | decision_required | concluded | stopped,
    arms: {control: {version, endpoint?, deployed_model?, approved_files?}, treatment: {...}},
    allocation: [{at, treatment_bp, by, reason}],      the history; the last entry is current
    analysis: the newest result (aggregates only), looks: [{at, p}] for sequential tests,
    decision: {required, approvals: [{group, by, at, reason, winner}], winner, at},
    breaker: {tripped_at, arm, error_rate} when the gateway tripped it, history: [...]
"""

from __future__ import annotations

import json
from collections.abc import Callable

from pipewright.gcp import Gcp, PreconditionFailed, gcs_list, gcs_read, gcs_write

PREFIX = "ab-tests"
ATTEMPTS = 5
HISTORY_KEPT = 300
STATES = ("running", "paused", "decision_required", "concluded", "stopped")
ACTIVE = ("running", "paused", "decision_required")


def test_name(usecase: str, name: str) -> str:
    return f"{PREFIX}/{usecase}/{name}.json"


def pointer_name(usecase: str) -> str:
    return f"{PREFIX}/{usecase}/current.json"


class TestStore:
    __test__ = False                       # not a pytest class

    def __init__(self, gcp: Gcp, bucket: str):
        self.gcp, self.bucket = gcp, bucket

    def _read(self, name: str) -> tuple[dict | None, int]:
        text, generation = gcs_read(self.gcp, self.bucket, name)
        return (json.loads(text) if text else None), generation

    def pointer(self, usecase: str) -> tuple[dict, int]:
        p, generation = self._read(pointer_name(usecase))
        return p or {"running": None, "last": None}, generation

    def get(self, usecase: str, name: str) -> dict | None:
        return self._read(test_name(usecase, name))[0]

    def current(self, usecase: str) -> dict | None:
        """The active test (running, paused or waiting for a decision), or None."""
        p, _ = self.pointer(usecase)
        return self.get(usecase, p["running"]) if p.get("running") else None

    def list(self, usecase: str) -> list[dict]:
        names = [n for n in gcs_list(self.gcp, self.bucket, f"{PREFIX}/{usecase}/")
                 if n.endswith(".json") and not n.endswith("/current.json")]
        tests = [t for t in (self._read(n)[0] for n in names) if t]
        return sorted(tests, key=lambda t: t.get("started_at") or "", reverse=True)

    def usecases(self) -> list[str]:
        # ab-tests/_layers/ holds the layers' registry (FR-123), not a use case
        return sorted({n.split("/")[1] for n in gcs_list(self.gcp, self.bucket, f"{PREFIX}/")
                       if n.count("/") >= 2 and not n.split("/")[1].startswith("_")})

    def create(self, record: dict) -> dict:
        """A new test: refused when the use case already has an active one, or a test of that name exists."""
        usecase, name = record["usecase"], record["name"]
        for _ in range(ATTEMPTS):
            pointer, pgen = self.pointer(usecase)
            if pointer.get("running"):
                running = self.get(usecase, pointer["running"]) or {}
                if running.get("state") in ACTIVE:
                    raise ValueError(f"{usecase} already has a test {pointer['running']} ({running.get('state')}): "
                                     "stop or conclude it first")
            try:
                gcs_write(self.gcp, self.bucket, test_name(usecase, name), json.dumps(record, indent=2,
                          sort_keys=True), if_generation_match=0)
            except PreconditionFailed:
                raise ValueError(f"a test named {name} already ran for {usecase}: a test's name is used once "
                                 "(it salts the assignment); give the new test a new name") from None
            try:
                gcs_write(self.gcp, self.bucket, pointer_name(usecase),
                          json.dumps({"running": name, "last": name}, indent=2), if_generation_match=pgen)
                return record
            except PreconditionFailed:
                continue
        raise RuntimeError("could not record the new test")

    def update(self, usecase: str, name: str, change: Callable[[dict], dict | None]) -> dict | None:
        """change(record) -> record to write or None. When the record leaves the active states, the pointer stops
        naming it."""
        last: Exception | None = None
        for _ in range(ATTEMPTS):
            record, generation = self._read(test_name(usecase, name))
            if record is None:
                return None
            new = change(json.loads(json.dumps(record)))
            if new is None:
                return record
            new["history"] = (new.get("history") or [])[-HISTORY_KEPT:]
            try:
                gcs_write(self.gcp, self.bucket, test_name(usecase, name), json.dumps(new, indent=2, sort_keys=True),
                          if_generation_match=generation)
            except PreconditionFailed as e:
                last = e
                continue
            if new.get("state") not in ACTIVE:
                pointer, pgen = self.pointer(usecase)
                if pointer.get("running") == name:
                    try:
                        gcs_write(self.gcp, self.bucket, pointer_name(usecase),
                                  json.dumps({"running": None, "last": name}, indent=2), if_generation_match=pgen)
                    except PreconditionFailed:
                        pass                      # someone else moved it; the record's state is what counts
            return new
        raise last or RuntimeError("test update failed")
