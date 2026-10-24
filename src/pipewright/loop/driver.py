"""The closed retrain loop's state machine. Every effect goes through a Backend, so the steps are tested without
Google Cloud (tests/test_loop.py)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from pipewright.errors import ConfigError
from pipewright.loop import TERMINAL, WAITING_FOR_APPROVAL

RUNNING = ("PENDING", "QUEUED", "RUNNING", "CANCELLING", "PIPELINE_STATE_PENDING", "PIPELINE_STATE_QUEUED",
           "PIPELINE_STATE_RUNNING")
SUCCEEDED = ("SUCCEEDED", "PIPELINE_STATE_SUCCEEDED")


class Backend(Protocol):
    def submit(self, env: str, body: dict) -> dict: ...
    def job_state(self, env: str, job_id: str) -> str | None: ...
    def versions(self, env: str) -> list[dict]: ...                 # {version, default, labels}
    def monitoring(self, env: str, run_id: str) -> dict | None: ...  # the run's monitoring row, or None
    def promote(self, version: str) -> dict: ...                     # test -> production, approved; {version}
    def approve(self, env: str, version: str) -> None: ...           # make it production's default
    def case(self) -> dict: ...
    def save(self, change) -> dict: ...                              # change(case) -> case, stored
    def emit(self, event_type: str, data: dict) -> None: ...
    def run_ids(self, pipeline: str, source_uri: str, run_key: str) -> tuple[str, str]: ...   # (run_id, job_id)


@dataclass
class Loop:
    """One loop: the production case and the two environments' names."""
    usecase: str
    case_id: str
    test: str
    production: str
    backend: Backend
    by: str | None = None
    log: list = field(default_factory=list)

    # ------------------------------------------------------------------ reading --

    def record(self) -> dict:
        loop = (self.backend.case() or {}).get("loop")
        if not loop:
            raise ConfigError(f"case {self.case_id} is not a closed retrain loop case: retraining.loop.enabled is off "
                              f"in {self.production}, or the case is not approved yet")
        return loop

    def _step(self, state: str, **data) -> dict:
        """Moves the loop to `state`, recording the step; emits loop.<state>."""
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        def change(case: dict) -> dict:
            loop = dict(case.get("loop") or {})
            loop.update({k: v for k, v in data.items() if not k.startswith("_")}, state=state)
            entry = {"at": now, "state": state, **({"by": self.by} if self.by else {}),
                     **({"note": data["_note"]} if data.get("_note") else {})}
            loop["history"] = [*(loop.get("history") or []), entry]
            return {**case, "loop": loop}

        case = self.backend.save(change)
        self.backend.emit(f"loop.{state}", {"case_id": self.case_id, "test": self.test,
                                            **{k: v for k, v in data.items() if not k.startswith("_")}})
        self.log.append(f"{self.case_id}: {state}" + (f" ({data['_note']})" if data.get("_note") else ""))
        return case["loop"]

    # -------------------------------------------------------------------- steps --

    def start(self, source_uri: str | None = None) -> dict:
        """Trains in test on the case's data, comparing with the version production serves."""
        loop = self.record()
        if loop.get("state") != "approved":
            return loop                                           # started already: nothing to do
        uri = source_uri or loop.get("source_uri")
        if not uri:
            raise ConfigError(f"case {self.case_id} has no training data: give --source-uri")
        champion = self._production_champion()
        run_id, job_id = self.backend.run_ids("training", uri, self.case_id)
        self.backend.submit(self.test, {"usecase": self.usecase, "pipeline": "training", "source_uri": uri,
                                        "run_key": self.case_id, "loop": self.case_id,
                                        **({"champion_version": champion["test_version"]} if champion else {})})
        return self._step("training", source_uri=uri, champion=champion,
                          training={"run_id": run_id, "job_id": job_id},
                          _note=f"training in {self.test}" + (f" against v{champion['test_version']} (production "
                                                               f"v{champion['production_version']})" if champion
                                                               else " (production has no model yet)"))

    def advance(self, approve: bool = False) -> dict:
        """One step, when the current one is done."""
        loop = self.record()
        state = loop["state"]
        if state == "approved":
            return self.start()
        if state == "training":
            return self._after_training(loop)
        if state == "candidate":
            return self._validate(loop)
        if state == "validating":
            return self._after_validation(loop)
        if state == "validated":
            if not approve:
                self.log.append(f"{self.case_id}: validated in {self.test}; promoting it needs the production "
                                "approval (`pipewright loop advance --approve`, or the CI gate)")
                return loop
            return self._promote(loop)
        if state == "verifying":
            return self._after_verification(loop)
        return loop                                               # terminal, or waiting

    def rollback(self, reason: str) -> dict:
        """Makes production's earlier default the default again (the version the loop replaced)."""
        loop = self.record()
        previous = (loop.get("promotion") or {}).get("previous_version")
        if loop.get("state") not in ("closed", "verification-failed", "verifying") or not previous:
            raise ConfigError(f"case {self.case_id}: nothing to roll back (state {loop.get('state')})")
        self.backend.approve(self.production, previous)
        return self._step("rolled-back", rolled_back_to=previous, reason=reason,
                          _note=f"production default is v{previous} again: {reason}")

    # ------------------------------------------------------------------ helpers --

    def _production_champion(self) -> dict | None:
        """The version production serves and the test version it was promoted from (D-106)."""
        default = next((v for v in self.backend.versions(self.production) if v["default"]), None)
        if default is None:
            return None
        labels = default.get("labels") or {}
        source = labels.get("source-version")
        if labels.get("promoted-from") != self.test or not source:
            raise ConfigError(f"production's default v{default['version']} was not promoted from {self.test}: the "
                              "loop compares with the model production serves, which must exist in test")
        return {"production_version": str(default["version"]), "test_version": str(source)}

    def _state(self, env: str, job_id: str) -> str:
        state = self.backend.job_state(env, job_id)
        return "MISSING" if state is None else state

    def _after_training(self, loop: dict) -> dict:
        training = loop["training"]
        state = self._state(self.test, training["job_id"])
        if state in RUNNING or state == "MISSING":
            return loop
        if state not in SUCCEEDED:
            return self._step("failed", _note=f"training run {training['run_id']} ended {state}")
        run = training["run_id"]
        found = next((v for v in self.backend.versions(self.test)
                      if (v.get("labels") or {}).get("pipewright-run") == run
                      or (v.get("labels") or {}).get("run_id") == f"run-{run}"), None)
        if found is None:
            # Gate 1 rejected it (or the champion comparison): nothing reached the registry, production untouched.
            return self._step("rejected", _note=f"run {run} registered no version: the gates or the comparison "
                                                f"with production's model rejected it")
        return self._step("candidate", candidate={"test_version": str(found["version"])},
                          _note=f"v{found['version']} in {self.test}")

    def _validate(self, loop: dict) -> dict:
        """A scoring run in test with the candidate, on the case's data (D-107)."""
        version, uri = loop["candidate"]["test_version"], loop["source_uri"]
        key = f"{self.case_id}-validate"
        run_id, job_id = self.backend.run_ids("inference", uri, key)
        self.backend.submit(self.test, {"usecase": self.usecase, "pipeline": "inference", "source_uri": uri,
                                        "run_key": key, "loop": self.case_id, "model_version": version})
        return self._step("validating", validation={"run_id": run_id, "job_id": job_id},
                          _note=f"scoring with v{version} in {self.test}")

    def _scored(self, env: str, run: dict) -> tuple[str | None, str]:
        """(outcome, note): outcome None while the run goes on, "ok" or "bad" once it ended."""
        state = self._state(env, run["job_id"])
        if state in RUNNING or state == "MISSING":
            return None, state
        if state not in SUCCEEDED:
            return "bad", f"run {run['run_id']} ended {state}"
        row = self.backend.monitoring(env, run["run_id"])
        if row is None:
            return "ok", f"run {run['run_id']} succeeded (no monitoring table: judged by the run only)"
        if row.get("alert"):
            return "bad", f"run {run['run_id']} alerted: {row.get('triggers') or 'drift'}"
        return "ok", f"run {run['run_id']} within thresholds"

    def _after_validation(self, loop: dict) -> dict:
        outcome, note = self._scored(self.test, loop["validation"])
        if outcome is None:
            return loop
        return self._step("validated" if outcome == "ok" else "validation-failed", _note=note)

    def _promote(self, loop: dict) -> dict:
        """The production approval: promote the candidate, make it the default, then a scoring run to verify it."""
        previous = next((str(v["version"]) for v in self.backend.versions(self.production) if v["default"]), None)
        promoted = self.backend.promote(loop["candidate"]["test_version"])
        version = str(promoted["version"])
        self.backend.approve(self.production, version)
        key = f"{self.case_id}-verify"
        run_id, job_id = self.backend.run_ids("inference", loop["source_uri"], key)
        self.backend.submit(self.production, {"usecase": self.usecase, "pipeline": "inference",
                                              "source_uri": loop["source_uri"], "run_key": key, "loop": self.case_id})
        return self._step("verifying", promotion={"production_version": version, "previous_version": previous,
                                                  "release_id": promoted.get("release_id")},
                          verification={"run_id": run_id, "job_id": job_id},
                          _note=f"production default v{previous} -> v{version}; verifying with a scoring run")

    def _after_verification(self, loop: dict) -> dict:
        outcome, note = self._scored(self.production, loop["verification"])
        if outcome is None:
            return loop
        if outcome == "ok":
            return self._step("closed", _note=note)
        return self._step("verification-failed", _note=note + ": `pipewright loop rollback` restores "
                                                           f"v{(loop.get('promotion') or {}).get('previous_version')}")


def needs_approval(loop: dict) -> bool:
    return loop.get("state") in WAITING_FOR_APPROVAL


def finished(loop: dict) -> bool:
    return loop.get("state") in TERMINAL
