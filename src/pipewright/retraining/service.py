"""What the launcher does with retrain proposals, decisions and ticks. Clients are passed in, so it is testable.

    submit(contract, pipeline, source_uri, run_key, trigger) -> {"status", "job_id", ...}   the launcher's start()
    emit(event_type, contract, data, attributes)                                             lifecycle events
    verify(token) -> email                                                                   identity.verify
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from pipewright.release.contract import ReleaseContract
from pipewright.retraining import cases as rc
from pipewright.retraining.store import CaseStore
from pipewright.retraining.summary import SUMMARY_LIMIT, summary

log = logging.getLogger("pipewright.retraining")


def ml_config(contract: ReleaseContract) -> dict:
    """The use case's resolved ML sections, as released (static parameter ml_config of a pipeline)."""
    for name in ("inference", "training"):
        if name not in contract.pipeline_names:
            continue
        value = contract.pipeline(name).static_parameters.get("ml_config")
        if isinstance(value, str):
            value = json.loads(value)
        if value:
            return value
    return {}


class Decisions:
    def __init__(self, store: CaseStore, manifest: Callable[[], dict], submit: Callable, emit: Callable,
                 verify: Callable[[str], str] | None = None, clock: Callable[[], datetime] | None = None,
                 champion_info: Callable[[ReleaseContract, dict], dict] | None = None):
        self.store, self.manifest, self.submit, self._emit = store, manifest, submit, emit
        self.verify = verify
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.champion_info = champion_info

    # ------------------------------------------------------------------------------------------------ helpers --
    def settings(self, contract: ReleaseContract) -> rc.Settings:
        policy = ((self.manifest().get("policies") or {}).get("retraining")) or {}
        return rc.Settings.from_config(ml_config(contract).get("retraining") or {}, policy)

    def emit(self, contract: ReleaseContract, s: rc.Settings, case: dict, event_type: str, data: dict) -> None:
        payload = {"case_id": case["case_id"], "state": case["state"], "mode": case.get("mode"),
                   "triggers": ",".join(case.get("triggers") or []), "summary": summary(case, event_type), **data}
        attributes = {"case_id": case["case_id"]}
        if s.publish:
            attributes["decision_feed"] = "true"      # the tenant's decision feed subscription filters on this
        self._emit(event_type, contract, payload, attributes)

    def _submit(self, contract: ReleaseContract, case: dict, submit: dict) -> dict:
        try:
            result = self.submit(contract, "training", submit["source_uri"], submit["run_key"],
                                 "retrain" if submit["kind"] == "retrain" else "candidate")
            training = {"kind": submit["kind"], "status": result.get("status"), "job_id": result.get("job_id"),
                        "source_uri": submit["source_uri"], "run_key": submit["run_key"],
                        "submitted_at": rc.iso(self.clock())}
        except Exception as e:  # noqa: BLE001 - recorded on the case; a person can retry with `retrain approve`
            log.exception("training for case %s not submitted", case["case_id"])
            training = {"kind": submit["kind"], "status": "error", "error": f"{type(e).__name__}: {e}"[:300],
                        "source_uri": submit["source_uri"], "run_key": submit["run_key"]}

        def record(c: dict) -> dict:
            return {**c, "training": training}

        return self.store.update_case(case["usecase"], case["case_id"], record) or {**case, "training": training}

    # ----------------------------------------------------------------------------------------------- proposal --
    def propose(self, contract: ReleaseContract, body: dict) -> dict:
        """An inference run's observation (body: run_id, observation, champion)."""
        s, now = self.settings(contract), self.clock()
        obs = {**(body.get("observation") or {}), "run_id": body.get("run_id")}
        champion = dict(body.get("champion") or {})
        if champion and self.champion_info and not champion.get("created"):
            try:
                champion.update(self.champion_info(contract, champion))
            except Exception as e:  # noqa: BLE001 - the pack is still useful without it
                log.warning("champion details unavailable: %s", e)
        ctx = {"usecase": contract.usecase, "environment": contract.environment, "tenant": contract.tenant,
               "release": contract.release_id, "requested_by": contract.runner_service_account,
               "run_id": body.get("run_id"), "champion": champion}
        held: dict = {}

        def change(current, pointer):
            if current and any(o.get("run_id") == obs["run_id"] for o in current.get("observations") or []):
                held["out"] = rc.Outcome(None, status=f"case {current['case_id']}: run already recorded")
                return None, None
            out = rc.observe(current, pointer.get("last_approved_at"), obs, s, now, ctx)
            held["out"] = out
            if out.case is None:
                return None, None
            return out.case, {**pointer, "open": out.case["case_id"], "last_case": out.case["case_id"]}

        self.store.update(contract.usecase, change)
        out: rc.Outcome = held["out"]
        case = out.case
        if case is None:
            return {"status": "no-case", "detail": out.status}
        for event_type, data in out.events:
            self.emit(contract, s, case, event_type, data)
        if out.submit:
            case = self._submit(contract, case, out.submit)
        return {"status": "recorded", "case_id": case["case_id"], "state": case["state"], "detail": out.status}

    # ----------------------------------------------------------------------------------------------- decision --
    def decide(self, contract: ReleaseContract, message: dict, group: str | None, via: str,
               by: str | None = None) -> dict:
        """A person's decision. by: an identity the caller already verified; otherwise message.id_token is."""
        s, now = self.settings(contract), self.clock()
        case_id = message.get("case_id")
        if not case_id:
            return {"status": "refused", "reason": "case_id is required"}
        if by is None:
            try:
                by = self.verify(message.get("id_token") or "") if self.verify else None
            except Exception as e:  # noqa: BLE001
                by, why = None, str(e)
            else:
                why = "" if by else "no identity"
            if not by:
                self._refuse(contract.usecase, case_id, now, None, group, message, why)
                return {"status": "refused", "case_id": case_id, "reason": why}
        held: dict = {}

        def change(case):
            try:
                held["out"] = rc.decide(case, message.get("action", ""), by, group, s, now,
                                        reason=message.get("reason") or "", because=message.get("because"),
                                        days=message.get("days"), source_uri=message.get("source_uri"), via=via)
                return held["out"].case
            except rc.DecisionRefused as e:
                held["refused"] = str(e)
                return self._refused_entry(case, now, by, group, message, str(e))

        case = self.store.update_case(contract.usecase, case_id, change)
        if case is None:
            return {"status": "refused", "case_id": case_id, "reason": f"no case {case_id} for {contract.usecase}"}
        if "refused" in held:
            log.warning("decision on %s by %s refused: %s", case_id, by, held["refused"])
            return {"status": "refused", "case_id": case_id, "by": by, "reason": held["refused"]}
        out: rc.Outcome = held["out"]
        if out.case["state"] == "approved":
            approved_at = out.case.get("approved_at")

            def close(current, pointer):
                if pointer.get("open") != case_id:
                    return None, None
                return None, {**pointer, "open": None, "last_approved_at": approved_at}

            self.store.update(contract.usecase, close)
        elif out.case["state"] in ("open", "snoozed", "declined"):
            def reopen(current, pointer):      # a decision on an older case makes it current again
                if pointer.get("open") in (None, case_id):
                    return None, {**pointer, "open": case_id}
                return None, None

            self.store.update(contract.usecase, reopen)
        for event_type, data in out.events:
            self.emit(contract, s, out.case, event_type, data)
        case = out.case
        if out.submit:
            case = self._submit(contract, case, out.submit)
        return {"status": "recorded", "case_id": case_id, "state": case["state"], "by": by, "detail": out.status,
                "training": case.get("training")}

    @staticmethod
    def _refused_entry(case: dict, now: datetime, by: str | None, group: str | None, message: dict,
                       why: str) -> dict:
        case = dict(case)
        case["history"] = ((case.get("history") or []) + [{
            "at": rc.iso(now), "action": "refused", "by": by, "group": group,
            "reason": f"{message.get('action')}: {why}"[:300]}])[-rc.HISTORY_KEPT:]
        return case

    def _refuse(self, usecase: str, case_id: str, now: datetime, by, group, message: dict, why: str) -> None:
        try:
            self.store.update_case(usecase, case_id, lambda c: self._refused_entry(c, now, by, group, message, why))
        except Exception as e:  # noqa: BLE001
            log.warning("could not record a refused decision on %s: %s", case_id, e)

    # ------------------------------------------------------------------------------------------------- tick --
    def tick(self, contracts: list[ReleaseContract], digest: bool = False) -> list[dict]:
        now, results = self.clock(), []
        for contract in contracts:
            s = self.settings(contract)
            if s.mode == "off":
                continue
            try:
                current = self.store.current(contract.usecase)
                if current:
                    results.append(self._tick_case(contract, s, current["case_id"], now))
                if digest and s.digest:
                    results.append(self.digest(contract, s, now))
            except Exception as e:  # noqa: BLE001 - one use case must not stop the others
                log.exception("tick failed for %s", contract.usecase)
                results.append({"usecase": contract.usecase, "error": str(e)[:300]})
        return results

    def _tick_case(self, contract: ReleaseContract, s: rc.Settings, case_id: str, now: datetime) -> dict:
        held: dict = {}

        def change(case):
            held["out"] = rc.tick(case, s, now)
            return held["out"].case

        case = self.store.update_case(contract.usecase, case_id, change)
        out = held.get("out")
        if out and out.case:
            for event_type, data in out.events:
                self.emit(contract, s, case, event_type, data)
        return {"usecase": contract.usecase, "case_id": case_id, "detail": out.status if out else "gone"}

    def digest(self, contract: ReleaseContract, s: rc.Settings, now: datetime) -> dict:
        week = now - timedelta(days=7)
        cases = [c for c in self.store.list(contract.usecase)
                 if c["state"] != "approved" or (rc.parse(c.get("approved_at")) or now) >= week]
        lines = [f"{contract.usecase} ({contract.environment}), week to {now:%Y-%m-%d}:"]
        for c in cases[:8]:
            if c["state"] == "open":
                waiting = sorted(set(c.get("required") or []) - set(rc.approved_groups(c)))
                days = (now - rc.parse(c["opened_at"])).days
                lines.append(f"- {c['case_id']} open {days} days" + (f", waiting for {', '.join(waiting)}"
                                                                     if waiting else ""))
            elif c["state"] in ("snoozed", "declined"):
                soon = " (expires within 2 days)" if rc.parse(c["until"]) - now <= timedelta(days=2) else ""
                lines.append(f"- {c['case_id']} {c['state']} until {c['until'][:10]}{soon}")
            else:
                lines.append(f"- {c['case_id']} approved {c.get('approved_at', '')[:10]} by "
                             f"{', '.join(a['by'] for a in c.get('approvals') or [])}")
        if len(lines) == 1:
            lines.append("- no open cases, no decisions this week")
        text = "\n".join(lines)[:SUMMARY_LIMIT]
        open_n = sum(c["state"] == "open" for c in cases)
        self._emit("retrain.digest", contract, {"summary": text, "open": open_n, "cases": len(cases)},
                   {"decision_feed": "true"} if s.publish else {})
        return {"usecase": contract.usecase, "digest": len(cases)}
