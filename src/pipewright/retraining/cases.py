"""A retrain decision case and its transitions. Pure: callers pass the time and do the I/O.

States:
    open       waiting for a decision (new, or re-opened)
    snoozed    a person put it off until `until`; re-opens then, or at once if things get worse
    declined   a person said no until `until` (declines expire too); re-opens like a snooze
    approved   quorum reached: training was submitted (propose) or the candidate was accepted (candidate). Final.

One case is current per use case (the pointer). An approved case stops being current; the next alert after
cooldown_days opens a new one.
"""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta

ACTIONS = ("approve", "decline", "snooze")
REASONS = ("data-problem", "seasonal", "no-harm", "other")      # why a decline or snooze: --because
OBSERVATIONS_KEPT = 20
HISTORY_KEPT = 200


class DecisionRefused(Exception):
    """The decision cannot be recorded (wrong state, wrong person, missing reason). Recorded in the history."""


@dataclass(frozen=True)
class Settings:
    """A use case's retraining section (ml_config.retraining) under the environment's platform policy."""
    mode: str = "off"
    when: tuple = ("drift", "performance")
    required: tuple = ()                    # approver group names; one approval from each
    separate_from_requester: bool = True
    max_snooze_days: int = 30
    reopen_when_worse_by: float = 0.1
    escalate_after_days: int = 3
    cooldown_days: int = 7
    source_uri: str | None = None
    notify: tuple = ()
    escalate_notify: tuple = ()
    publish: bool = False
    digest: bool = True
    loop: bool = False                      # retraining.loop.enabled: the closed retrain loop trains elsewhere

    @classmethod
    def from_config(cls, retraining: dict, policy: dict | None = None) -> Settings:
        """Platform policy is applied again here, so a policy change reaches cases opened by older releases."""
        policy = policy or {}
        r, dec = retraining or {}, (retraining or {}).get("decisions") or {}
        mode = r.get("mode")
        mode = "off" if mode is False else mode or ("propose" if r.get("enabled") else "off")
        required = list(dict.fromkeys([*policy.get("required_approvers", []),
                                       *((r.get("approvals") or {}).get("required") or [])]))
        snooze = r.get("snooze") or {}
        max_days = snooze.get("max_days", 30)
        if policy.get("max_snooze_days"):
            max_days = min(max_days, policy["max_snooze_days"])
        return cls(mode=mode, when=tuple(r.get("when") or ()), required=tuple(required),
                   separate_from_requester=(r.get("approvals") or {}).get("separate_from_requester", True),
                   max_snooze_days=max_days, reopen_when_worse_by=snooze.get("reopen_when_worse_by", 0.1),
                   escalate_after_days=dec.get("escalate_after_days", 3), cooldown_days=r.get("cooldown_days", 7),
                   source_uri=r.get("source_uri"), notify=tuple(dec.get("notify") or ()),
                   escalate_notify=tuple(dec.get("escalate_notify") or ()), publish=bool(dec.get("publish")),
                   digest=dec.get("digest", True), loop=bool((r.get("loop") or {}).get("enabled")))


@dataclass
class Outcome:
    case: dict | None                                   # the case to write (None: nothing to write)
    events: list = field(default_factory=list)          # (event_type, data)
    submit: dict | None = None                          # {"kind": "retrain"|"candidate", "source_uri", "run_key"}
    status: str = ""                                    # one line for the caller (monitoring table, CLI)


def iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat()


def parse(t: str | None) -> datetime | None:
    return datetime.fromisoformat(t) if t else None


def new_case_id(usecase: str, now: datetime, seed: str) -> str:
    return f"rc-{now:%Y%m%d}-{hashlib.sha256(f'{usecase}|{seed}'.encode()).hexdigest()[:6]}"


def worst_psi(obs: dict | None) -> float | None:
    values = [v for v in ((obs or {}).get("max_feature_psi"), (obs or {}).get("prediction_psi")) if v is not None]
    return max(values) if values else None


def worsening(baseline: dict | None, obs: dict, by: float) -> str | None:
    """Why the latest observation is worse than when the case was put off, or None."""
    if not baseline:
        return None
    before, now = worst_psi(baseline), worst_psi(obs)
    if before is not None and now is not None and now >= before + by:
        return f"worst PSI rose from {before:.3f} to {now:.3f}"
    new = sorted(set(obs.get("drifted_features") or []) - set(baseline.get("drifted_features") or []))
    if new:
        return f"newly drifting: {', '.join(new)}"
    if "performance" in (obs.get("triggers") or []) and "performance" not in (baseline.get("triggers") or []):
        return "performance fell below its minimum"
    return None


def _log(case: dict, now: datetime, action: str, **fields) -> None:
    case["history"] = (case.get("history") or []) + [{"at": iso(now), "action": action, **fields}]
    case["history"] = case["history"][-HISTORY_KEPT:]
    case["updated_at"] = iso(now)


def _reopen(case: dict, now: datetime, why: str) -> None:
    case.update(state="open", opened_at=iso(now), until=None, approvals=[], escalated_at=None, baseline=None)
    _log(case, now, "reopened", reason=why)


def observe(current: dict | None, last_approved_at: str | None, obs: dict, s: Settings, now: datetime,
            ctx: dict) -> Outcome:
    """An inference run reported drift or performance (obs). ctx: usecase, environment, tenant, release,
    requested_by, run_id; also champion (version, created) when known."""
    fired = sorted(set(obs.get("triggers") or []) & set(s.when))
    if s.mode == "off":
        return Outcome(None, status="no case: retraining.mode is off")
    if not fired:
        return Outcome(None, status="no case: nothing in retraining.when fired")
    obs = {**obs, "at": obs.get("at") or iso(now)}

    if current is None:
        last = parse(last_approved_at)
        if last and now - last < timedelta(days=s.cooldown_days):
            return Outcome(None, status=f"no case: cooldown ({s.cooldown_days} days) after the retrain approved "
                                        f"{last_approved_at}")
        cid = new_case_id(ctx["usecase"], now, ctx.get("run_id") or iso(now))
        case = {
            "case_id": cid, "usecase": ctx["usecase"], "environment": ctx["environment"], "tenant": ctx["tenant"],
            "release": ctx.get("release"), "mode": s.mode, "state": "open", "created_at": iso(now),
            "opened_at": iso(now), "updated_at": iso(now), "requested_by": ctx.get("requested_by"),
            "triggers": fired, "required": list(s.required), "source_uri": s.source_uri,
            "champion": ctx.get("champion") or {}, "observations": [obs], "approvals": [], "until": None,
            "baseline": None, "escalated_at": None, "training": None, "history": [],
        }
        _log(case, now, "proposed", by=ctx.get("requested_by"), run_id=ctx.get("run_id"), triggers=fired)
        submit = None
        if s.mode == "candidate":
            submit = {"kind": "candidate", "source_uri": s.source_uri, "run_key": f"{cid}-candidate"}
        return Outcome(case, [("retrain.proposed", {"triggers": fired})], submit, f"case {cid} opened ({s.mode})")

    case = copy.deepcopy(current)
    case["observations"] = ((case.get("observations") or []) + [obs])[-OBSERVATIONS_KEPT:]
    case["triggers"] = sorted(set(case.get("triggers") or []) | set(fired))
    case["updated_at"] = iso(now)
    cid = case["case_id"]
    if case["state"] in ("snoozed", "declined"):
        why = worsening(case.get("baseline"), obs, s.reopen_when_worse_by)
        if why:
            was = case["state"]
            _reopen(case, now, f"worse while {was}: {why}")
            return Outcome(case, [("retrain.reopened", {"reason": f"worse while {was}: {why}"})],
                           status=f"case {cid} re-opened: {why}")
        return Outcome(case, status=f"case {cid} {case['state']} until {case['until']}")
    return Outcome(case, status=f"case {cid} open: observation added")


def approved_groups(case: dict) -> list[str]:
    return sorted({a["group"] for a in case.get("approvals") or [] if a.get("group")})


def quorum(case: dict) -> bool:
    approvals = case.get("approvals") or []
    required = case.get("required") or []
    if not required:
        return bool(approvals)
    return set(required) <= set(approved_groups(case))


def _who(by: str, group: str | None, entry: dict) -> dict:
    return {"by": by, "group": group, "reason": entry["reason"], "approval": approved_how(entry)}


def approved_how(entry: dict) -> dict:
    """How a decision was approved, as its client recorded it (FR-56); a client older than 1.1 sends nothing."""
    return entry.get("approval") or {"via": "not recorded by the client"}


def decide(case: dict, action: str, by: str, group: str | None, s: Settings, now: datetime, reason: str = "",
           because: str | None = None, days: int | None = None, source_uri: str | None = None,
           via: str = "", approval: dict | None = None) -> Outcome:
    """A person's decision. group: the approver group whose topic carried it (None: a direct call). Raises
    DecisionRefused with the reason it cannot be recorded."""
    if action not in ACTIONS:
        raise DecisionRefused(f"unknown action {action!r}")
    if not by:
        raise DecisionRefused("the decision has no verified identity")
    if case["state"] == "approved":
        raise DecisionRefused(f"case {case['case_id']} is already approved")
    if not (reason or "").strip():
        raise DecisionRefused("give a reason (--reason)")
    required = case.get("required") or list(s.required)
    case = copy.deepcopy(case)
    case["required"] = required
    if required and group not in required:
        where = f"approver group {group!r}" if group else "a direct call"
        raise DecisionRefused(f"{where} cannot decide this case: decisions come from a required approver group "
                              f"({', '.join(required)})")
    if s.separate_from_requester and by == case.get("requested_by"):
        raise DecisionRefused(f"{by} proposed this case and cannot decide it (approvals.separate_from_requester)")
    cid = case["case_id"]
    entry = {"by": by, "group": group, "reason": reason.strip(), "via": via,
             **({"approval": approval} if approval else {})}

    if action == "approve":
        for a in case.get("approvals") or []:
            if a["by"] == by:
                raise DecisionRefused(f"{by} already approved this case (for {a.get('group') or 'it'}); one identity "
                                      "counts once")
            if group and a.get("group") == group:
                raise DecisionRefused(f"approver group {group} already approved (by {a['by']})")
        case["approvals"] = (case.get("approvals") or []) + [{**entry, "at": iso(now), "source_uri": source_uri}]
        if case["state"] != "open":
            case.update(state="open", until=None, baseline=None)
        _log(case, now, "approve", **entry, source_uri=source_uri)
        if not quorum(case):
            waiting = sorted(set(required) - set(approved_groups(case)))
            return Outcome(case, [("retrain.approval", {**_who(by, group, entry),
                                                        "waiting_for": waiting})],
                           status=f"case {cid}: approval recorded; waiting for {', '.join(waiting)}")
        case["state"] = "approved"
        case["approved_at"] = iso(now)
        data = {**_who(by, group, entry), "approvers": [a["by"] for a in case["approvals"]]}
        if case.get("mode") == "candidate":
            return Outcome(case, [("retrain.approved", {**data, "next": "promote the candidate with "
                                                                        "`pipewright model approve`"})],
                           status=f"case {cid} approved: the candidate stays unpromoted until `pipewright model "
                                  "approve`")
        chosen = next((a["source_uri"] for a in reversed(case["approvals"]) if a.get("source_uri")), None)
        uri = chosen or case.get("source_uri") or s.source_uri
        if not uri:
            raise DecisionRefused("no training data: give --source-uri (retraining.source_uri is not set)")
        if s.loop:
            # The closed retrain loop (D-104): production never trains. The case waits for `pipewright loop start`,
            # which trains in the environment this one's releases come from.
            case["loop"] = {"state": "approved", "source_uri": uri, "history": [{"at": iso(now), "state": "approved",
                                                                              "by": by}]}
            return Outcome(case, [("retrain.approved", {**data, "source_uri": uri, "loop": True})], None,
                           status=f"case {cid} approved: the closed retrain loop trains it where releases come from "
                                  f"(`pipewright loop start --case {cid}`)")
        submit = {"kind": "retrain", "source_uri": uri, "run_key": cid}
        return Outcome(case, [("retrain.approved", {**data, "source_uri": uri})], submit,
                       status=f"case {cid} approved: training submitted on {uri}")

    if because not in REASONS:
        raise DecisionRefused(f"say why with --because ({', '.join(REASONS)})")
    limit = s.max_snooze_days
    days = days if days is not None else (limit if action == "decline" else None)
    if days is None:
        raise DecisionRefused("snooze needs --days")
    if not 1 <= days <= limit:
        raise DecisionRefused(f"--days must be between 1 and {limit} (snooze.max_days or the platform limit)")
    until = iso(now + timedelta(days=days))
    state = "declined" if action == "decline" else "snoozed"
    latest = (case.get("observations") or [{}])[-1]
    case.update(state=state, until=until, approvals=[], escalated_at=None,
                baseline={k: latest.get(k) for k in ("max_feature_psi", "prediction_psi", "drifted_features",
                                                     "triggers")})
    _log(case, now, action, **entry, because=because, days=days, until=until)
    return Outcome(case, [(f"retrain.{state}", {**_who(by, group, entry),
                                                "because": because, "until": until})],
                   status=f"case {cid} {state} until {until}")


def tick(case: dict, s: Settings, now: datetime) -> Outcome:
    """Time passing: expiry of snoozes and declines, escalation of undecided cases."""
    cid = case["case_id"]
    if case["state"] in ("snoozed", "declined") and case.get("until") and now >= parse(case["until"]):
        case = copy.deepcopy(case)
        was = case["state"]
        _reopen(case, now, f"{was} expired")
        return Outcome(case, [("retrain.reopened", {"reason": f"{was} expired"})], status=f"case {cid} re-opened")
    if (case["state"] == "open" and not case.get("escalated_at")
            and now - parse(case["opened_at"]) >= timedelta(days=s.escalate_after_days)):
        case = copy.deepcopy(case)
        case["escalated_at"] = iso(now)
        _log(case, now, "escalated", reason=f"undecided for {s.escalate_after_days} days")
        return Outcome(case, [("retrain.escalated", {"reason": f"undecided for {s.escalate_after_days} days",
                                                     "notify": list(s.escalate_notify)})],
                       status=f"case {cid} escalated")
    return Outcome(None, status=f"case {cid}: nothing due")
