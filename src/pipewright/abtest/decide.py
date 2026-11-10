"""Recording an approver group's A/B decision (the launcher, from the group's topic: membership is publish access).

One approval per required group. When every required group has approved the same winner, the decision is recorded
(`decision.winner`); groups that disagree leave it open and say so, so nobody's choice is overwritten. A person may
not approve twice for different groups (the same rule as retrain decisions: one identity, one voice).
"""

from __future__ import annotations

from pipewright.abtest.store import ACTIVE

SHADOW = ("a shadow test shows how the candidate behaves on live calls, not that it is better for the business: "
          "conclude it with the control, then start an A/B test (or deploy the candidate as usual)")


class Refused(Exception):
    pass


def record(test: dict, group: str, by: str, winner: str, reason: str, at: str, approval: dict | None = None) -> dict:
    """The test with the approval added (and the winner when every group agrees). Raises Refused."""
    if test.get("state") not in ACTIVE:
        raise Refused(f"test {test['name']} is {test.get('state')}: nothing to decide")
    if winner not in ("treatment", "control"):
        raise Refused("winner must be treatment or control")
    if winner == "treatment" and test.get("mode") == "shadow":
        raise Refused(SHADOW)
    if not (reason or "").strip():
        raise Refused("a reason is required")
    d = test.setdefault("decision", {"required": [], "approvals": []})
    if group not in d.get("required", []):
        raise Refused(f"group {group} is not required by test {test['name']} ({', '.join(d.get('required', []))})")
    if any(a["group"] == group for a in d["approvals"]):
        raise Refused(f"group {group} has already decided test {test['name']}")
    if any(a["by"] == by for a in d["approvals"]):
        raise Refused(f"{by} has already decided for another group")
    d["approvals"].append({"group": group, "by": by, "winner": winner, "reason": reason.strip(), "at": at,
                           **({"approval": approval} if approval else {})})
    chosen = {a["winner"] for a in d["approvals"]}
    if len(d["approvals"]) == len(d["required"]) and len(chosen) == 1:
        d["winner"] = winner
        d["decided_at"] = at
    elif len(chosen) > 1:
        d["disagreement"] = sorted(chosen)
    test["history"] = (test.get("history") or []) + [{"at": at, "action": "decide", "by": by, "group": group,
                                                       "winner": winner, "reason": reason.strip()}]
    return test


# ------------------------------------------------------------------------------------------ what comes next --
def next_step(test: dict, default_version: str | None = None, served: list[str] | None = None) -> dict:
    """FR-121: what the test needs now, in one sentence, with the command that does it: the same rule for `ab status`
    and Fleet View's experiment page. default_version and served (the versions the endpoints serve now), where the
    caller knows them, say whether a winning treatment is serving yet.

    Returns {"action": a short key, "text": the sentence, "command": a pipewright command or None}."""
    from datetime import datetime, timedelta

    uc, env = test.get("usecase"), test.get("environment")
    where = f"--env {env} --usecase {uc}"
    state, res = test.get("state"), test.get("analysis") or {}
    trt = str(((test.get("arms") or {}).get("treatment") or {}).get("version") or "")
    ctl = str(((test.get("arms") or {}).get("control") or {}).get("version") or "")
    d = test.get("decision") or {}
    if state == "stopped":
        return {"action": "none", "text": f"stopped: v{ctl} serves everyone and the treatment's arm is gone",
                "command": None}
    if state == "concluded":
        if d.get("winner") != "treatment":
            return {"action": "none", "text": f"concluded for the control: v{ctl} stays the default", "command": None}
        if default_version is not None and str(default_version) != trt:
            return {"action": "check", "text": f"concluded for v{trt}, but v{default_version} is the default now "
                    "(a newer approval since)", "command": None}
        if served is not None and trt not in [str(s) for s in served]:
            return {"action": "deploy", "text": f"v{trt} won and is the default; the endpoint still serves "
                    f"{', '.join('v' + str(s) for s in served) or 'nothing'}: deploy it",
                    "command": f"pipewright deploy-model {where}"}
        return {"action": "none", "text": f"v{trt} won and is the default"
                + (", and the endpoint serves it" if served is not None else ""), "command": None}
    if state == "paused":
        return {"action": "resume", "text": "paused: every unit gets the control until it is resumed or stopped",
                "command": f"pipewright ab resume {where} --reason <why>"}
    if not res:
        return {"action": "analyse", "text": "not analysed yet (the scheduled analysis runs it, or by hand)",
                "command": f"pipewright ab analyse {where}"}
    if res.get("status") == "blocked":
        first = (res.get("blocked") or ["a share check failed"])[0]
        return {"action": "fix", "text": f"blocked, no decision can rest on it: {first}; fix the cause, or stop "
                "the test", "command": f"pipewright ab stop {where} --reason <why>"}
    if state == "decision_required" or res.get("status") == "decision_required":
        p = res.get("primary") or {}
        winner = "treatment" if p.get("treatment_better") else "control"
        why = "; ".join(res.get("decision_reasons") or []) or "a decision is due"
        missing = [g for g in d.get("required") or [] if g not in {a["group"] for a in d.get("approvals") or []}]
        if d.get("disagreement") and not d.get("winner"):
            chose = "; ".join(f"{a['group']} chose the {a['winner']}" for a in d.get("approvals") or [])
            return {"action": "stop", "text": f"the approver groups disagree ({chose}): each group decides once "
                    "and nobody's choice is overwritten, so no winner can be recorded; stop the test, which keeps "
                    "the control",
                    "command": f"pipewright ab stop {where} --reason <why>"}
        if missing and not d.get("winner"):
            return {"action": "decide", "text": f"{why}; the analysis supports the {winner}; waiting for "
                    f"{', '.join(missing)}",
                    "command": f"pipewright ab decide {where} --winner {winner} --group {missing[0]} --reason <why>"}
        chosen = d.get("winner") or winner
        return {"action": "conclude", "text": f"{why}; conclude with the {chosen}",
                "command": f"pipewright ab conclude {where} --winner {chosen} --reason <why>"}
    # continue: how much longer
    plan, matured = res.get("plan") or {}, res.get("matured") or {}
    bits = []
    if plan.get("needed"):
        more = max(int(plan["needed"].get(a, 0)) - int(matured.get(a, 0) or 0) for a in ("control", "treatment"))
        if more > 0:
            bits.append(f"about {more:,} more units with an outcome in the larger arm")
    dur = res.get("duration") or {}
    started = test.get("started_at")
    if dur.get("max_days") and started:
        end = datetime.fromisoformat(started) + timedelta(days=int(dur["max_days"]))
        bits.append(f"or until max_days on {end.date().isoformat()}")
    if dur.get("min_days") and res.get("age_days") is not None and res["age_days"] < dur["min_days"]:
        bits.insert(0, f"at least {dur['min_days'] - res['age_days']:.0f} more day(s) to min_days")
    return {"action": "continue", "text": "keep it running: " + (", ".join(bits) if bits else "no decision yet"),
            "command": None}
