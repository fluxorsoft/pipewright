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
