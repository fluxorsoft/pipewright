"""One question to Pipewright Assist, from start to finish.

    switches -> screen the question -> what kind of question -> (incident) the model reads through the tools and
    says which run it is about -> the answer is composed here, from the recorded facts -> screen the answer -> record

The model never writes the cause, the owner or the advice. Those lines are composed from what Pipewright recorded
with the run and from the fixed list of fixes, so they cannot differ from the run.failed event. The model's part is
finding the run the person means and a short explanation in plain words; an explanation that names a role, an email
address or a link is dropped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipewright.assist import fixes, replies
from pipewright.assist.tools import WITHHELD, Tools

FOOTER = "Generated advice (preview). Check it before you act: Assist changes nothing and approves nothing."
NOT_SCREENED = "Not screened by Model Armor."
_UNSAFE_EXPLANATION = re.compile(r"roles/|https?://|[\w.+-]+@[\w-]+\.[\w.-]+|\bgrant\b", re.IGNORECASE)
_STATE_WORDS = {"succeeded": "succeeded", "running": "is still running", "pending": "has not started yet",
                "cancelled": "was cancelled", "cancelling": "is being cancelled", "paused": "is paused"}


@dataclass
class Answer:
    text: str
    kind: str                                   # incident | later | action | unrelated | refused | off
    usecase: str | None = None
    run_id: str | None = None
    rule: str | None = None
    owner: str | None = None
    tool_calls: list = field(default_factory=list)
    model_calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    screened: bool = False
    withheld: list = field(default_factory=list)
    refused: str | None = None

    def to_record(self) -> dict:
        return {"kind": self.kind, "usecase": self.usecase, "run_id": self.run_id, "rule": self.rule,
                "owner": self.owner, "tools": [c["tool"] for c in self.tool_calls], "model_calls": self.model_calls,
                "tokens_in": self.tokens_in, "tokens_out": self.tokens_out, "screened": self.screened,
                "withheld": self.withheld, "refused": self.refused, "answer": self.text}


class Engine:
    def __init__(self, brain, tools: Tools, env_name: str, screen=None, usecase: str | None = None):
        self.brain = brain
        self.tools = tools
        self.env_name = env_name
        self.screen = screen
        self.usecase = usecase                  # the use case the person named, if any

    # ---------------------------------------------------------------------------------------------------
    def ask(self, question: str) -> Answer:
        screened = bool(self.screen is not None and self.screen.on)
        if screened:
            problem = self.screen.screen("question", question)
            if problem:
                return Answer(f"Your question was not sent to the model: {problem}. Ask it in other words.",
                              "refused", screened=True, refused=problem)
        kind, usage = self.brain.classify(question)
        a = Answer("", kind if kind in replies.KINDS else replies.UNRELATED, screened=screened, **usage)
        if a.kind != replies.INCIDENT:
            a.text = replies.reply(a.kind, question, self.env_name)
            return a
        found, usage = self.brain.investigate(question, self._context(), self.tools)
        a.model_calls += usage["model_calls"]
        a.tokens_in += usage["tokens_in"]
        a.tokens_out += usage["tokens_out"]
        a.tool_calls, a.withheld = list(self.tools.calls), list(dict.fromkeys(self.tools.withheld))
        body = self._compose(found or {}, a, question)
        if usage.get("ceiling") or self.tools.ceiling_reached:
            body += ("\n\nAssist stopped at its ceiling for one question "
                     f"({self.tools.max_calls} tool calls). Ask about one run at a time.")
        if a.withheld:
            body += ("\n\nNote: " + ", ".join(a.withheld) + " looked like an instruction, not a fact. Assist "
                     "ignored it and left it out.")
        a.text = body + "\n\n" + self._footer(a)
        if screened:
            problem = self.screen.screen("answer", a.text)
            if problem:
                return Answer(f"Assist's answer was withheld: {problem}. The facts are in "
                              f"`pipewright status --env {self.env_name}` and the run.failed event.", "refused",
                              a.usecase, a.run_id, a.rule, a.owner, a.tool_calls, a.model_calls, a.tokens_in,
                              a.tokens_out, True, a.withheld, problem)
        return a

    def _context(self) -> str:
        who = (f"The person asks for team {self.tools.tenant}." if self.tools.tenant
               else "The person asks as the platform team, about the whole environment.")
        named = f" They named the use case {self.usecase}." if self.usecase else ""
        return f"Environment: {self.env_name}. {who}{named}"

    def _footer(self, a: Answer) -> str:
        used = (f"{len(a.tool_calls)} tool call{'s' if len(a.tool_calls) != 1 else ''}, "
                f"{a.tokens_in + a.tokens_out:,} tokens")
        return f"({FOOTER} {used}.{'' if a.screened else ' ' + NOT_SCREENED})"

    # -- composing: recorded facts first, the model's words second ------------------------------------------
    def _named_in(self, question: str, usecase: str | None) -> tuple:
        """A run ID the person typed, among the runs the tools returned: no model needed to know which run it is."""
        for rid in re.findall(r"\br[0-9a-f]{8,}\b", question):
            for (u, r), run in self.tools.seen.items():
                if r == rid and (not usecase or u == usecase):
                    return u, rid, run
        return usecase, None, None

    def _compose(self, found: dict, a: Answer, question: str = "") -> str:
        if found.get("kind") == "summary" and self.tools.failures_seen is not None:
            return self._compose_summary(_explanation(found))
        usecase, run_id = found.get("usecase") or self.usecase, found.get("run_id")
        run = self.tools.seen.get((usecase, str(run_id))) if usecase and run_id else None
        if run is None:                       # the model's reply did not name a run the tools returned
            usecase, run_id, run = self._named_in(question, usecase)
        if run is None:
            return self._not_found(usecase)
        a.usecase, a.run_id = usecase, str(run_id)
        return self._compose_run(usecase, run, _explanation(found), a)

    def _compose_run(self, usecase: str, run: dict, explanation: str | None, a: Answer) -> str:
        name = f"The {run.get('pipeline') or 'pipeline'} run {run.get('run_id')} of {usecase}"
        state = run.get("state")
        cause = run.get("cause") if isinstance(run.get("cause"), dict) else None
        if state not in ("failed", "not_submitted", "cancelled"):
            a.owner = a.rule = None
            return (f"{name} {_STATE_WORDS.get(state, 'is ' + str(state))}. It did not fail, so there is no cause "
                    "to report." + (f"\n{explanation}" if explanation else ""))
        steps = ", ".join(run.get("failed_steps") or []) or (cause or {}).get("step")
        head = (f"{name} was not submitted: the orchestrator refused it." if state == "not_submitted"
                else f"{name} was cancelled." if state == "cancelled" and not cause
                else f"{name} failed" + (f" at {steps}." if steps else "."))
        if state == "cancelled" and not cause:
            cause = {"rule": "cancelled", "owner": fixes.UNCLEAR, "summary": "the run was cancelled"}
        lines = [head]
        if cause:
            a.rule, a.owner = cause.get("rule"), cause.get("owner") or fixes.UNCLEAR
            lines.append("Cause: " + _reason(cause))
            code = ((cause.get("record") or {}).get("code") or {})
            if code.get("code") and code["code"] != WITHHELD:
                where = "" if str(code.get("file")) in lines[1] else f"{code.get('file')}, line {code.get('line')}:  "
                lines.append(f"    {where}{code['code']}")
            for check in ((cause.get("record") or {}).get("checks") or [])[:5]:
                if check != WITHHELD and check not in lines[1]:
                    lines.append(f"    check: {check}")
            rule = "refused_submission" if state == "not_submitted" and a.rule == "permission" else a.rule
            fix = fixes.fix(rule, (cause.get("step") or (run.get("failed_steps") or [None])[0]))
        else:
            a.rule, a.owner = None, fixes.UNCLEAR
            error = run.get("error")
            lines.append("Cause: not recorded." + (f" The orchestrator said only: {error}"
                                                     if error and error != WITHHELD else ""))
            fix = fixes.fix(None)
        if explanation:
            lines.append(explanation)
        lines.append(f"Owner: {self._owner(a.owner)}.")
        lines.append(f"Next: {fix['what']}")
        lines.append(f"Where: {fix['where']}")
        return "\n".join(lines)

    def _owner(self, owner: str) -> str:
        """The owner in the asker's words: a team member reads "your team", a platform person "the team"."""
        if self.tools.platform:
            return {fixes.TEAM: "the use case's team", fixes.PLATFORM: "the platform team (yours)",
                    fixes.UNCLEAR: "unclear"}[owner]
        return fixes.OWNER_WORDS[owner]

    def _compose_summary(self, explanation: str | None) -> str:
        found, blocked = self.tools.failures_seen or [], self.tools.blocked_seen or []
        scope = f"team {self.tools.tenant}" if self.tools.tenant and not self.tools.platform else self.env_name
        if not found:
            return f"No failed or refused runs are recorded for {scope} in the last 7 days."
        n = {o: sum(1 for f in found if (f.get("owner") or fixes.UNCLEAR) == o)
             for o in (fixes.TEAM, fixes.PLATFORM, fixes.UNCLEAR)}
        lines = [f"Failed or refused runs in the last 7 days ({scope}): {len(found)}.",
                 f"By owner: {n[fixes.TEAM]} team, {n[fixes.PLATFORM]} platform, {n[fixes.UNCLEAR]} unclear."]
        platform = [f for f in found if f.get("owner") == fixes.PLATFORM]
        if platform:
            lines.append("Platform causes:")
            lines += [f"  {f.get('usecase')} {f.get('pipeline') or ''}: {f.get('rule')}"
                      + (f" ({f['why']})" if f.get("why") and f["why"] != WITHHELD else "") for f in platform[:10]]
        if blocked:
            lines.append("Blocked now (the latest run failed or was refused, and none has succeeded since):")
            lines += [f"  {b['usecase']} {b.get('pipeline') or ''}: since {str(b.get('since'))[:16]}, on "
                      f"{self._owner(b['on'])}" + (f" ({b['rule']})" if b.get("rule") else "")
                      for b in blocked[:15]]
        else:
            lines.append("Blocked now: none. Every pipeline that failed has succeeded since.")
        if explanation:
            lines.append(explanation)
        return "\n".join(lines)

    def _not_found(self, usecase: str | None) -> str:
        if self.tools.refusals:
            return "Assist could not read that: " + "; ".join(dict.fromkeys(self.tools.refusals)) + "."
        runs = [r for (u, _), r in self.tools.seen.items() if u == usecase]
        if not runs:
            return (f"Assist found no runs{' of ' + usecase if usecase else ''} to answer from. Name the use case "
                    "(--usecase) and the run, for example: \"why did training run r1a2b3c4d5e6f fail?\"")
        listed = "\n".join(f"  {r.get('run_id')}  {r.get('pipeline')}  {r.get('state')}  {str(r.get('created'))[:16]}"
                           for r in runs[:5])
        return f"Assist could not tell which run of {usecase} you mean. Its latest runs:\n{listed}\nAsk about one."


def _reason(cause: dict) -> str:
    """The recorded one-line reason, without the step's name in front (the line above says the step)."""
    text = str(cause.get("summary") or "the cause is not recorded")
    step = cause.get("step")
    if step and text.startswith(f"{step}: "):
        text = text[len(step) + 2:]
    return text.removeprefix("not submitted: ")          # the line above says it was not submitted


def _explanation(found: dict) -> str | None:
    """The model's own words: one short paragraph, dropped whole if it names a role, an address or a link."""
    text = found.get("explanation")
    if not isinstance(text, str) or not text.strip():
        return None
    text = " ".join(text.split())[:600]
    return None if _UNSAFE_EXPLANATION.search(text) else text
