"""The fixed replies to questions outside incidents. Written here, not composed by a model.

The preview answers incident questions only. The model's one job for anything else is to say which of these three
kinds a question is; the words are Pipewright's.
"""

from __future__ import annotations

INCIDENT, LATER, ACTION, UNRELATED = "incident", "later", "action", "unrelated"
KINDS = (INCIDENT, LATER, ACTION, UNRELATED)

LATER_TOPICS = (
    (("accura", "drift", "perform", "monitor", "retrain"),
     "pipewright monitor status --env {env} --usecase <use case>   (recorded drift and performance)"),
    (("deploy", "serv", "endpoint", "latency", "replica"),
     "pipewright serving status --env {env} --usecase <use case>   (what is served and how it is watched)"),
    (("gate", "evaluat", "metric", "champion", "version", "approve"),
     "pipewright model list --env {env} --usecase <use case>   (versions, metrics, gates, approvals)"),
    (("cost", "spend", "bill"), "the Google Cloud billing reports, by the pipewright-usecase label"),
)


def reply(kind: str, question: str, env: str) -> str:
    q = question.lower()
    if kind == ACTION:
        return ("Pipewright Assist never changes anything: it does not run, retrain, release, approve, deploy or "
                "grant. A person does that with their own identity, for example:\n"
                f"  pipewright run --env {env} --usecase <use case> --pipeline training\n"
                f"  pipewright model approve --env {env} --usecase <use case> --version <n>\n"
                "Who may run them is set by the platform (operators and approver groups).")
    if kind == LATER:
        where = next((cmd for words, cmd in LATER_TOPICS if any(w in q for w in words)),
                     "pipewright status --env {env}   (what is live)")
        return ("Not covered in the preview: Pipewright Assist answers incident questions only (why a run failed, "
                "whose it is, what is blocked). The facts are here today:\n  " + where.format(env=env))
    return "Pipewright Assist answers questions about failed Pipewright runs only."
