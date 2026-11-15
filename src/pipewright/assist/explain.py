"""`pipewright assist explain`: the page a team reads before it switches Pipewright Assist on.

Opting in is not a flag set in passing. The team's use case names the version of this page it read
(assist.understood); when a release changes what Assist reads or does, the version changes and the team confirms
again. The page also shows the settings in play, so nothing about the model is hidden.
"""

from __future__ import annotations

from pipewright.assist import settings as assist_settings

PRICING = "https://cloud.google.com/vertex-ai/generative-ai/pricing"
MODEL_ARMOR = "https://cloud.google.com/security-command-center/docs/model-armor-overview"

PAGE = """Pipewright Assist (preview)                                   explain page, version {version}

WHAT IT IS
  An assistant that answers incident questions about your pipeline runs:
    why did this run fail?   whose is it, my team's or the platform's?   what do I do next?
    (platform team)  which use cases are blocked?   which failures this week were platform causes?
  It answers when you ask, in your terminal:  pipewright assist ask "why did last night's training fail?"

WHAT IT READS
  - your use case's recent runs, and for a failed run the cause Pipewright recorded with it (since 1.4.4):
    the step, the error's type, the place in your code, your check results, Pipewright's own error messages
  - which release ran, and its settings
  - the environment checks `pipewright doctor` runs
  One line of your own source code can be part of a recorded cause (the line that raised).

WHAT IT NEVER READS
  data rows, feature values, predictions, step logs, monitoring results, audit logs, the cluster.

WHAT IT NEVER DOES
  It changes nothing: it does not run, retrain, release, approve, deploy or grant. No answer of Assist is an
  approval. Every answer is generated advice: check it before you act on it.

WHERE THE ANSWER COMES FROM
  The cause, the owner (team, platform or unclear) and the advice come from Pipewright's fixed rules, the same ones
  that write the run.failed event. The model finds the run you mean and explains the recorded facts in plain words.
  When the facts do not name a cause, Assist says "unclear" and where to look. It does not guess.

THE MODEL, AND WHO PAYS
  Assist calls a Google Gemini model in your own Google Cloud project. Nothing is sent to Pipewright's maker.
  Your project pays Google for the model calls, at Google's prices: {pricing}
  A question is one small routing call and, for an incident, a few calls with tools.

SCREENING
  Questions and answers should go through Google's Model Armor: the question, each text field a tool returns, and
  the answer before you see it. {screening_how}
  Model Armor is a second layer, not the protection itself: the protection is that tool text is treated as data and
  the owner and the advice come from fixed lists. Some Model Armor filters are not offered in every region.
  {model_armor}

WHAT IS KEPT
  Every question and answer is recorded: {record_where}
  Your question is kept as you typed it. Do not type personal data into a question.

A PREVIEW
  Off by default. Its answers and commands may change between releases; there is no stability promise yet.
  It answers incident questions only; other questions get a plain "not covered in the preview".

IN A PRIVATE ENVIRONMENT
  Your terminal must be allowed to reach the environment, as for every other pipewright command.

SWITCHING IT ON
  1. The platform allows it for the environment (where the environment has a platform manifest):
       starter Terraform:  assist = true        Pipewright Enterprise:  spec.assist.enabled: true
  2. Your team opts in, in the use case's usecase.yaml:
       spec:
         assist: {{ enabled: true, understood: "{version}" }}
     Switch on monitoring.failure_records too: without recorded causes Assist can only say "read the logs".
  3. `pipewright doctor --env <env>` says whether Assist is on, and why not.
  To switch it off: remove the block, or set enabled: false.
"""


def settings_table(env, uc=None, asked: int | None = None) -> str:
    s = assist_settings.resolve(env)
    off = assist_settings.why_off(env, uc)
    understood = (getattr(uc, "assist", None) or {}).get("understood") if uc is not None else None
    current = assist_settings.EXPLAIN_VERSION
    tenant = getattr(uc, "tenant", None)
    hosted = bool(((getattr(env, "tenants", None) or {}).get(tenant or "") or {}).get("agent_server"))
    rows = [
        ("Assist", "ON" if not off else "OFF"),
        ("Platform switch", ("allowed" if s.platform_allows else "not allowed") if s.platform_managed
         else "none: this environment has no platform manifest"),
        ("Team switch" + (f" ({uc.name})" if uc is not None else ""),
         "-" if uc is None else ("on" if (uc.assist or {}).get("enabled") else "off")),
        ("Explain page confirmed", "-" if uc is None else (understood or "not yet") + (
            "" if understood in (None, current) else f" (current: {current})")),
        ("Model", s.model),
        ("Called from", f"{s.location}" + (" endpoint" if s.location == "global" else " (regional)")),
        ("Answered", "by your team's hosted tool server, inside the environment" if hosted
         else "on this machine, with your own Google Cloud access"),
        ("Model called as", s.model_service_account or "you (your own Google Cloud access)"),
        ("Recorded", "in the environment's evidence dataset (ai_assist_answers)" if hosted
         else ".pipewright/assist-log.jsonl in this repository"),
        ("Temperature", f"{s.temperature:g}"),
        ("Longest answer", f"{s.max_output_tokens:,} tokens"),
        ("Ceiling per question", f"{s.max_tool_calls} tool calls, {s.max_tokens:,} tokens"),
        ("Questions per team per month", f"{s.questions_per_month:,}"
         + (" (enforced by the hosted tool server)" if hosted else
            "" if asked is None else f" (asked this month: {asked}; counted, not enforced)")),
        ("Screening (Model Armor)", f"on: {s.screening_template}" if s.screening
         else "OFF: answers are marked \"not screened\""),
    ]
    width = max(len(k) for k, _ in rows)
    lines = [f"  {k.ljust(width)}  {v}" for k, v in rows]
    if off:
        lines.append(f"\n  Why it is off: {off}")
    return "SETTINGS IN PLAY" + (f" ({env.name})" if getattr(env, "name", None) else "") + "\n" + "\n".join(lines)


def page(env=None, uc=None, asked: int | None = None) -> str:
    s = assist_settings.resolve(env) if env is not None else None
    if s is not None and s.screening:
        how = "It is ON in this environment."
    elif env is not None and s.platform_managed:
        how = ("It is OFF in this environment: the platform turns it on with a Model Armor template "
               "(ai.assist.screening). Until then every answer says \"not screened\".")
    else:
        how = ("It is OFF until you turn it on: enable Model Armor in your project, create a template, and name it "
               "in the environment (ai.assist.screening.template). Until then every answer says \"not screened\".")
    text = PAGE.format(version=assist_settings.EXPLAIN_VERSION, pricing=PRICING, model_armor=MODEL_ARMOR,
                       screening_how=how,
                       record_where=("in .pipewright/assist-log.jsonl in your repository (your own record; "
                                     "Pipewright Enterprise keeps it in the evidence dataset)."))
    return text + ("\n" + settings_table(env, uc, asked) + "\n" if env is not None else "")
