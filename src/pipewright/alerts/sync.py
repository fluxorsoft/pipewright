"""pipewright alerts sync: email alerts for a use case's Pipewright events (monitoring.alerts in usecase.yaml).

    monitoring:
      alerts:
        notify: [team-ml@example.com, ml-oncall@example.com]      # any addresses, including distribution lists
        events: [monitoring.alert, training.rejected]       # default: monitoring.alert
        severity: WARNING

Retrain decisions (retraining.decisions.notify) get their own policies: each decision event emails its summary and
case ID (retrain.escalated also emails escalate_notify; retrain.digest only with decisions.digest). Email never
approves; the email says how to decide.

For each event: one Cloud Monitoring log-based alert policy that matches the event's structured log entry (the
labels Pipewright puts on every entry) and emails every address. One email notification channel
per address, shared by all use cases that notify it. Owned objects carry the label pipewright-usecase; a policy for
an event no longer configured is deleted. Idempotent, like `schedules sync`.

A use case that is gone (Pipewright 1.1, prune): on a whole-repo sync, the environment's policies of a use case that
is neither in this repo nor released in the environment (no live release pointer) are deleted. Several teams' repos
may share an environment, so a use case another repo still releases keeps its policies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipewright.config import Environment, ResolvedUseCase
from pipewright.gcp import Gcp, error_text

MON = "https://monitoring.googleapis.com/v3"

# event -> (log filter on the event entry, title, details pulled into the email)
EVENTS = {
    "monitoring.alert": ('jsonPayload.pipewright_event.event_type="monitoring.alert"', "drift or performance alert",
                         ["model_version_id", "drifted_features", "max_feature_psi", "prediction_psi",
                          "performance_value", "retrain_status"]),
    "retrain.requested": ('jsonPayload.pipewright_event.event_type="retrain.requested"', "retrain requested",
                          ["triggers", "job_id", "source_uri"]),
    "training.rejected": ('jsonPayload.pipewright_event.event_type="training.evaluated" AND '
                          'jsonPayload.pipewright_event.data.decision="reject"', "model rejected by evaluation",
                          ["primary_metric", "primary_value", "champion_version", "champion_value"]),
    "training.evaluated": ('jsonPayload.pipewright_event.event_type="training.evaluated"', "training evaluated",
                           ["decision", "primary_metric", "primary_value", "champion_version"]),
    "model.registered": ('jsonPayload.pipewright_event.event_type="model.registered"', "model registered",
                         ["model", "version", "default", "primary_value"]),
    # from the launcher's hourly tick (Pipewright 1.2, FR-67)
    "run.failed": ('jsonPayload.pipewright_event.event_type="run.failed"', "pipeline run failed",
                   ["pipeline", "job_id", "state", "failed_steps", "error"]),
}

# Retrain decision events (pipewright.retraining): emailed to retraining.decisions.notify.
DECISION_EVENTS = {
    "retrain.proposed": "retrain decision needed",
    "retrain.reopened": "retrain case re-opened",
    "retrain.escalated": "retrain decision overdue",
    "retrain.approval": "retrain approval recorded",
    "retrain.approved": "retrain approved",
    "retrain.declined": "retrain declined",
    "retrain.snoozed": "retrain snoozed",
    "retrain.digest": "weekly retrain digest",
    "retrain.training_failed": "retrain training failed",     # Pipewright 1.1: the approval stays; retry
    "retrain.refused": "retrained model refused by the gates",   # Pipewright 1.2: the case is open again
    "retrain.training_retried": "retrain training resubmitted",
}
for _event, _title in DECISION_EVENTS.items():
    EVENTS[_event] = (f'jsonPayload.pipewright_event.event_type="{_event}"', _title, ["case_id", "summary"])


def _label(v: str) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", v.lower())[:63]


def policy_name(uc: ResolvedUseCase, event: str) -> str:
    return f"pipewright {uc.name} {uc.environment}: {EVENTS[event][1]}"


def policy_body(uc: ResolvedUseCase, event: str, channels: list[str], severity: str) -> dict:
    match, title, details = EVENTS[event]
    log_filter = (f'labels."pipewright-usecase"="{uc.name}" AND labels."pipewright-environment"="{uc.environment}" '
                  f"AND {match}")
    extractors = {d: f"EXTRACT(jsonPayload.pipewright_event.data.{d})" for d in details}
    extractors["run_id"] = 'EXTRACT(labels."run-id")'
    extractors["release"] = 'EXTRACT(labels."pipewright-release")'
    subject = f"[{uc.environment}] {uc.name}: {title}"
    if event in DECISION_EVENTS:
        subject += " (${log.extracted_label.case_id})" if event != "retrain.digest" else ""
        # A code block keeps the summary's line breaks and stops markdown turning "--env" into a dash, so the
        # commands in it can be copied.
        lines = [f"**{uc.name}** ({uc.environment}): {title}.", "", "```", "${log.extracted_label.summary}", "```", "",
                 "This email never approves anything. Decide with your own identity: `pipewright retrain show | "
                 f"approve | decline | snooze --env {uc.environment} --usecase {uc.name} --case <case>`.",
                 f"Owner: {uc.owner}."]
    else:
        lines = [f"**{uc.name}** ({uc.environment}): {title}.", "",
                 *[f"- {d}: ${{log.extracted_label.{d}}}" for d in details],
                 "- run: ${log.extracted_label.run_id}, release ${log.extracted_label.release}", "",
                 f"Owner: {uc.owner}. Details: `pipewright monitor status --env {uc.environment} --usecase "
                 f"{uc.name}`, or the run in Agent Platform Pipelines."]
    return {
        "displayName": policy_name(uc, event),
        "combiner": "OR",
        "conditions": [{"displayName": f"{event} event", "conditionMatchedLog": {
            "filter": log_filter, "labelExtractors": extractors}}],
        "alertStrategy": {"notificationRateLimit": {"period": "300s"}, "autoClose": "1800s"},
        "notificationChannels": channels,
        "documentation": {"content": "\n".join(lines), "mimeType": "text/markdown",
                          "subject": subject},
        "severity": severity,
        "userLabels": {"pipewright-usecase": _label(uc.name), "pipewright-environment": _label(uc.environment),
                       "pipewright-event": _label(event.replace(".", "-")), "pipewright-managed": "true"},
        "enabled": True,
    }


@dataclass
class AlertsPlan:
    channels_create: list[str] = field(default_factory=list)       # emails without a channel yet
    channels: dict = field(default_factory=dict)                    # email -> channel name (existing)
    create: list[str] = field(default_factory=list)                 # events
    update: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)                 # policy names (resource)
    unchanged: list[str] = field(default_factory=list)
    channels_delete: dict = field(default_factory=dict)             # email -> Pipewright channel no policy uses
    errors: list[str] = field(default_factory=list)


def _list(gcp: Gcp, url: str, key: str, **params) -> list[dict]:
    out, token = [], None
    while True:
        r = gcp.get(url, **params, **({"pageToken": token} if token else {}))
        if not r.ok:
            raise RuntimeError(error_text(r))
        out += r.body.get(key, [])
        token = r.body.get("nextPageToken")
        if not token:
            return out


def _same(live: dict, want: dict) -> bool:
    keys = ("displayName", "conditions", "alertStrategy", "documentation", "severity", "enabled")
    norm = lambda d: {k: d.get(k) for k in keys} | {"channels": sorted(d.get("notificationChannels", []))}  # noqa: E731
    live_c = [{"displayName": c.get("displayName"), "conditionMatchedLog": c.get("conditionMatchedLog")}
              for c in live.get("conditions", [])]
    return norm({**live, "conditions": live_c}) == norm(want)


def sync(uc: ResolvedUseCase, env: Environment, gcp: Gcp | None = None, dry_run: bool = False) -> AlertsPlan:
    gcp = gcp or Gcp()
    alerts = uc.ml["monitoring"]["alerts"]
    severity = alerts["severity"]
    wanted_emails = wanted_policies(uc)
    emails = sorted({e for addresses in wanted_emails.values() for e in addresses})
    base = f"{MON}/projects/{env.project}"
    plan = AlertsPlan()
    try:
        live_channels = _list(gcp, f"{base}/notificationChannels", "notificationChannels", filter='type="email"')
        live_policies = _list(gcp, f"{base}/alertPolicies", "alertPolicies",
                              filter=f'user_labels."pipewright-usecase"="{_label(uc.name)}" AND '
                                     f'user_labels."pipewright-environment"="{_label(uc.environment)}"')
    except RuntimeError as e:
        plan.errors.append(str(e))
        return plan
    # A project may have other email channels for the same address (a platform's own alerts): the one Pipewright
    # made wins, so a new channel elsewhere never repoints the use case's policies.
    mine = lambda c: (c.get("userLabels") or {}).get("pipewright-managed") == "true"  # noqa: E731
    by_email = {c["labels"].get("email_address"): c["name"]
                for c in sorted(live_channels, key=lambda c: (mine(c), c["name"]))}
    for email in emails:
        if email in by_email:
            plan.channels[email] = by_email[email]
        else:
            plan.channels_create.append(email)

    if not dry_run:
        for email in plan.channels_create:
            r = gcp.post(f"{base}/notificationChannels", {
                "type": "email", "displayName": f"pipewright: {email}", "labels": {"email_address": email},
                "userLabels": {"pipewright-managed": "true"}})
            if r.ok:
                plan.channels[email] = r.body["name"]
            else:
                plan.errors.append(f"channel {email}: {error_text(r)}")

    live_by_name = {p["displayName"]: p for p in live_policies}
    wanted = {policy_name(uc, e): e for e in wanted_emails}
    for name, live in live_by_name.items():
        if name not in wanted:
            plan.delete.append(live["name"])
    for name, event in wanted.items():
        event_channels = sorted(plan.channels[e] for e in wanted_emails[event] if e in plan.channels)
        body = policy_body(uc, event, event_channels, severity)
        live = live_by_name.get(name)
        if live is None:
            plan.create.append(event)
            if not dry_run:
                r = gcp.post(f"{base}/alertPolicies", body)
                if not r.ok:
                    plan.errors.append(f"{event}: {error_text(r)}")
        elif _same(live, body) and not plan.channels_create:
            plan.unchanged.append(event)
        else:
            plan.update.append(event)
            if not dry_run:
                r = gcp.patch(f"{MON}/{live['name']}", body)
                if not r.ok:
                    plan.errors.append(f"{event}: {error_text(r)}")
    if not dry_run:
        for name in plan.delete:
            r = gcp.delete(f"{MON}/{name}")
            if not r.ok and r.status != 404:
                plan.errors.append(f"delete {name}: {error_text(r)}")
    _unused_channels(gcp, base, uc, live_channels, plan, dry_run)
    return plan


def _unused_channels(gcp: Gcp, base: str, uc: ResolvedUseCase, live_channels: list[dict], plan: AlertsPlan,
                     dry_run: bool) -> None:
    """Pipewright 1.2 (FR-77): an email channel Pipewright created that no alert policy in the project uses once this
    use case's policies are in place is deleted (an address that left notify). Channels Pipewright did not create are
    never touched; the delete is not forced, so a channel a policy started using meanwhile stays."""
    if plan.errors:
        return
    try:
        policies = _list(gcp, f"{base}/alertPolicies", "alertPolicies")
    except RuntimeError as e:
        plan.errors.append(f"unused email channels not checked: {e}")
        return
    ours = lambda p: ((p.get("userLabels") or {}).get("pipewright-usecase") == _label(uc.name)  # noqa: E731
                      and (p.get("userLabels") or {}).get("pipewright-environment") == _label(uc.environment))
    # in a dry run this use case's policies are not changed yet: count the channels they would use instead
    used = {c for p in policies if not (dry_run and ours(p)) for c in p.get("notificationChannels", [])}
    used |= set(plan.channels.values()) if dry_run else set()
    for c in live_channels:
        if (c.get("userLabels") or {}).get("pipewright-managed") != "true" or c["name"] in used:
            continue
        email = (c.get("labels") or {}).get("email_address") or c["name"]
        if not dry_run:
            r = gcp.delete(f"{MON}/{c['name']}")
            if not r.ok and r.status != 404:
                plan.errors.append(f"delete email channel {email}: {error_text(r)}")
                continue
        plan.channels_delete[email] = c["name"]


def prune(env: Environment, repo_usecases: set[str], gcp: Gcp | None = None, dry_run: bool = False,
          released=None) -> tuple[list[str], list[str], list[str]]:
    """(deleted, kept, errors): the environment's Pipewright policies of use cases not in this repo. A policy is
    deleted only when its use case has no live release in the environment either; released(name) -> bool."""
    gcp = gcp or Gcp()
    if released is None:
        from pipewright.release import service

        def released(name):
            return service.read_pointer(gcp, env, name)[0] is not None
    try:
        live = _list(gcp, f"{MON}/projects/{env.project}/alertPolicies", "alertPolicies",
                     filter=f'user_labels."pipewright-environment"="{_label(env.name)}"')
    except RuntimeError as e:
        return [], [], [str(e)]
    mine = {_label(n) for n in repo_usecases}
    deleted, kept, errors, verdict = [], [], [], {}
    for p in live:
        name = (p.get("userLabels") or {}).get("pipewright-usecase")
        if not name or name in mine:
            continue
        if name not in verdict:
            verdict[name] = released(name)
        if verdict[name]:
            kept.append(p["displayName"])
            continue
        if not dry_run:
            r = gcp.delete(f"{MON}/{p['name']}")
            if not r.ok and r.status != 404:
                errors.append(f"delete {p['displayName']}: {error_text(r)}")
                continue
        deleted.append(p["displayName"])
    return deleted, kept, errors


def wanted_policies(uc: ResolvedUseCase) -> dict[str, list[str]]:
    """event -> email addresses: monitoring.alerts for its events, retraining.decisions for decision events."""
    alerts = uc.ml["monitoring"]["alerts"]
    out = {e: sorted(set(alerts["notify"])) for e in dict.fromkeys(alerts["events"])} if alerts["notify"] else {}
    retraining = uc.ml.get("retraining") or {}
    decisions = retraining.get("decisions") or {}
    notify = sorted(set(decisions.get("notify") or []))
    if retraining.get("mode", "off") != "off" and notify:
        for event in DECISION_EVENTS:
            if event == "retrain.digest" and not decisions.get("digest", True):
                continue
            extra = (decisions.get("escalate_notify") or []) if event == "retrain.escalated" else []
            out[event] = sorted(set(notify) | set(extra))
    return out


def render(uc: ResolvedUseCase, plan: AlertsPlan, dry_run: bool) -> str:
    alerts = uc.ml["monitoring"]["alerts"]
    head = f"Alerts for {uc.name} ({uc.environment})" + ("  [dry run]" if dry_run else "")
    if not alerts["notify"]:
        lines = [head, "  INFO  monitoring.alerts.notify is empty: no alert emails"]
    else:
        lines = [head, f"  notify: {', '.join(sorted(set(alerts['notify'])))}"]
    decisions = (uc.ml.get("retraining") or {}).get("decisions") or {}
    if (uc.ml.get("retraining") or {}).get("mode", "off") != "off":
        lines.append(f"  retrain decisions: {', '.join(sorted(set(decisions.get('notify') or []))) or 'nobody'}"
                     + ("" if decisions.get("notify") else "  (set retraining.decisions.notify)"))
    verb = (lambda w: f"would {w}") if dry_run else (lambda w: f"{w}d" if w.endswith("e") else f"{w}ed")
    for e in plan.channels_create:
        lines.append(f"  {'PLAN' if dry_run else 'DONE'}  email channel {e}: {verb('create')}")
    for e in plan.create:
        lines.append(f"  {'PLAN' if dry_run else 'DONE'}  {e}: {verb('create')}")
    for e in plan.update:
        lines.append(f"  {'PLAN' if dry_run else 'DONE'}  {e}: {verb('update')}")
    for e in plan.unchanged:
        lines.append(f"  OK    {e}: up to date")
    for n in plan.delete:
        status = "PLAN" if dry_run else "DONE"
        lines.append(f"  {status}  {n.rsplit('/', 1)[-1]}: {verb('delete')} (no longer configured)")
    for e in sorted(plan.channels_delete):
        lines.append(f"  {'PLAN' if dry_run else 'DONE'}  email channel {e}: {verb('delete')} (no policy uses it)")
    for err in plan.errors:
        lines.append(f"  FAIL  {err}")
    return "\n".join(lines)
