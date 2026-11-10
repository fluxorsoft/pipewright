"""Every event Pipewright sends, in one place (Pipewright 1.4.3): what it is, where it comes from, when, and who
usually wants it.

An event is one structured log line and, when the environment manifest sets observability.events_topic (the
starter and the Enterprise blueprints always do), one message on that Pub/Sub topic. Every message carries the
attributes event_type, usecase, environment and tenant (those that apply), so any team subscribes to the topic
with a filter and receives exactly what it tracks: `pipewright events --persona sre --filter` prints one.
Email (`pipewright alerts sync`) is one consumer among many; Pipewright has no vendor code.

tests/test_event_catalogue.py fails when the code sends an event this catalogue does not list, so it cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass

PERSONAS = {
    "ml": "ML team: data scientists and ML engineers who own a use case",
    "sre": "Platform and SRE: the team that runs the environments, the launcher and serving",
    "governance": "Governance and audit: risk, model approvers and auditors",
    "product": "Product and business owners: what is live and how it performs",
    "data": "Data team: tables, features and online stores the models read",
    "finops": "FinOps: what costs money and who started it",
}

FAMILIES = {
    "run": "Pipeline runs: submitted, refused, failed",
    "release": "Releases going live, promoted, rolled back",
    "model": "Model versions: registered, approved, deployed, protected",
    "training": "Training results and their gates",
    "data": "Data read by a run: snapshots, features, labels, erasure",
    "monitoring": "Drift and performance of models in use",
    "retrain": "Retrain decision cases",
    "loop": "The closed retrain loop across environments",
    "ab_test": "A/B tests of models",
    "online": "Online feature stores",
    "serving": "Shared serving pools",
    "synthetic": "Synthetic data",
    "ray": "Ray clusters",
    "policy": "Platform policy exceptions",
}


@dataclass(frozen=True)
class Event:
    type: str
    source: str           # what sends it
    when: str             # one sentence
    personas: tuple[str, ...]
    since: str = "1.0.0"

    @property
    def family(self) -> str:
        return self.type.split(".", 1)[0]


_L, _C, _S, _R = "launcher", "CLI", "pipeline step", "renewer job"

EVENTS: tuple[Event, ...] = (
    # run
    Event("run.submitted", _L, "A run request (file, schedule, API, Pub/Sub) became a pipeline job.", ("ml", "sre")),
    Event("run.duplicate", _L, "A request reused a run key: nothing new started.", ("sre",)),
    Event("run.integrity", _L, "A run was checked against what was approved before it was submitted (refused or "
          "warned).", ("governance", "sre", "ml"), "1.4.0"),
    Event("run.failed", _L, "A run ended failed or cancelled (the launcher's tick).", ("ml", "sre"), "1.2.0"),
    Event("run.unregistered", _L, "A training run the gates approved ended succeeded without registering its model.",
          ("ml", "sre", "governance"), "1.4.3"),
    # release
    Event("release.live", _C, "A release became the live one, with who released it.", ("ml", "sre", "governance",
                                                                                         "product")),
    Event("release.promoted", _C, "A release arrived in another environment by promotion.",
          ("ml", "sre", "governance", "product"), "1.1.0"),
    Event("release.rolled_back", _C, "The live release was moved back to an earlier one.",
          ("ml", "sre", "governance", "product")),
    # model
    Event("model.registered", _S, "A new model version is in the registry (and whether it is the champion).",
          ("ml", "governance")),
    Event("model.registration_skipped", _S, "The register step skipped on purpose (an exact replay ties itself).",
          ("ml",), "1.4.3"),
    Event("model.approved", _C, "A person approved a model version.", ("governance", "ml")),
    Event("model.promoted", _C, "A model version was copied to another environment's registry.",
          ("governance", "ml", "sre"), "1.1.0"),
    Event("model.deployed", _C, "A version started serving on an endpoint or KServe (canary, A/B arm or full).",
          ("ml", "sre", "product", "governance")),
    Event("model.undeployed", _C, "A version stopped serving on an endpoint.", ("ml", "sre", "product"), "1.4.2"),
    Event("model.duplicated", _L, "Two models carry a use case's model name; the newer one's versions are hidden.",
          ("ml", "sre"), "1.4.3"),
    Event("model.lineage.recorded", _S, "A run's lineage record (inputs, code, model) was written.",
          ("governance",), "1.1.0"),
    Event("model.integrity", _L, "An approved model record changed between deploys (the integrity watch).",
          ("governance", "sre"), "1.4.1"),
    Event("model.integrity.check", _L, "The integrity watch ran: its heartbeat, with what it checked.",
          ("sre", "governance"), "1.4.1"),
    Event("model.lock.renewed", _R, "Approved files in use had their deletion lock renewed.", ("sre", "governance"),
          "1.4.1"),
    Event("model.lock.failed", _R, "A lock on approved files in use could not be renewed.", ("sre", "governance"),
          "1.4.1"),
    Event("model.lock.reconciliation", _R, "The renewer's summary: what it locked, renewed or released.",
          ("sre",), "1.4.1"),
    Event("model.restored", _C, "A changed approved file was restored from its locked copy, with who did it.",
          ("governance", "sre"), "1.4.2"),
    Event("model.retired", _C, "A model version was retired from use.", ("governance", "ml"), "1.4.2"),
    # training
    Event("training.evaluated", _S, "The gates judged a trained model: approve or reject, with the metrics.",
          ("ml", "governance")),
    # data
    Event("data.snapshot.written", _S, "The rows a run read were kept, so it can be replayed exactly.",
          ("ml", "governance", "data"), "1.1.0"),
    Event("data.snapshot.replayed", _S, "A replay read exactly an earlier run's rows.", ("ml", "governance"),
          "1.1.0"),
    Event("data.features.resolved", _S, "A run joined its features from feature tables, as of each row's cut-off.",
          ("ml", "data"), "1.1.0"),
    Event("data.labels.resolved", _S, "A run joined its labels from the label source, within the maturity window.",
          ("ml", "data"), "1.1.0"),
    Event("data.erased", _C, "A person's rows were erased by key from a use case's tables.",
          ("governance", "data")),
    # monitoring
    Event("monitoring.recorded", _S, "A scoring run's drift and performance were recorded.", ("ml",)),
    Event("monitoring.alert", _S, "Drift or a performance drop crossed its threshold.", ("ml", "product", "sre")),
    # retrain
    Event("retrain.requested", _S, "A pipeline released before the decision cases asked for a retrain.", ("ml",)),
    Event("retrain.proposed", _L, "Drift or performance opened (or added to) a retrain decision case.",
          ("ml", "governance"), "1.1.0"),
    Event("retrain.approval", _L, "One approver group recorded its approval of a case.", ("governance", "ml"),
          "1.1.0"),
    Event("retrain.approved", _L, "A case reached its quorum: the retrain goes ahead.", ("governance", "ml"),
          "1.1.0"),
    Event("retrain.declined", _L, "A case was declined.", ("governance", "ml"), "1.1.0"),
    Event("retrain.snoozed", _L, "A case was snoozed until a date.", ("governance", "ml"), "1.1.0"),
    Event("retrain.reopened", _L, "A snoozed or declined case came back.", ("ml", "governance"), "1.1.0"),
    Event("retrain.escalated", _L, "A case waited too long for a decision.", ("governance", "ml"), "1.1.0"),
    Event("retrain.digest", _L, "The weekly summary of open cases.", ("governance", "ml"), "1.1.0"),
    Event("retrain.refused", _L, "The approved retrain trained a model the gates refused.", ("ml", "governance"),
          "1.2.0"),
    Event("retrain.training_finished", _L, "The approved retrain's training ended.", ("ml",), "1.2.0"),
    Event("retrain.training_failed", _L, "The approved retrain's training failed.", ("ml", "sre"), "1.2.0"),
    Event("retrain.training_retried", _L, "The approved retrain's training was submitted again.", ("ml", "sre"),
          "1.2.0"),
    # loop (one per step of the closed retrain loop: loop.<state>)
    *(Event(f"loop.{state}", "launcher (loop)", what, who, "1.1.0") for state, what, who in (
        ("training", "The loop started training in the test environment.", ("ml",)),
        ("candidate", "The loop's training registered a candidate.", ("ml",)),
        ("rejected", "The loop's training registered no candidate (gates or comparison).", ("ml",)),
        ("failed", "The loop's training never started or ended failed.", ("ml", "sre")),
        ("validating", "The candidate is being validated.", ("ml",)),
        ("validated", "The candidate passed validation.", ("ml", "governance")),
        ("validation-failed", "The candidate failed validation.", ("ml", "governance")),
        ("canary", "The candidate serves a canary share in production.", ("ml", "sre", "product")),
        ("verifying", "The promoted version is being verified in production.", ("ml", "sre")),
        ("verification-failed", "Verification failed; a rollback restores the previous version.",
         ("ml", "sre", "governance")),
        ("rolled-back", "The loop rolled production back to the previous version.", ("ml", "sre", "product")),
        ("closed", "The loop finished: the new version is in production.", ("ml", "product", "governance")),
        ("paused", "A person paused the loop.", ("ml", "governance")),
        ("resumed", "A person resumed the loop.", ("ml", "governance")),
    )),
    # ab_test
    Event("ab_test.started", _C, "An A/B test started: control, treatment and the treatment's share.",
          ("ml", "product", "governance"), "1.4.3"),
    Event("ab_test.allocation_changed", _C, "A test's treatment share changed.", ("ml", "product"), "1.4.3"),
    Event("ab_test.paused", _C, "A test was paused (every unit served by control).", ("ml", "product"), "1.4.3"),
    Event("ab_test.resumed", _C, "A paused test resumed.", ("ml", "product"), "1.4.3"),
    Event("ab_test.stopped", _C, "A test was stopped before a decision.", ("ml", "product", "governance"), "1.4.3"),
    Event("ab_test.analysed", "CLI or scheduled analysis",
          "A test's analysis ran: the verdict so far and whether a decision is due.", ("ml", "product"), "1.4.3"),
    Event("ab_test.guardrail_breached", _L, "A business, technical or integrity guardrail of a test was breached.",
          ("ml", "product", "sre"), "1.4.3"),
    Event("ab_test.decision", _L, "An approver group recorded its decision on a test.", ("governance", "product"),
          "1.4.3"),
    Event("ab_test.decided", _C, "A test was decided: the winner and why.", ("ml", "product", "governance"),
          "1.4.3"),
    # online
    Event("online.synced", _C, "An online feature store was loaded or its sync started.", ("data", "ml"), "1.4.2"),
    Event("online.stale", _L, "An online store has not been refreshed within its limit.", ("data", "sre", "ml"),
          "1.4.2"),
    Event("online.fresh", _L, "A stale online store is fresh again.", ("data", "sre"), "1.4.2"),
    Event("online.billed", _L, "Lookups billed against an online store (BigQuery bytes).", ("finops", "data"),
          "1.4.2"),
    # serving
    Event("serving.pool.change", _C, "A shared serving pool gained or lost a model, or changed size.",
          ("sre", "finops"), "1.4.2"),
    # synthetic
    Event("synthetic.generated", _S, "A synthetic table was written.", ("ml", "data", "governance")),
    Event("synthetic.evaluated", _S, "The synthetic candidates were judged against the privacy and utility gates.",
          ("ml", "governance")),
    Event("synthetic.rejected", _S, "Every synthetic candidate failed its gates: nothing was published.",
          ("ml", "governance")),
    # ray
    Event("ray.cluster.created", _L, "A Ray cluster was created for a run step (with its deadline).",
          ("sre", "finops"), "1.0.0"),
    Event("ray.cluster.used", _S, "A run step used a standing Ray cluster.", ("sre", "finops", "ml"), "1.0.0"),
    # policy
    Event("policy.override", _L, "A time-boxed break-glass exception to a platform policy is in force.",
          ("governance", "sre"), "1.4.1"),
)

BY_TYPE = {e.type: e for e in EVENTS}

# Message kinds sent on the run-requests topic: commands to the launcher, not events.
REQUEST_KINDS = ("retrain.proposal", "retrain.decision", "ab.decision", "online.lookup_service",
                 "online.lookup_service.delete", "ray.cluster")


def attributes(event_type: str) -> dict:
    """The persona attributes put on an event's Pub/Sub message (Pipewright 1.4.3): pw_<persona> = "1" for each
    persona the event is for, so a persona subscribes with the filter `attributes:pw_<persona>`. A subscription
    filter is at most 256 bytes; a list of event types would not fit, one attribute always does."""
    e = BY_TYPE.get(event_type)
    return {f"pw_{p}": "1" for p in sorted(e.personas)} if e else {}


def persona_filter(persona: str) -> str:
    """The Pub/Sub subscription filter for one persona's events."""
    if persona not in PERSONAS:
        raise ValueError(f"unknown persona {persona!r} (known: {', '.join(PERSONAS)})")
    return f"attributes:pw_{persona}"


def select(persona: str | None = None, family: str | None = None) -> list[Event]:
    return [e for e in EVENTS if (persona is None or persona in e.personas) and (family is None or e.family == family)]


def subscription_filter(chosen: list[Event]) -> str:
    """A Pub/Sub subscription filter that matches exactly these event types: whole families by prefix when every
    event of the family is chosen, the rest by name."""
    families = {e.family for e in chosen}
    parts = []
    for f in sorted(families):
        members = [e for e in chosen if e.family == f]
        if len(members) == len([e for e in EVENTS if e.family == f]) and len(members) > 1:
            parts.append(f'hasPrefix(attributes.event_type, "{f}.")')
        else:
            parts += [f'attributes.event_type = "{e.type}"' for e in members]
    return " OR ".join(parts)


def markdown() -> str:
    """docs/EVENTS.md, generated from this catalogue (tests/test_event_catalogue.py checks it is current)."""
    from pipewright.alerts import sync
    from pipewright.observability import events

    mail = set(sync.EVENTS)
    out = [
        "# Events",
        "",
        "<!-- Generated from src/pipewright/observability/catalogue.py by "
        "`python -m pipewright.observability.catalogue > docs/EVENTS.md`. Do not edit by hand. -->",
        "",
        "Pipewright reports everything it does as an event: one structured log line and one message on the "
        "environment's events topic (`observability.events_topic` in the manifest; the starter and the Enterprise "
        "blueprints always set it). Nothing is sent anywhere else: each team subscribes to the one topic with a "
        "filter and takes exactly what it tracks to its own tools (Slack, PagerDuty, Datadog, Splunk, BigQuery, a "
        "ticket queue). Email from `pipewright alerts sync` is one such consumer.",
        "",
        "## Filtering",
        "",
        "Every message carries these attributes, for Pub/Sub subscription filters:",
        "",
        "| Attribute | Value |",
        "|---|---|",
        "| `event_type` | the type, such as `run.failed` |",
        "| `usecase`, `environment`, `tenant` | where it happened, when they apply |",
        "| `pw_<persona>` | `\"1\"` for each persona the event is for (since 1.4.3) |",
        "",
        "A subscription filter is at most 256 bytes, so filter by persona attribute or by family prefix, not by "
        "a long list of types:",
        "",
        "```",
        "attributes:pw_sre                                    # everything the platform team tracks",
        'hasPrefix(attributes.event_type, "ab_test.")         # one family',
        'attributes:pw_ml AND attributes.usecase = "churn"    # one team\'s use case',
        "```",
        "",
        "`pipewright events --persona sre --filter` prints a persona's filter; `pipewright events --json` lists "
        "everything below.",
        "",
        "## Personas",
        "",
        "| Persona | Who | Events |",
        "|---|---|---|",
    ]
    for p, who in PERSONAS.items():
        out.append(f"| `{p}` | {who.split(': ', 1)[-1]} | {len(select(p))} |")
    out += ["", "## Every event", "",
            "Decision events carry who decided and how it was approved. Email: `monitoring.alerts.events` (or "
            "`retraining.decisions.notify`) can send it by email.", ""]
    for fam, what in FAMILIES.items():
        out += [f"### {fam}: {what}", "", "| Event | When | Sent by | For | Since | |", "|---|---|---|---|---|---|"]
        for e in select(family=fam):
            tags = ", ".join(t for t, on in (("decision", e.type in events.DECISIONS), ("email", e.type in mail)) if on)
            out.append(f"| `{e.type}` | {e.when} | {e.source} | {', '.join(e.personas)} | {e.since} | {tags} |")
        out.append("")
    out += [f"{len(EVENTS)} event types in {len(FAMILIES)} families.", ""]
    return "\n".join(out)


if __name__ == "__main__":
    import sys

    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    print(markdown(), end="")
