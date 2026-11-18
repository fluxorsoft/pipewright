"""The telemetry contract (FR-127): every metric and span attribute the launcher exports, in one place.

`docs/TELEMETRY.md` is generated from this module (`python -m pipewright.telemetry.catalogue > docs/TELEMETRY.md`)
and a test fails when the two differ. The names, types, units and attributes here are stable within 1.x
(STABILITY.md). Pipewright owns what its telemetry means; the backend owns how it is stored and shown.

Rules (the founder's architecture review, 2026-11-18):
- Telemetry never fails a run, changes an outcome or a refusal, modifies evidence or blocks the launcher.
- Only attributes listed here leave the launcher; any value outside an attribute's allowed values becomes `other`.
- Never on telemetry: file or object names, personal data, row values, SQL text, credentials or headers.
- Run, case and release IDs go on spans only, never on metrics.
"""

from __future__ import annotations

from dataclasses import dataclass

TRIGGERS = ("landing", "schedule", "api", "request", "loop", "ab_test", "retrain", "tick", "other")
PIPELINES = ("training", "inference", "monitoring", "synthetic", "online_sync", "other")
REASONS = ("no_live_release", "integrity", "features", "held", "submit_failed", "invalid_request", "policy", "other")
CHECKS = ("retraining", "integrity", "failed_runs", "registration", "held_runs", "capture", "ab_tests", "other")
LATENCY_BUCKETS_S = (1, 2, 5, 10, 30, 60, 120, 300, 600, 1800)
SERIES_BUDGET = 5000            # active series per environment; beyond it new combinations are recorded as `other`


@dataclass(frozen=True)
class Metric:
    name: str
    kind: str               # counter | histogram | gauge
    unit: str
    attributes: tuple
    description: str


METRICS = (
    Metric("pipewright.runs.started", "counter", "{run}", ("usecase", "pipeline", "trigger"),
           "A run the launcher submitted: the orchestrator accepted it."),
    Metric("pipewright.runs.refused", "counter", "{run}", ("usecase", "trigger", "reason"),
           "A trigger the launcher did not submit: no live release, an integrity or feature check, a scoring run held "
           "for a person, a submission the orchestrator refused, an invalid request."),
    Metric("pipewright.runs.deduplicated", "counter", "{run}", ("usecase", "trigger"),
           "A trigger that mapped to a run already started: the deterministic run ID absorbed a retry."),
    Metric("pipewright.trigger.latency", "histogram", "s", ("trigger", "tenant"),
           "From the trigger's own time (a landing file's creation time, a schedule's scheduled time, otherwise when "
           "the launcher received the request) to the orchestrator accepting the run. Execution start is the "
           "orchestrator's and not part of this."),
    Metric("pipewright.cases.open", "gauge", "{case}", ("usecase",),
           "Retrain cases waiting for a decision, read at each export."),
    Metric("pipewright.tick.failures", "counter", "{failure}", ("check",),
           "A failed pass of one of the launcher's scheduled checks."),
)

# Attributes allowed on metrics, with their allowed values (None: the environment's own names, checked at runtime)
METRIC_VALUES = {"trigger": TRIGGERS, "pipeline": PIPELINES, "reason": REASONS, "check": CHECKS,
                 "usecase": None, "tenant": None}

# Attributes allowed on spans (anything else is dropped before it reaches the SDK)
SPAN_ATTRIBUTES = {
    "pipewright.usecase": "the use case",
    "pipewright.tenant": "its team",
    "pipewright.pipeline": "training, inference, monitoring, synthetic, online_sync",
    "pipewright.trigger": "landing, schedule, api, request, loop, ab_test, retrain, tick",
    "pipewright.release": "the release ID",
    "pipewright.run_id": "the run ID",
    "pipewright.job_id": "the orchestrator's job name",
    "pipewright.case_id": "a retrain case",
    "pipewright.outcome": "submitted, refused, held, duplicate, submit_failed, error",
    "pipewright.reason": "why it was refused (the reasons above)",
    "pipewright.step": "a pipeline step's name (step spans)",
    "pipewright.state": "a run's or step's final state",
    "pipewright.check": "a scheduled check (the checks above)",
    "exception.type": "the error's type; its message only when Pipewright wrote it (value-free, FR-132)",
    "exception.message": "Pipewright's own message, never a library's or the data's",
}

SPANS = (
    ("pipewright.trigger", "one per trigger: a landing file, a schedule, an API call, a run request, a check"),
    ("pipewright.release.read", "the live release read for the use case"),
    ("pipewright.run.start", "policy checks, deduplication and the submission"),
    ("pipewright.run", "the run, written when it finishes, parented to its trigger"),
    ("pipewright.step", "one per pipeline step, from the orchestrator's own start and end times"),
)


def render() -> str:
    out = ["# Telemetry", "",
           "<!-- Generated from src/pipewright/telemetry/catalogue.py by `python -m pipewright.telemetry.catalogue > "
           "docs/TELEMETRY.md`. Do not edit by hand. -->", "",
           "With `observability.otlp` in the environment's manifest (FR-127, off by default), the launcher exports "
           "these metrics and traces over OTLP. Names, types, units and attributes are stable within 1.x. Telemetry is "
           "for operations, not evidence: the record stays the event log, and telemetry never fails a run, changes an "
           "outcome or blocks the launcher.", "",
           "## Metrics", "",
           "| Metric | Type | Unit | Attributes | Measures |", "|---|---|---|---|---|"]
    for m in METRICS:
        out.append(f"| `{m.name}` | {m.kind} | `{m.unit}` | {', '.join(f'`{a}`' for a in m.attributes)} | "
                   f"{m.description} |")
    out += ["", f"`pipewright.trigger.latency` buckets (seconds): {', '.join(str(b) for b in LATENCY_BUCKETS_S)}.", "",
            "## Attribute values", "", "| Attribute | Values |", "|---|---|"]
    for k, v in METRIC_VALUES.items():
        values = ", ".join(f"`{x}`" for x in v) if v else "the environment's own names"
        out.append(f"| `{k}` | {values} |")
    out += ["", f"A value outside its list becomes `other`. At most {SERIES_BUDGET:,} active series per environment: "
            "beyond it, new combinations are recorded as `other` and the launcher logs it once.", "",
            "## Spans", "", "| Span | What |", "|---|---|"]
    out += [f"| `{n}` | {w} |" for n, w in SPANS]
    out += ["", "Span attributes (any other is dropped before export):", "", "| Attribute | What |", "|---|---|"]
    out += [f"| `{k}` | {v} |" for k, v in SPAN_ATTRIBUTES.items()]
    out += ["", "## Never exported", "",
            "File and object names, personal data, row values, feature values, predictions, SQL text, credentials and "
            "headers. Run, case and release IDs are on spans only, never on metrics. A `traceparent` sent by a caller "
            "is recorded as a span link, never adopted as the parent, and never sets a team, use case or decision.", ""]
    return "\n".join(out)


if __name__ == "__main__":
    print(render(), end="")
