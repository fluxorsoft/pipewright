# Telemetry

<!-- Generated from src/pipewright/telemetry/catalogue.py by `python -m pipewright.telemetry.catalogue > docs/TELEMETRY.md`. Do not edit by hand. -->

With `observability.otlp` in the environment's manifest (FR-127, off by default), the launcher exports these metrics and traces over OTLP. Names, types, units and attributes are stable within 1.x. Telemetry is for operations, not evidence: the record stays the event log, and telemetry never fails a run, changes an outcome or blocks the launcher.

## Metrics

| Metric | Type | Unit | Attributes | Measures |
|---|---|---|---|---|
| `pipewright.runs.started` | counter | `{run}` | `usecase`, `pipeline`, `trigger` | A run the launcher submitted: the orchestrator accepted it. |
| `pipewright.runs.refused` | counter | `{run}` | `usecase`, `trigger`, `reason` | A trigger the launcher did not submit: no live release, an integrity or feature check, a scoring run held for a person, a submission the orchestrator refused, an invalid request. |
| `pipewright.runs.deduplicated` | counter | `{run}` | `usecase`, `trigger` | A trigger that mapped to a run already started: the deterministic run ID absorbed a retry. |
| `pipewright.trigger.latency` | histogram | `s` | `trigger`, `tenant` | From the trigger's own time (a landing file's creation time, a schedule's scheduled time, otherwise when the launcher received the request) to the orchestrator accepting the run. Execution start is the orchestrator's and not part of this. |
| `pipewright.cases.open` | gauge | `{case}` | `usecase` | Retrain cases waiting for a decision, read at each export. |
| `pipewright.tick.failures` | counter | `{failure}` | `check` | A failed pass of one of the launcher's scheduled checks. |

`pipewright.trigger.latency` buckets (seconds): 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800.

## Attribute values

| Attribute | Values |
|---|---|
| `trigger` | `landing`, `schedule`, `api`, `request`, `loop`, `ab_test`, `retrain`, `tick`, `other` |
| `pipeline` | `training`, `inference`, `monitoring`, `synthetic`, `online_sync`, `other` |
| `reason` | `no_live_release`, `integrity`, `features`, `held`, `submit_failed`, `invalid_request`, `policy`, `other` |
| `check` | `retraining`, `integrity`, `failed_runs`, `registration`, `held_runs`, `capture`, `ab_tests`, `other` |
| `usecase` | the environment's own names |
| `tenant` | the environment's own names |

A value outside its list becomes `other`. At most 5,000 active series per environment: beyond it, new combinations are recorded as `other` and the launcher logs it once.

## Spans

| Span | What |
|---|---|
| `pipewright.trigger` | one per trigger: a landing file, a schedule, an API call, a run request, a check |
| `pipewright.release.read` | the live release read for the use case |
| `pipewright.run.start` | policy checks, deduplication and the submission |
| `pipewright.run` | the run, written when it finishes, parented to its trigger |
| `pipewright.step` | one per pipeline step, from the orchestrator's own start and end times |

Span attributes (any other is dropped before export):

| Attribute | What |
|---|---|
| `pipewright.usecase` | the use case |
| `pipewright.tenant` | its team |
| `pipewright.pipeline` | training, inference, monitoring, synthetic, online_sync |
| `pipewright.trigger` | landing, schedule, api, request, loop, ab_test, retrain, tick |
| `pipewright.release` | the release ID |
| `pipewright.run_id` | the run ID |
| `pipewright.job_id` | the orchestrator's job name |
| `pipewright.case_id` | a retrain case |
| `pipewright.outcome` | submitted, refused, held, duplicate, submit_failed, error |
| `pipewright.reason` | why it was refused (the reasons above) |
| `pipewright.step` | a pipeline step's name (step spans) |
| `pipewright.state` | a run's or step's final state |
| `pipewright.check` | a scheduled check (the checks above) |
| `exception.type` | the error's type; its message only when Pipewright wrote it (value-free, FR-132) |
| `exception.message` | Pipewright's own message, never a library's or the data's |

## Never exported

File and object names, personal data, row values, feature values, predictions, SQL text, credentials and headers. Run, case and release IDs are on spans only, never on metrics. A `traceparent` sent by a caller is recorded as a span link, never adopted as the parent, and never sets a team, use case or decision.
