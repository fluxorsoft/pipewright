# Why a run failed (Pipewright 1.4.4)

When a pipeline run fails, Pipewright can say **why**, and **whose it is**: your team's or the platform's. It is off by
default. This page says what it records, how to switch it on, and what it never keeps.

## What you get

Without it, a failed run tells you that a step failed. Agent Platform keeps only "the replica exited with a non-zero
status, check the logs"; Kubeflow keeps no error text at all; a run the orchestrator refuses at submission leaves only
a line in the launcher's log.

With it, the `run.failed` event and its alert email carry the cause:

```
pipeline: training
job_id: churn-r4f7fc4901cbb
state: failed
failed_steps: prepare-data
error: prepare-data: The replica workerpool0-0 exited with a non-zero status of 1. ...
owner: team
why: prepare-data: TypeError in prepare.py, line 39
next: an error in the use case's own code: see the file and line
```

| What went wrong | `owner` | Where the cause comes from |
|---|---|---|
| Your data broke its contract (a check your step attached) | team | the step's failure record |
| A bug in your step's code | team | the step's failure record: file, line, the code line |
| The step's identity was refused a permission | platform | the step's failure record: Pipewright's own message |
| A service perimeter refused a call | platform | the record, or Google's error line |
| The step ran out of memory | team | Google's error line; on Kubeflow the pod's status (`OOMKilled`) |
| The region cannot provide the machine you asked for | team | Google's error line |
| A quota was reached | platform | Google's error line |
| The step's image is missing | platform | Google's error line; on Kubeflow the pod's status |
| Pipewright refused the step because of the use case's settings or data (an entity table that already has a feature's column, a champion whose model is gone, a registration the launcher refused) | team | Pipewright's own message, which says what to change |
| Pipewright refused the step because the environment is missing something (no launcher address, no ID token for the runner, a Ray cluster the launcher did not create) | platform | Pipewright's own message, which says what is missing |
| Anything else | unclear | nothing names the cause: read the step's logs |

The owner comes from a fixed, ordered table of rules (`pipewright.launcher.cause`). No model decides it, so the same
failure always gets the same answer.

## Switching it on

Either place is enough.

**For one use case**, in `usecase.yaml` (no Terraform; for a team without a platform team):

```yaml
spec:
  monitoring:
    failure_records: true     # a failed step records why; run.failed says why and whose
    submit_failed: true       # a run refused at submission is recorded, and not retried
```

**For a whole environment**, with the starter's or Enterprise's Terraform:

```hcl
failure_records = true
submit_failed   = true
```

Then **release the use case**. The piece that writes the record ships in the component image, so the switch takes
effect at the next release. `pipewright doctor --env <env>` says whether a use case's causes are recorded.

To be emailed, name the events in `monitoring.alerts.events` (`run.failed`, `run.submit_failed`) and run
`pipewright alerts sync`.

## What a failure record holds

One small JSON file per failed step:

| Field | Example | Rule |
|---|---|---|
| `usecase`, `release`, `step`, `function`, `run_id` | `churn`, `de6382971ed0`, `prepare-data` | names and IDs |
| `identity` | `pw-retail-run@...` | the service account the step ran as |
| `error_type` | `TypeError`, `Forbidden` | the error's type |
| `message` | `write_table: Forbidden: ... Permission bigquery.tables.get denied on table ...` | **only when Pipewright wrote it**: its messages are value-free already |
| `code` | `prepare.py`, line 39, `prepare_data`, `x = a / b` | the innermost place in your components; the code line **without its comment** |
| `checks` | `tenure_months: 715 values below 0` | only what your step attached |

**What it never keeps:** a message Pipewright did not write (your own errors, pandas', a library's: they can quote
data), rows, values, logs.

Your step attaches its check results before it raises, as the churn example's `validate` step does:

```python
from pipewright_runtime import failure_detail

failure_detail(checks=errors)        # counts and the contract's own limits only, never data values
raise ValueError("data contract failed")
```

## How it works

1. **The step drops the record.** With records on, the release compiles the record folder into every step of your
   pipelines, so every step records, whatever its parameters. A small hook in the component image runs when the step's Python process ends with an
   error. It writes the record under the tenant's own pipeline root (`<pipeline root>/failures/<date>/`), the one
   place the step's runner may write, then lets the step fail exactly as before. If it cannot write, it prints one line
   and gives up. It never changes the step's outcome.
2. **The launcher keeps the cause.** Its hourly check for failed runs reads the record and cleans it again: known
   fields only, names must look like names, text passes the value-free rule once more. A team's step is never trusted
   directly. The cause goes into `run.failed` and into the launcher's `runs/failed/<job>` marker.
3. **No record** (the process was killed, or never started): the cause comes from Google's own error line, or on
   Kubeflow from the failed pod's status. Never from pod logs.

## A run that could not be submitted

With `submit_failed` on, a run the orchestrator refuses (a machine shape the region cannot provide, a refused
permission, an invalid request) is recorded once as `run.submit_failed`, with Google's reason (value-free), the rule
and the owner. A refusal that cannot get better is answered, not retried. A timeout or an unavailable service keeps
the trigger's retry, as before.

## On Kubeflow (private GKE)

- The same record is written: the hook is in the same image.
- A failed Kubeflow run can list no failed step at all. With records on, the launcher reads the status of the run's
  failed pods (the reason a container stopped, such as `OOMKilled`) and takes the step from there.
- Reading pod status needs a role on pods in the Kubeflow namespace. Enterprise's blueprint grants it to the launcher
  when `failure_records` is on. Without it, the cause comes from the failure record only.

## Agents

An agent identity may read the launcher's `runs/failed/` markers (the starter's and Enterprise's Terraform grant it
with `failure_records` on), so the tool server's `runs` tool shows a failed run's cause, on Agent Platform and on
Kubeflow. The markers hold what `run.failed` holds: no data values.

## Cost

One small object per failed step and a few fields on an event: effectively nothing. A refused submission is retried
less, so slightly less than before.
