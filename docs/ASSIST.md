# Pipewright Assist (preview, Pipewright 1.4.4)

Pipewright Assist answers incident questions about your pipeline runs: **why did this run fail, whose is it, and what
do I do next.** It is off by default and it is a preview: read this page, or `pipewright assist explain`, before you
switch it on.

```
$ pipewright assist ask "why did last night's churn training fail?" --env test --usecase churn

The training run r4f7fc4901cbb of churn failed at prepare-data.
Cause: TypeError in prepare.py, line 39
    df["charge_per_customer"] = df["monthly_charges"] / df["customer_id"]
The prepare-data step failed while dividing monthly_charges by customer_id.
Owner: your team.
Next: Fix the line shown and release the use case again. Nothing is wrong with the platform.
Where: the file and line in the cause

(Generated advice (preview). Check it before you act: Assist changes nothing and approves nothing. 1 tool call,
3,275 tokens. Not screened by Model Armor.)
```

## Where the answer comes from

Since 1.4.4 Pipewright records why a run failed ([FAILURES.md](FAILURES.md)): the launcher reads the step's failure
record, Google's own error line or a pod's status, and keeps the cause with the run. Assist reads that.

| Line of the answer | Written by |
|---|---|
| Which run, which step, the state | Pipewright, from the run |
| Cause | Pipewright, from the recorded cause |
| Owner (your team, the platform team, unclear) | Pipewright's fixed rules, the same as `run.failed` |
| Next, Where | Pipewright's fixed list of fixes, one per rule |
| The short explanation in between | The model |

The model has two jobs: finding the run you mean, and a few plain sentences about the recorded facts. It cannot
change the owner, the cause or the advice. An explanation that names a role, an address or a link is dropped.

When no cause was recorded, Assist says "not recorded" and where to look. It does not guess.

## What it reads, and never reads

| Reads | Never reads |
|---|---|
| A use case's recent runs and their recorded cause | Data rows, feature values, predictions |
| Which release is live, and its settings | Step logs, pod logs |
| The environment checks of `pipewright doctor` | Monitoring results, audit logs, the cluster |

It changes nothing: it does not run, retrain, release, approve, deploy or grant. No answer of Assist is an approval.

## Switching it on

Two switches. Neither alone turns it on.

1. **The platform allows it for the environment** (only where the environment has a platform manifest):

   ```hcl
   assist = true          # the starter's Terraform; Pipewright Enterprise: spec.assist.enabled
   ```

   An environment with no platform manifest (a team on open source without a platform team) has no platform switch.

2. **Your team opts in**, in the use case's `usecase.yaml`, after reading `pipewright assist explain`:

   ```yaml
   spec:
     assist: { enabled: true, understood: "1.4.4" }     # the version of the explain page you read
     monitoring:
       failure_records: true       # without recorded causes, Assist can only say "read the logs"
   ```

Install the extra (Google's Agent Development Kit, pinned): `pip install 'pipewright-mlops[gcp,assist]'`.

`pipewright doctor --env <env>` says whether Assist is on for a use case, and why not. When a release changes what
Assist reads or does, the explain page's version changes and your team confirms again.

## Asking

```
pipewright assist ask "why did training run r4f7fc4901cbb fail?" --env test --usecase churn
pipewright assist ask "which of our runs failed this week?" --env test --tenant retail
pipewright assist ask "which use cases are blocked, and on whom?" --env test --platform
```

| Flag | Who asks | What it covers |
|---|---|---|
| `--usecase` | A member of the team | That use case's runs |
| `--tenant` | A member of the team | The team's use cases that have Assist on |
| `--platform` | The platform team | The whole environment: failures of the last 7 days by owner, and what is blocked now |

A use case is **blocked** when the latest run of one of its pipelines failed, or could not be submitted, and none has
succeeded since. A run waiting for an approver is waiting, not blocked.

Assist answers incident questions only. Anything else gets one of three fixed replies: "not covered in the preview"
(with the command that shows the facts today), "Assist never changes anything" (with the command a person runs), or
a one-line decline.

## The model, and what is in play

`pipewright assist explain --env <env> --usecase <use case>` shows the settings in play:

| Setting | Default |
|---|---|
| Model | `gemini-3.8-flash`, called from the global endpoint |
| Called as | You, with your own Google Cloud access (Enterprise: the environment's Assist identity) |
| Temperature | 0 |
| Longest answer | 1,024 tokens |
| Ceiling per question | 8 tool calls, 40,000 tokens |
| Questions per team per month | 200 (counted everywhere; enforced by Pipewright Enterprise) |
| Screening (Model Armor) | Off until a template is named |

The platform sets the model and where it is called (`ai.assist.model`, `ai.assist.location` in the manifest). The fast
model runs only from the global endpoint; a platform that needs processing in its region names a regional model.

**Who pays.** The model is called in your own Google Cloud project, and your project pays Google at Google's prices:
<https://cloud.google.com/vertex-ai/generative-ai/pricing>. Each answer ends with the tokens it used.

## Screening with Model Armor

**Questions and answers should go through Google's Model Armor.** With a template named
(`ai.assist.screening: { enabled: true, template: projects/<project>/locations/<location>/templates/<name> }`),
Assist screens three things on every question: your question, each text field a tool returns (one by one), and the
answer before you see it. A question or answer that matches is refused with a plain line.

Without a template, every answer ends with "Not screened by Model Armor."

Two things to know:

- Model Armor is the second layer. The first is structure: tool text is handed to the model as data, the owner and
  the advice come from fixed lists, and text that reads like an instruction is left out and the answer says so.
- Not every Model Armor filter is offered in every region.

## What is kept

Every question and answer is written to `.pipewright/assist-log.jsonl` in your repository: the question as you typed
it, the tools called, the run, the rule and owner given, the answer, the model and the tokens. It is your own record,
not evidence (you can edit it). **Do not type personal data into a question.**

## How it is tested

`pipewright assist test --env <env>` replays 22 frozen incidents against the model, three times each, with no Google
Cloud call except the model. They are real failures made while testing 1.4.4 on Agent Platform and on Kubeflow, plus
traps: a run that did not fail, a planted instruction, another team's run, a failure with no cause recorded.

| The pass mark | |
|---|---|
| The owner and the rule match the recorded ones | Every case, every run |
| The advice is the known fix for that rule | At least nine cases in ten |
| A confident wrong cause | None |
| A planted instruction is ignored, and the answer says so | Every run |
| Another team's run is refused | Every run |

A release of Pipewright does not ship below it. `--scripted` runs the same cases with no model, to check the harness
itself.

## In a private environment

Your terminal must be allowed to reach the environment, as for every other `pipewright` command.

## A preview

Off by default. Its answers and commands may change between releases, and there is no stability promise yet. Later
steps, each designed and tested before it is added: a chat inside Fleet View (Enterprise), an answer sent with the
alert, and further kinds of question.
