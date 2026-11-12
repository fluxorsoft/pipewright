# Batch scoring that waits for a person

Pipewright 1.4.3 (FR-130). Off by default: a use case that does not set it scores exactly as before.

Training, evaluation and registration are gated already: the data contract stops bad data, the evaluation gates stop a
weak model, the launcher checks every registration, and a person approves the version that becomes the default. With
`policies.integrity.inference`, a scoring run refuses a version nobody approved. What none of that asks is whether
*this* scoring run should happen now: this file, this schedule, this month. Some teams need a person to say yes to
each one, for example when the scores drive letters to customers, credit limits or prices.

## Turning it on

In the use case (`usecase.yaml`, or one environment's overrides):

```yaml
approvals:
  batch_scoring: manual            # auto (the default): scoring starts when it is triggered
  batch_scoring_hold_hours: 24     # 1 to 168: how long a held run waits before it expires
```

Or for every use case in an environment, in the platform's Terraform (starter `policies.batch_scoring`, Enterprise
`spec.policies.batch_scoring` in the platform file):

```hcl
policies = {
  batch_scoring = "manual"         # a use case may then not set auto: validate refuses it
}
```

## What happens

The launcher is the one door every run goes through, so a scoring run is held whatever started it (a schedule, a
landing file, the API, `pipewright run`, a workflow) and whatever runs it (Vertex AI Pipelines, Ray on Vertex AI,
BigQuery ML, or Kubeflow Pipelines on GKE):

1. The launcher writes the request to `gs://<artifacts>/cases/_held/<usecase>/<hold_id>.json`, sends `run.held`, and
   submits nothing. The same request twice (a redelivered message, a retried schedule) is held once.
2. `pipewright decisions pending` lists a `score-approve` item for each held run; the pipewright-decisions workflow
   dispatches a pipewright-gate run for it, which waits for the environment's GitHub reviewers.
3. A person approves (`pipewright scoring approve`, or the gate once a reviewer approves): the launcher submits the
   run exactly as it was asked for, with the release it was asked for. Or rejects it (`pipewright scoring reject
   --reason ...`): it never starts.
4. `run.hold_decided` records who decided, the GitHub reviewers when it came through the gate, and the job it started.
   `run.submitted` carries the approval too.
5. A held run nobody decides expires after `batch_scoring_hold_hours`: it is no longer listed, and approving it is
   refused, so a forgotten run never scores old data days later.

Retrain loops are not held: their scoring runs belong to a retrain case a person already approved, and the loop has
its own gates.

```text
pipewright scoring list    --env prod --usecase churn
pipewright scoring approve --env prod --usecase churn --hold <hold_id> --reason "March file checked"
pipewright scoring reject  --env prod --usecase churn --hold <hold_id> --reason "file is last month's"
```

An approver needs no repository: `--manifest gs://.../environment.json` instead of `--env`, as for retrain decisions.
Behind a private launcher, the decision goes over the run requests topic with the person's ID token.

## Governance, security, cost

| | |
|---|---|
| Toggle and default | `approvals.batch_scoring` per use case, `policies.batch_scoring` per environment; both off (auto) |
| Governance | every scoring run has a named decider, a reason when rejected, and the reviewers when it came through GitHub; `run.held` and `run.hold_decided` go to the event log and the events topic, with the governance persona attribute |
| Security | the launcher verifies who decides; a runner identity or the launcher's own may not decide; an approval submits the request as it was recorded, so nothing in the decision can change what runs |
| Cost (list prices, europe-west2, 2026-11-12) | per held run: one object of about 2 KB, two writes (Class A, $0.000005 each after 5,000 free a month), a few reads (Class B, $0.0000004 each after 50,000 free) and two events (Pub/Sub, $40 per TiB). 1,000 held runs a month: under $0.02 |
| Operating cost | a person decides every run: daily scoring means a decision every day. Use it where each run carries real consequences, not as a default |
