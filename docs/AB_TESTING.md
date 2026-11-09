# A/B testing of models

A gate tells you a candidate model scores better on held-out data. It does not tell you the candidate is better for
the business: more renewals, fewer complaints, more revenue. An A/B test answers that on live traffic. Some
customers are scored by the champion (the control), some by the candidate (the treatment), and a business outcome is
compared between the two groups, against a plan fixed before the test starts. A person concludes.

Pipewright runs the whole test as part of the platform: the assignment, the routing, the record of who saw which
model, the analysis, the guardrails, the decision and its approvers, and the lineage of both versions. It is off by
default: with `ab_test.enabled` false nothing changes.

## How it works

**Sticky assignment.** Each unit (a customer, an account, a device: a column of the data contract) is assigned by a
hash of the test's name and the unit. The first 7 bytes of SHA-256(`<name>:<unit>`) are compared with the treatment's
share in basis points. The same function runs in batch scoring, in SQL (BigQuery ML) and in the online gateway, so a
customer keeps its arm wherever it is scored. A different test name gives a different, independent split.

**Batch.** A scoring run of a use case with a running test gets an `ab_test` parameter. The scoring step scores each
unit with its arm's model and writes `ab_test` and `arm` beside each score in `inference.output_table`. Being scored
is the exposure. The treatment's files are checked against its approval (every file's generation and SHA-256) before
any customer is scored with it. When they cannot be checked under `policies.integrity.mode: refuse`, the run scores
everyone with the control and records why.

**Online.** `pipewright ab start` deploys the treatment on its own endpoint, `<usecase>-ab-endpoint` (dedicated
machines like the control's, or the use case's shared pool, where it costs no extra machines). The lookup gateway
becomes the router: it assigns each call's units, sends each group to its arm, and answers with each unit's arm. It
writes one exposure line per unit (test, arm, unit, the answering deployed model, the release) to Cloud Logging, off
the request path. A log sink copies those lines into the tenant's dataset (table `pipewright_ab_exposure`). A use case
whose callers send features gets a router-only gateway during the test: callers post `instances` with their `units`.

**Shadow.** `mode: shadow` sends every call to the control, which answers, and a copy to the treatment in the
background. Only a comparison is logged (how many scores, the mean absolute difference, how often they agree, the
treatment's latency and failures). Nothing acts on the treatment's answers. A shadow shows how the candidate behaves
on live calls; it cannot show it is better for the business, so a shadow always ends with the control. Start an A/B
test next, or deploy the candidate as usual.

## Turn it on

The test is part of the use case, reviewed and released like the rest: define it in `config/ab_test.yaml`.

```yaml
ab_test:
  enabled: false                       # on per environment, below
  name: lapse-v7                       # once per use case and environment
  unit: customer_id
  allocation: { treatment_bp: 1000 }   # 10% of customers
  metric:
    name: renewed
    source: bq://my-project.my_dataset.renewals
    time_column: renewed_at
    window_days: 30
    minimum_effect: 0.03
  guardrails:
    business:
      - { name: complaints, source: bq://my-project.my_dataset.complaints, time_column: raised_at, direction: lower,
          max_drop: 0.05 }                     # breached when 5% worse and significant
  decision:
    approvals: { required: [model-risk] }
```

Switch it on in the environment that runs it, with that environment's candidate (version numbers differ between
environments), in `environments/prod.yaml`:

```yaml
ab_test:
  enabled: true
  arms: { treatment: { model_version: "7" } }
```

Only `enabled`, `arms`, `allocation`, `duration`, `publish` and `decision` may differ per environment; the rest is the
same everywhere. The churn example's `config/ab_test.yaml` shows every key with its default; `pipewright explain
ab_test` describes each. For a batch test, add `ab_test` and `arm` (STRING, NULLABLE) to the predictions table's
schema: `validate` says so when they are missing.

## Run a test

```bash
pipewright ab plan --env prod --usecase churn --baseline 0.30 --per-day 400   # units needed, days, the arm's cost
pipewright ab start --env prod --usecase churn                                # a deploy: approvals.deploy_model
pipewright ab status --env prod --usecase churn
pipewright ab analyse --env prod --usecase churn                              # now, as you
pipewright ab set --allocation 500 --reason "fewer customers" ...             # only down
pipewright ab pause | resume | stop --reason "..." ...
pipewright ab decide --winner treatment --reason "..." --group model-risk ... # an approver group
pipewright ab conclude --reason "..." ...                                     # the groups' winner
```

`start` refuses a treatment without passed gates or files that match its approval, a second test on the same use
case, a test during a closed retrain loop, forecasts and unsupervised models, and a unit that is not a contract column.
The configuration is fixed at the start (its digest is recorded): an analysis refuses a changed `name`, `unit`,
`mode`, `metric`, `guardrails`, `analysis` or `arms`. Stop the test and start a new one instead.

## The analysis

The launcher starts the analysis on `analysis.schedule` (cron, UTC) as the tenant's runner, in the tenant's job, and
records the result; `pipewright ab analyse` runs it at once as you. Each run appends a row of aggregates to the results
table (`<usecase>_ab_results`) and emits `ab_test.analysed`. It never writes a unit.

1. **The share checks first.** The share per arm against the plan (a chi-square test, p < 0.001) and, because that test
   is weak at small samples, a direct check against a binomial bound. Units exposed to both arms, and the gateway's
   counts of unassigned, dropped and mismatched calls, and calls to the treatment's endpoint that the gateway did not
   send, also block a decision. Any of them means the comparison is not clean.
2. **The metric.** Per arm on exposed units, outcomes within `window_days` of each unit's first exposure, only for
   units whose window has closed. The difference and relative lift with a 95% interval: a two-proportion z-test, a
   Welch t-test for means, the delta method for ratios.
3. **The sample.** `ab plan` sizes the test from `minimum_effect`, `alpha`, `power` and the baseline, and warns when it
   cannot finish before `max_days`. A decision is due at the planned sample after `min_days`, or at `max_days`.
4. **CUPED** (`analysis.cuped`): a covariate per unit from before the test (a feature table, as of the start) removes
   variance the treatment could not have caused: the same confidence with fewer units.
5. **Sequential testing** (`analysis.method: sequential`): an always-valid p-value (mSPRT) that may be looked at every
   day and only falls, so a clear result can end a test early without inflating false positives.
6. **Segments** (`analysis.segments`): the result per value of contract columns, for batch tests.

## Guardrails

**Technical (online).** The gateway keeps a breaker on the treatment's last calls (`breaker.window`): above
`breaker.max_error_rate` the treatment's units go to the control at once, in that gateway instance. Each hour the
launcher reads the gateway's lines and Cloud Monitoring. It compares the arms as the canary does (error rate and
median latency against the control's endpoint, once the treatment has answered `min_requests`). A confirmed breach
sets the allocation to 0 on every instance, opens the decision and emits `ab_test.guardrail_breached`. The arm is kept
for diagnosis until someone concludes or stops the test.

**Business.** Each `guardrails.business` metric is analysed like the primary one. It is breached when the treatment
is worse by more than `max_drop` (relative) and the harm is significant (one-sided at `analysis.alpha`); a breach
opens the decision.

## Decisions

A test needs a person to end it. With `decision.approvals.required`, every listed approver group decides through its
topic, as retrain decisions do (`ab decide`), and one person has one voice. Groups that disagree leave the decision
open. `pipewright decisions pending` lists `ab-decide` and `ab-conclude` items with the analysis's recommendation, for
GitHub environments or a person.

`ab conclude` with the treatment as winner makes it the default through the normal approval (the released gates, its
files checked against its approval), removes its arm and returns routing to the use case's endpoint. Deploy the new
default there as usual (`deploy-model`). Any other outcome removes the treatment. The treatment can win only on
evidence: a decision-ready analysis without blocked share checks.

## The record

Events: `ab_test.started`, `ab_test.allocation_changed`, `ab_test.paused`, `ab_test.resumed`, `ab_test.analysed`,
`ab_test.guardrail_breached`, `ab_test.decision`, `ab_test.decided`, `ab_test.stopped`. The treatment's approval for
the test is a `model.approved` event with `ab_test`, and its arm's deploys are `model.deployed` and `model.undeployed`
with `ab_test`. `pipewright lineage` shows the tests a version was in. The test's record (configuration digest,
arms, allocation history, the newest analysis, the decision and its approvers, the history) is kept in
`gs://<artifacts>/ab-tests/<usecase>/<name>.json`.

## A feed for the team's own tools

`ab_test.publish.events` copies the test's events (aggregates) to the tenant's feed topic, which the platform makes
per tenant (`tenants.<t>.ab_test_feed`). `publish.exposures` also copies exposure lines, with a keyed hash of the unit
instead of the unit, only where the platform sets `policies.ab_test.feed_exposures`. A pseudonymised unit is still
personal data where the unit is a person. BigQuery stays the record; the feed is a copy, delivered at least once and
unordered.

## For the platform team

In the starter, give the team `ab_test` in `terraform.tfvars`:

```hcl
tenants = { churn = { ab_test = {} } }                       # or { feed = true, feed_exposures = true, ... }
```

This makes the exposure sink into the team's dataset, the gateway's identity (writes logs, may predict), the job the
scheduled analysis runs in, and the launcher's reads (logs, Monitoring) and test records. Where endpoints are in
`serving.yaml`, add `<usecase>-ab` beside the use case's endpoint, with the same tenant and privacy. The arm endpoint
admits any identity with predict on the project (Agent Platform checks predict on the project, not the endpoint), so
the launcher counts calls to it that the gateway did not send; any blocks a decision.

The manifest's `policies.ab_test` sets the rules: `allowed`, `required_approvers` (added to every test), the most a
treatment may get (`max_treatment_bp`), the shortest `min_days`, `required_guardrails` (business guardrails every test
declares), `overlap` (another use case's active test on the same unit at the start: `allow`, `warn` or `refuse`) and
`feed_exposures`.

**Pipewright Enterprise** makes the same resources in its blueprint (CMEK on the feed and its key), takes
`spec.policies.ab_test`, shows every test in Fleet View's Experiments pages (with tests that share a unit across use
cases and environments), and adds evidence control EV-18: a version's tests ran as promised (digest, allocation never
raised, the treatment approved for the test) and were decided by every required group.

## Cost

List prices, before a region's: a dedicated treatment arm about $80 a month per n1-standard-2 replica, nothing extra
on a pool; the gateway about $0.40 per million calls plus its CPU time; exposures in BigQuery about 7 cents a month per
million lines kept (log routing is free); the analysis one query per run. The feed is $40 per TiB published.

## Limits

- An online test needs `model.serving.target: vertex`: the gateway routes to Agent Platform endpoints. KServe
  (private GKE) runs batch tests.
- One active test per use case. Tests on the same unit in different use cases overlap; `policies.ab_test.overlap`
  decides what happens.
- Forecasts and unsupervised models have no unit-level outcome to compare, and are refused.
