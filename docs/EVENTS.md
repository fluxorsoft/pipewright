# Events

<!-- Generated from src/pipewright/observability/catalogue.py by `python -m pipewright.observability.catalogue > docs/EVENTS.md`. Do not edit by hand. -->

Pipewright reports everything it does as an event: one structured log line and one message on the environment's events topic (`observability.events_topic` in the manifest; the starter and the Enterprise blueprints always set it). Nothing is sent anywhere else: each team subscribes to the one topic with a filter and takes exactly what it tracks to its own tools (Slack, PagerDuty, Datadog, Splunk, BigQuery, a ticket queue). Email from `pipewright alerts sync` is one such consumer.

## Filtering

Every message carries these attributes, for Pub/Sub subscription filters:

| Attribute | Value |
|---|---|
| `event_type` | the type, such as `run.failed` |
| `usecase`, `environment`, `tenant` | where it happened, when they apply |
| `pw_<persona>` | `"1"` for each persona the event is for (since 1.4.3) |

A subscription filter is at most 256 bytes, so filter by persona attribute or by family prefix, not by a long list of types:

```
attributes:pw_sre                                    # everything the platform team tracks
hasPrefix(attributes.event_type, "ab_test.")         # one family
attributes:pw_ml AND attributes.usecase = "churn"    # one team's use case
```

`pipewright events --persona sre --filter` prints a persona's filter; `pipewright events --json` lists everything below.

## Personas

| Persona | Who | Events |
|---|---|---|
| `ml` | data scientists and ML engineers who own a use case | 63 |
| `sre` | the team that runs the environments, the launcher and serving | 33 |
| `governance` | risk, model approvers and auditors | 43 |
| `product` | what is live and how it performs | 18 |
| `data` | tables, features and online stores the models read | 9 |
| `finops` | what costs money and who started it | 4 |

## Every event

Decision events carry who decided and how it was approved. Email: `monitoring.alerts.events` (or `retraining.decisions.notify`) can send it by email.

### run: Pipeline runs: submitted, refused, failed

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `run.submitted` | A run request (file, schedule, API, Pub/Sub) became a pipeline job. | launcher | ml, sre | 1.0.0 |  |
| `run.duplicate` | A request reused a run key: nothing new started. | launcher | sre | 1.0.0 |  |
| `run.integrity` | A run was checked against what was approved before it was submitted (refused or warned). | launcher | governance, sre, ml | 1.4.0 | email |
| `run.failed` | A run ended failed or cancelled (the launcher's tick). | launcher | ml, sre | 1.2.0 | email |
| `run.unregistered` | A training run the gates approved ended succeeded without registering its model. | launcher | ml, sre, governance | 1.4.3 | email |

### release: Releases going live, promoted, rolled back

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `release.live` | A release became the live one, with who released it. | CLI | ml, sre, governance, product | 1.0.0 | decision |
| `release.promoted` | A release arrived in another environment by promotion. | CLI | ml, sre, governance, product | 1.1.0 | decision |
| `release.rolled_back` | The live release was moved back to an earlier one. | CLI | ml, sre, governance, product | 1.0.0 | decision |

### model: Model versions: registered, approved, deployed, protected

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `model.registered` | A new model version is in the registry (and whether it is the champion). | pipeline step | ml, governance | 1.0.0 | email |
| `model.registration_skipped` | The register step skipped on purpose (an exact replay ties itself). | pipeline step | ml | 1.4.3 |  |
| `model.approved` | A person approved a model version. | CLI | governance, ml | 1.0.0 | decision |
| `model.promoted` | A model version was copied to another environment's registry. | CLI | governance, ml, sre | 1.1.0 | decision |
| `model.deployed` | A version started serving on an endpoint or KServe (canary, A/B arm or full). | CLI | ml, sre, product, governance | 1.0.0 | decision |
| `model.undeployed` | A version stopped serving on an endpoint. | CLI | ml, sre, product | 1.4.2 | decision |
| `model.duplicated` | Two models carry a use case's model name; the newer one's versions are hidden. | launcher | ml, sre | 1.4.3 | email |
| `model.lineage.recorded` | A run's lineage record (inputs, code, model) was written. | pipeline step | governance | 1.1.0 |  |
| `model.integrity` | An approved model record changed between deploys (the integrity watch). | launcher | governance, sre | 1.4.1 | email |
| `model.integrity.check` | The integrity watch ran: its heartbeat, with what it checked. | launcher | sre, governance | 1.4.1 |  |
| `model.lock.renewed` | Approved files in use had their deletion lock renewed. | renewer job | sre, governance | 1.4.1 |  |
| `model.lock.failed` | A lock on approved files in use could not be renewed. | renewer job | sre, governance | 1.4.1 |  |
| `model.lock.reconciliation` | The renewer's summary: what it locked, renewed or released. | renewer job | sre | 1.4.1 |  |
| `model.restored` | A changed approved file was restored from its locked copy, with who did it. | CLI | governance, sre | 1.4.2 |  |
| `model.retired` | A model version was retired from use. | CLI | governance, ml | 1.4.2 |  |

### training: Training results and their gates

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `training.evaluated` | The gates judged a trained model: approve or reject, with the metrics. | pipeline step | ml, governance | 1.0.0 | email |

### data: Data read by a run: snapshots, features, labels, erasure

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `data.snapshot.written` | The rows a run read were kept, so it can be replayed exactly. | pipeline step | ml, governance, data | 1.1.0 |  |
| `data.snapshot.replayed` | A replay read exactly an earlier run's rows. | pipeline step | ml, governance | 1.1.0 |  |
| `data.features.resolved` | A run joined its features from feature tables, as of each row's cut-off. | pipeline step | ml, data | 1.1.0 |  |
| `data.labels.resolved` | A run joined its labels from the label source, within the maturity window. | pipeline step | ml, data | 1.1.0 |  |
| `data.erased` | A person's rows were erased by key from a use case's tables. | CLI | governance, data | 1.0.0 |  |

### monitoring: Drift and performance of models in use

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `monitoring.recorded` | A scoring run's drift and performance were recorded. | pipeline step | ml | 1.0.0 |  |
| `monitoring.alert` | Drift or a performance drop crossed its threshold. | pipeline step | ml, product, sre | 1.0.0 | email |

### retrain: Retrain decision cases

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `retrain.requested` | A pipeline released before the decision cases asked for a retrain. | pipeline step | ml | 1.0.0 | email |
| `retrain.proposed` | Drift or performance opened (or added to) a retrain decision case. | launcher | ml, governance | 1.1.0 | email |
| `retrain.approval` | One approver group recorded its approval of a case. | launcher | governance, ml | 1.1.0 | decision, email |
| `retrain.approved` | A case reached its quorum: the retrain goes ahead. | launcher | governance, ml | 1.1.0 | decision, email |
| `retrain.declined` | A case was declined. | launcher | governance, ml | 1.1.0 | decision, email |
| `retrain.snoozed` | A case was snoozed until a date. | launcher | governance, ml | 1.1.0 | decision, email |
| `retrain.reopened` | A snoozed or declined case came back. | launcher | ml, governance | 1.1.0 | email |
| `retrain.escalated` | A case waited too long for a decision. | launcher | governance, ml | 1.1.0 | email |
| `retrain.digest` | The weekly summary of open cases. | launcher | governance, ml | 1.1.0 | email |
| `retrain.refused` | The approved retrain trained a model the gates refused. | launcher | ml, governance | 1.2.0 | email |
| `retrain.training_finished` | The approved retrain's training ended. | launcher | ml | 1.2.0 |  |
| `retrain.training_failed` | The approved retrain's training failed. | launcher | ml, sre | 1.2.0 | email |
| `retrain.training_retried` | The approved retrain's training was submitted again. | launcher | ml, sre | 1.2.0 | email |

### loop: The closed retrain loop across environments

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `loop.training` | The loop started training in the test environment. | launcher (loop) | ml | 1.1.0 |  |
| `loop.candidate` | The loop's training registered a candidate. | launcher (loop) | ml | 1.1.0 |  |
| `loop.rejected` | The loop's training registered no candidate (gates or comparison). | launcher (loop) | ml | 1.1.0 |  |
| `loop.failed` | The loop's training never started or ended failed. | launcher (loop) | ml, sre | 1.1.0 |  |
| `loop.validating` | The candidate is being validated. | launcher (loop) | ml | 1.1.0 |  |
| `loop.validated` | The candidate passed validation. | launcher (loop) | ml, governance | 1.1.0 |  |
| `loop.validation-failed` | The candidate failed validation. | launcher (loop) | ml, governance | 1.1.0 |  |
| `loop.canary` | The candidate serves a canary share in production. | launcher (loop) | ml, sre, product | 1.1.0 |  |
| `loop.verifying` | The promoted version is being verified in production. | launcher (loop) | ml, sre | 1.1.0 |  |
| `loop.verification-failed` | Verification failed; a rollback restores the previous version. | launcher (loop) | ml, sre, governance | 1.1.0 |  |
| `loop.rolled-back` | The loop rolled production back to the previous version. | launcher (loop) | ml, sre, product | 1.1.0 |  |
| `loop.closed` | The loop finished: the new version is in production. | launcher (loop) | ml, product, governance | 1.1.0 |  |
| `loop.paused` | A person paused the loop. | launcher (loop) | ml, governance | 1.1.0 |  |
| `loop.resumed` | A person resumed the loop. | launcher (loop) | ml, governance | 1.1.0 |  |

### ab_test: A/B tests of models

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `ab_test.started` | An A/B test started: control, treatment and the treatment's share. | CLI | ml, product, governance | 1.4.3 |  |
| `ab_test.allocation_changed` | A test's treatment share changed. | CLI | ml, product | 1.4.3 |  |
| `ab_test.paused` | A test was paused (every unit served by control). | CLI | ml, product | 1.4.3 |  |
| `ab_test.resumed` | A paused test resumed. | CLI | ml, product | 1.4.3 |  |
| `ab_test.stopped` | A test was stopped before a decision. | CLI | ml, product, governance | 1.4.3 |  |
| `ab_test.analysed` | A test's analysis ran: the verdict so far and whether a decision is due. | CLI or scheduled analysis | ml, product | 1.4.3 |  |
| `ab_test.guardrail_breached` | A business, technical or integrity guardrail of a test was breached. | launcher | ml, product, sre | 1.4.3 |  |
| `ab_test.decision` | An approver group recorded its decision on a test. | launcher | governance, product | 1.4.3 |  |
| `ab_test.decided` | A test was decided: the winner and why. | CLI | ml, product, governance | 1.4.3 |  |

### online: Online feature stores

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `online.synced` | An online feature store was loaded or its sync started. | CLI | data, ml | 1.4.2 |  |
| `online.stale` | An online store has not been refreshed within its limit. | launcher | data, sre, ml | 1.4.2 | email |
| `online.fresh` | A stale online store is fresh again. | launcher | data, sre | 1.4.2 |  |
| `online.billed` | Lookups billed against an online store (BigQuery bytes). | launcher | finops, data | 1.4.2 | email |

### serving: Shared serving pools

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `serving.pool.change` | A shared serving pool gained or lost a model, or changed size. | CLI | sre, finops | 1.4.2 |  |

### synthetic: Synthetic data

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `synthetic.generated` | A synthetic table was written. | pipeline step | ml, data, governance | 1.0.0 |  |
| `synthetic.evaluated` | The synthetic candidates were judged against the privacy and utility gates. | pipeline step | ml, governance | 1.0.0 |  |
| `synthetic.rejected` | Every synthetic candidate failed its gates: nothing was published. | pipeline step | ml, governance | 1.0.0 |  |

### ray: Ray clusters

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `ray.cluster.created` | A Ray cluster was created for a run step (with its deadline). | launcher | sre, finops | 1.0.0 |  |
| `ray.cluster.used` | A run step used a standing Ray cluster. | pipeline step | sre, finops, ml | 1.0.0 |  |

### policy: Platform policy exceptions

| Event | When | Sent by | For | Since | |
|---|---|---|---|---|---|
| `policy.override` | A time-boxed break-glass exception to a platform policy is in force. | launcher | governance, sre | 1.4.1 |  |

78 event types in 14 families.
