# Pipewright

**Configurable, governed MLOps on Google Cloud.** Build, release, run and monitor models from configuration, with a
human in the loop for every retrain decision.

Pipewright is open source under the [Apache License 2.0](https://github.com/fluxorsoft/pipewright/blob/main/LICENSE) and maintained by
[Fluxor Software](https://www.fluxorsoft.com/pipewright.html).

> **Status: 1.1.0.** Working end to end on Google Cloud, including private environments with
> customer-managed keys, enforced organization policies and an enforced VPC Service Controls perimeter. The
> configuration format is stable ([STABILITY.md](https://github.com/fluxorsoft/pipewright/blob/main/STABILITY.md)). Releases are on GitHub (the wheel as a release
> file, the launcher image in GitHub Container Registry) and on PyPI as `pipewright-mlops`.

## At a glance

![Pipewright (open source) on Google Cloud: the building blocks and the main flows](https://github.com/fluxorsoft/pipewright/raw/main/docs/images/pipewright-architecture.png)

<sub>Editable source: [docs/images/pipewright-architecture.drawio](https://github.com/fluxorsoft/pipewright/blob/main/docs/images/pipewright-architecture.drawio) (draw.io, official Google Cloud icons).</sub>

**The same environment on private GKE.** One setting in the manifest (`orchestrator: kfp`) runs the same pipelines
on Kubeflow Pipelines in a private GKE cluster, with Ray on KubeRay and online serving on KServe. Templates,
contracts, the CLI and everything outside the cluster stay the same.

![Pipewright (open source) on Google Cloud with private GKE: Kubeflow Pipelines, KubeRay and KServe in the cluster](https://github.com/fluxorsoft/pipewright/raw/main/docs/images/pipewright-gke-architecture.png)

<sub>Editable source: [docs/images/pipewright-gke-architecture.drawio](https://github.com/fluxorsoft/pipewright/blob/main/docs/images/pipewright-gke-architecture.drawio).</sub>

The platform team builds the environment once and publishes an **environment manifest**: the names of everything a
team may use. ML teams then work only with their own configuration and code; the `pipewright` CLI and the shared
launcher do the rest.

## Building blocks: what you need on Google Cloud

Pipewright does not create infrastructure. Before an ML team runs `pipewright`, the platform side provides these
resources in each environment's project and describes them in the manifest
(`src/pipewright/config/schemas/manifest.schema.json`).

| Building block | Google Cloud service | What Pipewright uses it for | Needed |
|---|---|---|---|
| Artifacts bucket | Cloud Storage | The environment manifest, release contracts and live pointers, pipeline roots, retrain cases | Always |
| Landing bucket | Cloud Storage, with a Pub/Sub notification | Files dropped under a team's prefix start runs | For file triggers |
| Registries | Artifact Registry: a Docker and a KFP repository (Python optional) | Component images and compiled pipeline templates, pinned by digest | Always |
| Pipelines | Agent Platform Pipelines (formerly Vertex AI Pipelines), or Kubeflow Pipelines v2 in a private GKE cluster (manifest `orchestrator: kfp`) | Training and inference runs | Always (one of the two) |
| Kubernetes (private GKE only) | GKE: private nodes and endpoint, the API through its DNS endpoint, Workload Identity; Kubeflow Pipelines, KubeRay and optionally KServe installed, their images mirrored into Artifact Registry | Kubeflow runs as each team's identity, Ray clusters, online serving, optionally schedules as CronJobs and the launcher as a pull worker | With `orchestrator: kfp` |
| Models | Model Registry; Agent Platform endpoints or KServe for online serving | Versions, the champion, approvals; serving (canary on endpoints) | Always (serving optional) |
| Team data | BigQuery: one dataset per team | Raw data, predictions, metrics and monitoring tables, synthetic data | Always |
| Launcher | Cloud Run, from `images/launcher.Dockerfile`, with its own service account | Turns files, schedules, API calls and run requests into pipeline runs; handles retrain decisions | Always |
| Identities | IAM service accounts: the launcher, one runner per team, CI (Workload Identity Federation) | Every pipeline step runs as its team's runner; nothing uses keys | Always |
| Messaging | Pub/Sub topics: landing events, run requests, lifecycle events, one per approver group | Triggers, runs from inside a private network, events for alerts and audit, approvals | Events and approvals recommended; run requests for private networks |
| Schedules | Cloud Scheduler, or Kubernetes CronJobs on private GKE | Scheduled runs; the hourly retrain tick and weekly digest | For schedules and retrain decisions |
| Alerts | Cloud Logging and Cloud Monitoring | Labelled logs, email alerts per use case, decision emails | Recommended |
| Encryption | Cloud KMS (customer-managed keys) | Applied to pipeline jobs, models, endpoints and BigQuery queries | Optional (regulated environments) |
| Private network | VPC, Private Service Connect, Private Services Access, VPC Service Controls | No public endpoints; pipeline workers peered with your network | Optional (regulated environments) |

**APIs to enable:** `aiplatform`, `artifactregistry`, `bigquery`, `cloudscheduler`, `iam`, `iamcredentials`,
`logging`, `monitoring`, `pubsub`, `run`, `storage` (all `.googleapis.com`), plus `cloudkms` for customer-managed
keys and `container` for private GKE.

**Setting up the platform side.** This repository does not include Terraform for the environment. If you would like
help building one, or the Terraform we use, email [hello@fluxorsoft.com](mailto:hello@fluxorsoft.com) or open a
[GitHub Discussion](https://github.com/fluxorsoft/pipewright/discussions): we are happy to share, and it helps us
shape what comes next.

---

## The problem

Most organisations that put machine learning into production on Google Cloud hit the same gaps, team after team:

1. **Every ML team rebuilds the same plumbing.** Triggers, release scripts, registry conventions, drift monitoring,
   alerting and IAM are written again for each use case. Months pass between a working notebook and a model in
   production, and every team does it slightly differently.
2. **A pipeline run is not a release.** Ask "what exactly is running in production, and can we go back?" and the
   answer is usually a guess: images tagged `latest`, templates overwritten in place, no single record of what was
   approved.
3. **Platform and ML teams block each other.** ML teams wait on infrastructure tickets for buckets, datasets and
   permissions; platform changes break pipelines nobody told them about. There is no contract between the two.
4. **Settings live in code.** A gate threshold, a drift limit or a machine size needs a code change and an engineer.
   Dev and production quietly drift apart, so the model you tested is not quite the model you ship.
5. **Drift alerts lead to blind retraining, or to nothing.** Drift says the inputs changed, not why. An upstream
   data bug, a seasonal blip and a real change look the same. Automatic retraining bakes the bug into the next
   model or fits the blip; retraining on the same data does not improve anything. Ignoring alerts is worse.
6. **Governance is assembled after the fact.** Auditors and model-risk teams ask which data trained a model, who
   approved it, where it runs, which key encrypts it and why it was retrained. The answers come from spreadsheets
   and memory.
7. **Regulated environments break the usual tools.** Customer-managed keys on every query, no public endpoints,
   VPC Service Controls and enforced organization policies stop most off-the-shelf MLOps tooling at the door.

## How Pipewright closes them

| Gap | What Pipewright does |
|---|---|
| 1. Rebuilt plumbing | One shared launcher per environment starts runs from file drops, schedules and API calls for every team. A use case is a folder of configuration plus your own pipeline code; `pipewright new-usecase` starts one from a working example |
| 2. Runs are not releases | `pipewright release` builds the component image and every pipeline template, pins both by digest, writes a **write-once release contract**, verifies it, then moves a live pointer. `rollback` moves the pointer back |
| 3. Teams blocking each other | The platform team publishes one **environment manifest** per environment: the names of everything a team may use. App configuration refers to those names and never creates infrastructure. `pipewright doctor` checks the live environment and says who to ask for anything missing |
| 4. Settings in code | Data contract, features, training, evaluation gates, triggers, monitoring and retraining policy are declared in YAML, validated against JSON Schemas and checked against each other. Only a fixed list of keys may differ per environment, so production is the same model judged the same way |
| 5. Blind retraining | Drift or a performance drop opens a **retrain decision case** with a short decision pack (what moved, the trend, data checks, the champion, what retraining would use). Named approver groups approve, decline or snooze with their own identity; nothing trains until the required groups agree |
| 6. Governance after the fact | Every key moment is a lifecycle event and a labelled log line: releases, runs, evaluations, registrations, approvals, deployments, alerts and decisions, each with who did it. Tools and auditors read the record instead of reconstructing it |
| 7. Regulated environments | The manifest's `security` block (CMEK key, network, reserved ranges) is applied to every pipeline job, model, endpoint and BigQuery query at run time. Run requests travel over Pub/Sub, so the launcher can stay internal-only |

## What it looks like

A use case's operations file, owned and reviewed by the ML team:

```yaml
# usecases/churn/config/operations.yaml
monitoring:
  output_table: churn_model_monitoring
  drift: { enabled: true, method: psi, threshold: 0.2 }
  performance: { enabled: true, metric: auc, min: 0.7,
                 labels: { table: churn_outcomes, key: customer_id, label: churned } }
  alerts: { notify: [ml-team@example.com], events: [monitoring.alert, training.rejected] }

retraining:
  mode: propose                     # off | propose | candidate (trains, never promotes)
  when: [drift, performance]
  decisions: { notify: [ml-team@example.com], publish: true }
  approvals: { required: [ml-leads, model-risk] }
  snooze: { max_days: 14 }
```

And the lifecycle, from the MLOps repository:

```bash
pipewright new-usecase --name churn --tenant retention --owner team-retention
pipewright validate     --env dev                     # schemas, references, cross-checks, offline
pipewright doctor       --env dev                     # read-only checks against the live project
pipewright tables sync  --env dev                     # create tables; additive changes only
pipewright release      --env dev                     # build, pin by digest, verify, go live
pipewright run          --env dev --usecase churn --pipeline inference --source-uri gs://...
pipewright monitor status --env dev --usecase churn   # drift, performance, open retrain cases
pipewright retrain show   --env dev --usecase churn   # the decision pack
pipewright retrain approve --env dev --usecase churn --reason "real shift after the price change"
```

## Features

| Stage | Capabilities |
|---|---|
| Configure | Multi-file use cases (one concern per file), JSON Schema validation with path-qualified errors, `${...}` references to manifest names, per-environment overrides on a fixed list of keys, `resolve` to print the effective configuration |
| Data | Data contracts (types, ranges, allowed values, uniqueness, minimum rows), CSV and Parquet on GCS or BigQuery tables and views as sources, **features from feature tables** joined point in time (no value from after a row's cut-off reaches training), tables in BigQuery, Parquet on GCS (with external tables) or both, from one schema file |
| Train | Containers, or **BigQuery ML** with no pipeline code (classification, regression, k-means, PCA, forecasts, imported models); random, stratified and time splits; tuning with parallel trials; experiments; compute per step set in configuration (`steps`), patched into the compiled template with no code change; **Ray** for a step that needs a cluster: on Agent Platform, or on KubeRay in private GKE; one cluster per run, sized in `steps.<step>.ray` within the platform's limits and deleted when the step ends, or on private GKE a standing cluster per team that scales its workers to zero |
| Evaluate | Metric gates, slice gates, calibration, explainability, comparison with the current champion on the same data, a model card |
| Release | Component image and pipeline templates by digest, write-once release contracts, a verify gate before going live, live pointers, rollback, several pipelines per use case (training and inference) released together |
| Run | A shared, multi-tenant launcher on Cloud Run, or inside a private GKE cluster pulling its subscriptions: file drops, schedules (Cloud Scheduler, or Kubernetes CronJobs where the platform's manifest says so), API calls and Pub/Sub run requests; deterministic run IDs, so retries never start duplicates |
| Serve | Model Registry with the default version as the champion, promotion following an approval policy, online serving with an approval gate, canary, promote and undeploy (Agent Platform endpoints, or KServe on private GKE with the same request format), `pipewright model predict` |
| Monitor | Feature drift and prediction drift (PSI) against training baselines, live performance once outcomes arrive, `monitor status`, email alerts configured in YAML |
| Decide | Retrain decision cases: modes, decision packs, approver groups with a quorum, snoozes and declines that expire, re-opening when things get worse, escalation, a weekly digest, an opt-in Pub/Sub feed for your own apps; the closed retrain loop (production never trains); a failed retrain recorded on its case and retried; approvals in GitHub |
| Observe | Lifecycle events on Pub/Sub and labelled structured logs for every key moment, with no vendor code in pipelines |
| Protect | Personal data declared in the data contract and never used as a feature; erasure by key across a team's tables (BigQuery and Parquet) |
| Synthesize | **Synthetic data**, off until a team switches it on: learned inside the environment by an engine (MOSTLY AI local mode by default; SDV if the team installs it), gated on fidelity, utility and privacy before anything is published; one table, or an entity table with its feature tables and event sequences, keys and timing intact; or rows from the data contract alone, with no real data |
| Automate | `pipewright ci init` writes GitHub Actions workflows for pull requests, releases and operations, including self-hosted runners |
| Extend | Other packages add CLI commands through the `pipewright.commands` entry point, without patching the core |

## How it works

Two personas, one contract between them (the diagram at the top shows the flow):

- **The platform team** owns durable, shared and security-relevant resources, and publishes the environment manifest.
- **ML teams** own pipeline code, configuration, tables inside their dataset, releases, schedules and model
  versions. Neither side edits the other's files.
- **The launcher** is deployed once per environment and never redeployed for a model release: a release only moves
  a pointer to a new, verified contract.
- **Every step runs as the team's own identity**, with the environment's key and network applied from the manifest.

[CONTEXT.md](https://github.com/fluxorsoft/pipewright/blob/main/CONTEXT.md) describes the design, the decisions behind it and every interface in detail.

## Your code and Pipewright

Teams write their pipelines and ML code their own way. Pipewright needs only a small contract at the edges:

| | Free (your choice) | Fixed (the contract) |
|---|---|---|
| Layout | Pipeline file and function names, the components folder, how config is split into files (`include:` patterns), table schema file names | `usecases/<name>/usecase.yaml`, with `metadata.name` equal to the folder; overrides in `usecases/<name>/environments/<env>.yaml` |
| Pipelines | Steps, libraries, frameworks, models; anything inside a step, including Ray jobs on a cluster Pipewright makes for the step (`ray_cluster()`, below) | Kubeflow Pipelines v2 (`kfp` pinned to the version Pipewright compiles with); every pipeline accepts `project`, `region`, `source_uri` and `run_id`; components take their image from `COMPONENT_IMAGE` |
| Configuration | Which sections you use; most have defaults | Optional `ml_config` parameter: when a pipeline declares it, it receives the resolved configuration (data contract, gates, monitoring, retraining) |
| Evaluation | How you compute metrics | Training writes an **evaluation record**, `evaluation.json` next to the model artifacts, with the metrics, slices and champion comparison (see below) |
| Files | | Configuration files end in `.yaml`; a `.yml` file is refused with a message to rename it |

**Pipewright checks the gates itself.** Your evaluation step may apply the gates, but Pipewright does not take its
word for it. Before a version becomes the champion (`pipewright model approve`) or is served (`pipewright
deploy-model`), Pipewright recomputes every metric gate, slice gate and the champion margin from the version's
evaluation record, against the configuration in force now. A rewritten evaluation step that drops a gate cannot
promote a failing model, and `pipewright model list` shows each version's result. The record:

```json
{
  "metrics":  {"auc": 0.81, "calibration_error": 0.03},
  "slices":   {"contract_length_months": {"12": {"rows": 400, "auc": 0.79}}},
  "champion": {"auc": 0.80, "_version": "3"},
  "decision": "approve"
}
```

`slices` is needed for slice gates; `champion` holds the champion's metrics on the same test data, or `null` when
there was no champion yet. The churn example writes this record in its evaluate and register steps.

## Ray steps

A step that needs a cluster asks for one in configuration and uses it in code. The cluster exists only for that
step of that run:

```yaml
steps:
  tune-on-ray:
    ray: { head_machine: n1-standard-4, workers: 1 }   # within the platform's limits; may differ per environment
components:
  ray_requirements: ray-requirements.txt               # extra packages for the cluster's nodes
```

```python
from pipewright_runtime import ray_cluster, ray_job

with ray_cluster(ml_config, "tune-on-ray", run_id) as cluster:      # created by the launcher, deleted after
    ray_job(ml_config, cluster, f"python -m ray_tune_job {trials.uri}", run_id=run_id)
```

- **Who makes the cluster:** the launcher, as the tenant's runner identity, from the live release (the step only
  names itself). It applies the environment's key and network, and labels the cluster with the run.
- **What it runs:** a Ray image `pipewright release` builds for the use case, on the platform's Ray on Agent Platform
  base image, with the component code and `ray_requirements`.
- **Cleanup:** the step deletes the cluster when it ends, even on errors; the launcher's tick deletes any cluster
  whose run ended or whose `max_minutes` passed, so a killed step never leaves one running.
- **The platform's side:** an optional `ray` block in the manifest (environment and per tenant): whether Ray is on,
  the Ray version, base image, a Private Service Connect network attachment, and limits on machines, workers,
  accelerators and minutes. Without it: a cluster per run on a public endpoint, the runner identity, a prebuilt
  image, small limits.
- **What the platform must grant (not in the starter Terraform):**
  - the launcher's identity: `aiplatform.persistentResources.create`, `delete`, `get` and `list` (a custom role;
    `roles/aiplatform.user` only gets and lists);
  - each tenant runner, on itself: `roles/iam.serviceAccountUser` (it reaches its own cluster's job API only if it
    may act as itself) and `roles/iam.serviceAccountOpenIdTokenCreator` (the step signs the ID token it asks the
    launcher with: Agent Platform workers' metadata server does not reliably issue one).

  Pipewright Enterprise's Terraform grants all of it, with the Private Service Connect network for private
  environments.
- **A Ray head needs at least 16 vCPUs and more than 18 GB of memory:** with fewer vCPUs the cluster runs but
  its job API never answers (found live), and Agent Platform refuses less memory. `validate` checks both.
- **A Ray step does not size itself:** no `cpu`, `memory` or `gpu` on a Ray step (Agent Platform runs sized steps
  where the metadata server issues no ID tokens, so the step could not ask the launcher); size the cluster instead.
- **Checks:** `validate` refuses a step over the limits and a Ray client that does not match the clusters
  (`ray[default]` pinned in the component requirements, with `google-cloud-aiplatform[ray]`).
- **On private GKE** (`orchestrator: kfp`): the same `ray_cluster()` and `ray_job()` calls get a KubeRay RayCluster
  on the cluster's Ray node pool, from the mirrored Ray image, and the job is submitted inside the cluster. The
  platform can instead give a team a **standing** cluster (`tenants.<t>.ray.mode: standing` in the manifest's `ray`
  block): `ray-<tenant>`, kept by the platform, whose autoscaler adds workers for a job and removes them after the
  idle minutes. A release records the mode, so changing it makes a new release. The step code is the same.

`pipewright new-usecase --from ray` starts from an example that runs the churn tuning trials in parallel on Ray.

**Development and pipelines:** while developing, data scientists use a standalone Ray cluster of their own (made
in the console, used from a notebook with `ray.init("vertex_ray://<cluster>")`, Python and Ray versions matching
the cluster). Pipelines never use it: every run, dev to prod, gets its own cluster from Pipewright. The same Ray
code runs in both; only who owns the cluster changes.

## Features from feature tables

Instead of one prepared table per use case, a use case can name the features it needs and the BigQuery feature
tables that hold them. Feature tables belong to whoever produces the features (a data team, or the team itself);
Pipewright only reads them. The recommended way to use features, how a feature table must look and the mistakes to
avoid: [docs/FEATURES.md](https://github.com/fluxorsoft/pipewright/blob/main/docs/FEATURES.md).

```yaml
# config/data.yaml
data:
  contract: { ... }                       # unchanged: every column the model and the checks see
  features_from:
    entity: { columns: [customer_id], time_column: snapshot_date }    # in the run's source, the entity table
    sources:
      - table: bq://features.customer_profile          # entity columns, feature_timestamp, one column per feature
        features: [tenure_months, monthly_charges]
      - table: bq://features.support_activity
        features: [num_support_tickets]
```

- **The run's source is the entity table:** who, as of when, and the label (`pipewright run --source-uri
  bq://...`). Each row gets the newest value of each feature dated at or before its cut-off, so nothing from after
  it reaches training. A `date` cut-off is the start of that day (UTC).
- **Scoring** uses the same query, so training and batch scoring read features the same way. A scoring table
  usually carries the cut-off column as the scoring date, which also makes the scores reproducible. Without it
  (the contract must then mark that column `required: false`) the cut-off is the query time.
- **One query, in BigQuery**, run as the runner with the environment's key; everything after ingest (the data
  contract, validation, training, gates, monitoring) is unchanged. A `data.features.resolved` event records the
  tables, features, query job and time, and how many rows found a value.
- **Checks:** `validate` checks the block against the data contract (every feature in the contract, no clashes,
  not the label, a date or timestamp cut-off) and refuses landing-file triggers, since the source must be a
  `bq://` table; `doctor` checks each feature table has the columns, with types the contract accepts.
- **The platform decides where features may come from** (manifest): `tenants.<t>.features.datasets` lists a
  tenant's feature datasets, and `features.required: true` makes feature tables the only source of model
  features. `validate` refuses anything else.

Why tables and not a feature store product: the offline store of Google's Agent Platform Feature Store is
BigQuery itself, and its training-data fetch is still in Preview. Registered feature groups are a second kind of
source that resolves to the same tables: `- feature_group: customer_profile` with registered feature names, read
from the Feature Registry at each run. `pipewright new-usecase --from features` starts from an example with two
feature tables.

## Online features

An endpoint can take entity IDs and read the features itself (new in 1.1):

```yaml
model:
  serving: { enabled: true, features: online }
data:
  features_from:
    sources:
      - feature_group: customer_profile        # served by the feature view of the same name
        features: [tenure_months, monthly_charges]
      - table: bq://feats.support
        online_view: support_activity          # a table source names its view
        features: [num_support_tickets]
```

The platform team gives the tenant a Feature Store online store (Bigtable) in the manifest
(`tenants.<tenant>.features.online_store`); its feature views serve the same features training and batch scoring read
from `data.features_from`, so there is one feature definition. A request is then `{"instances": ["c-42"]}` (or
`[{"customer_id": "c-42"}]`): the serving handler reads each view's latest values and builds the row in the model's
feature order; a missing value fails the request rather than defaulting. `validate` refuses a model feature no view
serves, `doctor` checks the store, the views and how fresh their last sync is, and `deploy-model` runs the endpoint as
the tenant's runner, which may read the views. Online stores have a standing cost: they are off unless the platform
creates one. Both Agent Platform endpoints and KServe on private GKE serve this way; on KServe send objects
(`[{"customer_id": "c-42"}]`), since its serving envelope refuses bare strings. A new feature view serves nothing
until its first sync, which `doctor` reports.

## Reproducible runs and label windows

Two optional keys in `config/data.yaml`, both off unless set (new in 1.1):

```yaml
data:
  snapshot_table: churn_raw_customers   # in outputs.bigquery.tables, with run_id, source_uri and ingested_at
  label_source:                          # the label from a table of events, within a window
    table: bq://${project}.sales.purchases
    time_column: purchased_at
    window_days: 20                      # a purchase within 20 days of the row's cut-off is a 1
```

- **Replay a run:** the ingest step writes the rows each run read to `snapshot_table`, tagged with the run
  (`write_snapshot`), and `pipewright run --replay <run_id>` trains again on exactly those rows, whatever has changed
  in the source since. Events `data.snapshot.written` and `data.snapshot.replayed` record both.
- **Labels that are still maturing are not negatives:** a row is 1 when the label table has an event for its key
  within `window_days` after its cut-off. Rows whose window has not closed by the newest event are dropped, 1s and
  0s alike, so a sign-up from last week is not counted as a 0. The anchor is the newest event, not today, so late
  data never turns real buyers into 0s; `data.labels.resolved` records it with the run. Training's ingest step
  applies it (`read_source(..., labels=True)`); scoring never does. The key and cut-off default to
  `data.features_from.entity`.

## Regression and multi-class models

`data.label.task` says what the model predicts: `binary` (the default, as in 1.0), `multiclass` or `regression`
(new in 1.1). The task decides which metrics evaluation, gates, the champion comparison, tuning and monitoring
accept, and the defaults when the use case names none:

| Task | Metrics | Default primary metric |
|---|---|---|
| binary | auc, pr_auc, accuracy, f1, precision, recall, log_loss, brier, calibration_error | auc |
| multiclass | accuracy, balanced_accuracy, f1_macro, f1_weighted, precision_macro, recall_macro, log_loss | f1_macro |
| regression | rmse, mae, mape, r2, median_absolute_error | rmse |

Errors (rmse, mae, mape, median_absolute_error, log_loss) are lower-is-better: a gate on them takes a `max`, the
champion must have a lower value, and tuning minimises them. A regression use case splits at random unless
`data.split.strategy` says `time`; stratified needs classes. The example's `mlkit.py` has `regression_metrics` and
`multiclass_metrics` (with per-class precision, recall and f1) next to the binary `compute_metrics`. Prediction
drift works for both: `psi_baseline` bins a regression model's predictions like a probability, and `class_baseline`
records a multi-class model's share of each predicted class, which `psi` compares (a class training never predicted
counts as drift).

## BigQuery ML

A use case can train and score in BigQuery instead of in containers (new in 1.1). It has a `bqml` section and no
pipeline code: Pipewright provides the training and scoring pipelines, and runs them on your environment's
orchestrator (Agent Platform Pipelines or Kubeflow Pipelines on GKE) like any other. Their steps submit BigQuery
jobs, so the data and the model stay in BigQuery. `pipewright new-usecase --from bqml` starts one.

```yaml
spec:
  bqml:
    model_type: logistic_reg            # boosted_tree_classifier, random_forest_classifier; for regression
                                        # linear_reg, boosted_tree_regressor, random_forest_regressor
    options: { l2_reg: 0.1 }            # CREATE MODEL options (Pipewright sets the label, split, key, registry)
    prepare: sql/prepare.sql            # optional: SQL over {rows}, the run's rows
    transform: sql/transform.sql        # optional: the TRANSFORM clause's expressions
  inference:
    output_table: scores                # run_id, model_version_id, scored_at, predicted_label and score
                                        # (classification) or prediction (regression), and any input column
```

- **Training:** the run's rows (the entity table with its features at each row's cut-off, the label window when
  `data.label_source` is set, then `prepare.sql`) are written to a table with a held-out test flag, from
  `data.split`. The data contract is checked in SQL. `CREATE MODEL` sees the training rows only.
- **Evaluation:** the new model and the champion score the same test rows, and Pipewright computes its own metrics
  in SQL, defined as in the container path (`data.label.task`: binary, multiclass, regression), per slice too.
  Gate 1 is the same check `pipewright model approve` makes; the evaluation record goes to the use case's pipeline
  root.
- **Registry:** only a model that passes gate 1 is registered (`ALTER MODEL ... SET OPTIONS (vertex_ai_model_id)`),
  as a new version of `model.name`; approval and `deploy-model` work as for any model. Each run trains a new
  BigQuery model, so versions never overwrite each other. A BigQuery ML model is promoted by retraining in the next
  environment (`model.promotion: retrain`); a `logistic_reg` or `linear_reg` model may instead be promoted by its
  files (`model.promotion: model`): exported, copied, imported and registered in the next environment only when it
  scores the run's test rows as the source does.
- **Online serving:** `deploy-model` serves a BigQuery ML version on an Agent Platform endpoint and first checks
  the endpoint's scores against `ML.PREDICT` on the run's test rows.
- **Scoring:** `ML.PREDICT` with the default version into `inference.output_table`, then feature and prediction
  drift (PSI against the training baselines in the record) and performance against observed outcomes
  (`monitoring.performance.labels`: the latest score of each key from this version, the same metrics SQL) into
  `monitoring.output_table`, with the same events. What fires and matches `retraining.when` goes to the launcher
  as a retrain proposal, which opens or updates a decision case (`pipewright retrain`), as for any use case.
- **Preparation:** TRANSFORM is applied by `ML.PREDICT` and by a Model Registry endpoint, so callers send raw
  values; use it for preparation the model needs when served. `prepare.sql` is pinned by the release, written into
  the run's rows and run again when scoring; an endpoint runs no SQL, so online callers send its columns themselves.
- **Cost:** every job has a byte limit (`bqml.max_bytes_billed`, default 100 GiB, or the platform's
  `policies.bqml.max_bytes_billed`). Tree models bill far more bytes than linear ones and take minutes even on
  small data; BigQuery's pricing lists them at a lower rate per TiB.
- **Removing models:** a registered BigQuery ML model is removed with `DROP MODEL`, which also removes its Model
  Registry version. Model Registry refuses to delete it itself, and deleting the dataset that holds it leaves the
  registry entry behind with no way to remove it: drop the models before deleting a dataset.
- **Not yet:** hyperparameter tuning, and XGBoost imports.

More model types (new in 1.1), with the same gates, approvals, scoring and evidence:

- **Segments and components:** `model_type: kmeans` (`data.label.task: clustering`) or `pca`
  (`decomposition`) need no label. They are measured with `ML.EVALUATE` (davies_bouldin_index and
  mean_squared_distance; total_explained_variance_ratio) and gated on those; scoring writes each row's cluster or
  components.
- **Forecasts:** `data.label.task: forecast` with a `bqml.forecast` section (horizon, `id_columns` for one series
  per store or product, confidence level). `arima_plus` holds out the latest period, forecasts it and measures the
  forecast against what happened (mae, rmse, mape), then refits on the whole series before registration; scoring
  writes the next `horizon` points with their prediction interval. `engine: ai_forecast` uses AI.FORECAST with
  Google's TimesFM model instead: nothing is trained, and the version records the settings it was measured with.
  It sends the series to a Google foundation model, so it is refused unless the platform sets
  `policies.bqml.ai_forecast: true`. A forecast is not served online.
- **Models trained elsewhere:** `bqml.import` (TensorFlow SavedModel or ONNX files on `gs://`, and which output
  holds the score, label or prediction) instead of `model_type`. The model is imported, evaluated on the run's test
  rows, gated and registered like a trained one; the files' checksums are recorded with the run.

## The closed retrain loop

Production detects, decides, approves and serves; it never trains (new in 1.1). With `retraining.loop.enabled` in
the production environment, an approved retrain case is trained in the environment production's releases come from
(its `promotion.from`, usually test), on production's data read in place, and compared with the model production
serves. `pipewright loop` takes it the rest of the way:

```bash
pipewright loop start   --env prod --usecase churn --case rc-20261024-1a2b3c   # trains in test
pipewright loop advance --env prod --usecase churn --case rc-20261024-1a2b3c   # next step when the last is done
pipewright loop advance --env prod --usecase churn --case rc-20261024-1a2b3c --approve   # promote, approve, verify
pipewright loop show    --env prod --usecase churn --case rc-20261024-1a2b3c   # the loop as one record
pipewright loop rollback --env prod --usecase churn --case rc-20261024-1a2b3c --reason "complaints"
```

The steps: training in test (a candidate, or rejected by the gates with production untouched); a scoring run in
test with the candidate on current data; the production approval, which promotes it and makes it the default; a
scoring run in production to verify it. A failed validation or verification stops the loop; rollback restores the
version it replaced. One loop runs at a time per use case, each step is recorded on the case and emitted as
`loop.<state>`, and every run carries the case ID. The training and scoring pipelines take `champion_version` and
`model_version` parameters for it (the example and the BigQuery ML pipelines declare them).
`loop advance --approve` promotes, so run it from a clean checkout of the release live in test, as for
`pipewright promote`.

## When retraining fails

An approved retrain whose training fails keeps its approval (new in 1.1): nobody has to decide again. The
launcher's tick follows the case's training run and records how it ended on the case: a registered version, a
version the gates rejected (the case closes as rejected), or a failure with the failed step and its error
(`retrain.training_failed`). `pipewright retrain show` shows it with the attempts so far.

- **Transient failures retry on their own:** capacity, quota, preemption and unavailable services are retried
  automatically, at most twice per case (`retrain.training_retried`, marked automatic).
- **Anything else waits for a person:** a broken data contract, code or a missing table fails the same way again.
  Fix it, then `pipewright retrain retry --env prod --usecase churn --case <id> --reason "contract fixed"`
  (optionally `--source-uri`). The retry is a new run with its own run key (`<case>-retry-N`) and is recorded with
  your name. A loop case is retried through `pipewright loop`, not here.

## Approvals in GitHub

Every decision a person makes (a model approval, a retrain case, the closed loop's production approval) can wait in
GitHub instead of on someone's laptop (new in 1.1). `pipewright decisions pending --env prod` lists what waits, with
the GitHub environment that gates each item: the environment itself, or `<environment>-<group>` for each approver
group of a retrain case. `pipewright ci init` writes two workflows for it:

- `pipewright-decisions`, every 15 minutes: advances closed retrain loops (no person needed) and starts one
  `pipewright-gate` run per pending decision, unless one is already waiting. It runs in the GitHub environment
  `<environment>-decisions` (the same variables, no reviewers).
- `pipewright-gate`: waits for the required reviewers of the decision's GitHub environment, then reads the pending
  decisions again and runs that decision's own `pipewright` command. Turn on "Prevent self-review".

The decision's event records the reviewers, their comment and the workflow run next to the CI identity that acted.
Decisions made with the CLI by hand still work and are recorded as approved outside CI.

## Lineage

`pipewright lineage --env prod --usecase churn [--version 5]` leads a model version back to where it came from:
the run, the release it ran (commit, template and image digests), a digest of its `ml_config` and SQL files, the
exact rows it trained on (its snapshot or BigQuery ML rows table, with the row count and read time), the entity,
feature and label sources, and its evaluation against the champion. A promoted version points to the version in
the environment it came from. The register step writes `lineage.json` next to the evaluation record
(`write_lineage()` in the component runtime; the example and the BigQuery ML pipelines call it), new in 1.1.

## Synthetic data

Off until your team needs it. A `synthetic` section in the use case switches it on; without it nothing is built,
compiled or released for it, and the use case behaves exactly as before.

```yaml
# config/synthetic.yaml
synthetic:
  enabled: true                          # may differ per environment
  purpose: test-data                     # sets the gates: test-data stays in the organisation, sharing leaves it
  source_uri: bq://${project}.${resources.bigquery_dataset}.customers    # read like training data, point in time
  output_table: synthetic_customers      # in outputs.bigquery.tables: the contract's columns plus run_id
  columns:
    customer_id: { rule: key, prefix: "SYN-" }     # keys and personal data are never learned
```

- **Two tiers.** `learned`: an engine learns from the real data inside the environment, in a `synthetic`
  pipeline Pipewright provides (no code to write): `pipewright run --pipeline synthetic`, or a schedule with
  `pipeline: synthetic`. `contract`: no real data at all; `pipewright synthetic generate --out rows.csv` writes rows
  from the data contract and the column rules, for development, CI and demos.
- **Engines.** MOSTLY AI's Synthetic Data SDK in local mode (Apache-2.0) is the default; it makes no network
  calls. SDV is available as an engine your team installs itself (`synthetic.requirements`), under its own
  licence (Business Source Licence 1.1); Pipewright never ships it.
- **Gated, never trusted.** Each candidate (by default the engine at sampling temperatures 1.0 and 1.3, in
  parallel) is scored the same way: fidelity, utility (a model trained on synthetic data tested on a real holdout)
  and privacy (exact copies, distance to the closest record, singling-out, linkability and inference attacks).
  Pipewright recomputes the purpose's gates and publishes the best candidate that passes all of them, or nothing.
  `pipewright synthetic gates` lists them; privacy gates may only be made stricter.
- **Never learned:** personal-data columns (`data_protection.personal_data_columns`) and keys get new values from
  rules; a real holdout is kept back from the engine; the generator stays inside the step.
- **Released like any pipeline:** the synthetic pipeline runs on two images Pipewright builds (the engine, and
  the evaluation), recorded in the release contract by digest, verified, and copied by `promote`.
- **Relational and sequential** (`mode: relational`): the entity table and its child tables in one model, the
  feature tables of `data.features_from` and event tables in `synthetic.sequences` (ordered per entity). Keys
  stay consistent across tables, history keeps its timing against each cut-off, and the gates are measured on
  the point-in-time join of the synthetic tables; each table goes to its own output table.
- **Events:** `synthetic.evaluated`, `synthetic.rejected` and `synthetic.generated` carry the gates, the chosen
  candidate and where the rows went.

Approval of a synthetic release by named approver groups, releasing it into other environments, evidence and
per-environment privacy policy are in Pipewright Enterprise.

## Fairness

A binary classifier can be measured and gated per attribute (new in 1.1):

```yaml
evaluation:
  fairness:
    attributes: [gender, age_band]       # data contract columns; need not be features
    gates:
      - { metric: demographic_parity_difference, max: 0.1 }
      - { metric: equalized_odds_difference, max: 0.15 }
```

On the test set, each group's selection rate and true and false positive rates are taken at the decision
threshold; `demographic_parity_difference`, `equal_opportunity_difference` and `equalized_odds_difference` are the
largest gaps between groups. Groups with fewer than `min_rows` test rows (default 30) are listed but not measured.
The measures go into the evaluation record and the model card; a gate above its maximum rejects the model, and
Pipewright checks the gates again before a version becomes the default (a measure that could not be taken fails
its gate). The example's evaluate step calls `fairness_measures()` from the component runtime; BigQuery ML use
cases get it from the same configuration.

## Approved images

A platform can limit what a use case's images are built from (new in 1.1). In the environment manifest:

```json
"policies": {"images": {
  "base_images": ["europe-west2-docker.pkg.dev/acme-ml/mirror/*"],
  "package_indexes": ["https://europe-west2-python.pkg.dev/acme-ml/pypi-mirror/simple/"],
  "default_base_image": "europe-west2-docker.pkg.dev/acme-ml/mirror/python:3.11-slim"
}}
```

Every base image (component, Ray and synthetic images) must match a pattern; requirement files may name only the
approved indexes, and pip uses the first when a file names none; direct references (`pkg @ https://...`, `git+`,
local paths) are refused. `validate` reports what breaks the policy, `release` refuses it before building, and the
release contract records the base images by digest. Without `policies.images` nothing changes.

## Use cases for some environments only

A use case that only one environment can run, such as one serving on KServe or tuning on KubeRay where only test has
GKE, names its environments (new in 1.1):

```yaml
spec:
  tenant: churn
  run_in: [test]
```

Whole-repo commands (`validate`, `doctor`, `release`, `tables sync`, `schedules sync`, `alerts sync`, `status`,
`test`) skip it elsewhere with `SKIP churn-gke: runs only in test (spec.run_in)`; naming it with `--usecase` in
another environment, or promoting it to one, is refused. Left out, a use case runs in every environment.

A whole-repo `alerts sync` also removes the alert policies of use cases that are gone: those neither in the repo nor
released in the environment. A use case another repo still releases there keeps its policies, and `--dry-run` shows
what would be deleted.

## Promotion between environments

What runs in production is what was tested: `pipewright promote --from test --to prod --usecase <name>` takes the
release live in test (and its approved model) to production without rebuilding anything.

- **Allowed source:** only from the environment the destination names (`promotion: { from: test }` in its
  environment file), and from a clean checkout of the source release's commit. A use case whose `run_in` leaves out
  the destination is refused.
- **A model this release trained:** the version promoted (the default in test, or `--model-version`) must have been
  trained by the release being promoted; this is checked before anything is copied. After a new release, retrain in
  test, approve, then promote.
- **Destination policies first:** the use case is resolved under production's manifest (approval policies, feature
  rules, Ray limits); a refusal stops before anything is copied.
- **The same code:** images are copied by digest and checked. Templates are compiled for production and must equal
  the tested ones with only the registry address changed; an override that changes a pipeline's shape is refused
  unless accepted with `--accept-template-change` (recorded).
- **The same model** (`model.promotion: model`): the approved version's files, verified, registered in production as
  a candidate; `pipewright model approve` there makes it the default. A model's first version needs `--approve`
  with the promotion, since Model Registry makes it the default. With `model.promotion: retrain`, production trains
  its own candidate.
- **Safe to repeat, recorded, reversible:** the contract is written once and the pointer moved last; repeating is a
  no-op; `promoted_from` and a `release.promoted` event record what came from where, by whom; `pipewright rollback`
  goes back without rebuilding.

## Getting started

### What you need

- **On your machine:** Python 3.11, [uv](https://docs.astral.sh/uv/), Docker (component images), and the Google
  Cloud CLI or Application Default Credentials.
- **On Google Cloud:** an environment with the [building blocks](https://github.com/fluxorsoft/pipewright#building-blocks-what-you-need-on-google-cloud)
  above and its environment manifest. [docs/ONBOARDING.md](https://github.com/fluxorsoft/pipewright/blob/main/docs/ONBOARDING.md) lists what the platform team hands to
  each ML team.

### Install a release

```bash
pip install "pipewright-mlops[gcp]==1.1.0"     # the command is `pipewright`
```

Each release is also on GitHub, with the wheel and sdist as release files
(`gh release download v1.1.0 -R fluxorsoft/pipewright`). Pin the version everywhere, CI included, directly from
PyPI or through your organisation's package index. The launcher image for platform teams is in GitHub Container Registry
(`ghcr.io/fluxorsoft/pipewright-launcher`), with its digest in the release notes.

### Install from source

```bash
git clone https://github.com/fluxorsoft/pipewright.git
cd pipewright
uv sync --all-extras                # Python 3.11 virtual environment with the gcp and launcher extras
uv run pipewright --help
```

Or install the CLI into another environment from the checkout: `pip install "<path to the checkout>[gcp]"`.

### Your first use case

From an MLOps repository with an `environments/<env>.yaml` that points at your manifest:

```yaml
apiVersion: pipewright/v1
kind: Environment
metadata: { name: dev }
spec:
  manifest: gs://<artifacts-bucket>/pipewright/environment.json
```

```bash
pipewright new-usecase --name churn --tenant <your-tenant> --owner <your-team>   # copies the churn example
pipewright validate --env dev
pipewright doctor   --env dev
pipewright tables sync --env dev
git commit -am "churn: first release"     # releases are named after the commit
pipewright release  --env dev
```

The churn example (`examples/churn/`) is a complete reference: training and inference pipelines, components, a
data contract, gates, monitoring, retrain decisions, table schemas, tests and generated sample data. The full
walkthrough, including what the platform team hands over, is in [docs/ONBOARDING.md](https://github.com/fluxorsoft/pipewright/blob/main/docs/ONBOARDING.md).

### Help with the YAML

| You want | Use |
|---|---|
| Fewer keys to start with | `pipewright new-usecase --from minimal ...`: the churn example's training code with only the required configuration (two files) and one gate; everything else takes its default |
| The data contract written for you | `pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from-bq <project.dataset.table> --label <column>` drafts `config/data.yaml` (columns, types, label, split, features) and the raw table's schema from a BigQuery table or view. Then train on it with `pipewright run --source-uri bq://<project.dataset.table>` |
| Completion and hover help in the editor | Every YAML file `new-usecase` writes starts with `# yaml-language-server: $schema=...`, pointing at schemas in `.pipewright-schemas/` (in VS Code, with the Red Hat YAML extension). Keys are suggested, explained on hover, and mistakes underlined. `pipewright schemas` adds the line to existing files and refreshes the schemas after an upgrade |
| What a key means | `pipewright explain data.split.strategy` prints its description, type, allowed values, default and whether it may vary per environment; `pipewright explain --list` lists every key |

**Credentials:** Application Default Credentials, or `GCLOUD_CONFIG=<gcloud configuration>` for refreshable gcloud
user credentials. Behind a TLS-inspecting proxy, set `REQUESTS_CA_BUNDLE`, `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` and
`PIPEWRIGHT_BUILD_CA_BUNDLE`.

## Commands

| Stage | Commands |
|---|---|
| Configure | `new-usecase`, `validate`, `resolve`, `explain`, `schemas`, `doctor`, `test` |
| Release | `tables sync`, `release`, `status`, `rollback`, `promote`, `schedules sync`, `alerts sync`, `ci init` |
| Operate | `run`, `model list`, `model check`, `model approve`, `deploy-model`, `model endpoint`, `model predict`, `monitor status`, `lineage`, `images prune`, `data erase`, `synthetic generate`, `synthetic gates`, `pipelines-ui` (private GKE) |
| Decide | `retrain list`, `retrain show`, `retrain approve`, `retrain decline`, `retrain snooze`, `retrain retry`, `loop start`, `loop advance`, `loop show`, `loop rollback`, `decisions pending` |
| Extend | `plugins` (commands added by installed packages) |

Every command takes `--env` and most take `--usecase`. `pipewright <command> --help` lists the options. `status`, `model list`, `monitor status`, `retrain list`, `lineage`, `retrain show` and `decisions pending` take `--json` for scripts and reports.

## Open source and Enterprise

Pipewright is open core. Everything an ML team uses is open source; Pipewright Enterprise adds what regulated
organisations need around it, through published extension points (manifest fields, launcher routes, CLI plugins),
never by patching this package.

| | Pipewright (this repository) | Pipewright Enterprise |
|---|---|---|
| CLI, configuration model, examples | Yes | Yes |
| Shared launcher, release contracts, rollback | Yes | Yes |
| Monitoring, alerts, lifecycle events | Yes | Yes |
| Retrain decisions with approver quorum | Yes | Yes |
| Promotion between environments by digest | Yes | Yes |
| Features from feature tables and Feature Store groups, point in time | Yes | Yes |
| Ray steps; Kubeflow Pipelines, KubeRay and KServe on private GKE | Yes | Yes |
| Synthetic data: learned and gated in the environment, one table or related tables and sequences | Yes | Yes |
| Private-managed Terraform blueprint (CMEK everywhere, private network, internal launcher, VPC Service Controls, organization policies), with private GKE | No | Yes |
| Approver groups, decision feeds and scheduler jobs provisioned | You provide them | Yes |
| Evidence packs: a tamper-evident audit record per model version | No | Yes |
| Synthetic data releases between environments: approver groups, policy per environment, evidence; settings drafted with Gemini | No | Yes |
| Fleet View: every environment, team and model in one read-only view | No | Yes |
| Support, onboarding and platform build-outs | Community | Fluxor Software |

Enterprise enquiries: [fluxorsoft.com/pipewright](https://www.fluxorsoft.com/pipewright.html).

## Project status and roadmap

| Area | Status |
|---|---|
| App-team CLI, configuration model, ML lifecycle sections | Built and used end to end |
| Shared launcher, release contract v3, multi-pipeline releases | Built and used end to end |
| Retrain decisions | Built and used end to end |
| Private-environment support (CMEK, VPC, internal launcher, Pub/Sub run requests) | Built and used end to end |
| Promotion of a release and its approved model between environments | Built and used end to end |
| Kubeflow Pipelines on private GKE (`kfp` adapter), KubeRay, KServe, CronJob schedules, the launcher in the cluster, a read-only Kubeflow UI (`pipewright pipelines-ui`) | Built and used end to end, inside an enforced perimeter |
| Per-tenant pipeline roots | Built |
| OpenTelemetry export | Planned |
| A/B testing of models: sticky assignment by customer, a business metric and guardrails, a person decides the winner | Planned (phase 5) |
| 1.0.0 release on GitHub (package as release files, launcher image in GitHub Container Registry) | Released |
| PyPI release (`pipewright-mlops`) | Released (1.0.0, 1.1.0) |
| Ray on Agent Platform or KubeRay, a cluster per Ray step of a run | Built |
| Features from BigQuery feature tables, point in time, for training and batch scoring | Built |
| Agent Platform Feature Store feature groups as a feature source | Built |
| Online serving from Bigtable (endpoints read features by entity ID) | Built (1.1); used end to end on Agent Platform endpoints and KServe |
| BigQuery ML use cases: classification, regression, k-means, PCA, ARIMA_PLUS and AI.FORECAST forecasts, imported models | Built (1.1); used end to end on both orchestrators |
| The closed retrain loop, failed retrains and their retry | Built (1.1); used end to end, test to production |
| Fairness gates, approved images, approvals in GitHub, lineage per run | Built (1.1) |
| Standing Ray clusters, one per team (private GKE; workers scale to zero) | Built; a cluster per run stays the default |
| Synthetic data (learned inside the environment and gated, or from the data contract alone) | Released in 1.0.0; used end to end on Agent Platform Pipelines and on Kubeflow in private GKE, inside an enforced perimeter |
| Relational and sequential synthetic data (`mode: relational`: the entity table with its feature tables and event sequences) | Released in 1.0.0; used end to end on Kubeflow in private GKE |
| SmartNoise as a differentially private engine | Roadmap (later) |

Google Cloud only, by design. Pipelines are Kubeflow Pipelines v2; bring any Python ML framework (the example uses
PyTorch).

## Development

```bash
uv sync --all-extras
uv run pytest                     # unit tests
uv run ruff check .               # lint
uv run --no-project --with-requirements examples/churn/tests/requirements.txt python -m pytest examples/churn/tests
```

[CONTEXT.md](https://github.com/fluxorsoft/pipewright/blob/main/CONTEXT.md) is the place to start before changing anything: requirements, decisions, design and
conventions, with stable IDs to refer to. See [CONTRIBUTING.md](https://github.com/fluxorsoft/pipewright/blob/main/CONTRIBUTING.md) for how to propose changes, and
[SECURITY.md](https://github.com/fluxorsoft/pipewright/blob/main/SECURITY.md) to report a vulnerability.

## License

Copyright 2026 Fluxor Software Limited. Licensed under the [Apache License, Version 2.0](https://github.com/fluxorsoft/pipewright/blob/main/LICENSE); see
[NOTICE](https://github.com/fluxorsoft/pipewright/blob/main/NOTICE). "Pipewright" and "Fluxor" are names of Fluxor Software Limited; the license does not grant
permission to use them beyond describing the origin of the work.
