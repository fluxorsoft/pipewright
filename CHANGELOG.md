# Changelog

All notable changes to Pipewright. The format follows [Keep a Changelog](https://keepachangelog.com/), and versions
follow [semantic versioning](https://semver.org/); [STABILITY.md](STABILITY.md) says what it covers.

## [Unreleased] (1.1.0)

Additive: a 1.0 use case, manifest and release behave exactly as before.

### Added

- **Reproducible runs** (D-110): `data.snapshot_table` names the table where the ingest step keeps the rows each run
  read; `write_snapshot()` in the component runtime writes them with the run; `pipewright run --replay <run_id>`
  trains again on exactly those rows. Events `data.snapshot.written` and `data.snapshot.replayed`.
- **Label maturity window** (D-111): `data.label_source` takes the label from a table of events within
  `window_days` after each row's cut-off, drops rows whose window has not closed by the newest event and records
  that anchor (`data.labels.resolved`). Applied by training's ingest step with `read_source(..., labels=True)`.
- **Regression and multi-class models** (FR-52): `data.label.task` is `binary` (the default), `multiclass` or
  `regression`. Each task has its own metrics (multiclass: accuracy, balanced_accuracy, f1_macro, f1_weighted,
  precision_macro, recall_macro, log_loss; regression: rmse, mae, mape, r2, median_absolute_error), checked in
  primary metric, metrics, gates, slice gates, tuning and monitoring. rmse, mae, mape and median_absolute_error are
  lower-is-better in gates, the champion comparison and tuning. A regression use case defaults to rmse and a random
  split, and refuses a stratified one. The example's `mlkit` gains `regression_metrics`, `multiclass_metrics` and
  `class_baseline` (prediction drift on class shares; `psi` reads both baseline kinds).
- **BigQuery ML** (`spec.bqml`): a use case trains and scores in BigQuery with pipelines Pipewright provides, on the
  environment's orchestrator. The run's rows (features at each cut-off, the label window, the team's
  `prepare.sql`) with a held-out test set; the data contract in SQL; `CREATE MODEL` on the training rows (with the
  team's TRANSFORM); the model and the champion evaluated on the same test rows with Pipewright's metrics in SQL;
  gate 1; registration in Model Registry only after it (`ALTER MODEL`); scoring into `inference.output_table`;
  drift and performance against observed outcomes into `monitoring.output_table`; retrain proposals to the launcher
  when `retraining.when` matches. Binary, multi-class and regression model types; byte limits per job
  (`bqml.max_bytes_billed`, manifest `policies.bqml`); `examples/bqml` (`pipewright new-usecase --from bqml`).
  `spec.components` is required only without `bqml`.

### Fixed

- **Retrain decisions in private environments** without a required approver group: the CLI sent them to the
  launcher directly, which a private launcher refuses. They now go over the run requests topic (`kind:
  retrain.decision`, with the person's ID token, verified as for a direct call); the launcher accepts them on
  `/pubsub/run`. A launcher older than 1.1 drops them, which the CLI says when nothing is recorded.

## [1.0.0] - 2026-10-18

First release, on GitHub and on PyPI (`pipewright-mlops`). First published on GitHub on 2026-10-17 and re-cut to
include synthetic data and, for PyPI, a README whose links work outside the repository.

### Added

- **Help with the YAML.** `pipewright explain <key>` (description, type, allowed values, default, whether it may vary
  per environment); editor schemas in `.pipewright-schemas/` and a `yaml-language-server` line in every generated
  YAML file, for completion, hover help and underlined mistakes (`pipewright schemas` for existing repos); every use
  case key described in `config/schemas/reference.yaml`.
- **Minimal example** (`new-usecase --from minimal`): the churn training code with only the required configuration.
- **Data contract from BigQuery** (`new-usecase --from-bq <project.dataset.table> --label <column>`): drafts
  `config/data.yaml` and the raw table's schema from a table or view.
- **Evaluation gates checked by Pipewright.** `model approve` and `deploy-model` recompute the metric gates, slice
  gates and champion margin from the version's evaluation record (`evaluation.json`) and refuse a version that fails;
  `model list` shows each version's result.
- **Write-once release contracts** and component image tags derived from the release, so re-releases are
  idempotent and immutable tags work. Contract metadata (owner, objective, description, contacts, model) for tools
  without a repository checkout.
- **Per-tenant pipeline roots.** Runs write their artifacts under `pipeline-root/<tenant>/<usecase>/` of the
  artifacts bucket, so each tenant's runner can be granted its own prefix only.
- **`pipewright promote`.** A tested release, and optionally its approved model, from one environment to the next:
  destination policies first, images by digest, templates checked equal (or an accepted, recorded difference), model
  files verified, the contract written once with `promoted_from`, the pointer moved last; idempotent; a
  `release.promoted` event. Models carry the release that trained them (a `release` label).
- **Retrain decisions.** Drift or a performance drop opens a decision case instead of retraining: modes `off`,
  `propose` and `candidate`; a decision pack (what drifted, the trend, data checks, the champion, what retraining
  would use); approver groups with a quorum; snoozes and declines that expire; re-opening when things get worse;
  escalation and a weekly digest; decision events and an opt-in per-tenant feed; `pipewright retrain list | show |
  approve | decline | snooze`; launcher routes `/retraining/propose`, `/retraining/decide`, `/retraining/tick`.
- **Features from feature tables.** `data.features_from` names BigQuery feature tables and their features; the
  run's source is then the entity table and `read_source` joins each feature as it was at the row's cut-off
  (the newest value at or before it), in one BigQuery query with the environment's key. Scoring without the
  cut-off column uses the latest values. A `data.features.resolved` event, `validate` and `doctor` checks, and
  `new-usecase --from features`.
- **Feature groups as a feature source.** A `data.features_from` source may name a Feature Store feature group; the
  Feature Registry gives its table and columns at each run. The manifest can list a tenant's feature datasets and
  require feature tables (`features.required`); `validate`, `doctor` and `read_source` enforce both.
- **Ray on Agent Platform steps.** `steps.<step>.ray` gives a step a Ray cluster for one run, within the platform's
  limits (manifest `ray`, per environment and tenant), created by the launcher as the runner identity with the
  environment's key and Private Service Connect network attachment, and always deleted (by the step, or by the
  launcher's tick). `pipewright release` builds the use case's Ray image; `ray_cluster()` and `ray_job()` in the
  component runtime; a Ray client version check; `new-usecase --from ray`; a `ray.cluster.created` event records
  what each cluster used.
- **Private-environment support.** The manifest `security` block (CMEK key, BigQuery key, network, reserved IP
  ranges) is applied to every pipeline job, model, endpoint and BigQuery query; run requests over Pub/Sub
  (`launcher.run_requests_topic`, `/pubsub/run`); events published through the regional Pub/Sub endpoint.
- **Manifest forward compatibility.** Manifest keys this version does not know are ignored with a warning
  (`validate`, `doctor`, the launcher's log) instead of failing; an optional `requires` (for example `">=1.1"`)
  makes an older version refuse the manifest with a clear message.
- **`predict` in the generated operate workflow** (`pipewright ci init`): runs `pipewright model predict` against the
  use case's endpoint or InferenceService with the `instances` input, inline JSON or a JSON file in the repository
  (for example `usecases/<name>/tests/predict.json`); the predictions go to the job summary.
- **CLI plugins** through the `pipewright.commands` entry point group.

### Added: private GKE

- **Orchestrator `kfp`: Kubeflow Pipelines on private GKE.** The same use case YAML, compiled templates and release
  contracts run on Kubeflow Pipelines inside the cluster, submitted through the Kubernetes API (no exposed endpoint)
  as each tenant's Kubernetes service account, bound to its runner identity. Tasks inside `dsl.OneOf` branches run
  uncached on Kubeflow (it cannot resolve a cached branch).
- **Ray on KubeRay.** On GKE a step's cluster is a RayCluster per run on the platform's Ray node pool, reached
  inside the cluster; or a tenant's **standing** cluster (`ray.mode: standing`), long-lived and autoscaling, to
  which steps submit with their release's code. The release records the mode it was built for.
- **Online serving on KServe** (`model.serving.target: kserve`): the registered container and its Agent Platform
  environment as an InferenceService in the tenant's serving namespace, so requests and responses match Agent
  Platform endpoints; gates, approvals, rolling updates, undeploy; exposure in the cluster or an internal load
  balancer. `pipewright model predict` for either platform.
- **Schedules as Kubernetes CronJobs** where the manifest says `schedules.backend: kubernetes`.
- **The launcher as a pull worker** (`launcher.runtime: gke`): the same routes fed from the same subscriptions,
  for a launcher inside the cluster; `launcher.audiences` so releases keep working across a switch.
- **`pipewright pipelines-ui`**: the Kubeflow Pipelines UI on localhost, read only, through the cluster's API.
- A small Kubernetes client (`pipewright.kube`); credentials ask for `userinfo.email` (GKE names callers by email).

### Added: synthetic data

- **Off until a team switches it on.** A `synthetic` section in the use case turns it on, per environment; without
  it nothing is built, compiled or released for it, and templates and digests are unchanged.
- **Learned inside the environment.** A built-in `synthetic` pipeline (`pipewright run --pipeline synthetic`, or a
  schedule with `pipeline: synthetic`): the source is read as training reads it, a real holdout is kept back,
  candidates are generated in parallel (by default the MOSTLY AI Synthetic Data SDK in local mode, which makes no
  network calls, at sampling temperatures 1.0 and 1.3; SDV as an engine a team installs itself), and each is scored
  on fidelity, utility (a model trained on synthetic data, tested on the real holdout) and privacy (exact copies,
  distance to the closest record, singling-out, linkability and inference attacks). The best candidate that passes
  every gate is published; otherwise nothing is.
- **Gates per purpose** (`test-data`, `sharing`), recomputed by Pipewright; `pipewright synthetic gates` lists them;
  a use case may make privacy gates stricter, never looser. The platform's `policies.synthetic` in the manifest sets
  the allowed purposes and engines, purposes that need differential privacy, gate floors and required approvers.
- **Never learned:** keys and personal-data columns get new values from column rules.
- **Relational and sequential** (`mode: relational`): the entity table with the feature tables of
  `data.features_from` and the event tables in `synthetic.sequences`, learned together. Keys stay consistent across
  tables, each history keeps its timing against the cut-off, and the gates are measured on the point-in-time join of
  the synthetic tables; each table goes to its own output table.
- **Without real data:** `pipewright synthetic generate` writes rows from the data contract and the column rules,
  for development, CI and demos.
- **Released like any pipeline:** `pipewright release` builds two images (the engine, and the evaluation), recorded
  in the contract by digest, verified, and copied by `promote`. `doctor` checks the section against the live
  environment. Events `synthetic.evaluated`, `synthetic.rejected` and `synthetic.generated`.

### Changed

Compared with the pre-release builds early users ran:

- `retraining.enabled: true` now means `retraining.mode: propose`: drift no longer requests training directly.
- Decision emails show the summary as a code block, so line breaks and commands are kept.
- `.yml` configuration files are refused with a message to rename them to `.yaml`, instead of being ignored.
- Licensed under the Apache License 2.0.
- Distribution name `pipewright-mlops` (PyPI); the import and the command stay `pipewright`.

### Fixed

- **`alerts sync` keeps its own notification channels.** Another email channel for the same address (a platform's
  own alerts) made every sync try to repoint the use case's policies; the channel Pipewright made now wins.
- **Steps with only `retries` or `cache`** no longer get an empty `resources` block in the compiled template: Agent
  Platform ran them on its sized-step backend, where the metadata server issues no ID tokens.
- **A release reuses a contract only when its images and compiled templates are unchanged**, so a Pipewright
  upgrade that changes how templates compile is released (as `<commit>-<hash>`) instead of silently reusing the old
  template.
- **Generated CI workflows fail when a command fails.** Steps run as `bash -eo pipefail`, so a `pipewright ...`
  piped to `tee` no longer passes when the command fails.
- A pipeline job that cannot join its Agent Platform experiment (for example in a new project) is reported as
  submitted with a warning, instead of failing the trigger and being retried as a duplicate.
