# Changelog

All notable changes to Pipewright. The format follows [Keep a Changelog](https://keepachangelog.com/), and versions
follow [semantic versioning](https://semver.org/); [STABILITY.md](STABILITY.md) says what it covers.

## [1.0.0] - 2026-10-17

First release.

### Added (private GKE)

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

### Added

- **`predict` in the generated operate workflow** (`pipewright ci init`): runs `pipewright model predict` against the
  use case's endpoint or InferenceService with the `instances` input, inline JSON or a JSON file in the repository
  (for example `usecases/<name>/tests/predict.json`); the predictions go to the job summary.
- **Retrain decisions.** Drift or a performance drop opens a decision case instead of retraining: modes `off`,
  `propose` and `candidate`; a decision pack (what drifted, the trend, data checks, the champion, what retraining
  would use); approver groups with a quorum; snoozes and declines that expire; re-opening when things get worse;
  escalation and a weekly digest; decision events and an opt-in per-tenant feed; `pipewright retrain list | show |
  approve | decline | snooze`; launcher routes `/retraining/propose`, `/retraining/decide`, `/retraining/tick`.
- **Private-environment support.** The manifest `security` block (CMEK key, BigQuery key, network, reserved IP
  ranges) is applied to every pipeline job, model, endpoint and BigQuery query; run requests over Pub/Sub
  (`launcher.run_requests_topic`, `/pubsub/run`); events published through the regional Pub/Sub endpoint.
- **Write-once release contracts** and component image tags derived from the release, so re-releases are
  idempotent and immutable tags work.
- **CLI plugins** through the `pipewright.commands` entry point group.
- Contract metadata (owner, objective, description, contacts, model) for tools without a repository checkout.

### Changed

- `retraining.enabled: true` now means `retraining.mode: propose`: drift no longer requests training directly.
- Decision emails show the summary as a code block, so line breaks and commands are kept.
- Licensed under the Apache License 2.0.
- Distribution name `pipewright-mlops` (PyPI); the import and the command stay `pipewright`.

- **Evaluation gates checked by Pipewright.** `model approve` and `deploy-model` recompute the metric gates, slice
  gates and champion margin from the version's evaluation record (`evaluation.json`) and refuse a version that fails;
  `model list` shows each version's result.
- `.yml` configuration files are refused with a message to rename them to `.yaml`, instead of being ignored.

- **Help with the YAML.** `pipewright explain <key>` (description, type, allowed values, default, whether it may vary
  per environment); editor schemas in `.pipewright-schemas/` and a `yaml-language-server` line in every generated
  YAML file, for completion, hover help and underlined mistakes (`pipewright schemas` for existing repos); every use
  case key described in `config/schemas/reference.yaml`.
- **Minimal example** (`new-usecase --from minimal`): the churn training code with only the required configuration.
- **Data contract from BigQuery** (`new-usecase --from-bq <project.dataset.table> --label <column>`): drafts
  `config/data.yaml` and the raw table's schema from a table or view.

- **Ray on Agent Platform steps.** `steps.<step>.ray` gives a step a Ray cluster for one run, within the platform's
  limits (manifest `ray`, per environment and tenant), created by the launcher as the runner identity with the
  environment's key and Private Service Connect network attachment, and always deleted (by the step, or by the
  launcher's tick). `pipewright release` builds the use case's Ray image; `ray_cluster()` and `ray_job()` in the
  component runtime; a Ray client version check; `new-usecase --from ray`; a `ray.cluster.created` event records
  what each cluster used.
- **Features from feature tables.** `data.features_from` names BigQuery feature tables and their features; the
  run's source is then the entity table and `read_source` joins each feature as it was at the row's cut-off
  (the newest value at or before it), in one BigQuery query with the environment's key. Scoring without the
  cut-off column uses the latest values. A `data.features.resolved` event, `validate` and `doctor` checks, and
  `new-usecase --from features`.
- **Feature groups as a feature source.** A `data.features_from` source may name a Feature Store feature group; the
  Feature Registry gives its table and columns at each run. The manifest can list a tenant's feature datasets and
  require feature tables (`features.required`); `validate`, `doctor` and `read_source` enforce both.

- **Manifest forward compatibility.** Manifest keys this version does not know are ignored with a warning
  (`validate`, `doctor`, the launcher's log) instead of failing; an optional `requires` (for example `">=0.5"`)
  makes an older version refuse the manifest with a clear message.

- **Per-tenant pipeline roots.** Runs write their artifacts under `pipeline-root/<tenant>/<usecase>/` of the
  artifacts bucket, so each tenant's runner can be granted its own prefix only. Existing releases keep their root
  until re-released.

- **`pipewright promote`.** A tested release, and optionally its approved model, from one environment to the next:
  destination policies first, images by digest, templates checked equal (or an accepted, recorded difference), model
  files verified, the contract written once with `promoted_from`, the pointer moved last; idempotent; a
  `release.promoted` event. Models now carry the release that trained them (a `release` label).

### Fixed

- **Steps with only `retries` or `cache`** no longer get an empty `resources` block in the compiled template: Agent
  Platform ran them on its sized-step backend, where the metadata server issues no ID tokens.
- **A release reuses a contract only when its images and compiled templates are unchanged**, so a Pipewright
  upgrade that changes how templates compile is released (as `<commit>-<hash>`) instead of silently reusing the old
  template.

- **Generated CI workflows fail when a command fails.** Steps run as `bash -eo pipefail`, so a `pipewright ...`
  piped to `tee` no longer passes when the command fails.
- A pipeline job that cannot join its Agent Platform experiment (for example in a new project) is reported as
  submitted with a warning, instead of failing the trigger and being retried as a duplicate.
