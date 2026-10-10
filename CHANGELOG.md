# Changelog

All notable changes to Pipewright. The format follows [Keep a Changelog](https://keepachangelog.com/), and versions
follow [semantic versioning](https://semver.org/) once 1.0 is released.

## [0.4.0] - unreleased

First open-source release.

### Added

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

- **Ray on Vertex AI steps.** `steps.<step>.ray` gives a step a Ray cluster for one run, within the platform's
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

### Fixed

- **Steps with only `retries` or `cache`** no longer get an empty `resources` block in the compiled template: Agent
  Platform ran them on its sized-step backend, where the metadata server issues no ID tokens.
- **A release reuses a contract only when its images and compiled templates are unchanged**, so a Pipewright
  upgrade that changes how templates compile is released (as `<commit>-<hash>`) instead of silently reusing the old
  template.

- A pipeline job that cannot join its Agent Platform experiment (for example in a new project) is reported as
  submitted with a warning, instead of failing the trigger and being retried as a duplicate.
