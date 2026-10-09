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

### Fixed

- A pipeline job that cannot join its Agent Platform experiment (for example in a new project) is reported as
  submitted with a warning, instead of failing the trigger and being retried as a duplicate.
