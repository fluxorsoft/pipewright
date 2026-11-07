# Pipewright: context

> **Read this first.** This file is the entry point to Pipewright for people and for AI assistants working on it or
> with it. It states what Pipewright is for (requirements), why it is built the way it is (decisions), how it works
> (design), the interfaces it promises, and how to work on it (conventions). It summarises; the code is the final
> authority.
>
> **Last updated:** 2026-10-26. **Version:** 1.1.0 (released 2026-10-26; 1.0.0 released 2026-10-17, re-cut 2026-10-18). **Release contract:** 3. **Configuration and manifest
> API:** `pipewright/v1`.

**How to use this file**

- **Giving feedback or proposing a change:** requirements (section 4) and decisions (section 7) carry stable IDs
  (`FR-`, `NFR-`, `C-`, `D-`, `R-`). Refer to them by ID, for example "D-41 should allow ...". IDs that are not
  listed here belong to Pipewright Enterprise and are documented there; numbers are never reused.
- **Changing Pipewright:** check the requirement and decision you touch. If the change alters either, update this
  file in the same pull request.
- **AI assistants:** treat sections 7 (decisions) and 12 (conventions) as binding unless the person you work for
  says otherwise. When this file and the code disagree, the code wins: report the difference and fix this file.
- **Using Pipewright rather than changing it:** start with the [README](README.md) and
  [docs/ONBOARDING.md](docs/ONBOARDING.md); come here for the reasons behind the rules.

---

## Contents

1. [Product in brief](#1-product-in-brief)
2. [Status](#2-status)
3. [Repository layout](#3-repository-layout)
4. [Requirements](#4-requirements)
5. [Personas and ownership](#5-personas-and-ownership)
6. [Architecture](#6-architecture)
7. [Design decisions](#7-design-decisions)
8. [Designs in detail](#8-designs-in-detail)
9. [Interfaces and contracts](#9-interfaces-and-contracts)
10. [Testing](#10-testing)
11. [Known gaps, risks and open questions](#11-known-gaps-risks-and-open-questions)
12. [Working conventions](#12-working-conventions)
13. [Roadmap](#13-roadmap)
14. [Glossary](#14-glossary)

---

## 1. Product in brief

**Pipewright is configurable, governed MLOps for many teams on Google Cloud.**

- A **platform team** builds each environment (dev, test, prod) once and publishes an **environment manifest**: the
  names of everything a team may use.
- **App (ML) teams** describe each use case in small, reviewed YAML files plus their own pipeline code. The
  `pipewright` CLI turns that into tables, verified releases, triggers, model promotion, serving, monitoring, alerts
  and retrain decisions.
- Neither side edits the other's files. They meet at the manifest.

**What it replaces:** every ML team rebuilding the same plumbing (triggers, releases, registry conventions, drift
monitoring, IAM); "what is running in production" answered by guesswork; drift alerts that trigger blind
retraining; governance assembled after the fact.

**Open core (D-31):** this repository is Pipewright, licensed under Apache-2.0. Pipewright Enterprise, from Fluxor
Software, adds a private-by-default platform blueprint, evidence packs (tamper-evident audit records per model
version), governed synthetic data releases, Fleet View across environments, and support. Enterprise uses published extension points and never patches this package (D-34).

---

## 2. Status

"Built" means working code with tests. "Used end to end" means exercised on a real Google Cloud project, including a
private environment with customer-managed keys, enforced organization policies and an enforced VPC Service Controls
perimeter, on Agent Platform and on private GKE.

| Area | Status |
|---|---|
| CLI for app teams, configuration model, ML lifecycle sections | Built, used end to end |
| Shared launcher (Cloud Run), release contract v3, multi-pipeline releases | Built, used end to end (internal ingress) |
| Component runtime (`write_table`, `read_source`, `bigquery_client`, `log`, `emit_event`) | Built, used end to end |
| Outputs store: BigQuery, Parquet on GCS, both | Built, used end to end |
| Lifecycle events, structured logs, email alerts | Built, used end to end |
| Retrain decisions (modes, decision pack, approver groups, decision events) | Built, used end to end |
| Private-environment hooks (manifest `security`, Pub/Sub run requests, CMEK on queries, write-once contracts, plugins) | Built, used end to end |
| CI for app teams (`pipewright ci init`, self-hosted runners) | Built |
| Product CI and release workflows (`.github/workflows/`) | Built |
| Promotion between environments (`pipewright promote`) | Built, tested live 2026-10-11 |
| First release, 1.0.0 (wheel as GitHub release files, launcher image in GitHub Container Registry) | Released 2026-10-17; re-cut 2026-10-18 with synthetic data |
| Private GKE (section 8.15): orchestrator `kfp`, Ray on KubeRay per run or standing, KServe serving, CronJob schedules, the launcher as a pull worker, the read-only Kubeflow UI | Built, used end to end under an enforced perimeter (2026-10-17) |
| Features from feature tables and Feature Store feature groups, point in time (section 8.11) | Built, used end to end |
| Synthetic data (FR-49): one table, or an entity table with its feature tables and event sequences | Built, used end to end on Agent Platform and private GKE |
| OpenTelemetry export | Phase 11: to be reviewed and prioritised (user, 2026-11-05); was planned |
| Terraform for the platform side | In a separate starter repository, shared on request (R-20) |

---

## 3. Repository layout

```
CONTEXT.md                  this file
README.md                   what Pipewright is, the gaps it closes, getting started
CONTRIBUTING.md, SECURITY.md, CHANGELOG.md, LICENSE, NOTICE
pyproject.toml, uv.lock     the package (hatchling), dev environment (uv)
src/pipewright/             the Python package, one subpackage per responsibility (section 8.1)
  config/schemas/           JSON Schemas: environment, use case, environment manifest (the public contracts);
                            reference.yaml: what each use case key means (editor hover, `pipewright explain`)
  build/templates/          components.Dockerfile, pipewright_runtime.py (injected into every component image)
  scaffold/ci_templates/    GitHub Actions workflows written by `pipewright ci init`
examples/churn/             the reference use case copied by `pipewright new-usecase`
examples/minimal/           the churn training code with only the required configuration (example.yaml)
examples/ray/               the churn training code with the tuning trials run in parallel on a Ray cluster
examples/features/          the churn sample as an entity table and two feature tables (section 8.11)
images/launcher.Dockerfile  the launcher image, built from the wheel
tests/                      offline unit tests (fakes for Google Cloud)
scripts/                    check_package.py (wheel contents), smoke_test.sh (installed CLI, no cloud)
docs/ONBOARDING.md          how a platform team prepares a tenant and an ML team starts a use case
docs/FEATURES.md            features the recommended way: feature tables and groups, read point in time
.github/workflows/          ci.yml (tests, wheel, smoke test, launcher image), release.yml (tagged releases)
```

---

## 4. Requirements

Status: **Met** (built and verified), **Partial**, **Planned**, **Out** (deliberately not done), **Replaced** (by the
requirements named).

### 4.1 Functional requirements

| ID | Requirement | Status | Where |
|---|---|---|---|
| FR-01 | App teams describe a use case in YAML (identity, pipelines, triggers, tables, data contract, training, evaluation, operations), split across files, one topic per file | Met | `config/`, `usecase.schema.json` |
| FR-02 | Configuration is validated against JSON Schemas with plain-language, path-qualified errors | Met | `config/schema.py` |
| FR-03 | A fixed list of keys may differ per environment; everything that defines the model and how it is judged is identical everywhere | Met | `config/merge.py`, schema `overrides` |
| FR-04 | App configuration refers to platform resources by name, resolved from the environment manifest; it never creates infrastructure | Met | `config/resolve.py` |
| FR-05 | Read-only pre-flight check (`doctor`) against the live project, naming who to ask for anything missing | Met | `checks/doctor.py` |
| FR-06 | Tables are created and evolved only by `tables sync`, additive changes only; breaking changes are refused | Met | `tables/` |
| FR-07 | A release builds the component image and one pipeline template per pipeline, pins both by digest, writes an immutable release contract, verifies it and moves a per-use-case live pointer | Met | `release/` |
| FR-08 | Rollback is a pointer move to an earlier verified release | Met | `release/service.py` |
| FR-09 | Several pipelines per use case (training, inference) released together | Met | contract v3 |
| FR-10 | Runs start from a file landing under the tenant's prefix, a schedule, or an API or run request; duplicates never start a second run | Met | `launcher/` |
| FR-11 | Sources may be CSV or Parquet on GCS, or BigQuery tables and views (`bq://`) | Met | `pipewright_runtime.read_source` |
| FR-12 | Training: data contract checks, splits (random, stratified, time), tuning with parallel trials, experiments | Met | example, `config/ml.py` |
| FR-13 | Evaluation: metric gates, slice gates, calibration, explainability, comparison with the current champion on the same data; a model card | Met | example |
| FR-14 | Model registry: the default version is the champion; who promotes follows the approval policy | Met | `models/registry.py` |
| FR-15 | Online serving with approval gate, canary, promote and undeploy | Met | `models/deploy.py` |
| FR-16 | Inference scores with the champion and monitors feature drift, prediction drift and live performance (PSI) | Met | example, `monitoring/` |
| FR-17 | Retraining requested automatically when policy allows | **Replaced** by FR-39 to FR-42 (D-68): drift opens a human decision; `retraining.enabled: true` means `mode: propose` | example `request_retraining` (a proposal) |
| FR-18 | Results stored in BigQuery, Parquet on GCS (with external tables), or both, with one schema file | Met | `pipewright_runtime.write_table`, `tables/external.py` |
| FR-19 | Lifecycle events (Pub/Sub) and labelled structured logs for every key moment | Met | `observability/events.py` |
| FR-20 | Email alerts per use case, configured in YAML | Met | `alerts/sync.py` |
| FR-21 | Personal data declared in the data contract, never a feature; erasure by key across a team's tables | Met | `data/erase.py`, `erase_gcs.py` |
| FR-22 | GitHub Actions workflows for app teams generated by the CLI, including self-hosted runners | Met | `scaffold/ci.py` |
| FR-23 | New use cases scaffolded from an example | Met | `scaffold/` |
| FR-24 | Other packages can add CLI commands without patching the core | Met | `plugins.py` (D-36) |
| FR-29 | **Promote a release between environments:** images copied by digest, templates checked equal, configuration resolved for the destination, the model copied unchanged (or retrained under the destination's approvals), destination policies applied first, the pointer moved last, idempotent, fully recorded | Met (live 2026-10-11) | `release/promote.py`, `pipewright promote`, section 8.12 |
| FR-30 | Self-managed pipelines on GKE (Kubeflow Pipelines, KServe) | Met 2026-10-17 | section 8.15, `adapters/kfp.py` |
| FR-39 | **Retrain decision case with a decision pack:** drift or a performance drop opens a case holding what is needed to decide (is it real, is it a data problem, what would retraining use, the options) | Met | `retraining/` |
| FR-40 | **Retraining modes per use case:** `off` (alerts only), `propose` (nothing trains until a person approves), `candidate` (trains a candidate, never promotes); the platform can limit the modes per environment | Met | `retraining/`, `config/ml.py` |
| FR-41 | **Approver groups with a quorum:** several groups may be required, the platform can add required groups per environment, the proposer cannot approve, one identity counts once, approvals are authenticated actions by named identities | Met | `retraining/` |
| FR-42 | **Decision events:** always on the lifecycle events topic; an opt-in per-tenant decision feed; email to configured addresses; a weekly digest | Met | `retraining/`, `alerts/sync.py` |
| FR-43 | **Gates checked by Pipewright, not trusted from the pipeline:** a version becomes the champion or is served only when its evaluation record passes the configured gates, recomputed by Pipewright | Met | `models/gates.py`, `model approve`, `deploy-model`, `model list` |
| FR-44 | **Help with the YAML:** editor completion, hover text and underlined mistakes from exported schemas; `pipewright explain <key>`; a minimal example; a data contract drafted from a BigQuery table's schema | Met | `config/reference.py`, `scaffold/from_bq.py`, `examples/minimal/` |
| FR-45 | **Ray on Agent Platform inside the lifecycle:** a step asks for a cluster (`steps.<step>.ray`) and gets one for that run only, within the platform's limits, with the environment's key, network and the runner identity; it is always deleted (by the step, or by the launcher's cleanup) | Met | `ray_clusters/`, launcher `/ray/clusters`, `pipewright_runtime.ray_cluster`, `examples/ray/` |
| FR-46 | **Features from feature tables:** a use case names features and the BigQuery feature tables that hold them (`data.features_from`); ingest builds point-in-time correct data from the run's entity table; batch scoring reads the latest values the same way; a `data.features.resolved` event records what was read | Met | `pipewright_runtime.read_source`, `config/ml.py`, `checks/doctor.py`, `examples/features/`, section 8.11 |
| FR-47 | **Features by registered name:** a source may name a Feature Store feature group; the Feature Registry gives its table, columns and each feature's column at run time, and the same point-in-time query runs | Met | `pipewright_runtime.resolve_feature_groups`, `checks/doctor.py`, section 8.11 |
| FR-48 | **The closed retrain loop across environments:** a case opened in production trains in test on production-eligible data, is compared with the model production serves, is validated in test, promoted, approved and verified in production, and the case closes with the final production model; one case ID end to end | Phases 1 to 3 released: 1.1.0 (2026-10-26). The rest done in 1.4.0 (2026-11-06), each off by default: safeguards, the online canary with automatic rollback, production's data for training in test, BigQuery ML use cases in the loop; live-tested test to production | section 8.13 |
| FR-49 | **Synthetic data generation:** a `synthetic` section in the use case configuration; contract-only generation from the data contract (no real data); learned generation through an engine interface (MOSTLY AI Synthetic Data SDK in local mode by default; SDV as an opt-in adapter that loads only if the team installed SDV, whose Business Source Licence the team accepts directly; never a dependency); evaluation of fidelity, utility (train on synthetic, test on real) and privacy, with gates Pipewright recomputes. Approval, release across environments and evidence are Pipewright Enterprise | Met (released in 1.0.0, 2026-10-18): the section (off unless enabled), the synthetic pipeline on two images, `policies.synthetic`, `synthetic generate` and `synthetic gates`, relational and sequential mode; used end to end on Agent Platform and on Kubeflow in private GKE | `synthetic/`, section 13 |
| FR-51 | **Online feature lookup, a toggle:** online serving fetches a request's features by entity ID from a Feature Store Bigtable online store instead of the caller sending the feature vector. Off unless the use case switches it on (`model.serving.features: online`) and the platform allows it per environment (`policies.online_features`, with the tenant's online store and node limits), because a Bigtable online store has a standing cost. The feature views come from the same `data.features_from` block training and batch scoring read, so online, batch and training share one feature definition (no training-serving skew); the serving handler fetches by ID, then calls the model, on Agent Platform endpoints and KServe; `doctor` checks the views exist and are fresh; events record which view served. The online store and its views belong to the platform or the features' owners (D-79). Enterprise: Terraform for the online store (CMEK, in the perimeter), cost limits in the platform file, an evidence control for online features | Released: 1.1.0 (2026-10-26) | section 8.11 |
| FR-52 | **Regression and multi-class models:** evaluation metrics (for example rmse, mae, r2; macro and weighted f1, per-class recall), gates with the right direction, champion comparison, slice gates and prediction drift for continuous and multi-class outputs, for container and BigQuery ML use cases alike. Today `config/ml.py` knows binary classification metrics only (auc, pr_auc, accuracy, f1, precision, recall, log_loss, brier, calibration_error) | Released: 1.1.0 (2026-10-26) | `config/ml.py`, `models/gates.py` |
| FR-53 | **Fairness metrics and gates:** bias measures per protected or sensitive attribute (for example demographic parity difference, equal opportunity and equalised odds differences), computed on the test set next to slice metrics, gated like other metrics and recorded in the evaluation record and the model card. The attributes are declared in the data contract; they may be measured without being features | Released: 1.1.0 (2026-10-26) | `config/ml.py`, `models/gates.py` |
| FR-54 | **Approved image allow-list:** the platform lists, per environment, the base images and package indexes a use case's images may be built from (component, Ray and synthetic images); `validate` and `release` refuse anything else, and the release contract records the base images by digest. Declared in the manifest like other platform policy (D-81); Pipewright Enterprise sets it in the platform file. Raised with the forecasting route of the BigQuery ML design (approved libraries such as statsmodels and Prophet) | Released: 1.1.0 (2026-10-26) | manifest `policies`, `build/images.py` |
| FR-55 | **Lineage per run:** a model version leads to its run, the release it ran (commit, images, templates, `ml_config` with its SQL digests), the exact rows it trained on (the run's snapshot or BigQuery ML rows), the sources those came from (entity table, feature tables or groups and their read time, the label table and window) and its evaluation and approvals: one `lineage.json` per run with the evaluation record, and a lineage section in Fleet View. Upstream lineage of the sources (Data Lineage API) later | Released: 1.1.0 (2026-10-26) | runtime, bqml steps, Enterprise Fleet View |
| FR-56 | **Approval gates in CI:** every human decision (model approval, deployment, promotion, retrain decisions, the closed loop's steps, synthetic data releases) can be a GitHub Actions job that waits for its environment's required reviewers, then runs the `pipewright` command; a scheduled workflow lists what needs a decision and opens one gated job per item. Pipewright records the GitHub reviewer, the approval time and comment and the workflow run next to the CI identity | Released: 1.1.0 (2026-10-26) | `pipewright ci init`, events, evidence, Enterprise Fleet View |
| FR-57 | **A/B testing of models (section 8.16):** a controlled experiment between the champion and a candidate, with sticky assignment by a unit key, a business metric and guardrails fixed in advance, a minimum sample and duration, analysis per arm, and a person deciding the winner through a decision case; recorded in the winning version's lineage. Batch first, then online | Phase 10, after FR-92 (user, 2026-11-05); the user reviews the design before it is built |
| FR-58 | **GPU limits for pipeline steps:** the platform lists, per environment, the accelerator types (and the most per step) that `steps.<name>.gpu` may ask for, as it already can for Ray clusters (`ray.accelerators`); `validate` and `release` refuse anything else. Today a plain step may ask for any GPU the project's quota allows, so quota is the only limit. Off unless the manifest sets the list | Done: 1.2.0 (2026-10-31), tested by unit tests only (no GPU quota) | manifest `policies`, `config/ml.py`, `checks/doctor.py` |
| FR-59 | **A base image per use case:** `components.base_image` (for example a CUDA image for GPU training) instead of the environment's `images.default_base_image` for every use case; checked against the approved image allow-list (`images.base_images`) and recorded by digest in the release contract like today's base. Today a team needing its own base passes a pre-built image with `release --component-image`. Off unless set | Done: 1.2.0 (2026-10-31) | `build/images.py`, `release/service.py`, usecase schema |
| FR-60 | **How to call a deployed model:** `pipewright deploy-model` (and `--promote`) ends by printing the endpoint ID, the full predict URL (`https://<region>-aiplatform.googleapis.com/v1/projects/.../endpoints/<id>:predict`), the role a caller needs (`aiplatform.endpoints.predict`) and a sample request built from the model's features (feature values, or entity IDs with `model.serving.features: online`); the same from a read-only `pipewright model endpoint`. Today it prints only the endpoint's display name, so developers look the ID up and build the URL by hand | Done: 1.2.0 (2026-10-31) | `models/deploy.py`, `cli/main.py` |
| FR-61 | **GPU serving:** `model.serving.accelerator: {type, count}` deploys the model on a GPU (Agent Platform endpoints: the accelerator type and count on the deployment, with a machine family that takes it, for example g2 with NVIDIA_L4); the platform lists the accelerator types and most per replica each environment allows, as FR-58 does for steps; `validate` refuses anything else and `doctor` checks the region offers it. Off unless set: CPU serving as today | Done: 1.2.0 (2026-10-31), tested by unit tests only (no GPU quota) | `models/deploy.py`, usecase and manifest schemas |
| FR-62 | **Scale to zero:** `model.serving.replicas.min: 0` lets an endpoint with no traffic run no replica, where Agent Platform supports it; the first request after a quiet spell waits for a replica to start, which `deploy-model` says. Today `deploy-model` deploys at least one replica whatever `min` says, so a deployed model is billed while idle. Off unless set | Done: 1.2.0 (2026-10-31) | `models/deploy.py` |
| FR-63 | **Serving gated in CI, or automatic:** `approvals.deploy_model` gains two values beside `manual` (the default, as in 1.0: a model reaches the registry and serves only when someone runs `pipewright deploy-model`) and `auto` (no confirmation, as today): `ci`, where `pipewright decisions pending` lists a `deploy-approve` item for the newest default version that passes its gates and is not yet served, and `pipewright-decisions` dispatches one `pipewright-gate` run per item, whose GitHub environment's required reviewers approve before it runs `deploy-model` (canary when `canary_percent` is set), with the reviewer recorded on the deployment event (D-120 to D-123); and `on_register`, where the launcher deploys a version as soon as it is registered and passes its gates. The manifest's `policies.deploy_model` still has the last word per environment (for example `ci` required in production). Live-tested in the e2e repository with real reviewers | Done: 1.2.0 (2026-10-31) | `decisions.py`, `models/deploy.py`, launcher, `scaffold/ci_templates/` |
| FR-64 | **Examples that show every setting:** (a) the examples become real YAML (`usecase.yaml`, `config/*.yaml`), not `.yaml.tmpl`: placeholders become readable YAML-safe values (for example `name: churn`, `tenant: my-tenant`) that `new-usecase` replaces, so the files read and highlight on GitHub and are schema-checked in CI; (b) one example, `churn` (the reference), lists every key a use case can set with its default value, marked as a default and explained (for example `model.serving` with `machine`, `replicas: { min: 1, max: 1 }`, `canary_percent`, and `approvals.deploy_model` beside it), so adopters see the whole range of options in one place; `minimal` stays only what is required, and the other examples show the keys their feature needs, each README pointing to `churn` for the rest; (c) a README per example and `examples/README.md` say which keys may be left out and which defaults Pipewright fills in, pointing to `pipewright resolve` and `pipewright explain`; (d) a test compares the defaults shown with the resolver's defaults (`config/resolve.py`), so the examples never drift. What `new-usecase` writes for a team stays the same unless the example changes | Done: 1.2.0 (2026-10-31) | `examples/`, `scaffold/new_usecase.py`, tests |
| FR-65 | **Synthetic data reads labels like training:** the synthetic pipeline's source read applies `data.label_source` (the label window and anchor) as training's ingest step does, so `synthetic.source_uri` may be the entity table itself. Today it joins `data.features_from` but not the label window, so a use case with a label source points the synthetic pipeline at already-labelled rows (for example a run's snapshot); found writing the ebook's Chapter 6 | Done: 1.2.0 (2026-10-31) | `synthetic/synthetic_steps.py` (`read_source(..., labels=True)`) |
| FR-66 | **Tests before a release:** `pipewright release` runs the use case's `tests.command` before it builds anything and stops on a non-zero exit, when the use case sets `tests.on_release: true` (off by default, so 1.x behaviour is unchanged). Today only `pipewright test` and the CI pull-request workflow run it, while the reference says the release checks it too: fix that wording either way. Found writing the ebook's Chapter 7 (a renamed helper released, verified and failed at its first run) | Done: 1.2.0 (2026-10-31) | `release/service.py`, `testing/runner.py`, `config/schemas/reference.yaml` |
| FR-67 | **A failed run is an event and an alert:** the launcher records a `run.failed` event when a pipeline run it started ends failed or cancelled (the failed steps and their error lines, value-free), and `monitoring.alerts.events` may name it, so a data contract that stops a run (Chapter 3) or a broken release (Chapter 7) reaches the team without a separate log-based alert. Today the alertable events are monitoring.alert, retrain.requested, training.rejected, training.evaluated and model.registered | Done: 1.2.0 (2026-10-31) | launcher, `alerts/sync.py`, events |
| FR-68 | **The cut-off is never a feature by default:** the default `features.include` leaves out `data.features_from.entity.time_column` and the entity key columns, as it already leaves out the label, `data.split.time_column` and personal data. Today a use case with `features_from` and a split that is not by time (random or stratified, clustering) gets its cut-off date as a feature: the BigQuery ML evaluation then fails profiling it (`Invalid cast from DATE to FLOAT64`), and a model that does not fail learns from the date. Workaround: `features.exclude`. Found writing the ebook's Chapter 8 (customer-segments, k-means) | Done: 1.2.0 (2026-10-31) | `config/ml.py` (`skip`), bqml steps |
| FR-69 | **Forecasts evaluated per series:** the evaluation record of a forecast holds its metrics per series (`bqml.forecast.id_columns`) next to the overall ones, `pipewright model` shows the worst series, and a gate may be set per series (for example `mape` at most 0.6 in every series) as well as overall. Today only the overall mae, rmse and mape are recorded, so a team finds the series that failed the gate by querying the run's test table. Found writing the ebook's Chapter 8 (one stocked-out product failed a ten-product forecast) | Done: 1.2.0 (2026-10-31) | bqml steps (`forecast_*` SQL), evaluation record, `models/gates.py` |
| FR-70 | **One order for the contract and preparation:** in the BigQuery ML path the data contract is checked on the run's rows after `bqml.prepare` has run, while the container path checks it before preparation; check the contract on the rows as read (before `prepare.sql`) in both, so a team can rescale or derive columns in prepare without breaking the contract's ranges, and say so in the reference. Found writing the ebook's Chapter 8 (log-scaled features in prepare.sql failed `min: 1` on orders) | Done: 1.2.0 (2026-10-31) | bqml steps (`bqml_validate`, rows SQL), reference |
| FR-71 | **The starter moves to 1.1.0:** pipewright-starter's README and `publish_launcher.sh` default to the 1.1.0 launcher and package. Ray stays out of the starter by design (user, 2026-10-30): a team that wants Ray steps on a starter environment makes the grants the README lists by hand (the launcher's custom role for `aiplatform.persistentResources` create, delete, get and list; each tenant runner `roles/iam.serviceAccountUser` and `roles/iam.serviceAccountOpenIdTokenCreator` on itself), as the ebook's Chapter 9 shows; Pipewright Enterprise's Terraform already grants them (`modules/ray-network`, the private-managed blueprint) | Done: 1.2.0 (2026-10-31) | pipewright-starter (README, `scripts/publish_launcher.sh`) |
| FR-72 | **Re-check the champion against today's gates:** `pipewright model list` already shows each version against the gates in force (the ebook's Chapter 10: `v5 ... [gates FAIL: slice gate auc: the evaluation record has no slices]`), but nothing measures a version that predates a gate. Add `pipewright model check --version <v>`, which evaluates the version on its own test rows against today's gates, slices and fairness, records the measures with it, and says what it would fail; and record the champion's slices and fairness measures in each challenger's evaluation next to its metrics. Today a default version from before a gate stays unmeasured until a challenger replaces it | Done: 1.2.0 (2026-10-31) | evaluate step, `models/gates.py`, `cli/main.py` |
| FR-73 | **Online serving fills missing values as training did:** batch scoring fills an empty feature with the training median (`training_stats.json`), but the TorchServe handler builds the tensor from the request as it is, so an empty value (`null`) fails with HTTP 503 "Prediction failed" (the ebook's Chapter 13: `avg_days_between_orders` is empty for 38.5% of customers). The same customer can be scored in batch and not online: training/serving skew. Package `training_stats.json` in `model.mar` and fill empty values with its medians in `_preprocess_rows`, as `fill_missing` does; a value that cannot be filled (no median, not a number) answers 400 with the feature's name, never 503. Opt out with `model.serving.fill_missing: false` for a team that wants a refusal. The same holds for online features: the handler refuses an entity whose view has no value (`the online store has no value of avg_days_between_orders for this entity`), so a one-order customer cannot be scored online at all; fill it from the same medians | Done: 1.2.0 (2026-10-31) | `torchserve_handler.py`, the packaging in `register.py`, examples |
| FR-74 | **`deploy-model` is safe to repeat:** running `deploy-model` while the same version is already serving deploys it again (the ebook's Chapter 13: a second deploy of v8 started while the first was still running, and Google Cloud refused it only because the traffic split changed under it: `400 Fail to update the traffic split which has been changed after the operation starts`). Make it a no-op when the version is already serving with the intended traffic (`lapse-risk-endpoint already serves v8: nothing to do`, as `promote` does), and refuse with a clear message while another deploy to the same endpoint is in progress (an endpoint operation still running), instead of starting a second | Done: 1.2.0 (2026-10-31) | `models/deploy.py`, `cli/main.py` |
| FR-75 | **The starter creates an online store:** `model.serving.features online` needs `tenants.<t>.features.online_store` in the manifest, and the starter writes no `features` block and creates no store, so a starter environment cannot use online features without hand-written Terraform (the ebook's Chapter 13 adds it to its copy). Port Enterprise's `online_features.tf` to the starter, without CMEK: optional `tenants.<t>.online_features { nodes, views { feature_groups, sync } }`, a Bigtable online store and feature views per tenant, `roles/bigquery.dataViewer` on the tenant dataset for the Agent Platform service agent, and `features.online_store` in the manifest. Document that a view's first sync, started when it is created, can fail for a minute while that grant propagates (`bigtable.tables.mutateRows` denied, seen live 2026-10-30); `pipewright doctor` already warns of a view with no finished sync. Also give the data team a way to register feature groups (`pipewright features register`, or the starter's README), as the book's `tools/data_team.py feature-group` does | Done: 1.2.0 (2026-10-31), the starter's store; `pipewright features register`: 1.4.0 (2026-11-06) | pipewright-starter (`online_features.tf`, `variables.tf`, `manifest.tf`, README) |
| FR-76 | **A promoted model reads the destination's online store, not the source's (bug):** with `model.serving.features online`, the training step packages the online lookup in `serving.json` (and in `model.mar`) with the store of the environment it trained in. `pipewright promote` copies the files unchanged, so production's endpoint reads **test's** online store (the ebook's Chapter 13: prod v2, promoted from test v10, answered every ID with `the online store answered HTTP 403 (view customer_activity)`, because `serving.json` named `projects/pw-ebook/.../featureOnlineStores/pw_retail_online`). It failed safely only because prod's runner cannot read the test project; with access it would have served test's feature values in production without an error. Fix: environment-specific settings are never baked into a model. `deploy-model` passes the destination's resolved online features (store, views, key) to the deployed model as an environment variable, and the handler prefers it over `serving.json`; `deploy-model` and `doctor` refuse a model whose packaged store is in another project than the environment's; a test that promotes a model with online features and checks the store the endpoint reads | Done: 1.2.0 (2026-10-31) | `models/deploy.py`, `torchserve_handler.py`, `checks/doctor.py`, `promote` tests |
| FR-77 | **`alerts sync` removes the email channels it no longer uses:** when an address leaves `monitoring.alerts.notify` (or `retraining.decisions.notify`), sync repoints the policy but leaves the Pipewright-managed email channel behind (the ebook's Chapter 14: a test address swapped for `ml-team@example.com` left a channel holding a personal address, deleted by hand). Delete a `pipewright-managed` channel that no policy in the project uses any more, after the policies are updated; never touch channels Pipewright did not create. `--dry-run` lists them as `would delete` | Done: 1.2.0 (2026-10-31) | `alerts/sync.py` |
| FR-78 | **Performance monitoring joins outcomes to the scores they judge (bug):** `monitoring.performance` joins the outcomes table to each key's *latest* prediction of the model version, by `key` only, and the scores table does not record what time a score was for. Once a newer batch is scored, outcomes are matched to the wrong scores. The ebook's Chapter 14: v10 scored September 2011 and measured AUC 0.770 on September's outcomes (right); the December scoring then reported AUC 0.955 on the same outcomes, December's scores against September's outcomes, with December's features already showing who came back (leakage, false reassurance). Fix: the scores table records the entity time column (`data.features_from.entity.time_column`, or `scored_for`), `monitoring.performance.labels` takes `time_column`, and the join is on key and time; until both are present, performance is not computed and the monitoring row says why. A test with two scored batches and outcomes for the older one | Done: 1.2.0 (2026-10-31) | monitor component, `score.py`, scores schema, `usecase.schema.json` |
| FR-79 | **A retrain case records what its training did:** after `pipewright retrain approve`, the case shows `Training: retrain submitted job ...` and stays `approved` whatever happens next. The ebook's Chapter 15: the approved retrain ran, the gates refused it (`gate calibration_error=0.1327 above max 0.05`), no version was registered, and the case, `retrain show` and the emails said nothing of it; the cooldown then suppressed new cases for a week, so the approver would believe a new model was coming. Record the outcome on the case from the run's `training.evaluated` and `model.registered` events: `trained: registered v11 (default|candidate)` or `trained: refused by the gates: <reasons>` (state `refused`); emit `retrain.refused` (an email under `decisions.notify`); a refused retrain does not start the cooldown, and the case can be approved again with another `--source-uri` or declined. `retrain list` shows the outcome column. Also: the monitoring row's `retrain_status` says `proposed (drift): message ...` when the launcher answered `no case: cooldown (7 days) ...` (seen live in Chapter 15); over Pub/Sub the run cannot know the answer, so record the launcher's answer on the row afterwards (or write `sent`, not `proposed`), and attach a cooldown alert to the case as an observation | Done: 1.2.0 (2026-10-31) | launcher (retraining cases), `retrain show/list`, `alerts/sync.py` |
| FR-80 | **`ci init` promotes instead of releasing to a `promotion.from` environment, and the starter makes the CI identity:** `pipewright ci init --environments test,prod` writes a `release-prod` job that runs `pipewright release --env prod`, rebuilding in production although `environments/prod.yaml` says `promotion: { from: test }` (`release` does not check it either), and no workflow ever runs `pipewright promote` (the ebook's Chapter 16 replaced the job by hand with a `pipewright-promote.yml` gated by the `prod` GitHub environment). Fix: (1) `release` refuses an environment with `promotion.from` and points at `promote`; (2) `ci init` writes, for each such environment, a promote workflow (dispatch, or after the source environment's training) in that GitHub environment, and an operate action `promote`; (3) `pipewright-decisions` runs every 15 minutes, which uses up a private repository's free Actions minutes: make the cadence an option (`--decisions-cron`, default hourly in working hours) and say so in the output; (4) the starter has no CI identity: add an optional `github = { repository = "org/repo" }` that creates a Workload Identity pool and provider limited to that repository, a CI service account in `operators`, the `workloadIdentityUser` binding, and outputs `GCP_WIF_PROVIDER` and `PIPEWRIGHT_SERVICE_ACCOUNT` for the GitHub environment; for an environment with `promotion.from`, read access to the source environment's registry, artifacts bucket and models; (5) operators cannot run `pipewright doctor`: `storage.objectAdmin` lacks `storage.buckets.get`, and nothing grants reading the launcher service, its subscription or topics (CI's first PR check: `FAIL artifacts bucket ...: no permission to read it`, five checks). The book's starter copies add `roles/run.viewer`, `roles/pubsub.viewer` and `roles/storage.legacyBucketReader` on both buckets to every operator; (6) operators may not publish to the events topic, so a CI identity's releases and promotions never reach the event log (`event release.live not published to .../pw-events: 403`, and `release.promoted` the same; the command still succeeds and only logs it): grant operators `roles/pubsub.publisher` on the events topic, and make a lost lifecycle event a warning in the command's summary, not a line in the log | Done: 1.2.0 (2026-10-31) | `ci/`, `release/`, pipewright-starter |
| FR-81 | **Installing a released Pipewright gives the kfp it was tested with:** `pyproject.toml` declares `kfp>=2.9,<3`, but `release` refuses to compile unless kfp's minor version equals the one pinned in the components' `requirements.txt`. A fresh `pip install "pipewright-mlops[gcp]==1.1.0"` today resolves kfp 2.18.0, so every release fails on a new machine or CI runner (the ebook's Chapter 16, first GitHub Actions run: `kfp==2.17.0 in requirements.txt does not match Pipewright's compiler (kfp 2.18.0)`; the book's local venv had 2.17.3). Fix: pin kfp to the tested minor in each release (`kfp>=2.17,<2.18` for the 1.1 line, raised deliberately with the examples' requirements), publish a constraints file with each release, and have `ci init` write `.github/pipewright-constraints.txt` and install with it, as the book's repository now does. Also: the error should say which install to change (`pip install "kfp==2.17.*"`) | Done: 1.2.0 (2026-10-31) | `pyproject.toml`, `ci/` templates, release notes |
| FR-82 | **`pipewright test` runs where the components' requirements are installed:** `tests.command` runs in the CLI's own environment, which has Pipewright's dependencies but not the components' (`numpy`, `pandas`, `torch`...), so a test that imports a component fails in CI with `ModuleNotFoundError: No module named 'numpy'` (the ebook's Chapter 16, first PR check) while passing on a laptop that happens to have them. Fix: `pipewright test` builds (and caches by the requirements' hash) a virtual environment from `components.requirements` and runs the command there, or runs it in the built component image with `--in-image`; `ci init` writes the step. A components venv first on PATH does not help: `testing/runner.py` replaces a leading `python` with Pipewright's own interpreter. The book's PR workflow therefore installs the components' requirements into Pipewright's venv, with the CI constraints file | Done: 1.2.0 (2026-10-31) | `cli/main.py` (test), `ci/` templates |
| FR-83 | **A first version is not the champion until a person says so, where policy is manual:** Model Registry makes a model's first version its default, so under `promote_model: manual` the first version still becomes the champion with nobody's approval. `promote --approve` covers the first promoted version (Chapter 12), but a model production trains itself (`model.promotion: retrain`) never passes that step: the ebook's Chapter 16 promoted demand-forecast through CI, production trained it, and v1 became DEFAULT in prod with no approval recorded. Fix: under a manual policy, register the first version under a `pending` alias and keep scoring and `deploy-model` refused (with `pipewright model approve` as the way out) until a person approves it; `model list` shows `DEFAULT (not approved)` for a version that became default without an approval, and `doctor` warns | Done: 1.2.0 (2026-10-31) | `models/registry.py`, bqml register step, `doctor` |
| FR-84 | **The starter keeps the event log, and lineage shows the decisions (bug-level gap):** the starter creates the `events` topic but nothing subscribes to it, so Pub/Sub discards every lifecycle event the CLI and launcher publish: promotions, model approvals, deployments, rollbacks, retrain decisions, each with its `approval` record (GitHub actor and run URL in CI). The ebook's Chapter 17 found production's log held only the pipeline runs' own log entries: Chapters 12, 13 and 16's promotions, approvals and deployments had left no record anywhere. Fix: (1) the starter ports Enterprise's observability module without CMEK: an evidence dataset, an append-only `events` table, a BigQuery subscription on the topic, operators read it (the book's starter copies now have `evidence.tf`); `doctor` fails when the events topic has no subscription; (2) `pipewright lineage` adds the version's decisions from the events table when the manifest names it (approved by, promoted by, deployed and undeployed, with how each was approved), not only in Fleet View; (3) the release pointer records the GitHub actor and run when a release is made in CI (`status` shows `by pw-github-ci@...` today, not the person who merged) | Done: 1.2.0 (2026-10-31) | pipewright-starter, `models/lineage.py`, `release/`, `checks/doctor.py` |
| FR-85 | **A replay reproduces the run exactly:** the churn example's prepare step trains on rows in the order BigQuery returns them, and training shuffles from that order with a fixed seed, so a replay of the same snapshot differs in the fourth decimal (the ebook's Chapter 5 noted it). Chapter 17 showed it matters: replaying the run behind production's model gave AUC 0.785136 against the original's 0.78617, under both the original and a new release (the two replays agree exactly), and the small difference moved the fairness gap from inside 0.15 to 0.1517, so the same data, code and image got a different gate decision. Fix: the examples' prepare step sorts by `data.contract.unique` before splitting (the book's ch17 does); the ingest step records a fingerprint of the rows (a hash over the sorted rows) in lineage.json, and a replay compares its own with the original's and says `replayed rows identical` or how they differ; `lineage` shows it | Done: 1.2.0 (2026-10-31) | examples (prepare.py), ingest step, `models/lineage.py` |
| FR-86 | **Machine-readable fleet state, and BigQuery ML runs in the metrics table:** the ebook's Chapter 18 builds a one-page fleet report from open-source Pipewright and had to read the release pointers, registry labels and BigQuery tables directly, because `status`, `model list` and `monitor status` print text only (`lineage` and `decisions pending` have `--json`). Add `--json` to `status`, `model list`, `monitor status` and `retrain list`, with stable field names. The BigQuery ML use cases write no `<use case>_model_metrics` rows, so their training history (refused runs included) exists only in the event log: write the same row from the bqml evaluate step. Consider shipping the book's `tools/fleet_report.py` as `pipewright report fleet` (read only, static HTML, emails masked on request): the open-source counterpart of Fleet View for a handful of use cases | Done: 1.2.0 (2026-10-31), `--json` and the metrics table; `pipewright report fleet`: 1.4.0 (2026-11-06) | `cli/main.py`, bqml evaluate step, a report command |
| FR-87 | **Old images are cleaned up, never the ones a release still needs:** every release builds and pushes its images, and nothing removes them: after the ebook's chapters, pw-ebook's registry held 36.9 GB (20 builds of the synthetic image at about 0.9 GB each, 23 of the components image) and production 3.5 GB, a standing storage cost that grows with every release. A blanket cleanup policy would break rollback and replay, which need the images of earlier releases. Fix: the starter sets an Artifact Registry cleanup policy that deletes untagged images older than N days, and `release` tags each image with the release ID it belongs to; `pipewright images prune --keep <n>` (dry run first) deletes images no release in any use case's pointer history, nor a promoted release in another environment, references; `doctor` reports the registry's size | Done: 1.2.0 (2026-10-31) | `release/`, a new `images prune` command, pipewright-starter (`registry.tf`), `checks/doctor.py` |
| FR-89 | **An approval keeps the evaluation it was judged on:** a question on the 1.3.0 launch post asked whether the trail keeps the evaluation a reviewer was looking at, or only the approval. Only the approval: the `model.approved` event names the version, the approver and how it was approved, and the gates are re-checked against `evaluation.json` at approval time, but nothing records which record that was, so a record changed or removed later cannot be told apart from the one approved. Fix: the approval event (first versions, and approvals made with `promote --approve`, too) carries `evaluation: {record, generation, sha256}`, the record's location, its Cloud Storage object generation and the SHA-256 of its bytes; a record that cannot be read never blocks the approval (the event then has no digest); `pipewright lineage` shows the digest under each approval and checks it against the record on file now: unchanged, changed or missing. Additive: older approvals show no digest line | Done: 1.3.1 | `models/gates.py`, `models/registry.py`, `models/lineage.py`, `release/promote.py` |
| FR-92 | **Online endpoints in Terraform: the endpoint, its IAM and its network (public or private), with `deploy-model` unchanged:** asked by the user 2026-11-04. **What stays as it is:** `pipewright deploy-model` keeps today's behaviour exactly: it looks for the use case's endpoint by name (`<use case>-endpoint`, in the environment's region), creates a standard (public) one only if none exists (CMEK from the manifest, use case and tenant labels), and deploys the default version onto it with the use case's `model.serving` (machine, replicas, accelerator, scale to zero, canary, promote, roll back). One endpoint per use case per environment; each version is a deployed model on it, so no deployment creates an endpoint after the first. Nothing existing breaks: an environment without the Terraform below works as today. **What FR-92 adds, in Terraform only** (the parts that change rarely): the **pipewright-starter** example (optional, off by default) and the **Enterprise private-managed blueprint** can create each listed use case's endpoint up front, named `<use case>-endpoint` so `deploy-model` finds and reuses it, with its CMEK and labels; the IAM to call it (which identities may predict) and to deploy onto it; and for a private endpoint the networking: Private Service Connect settings on the endpoint (set at creation, never changed by a deploy), a service connection policy or PSC endpoint in the environment's VPC and subnet, a private DNS zone for its name, firewall rules for the callers, and the perimeter rules (Enterprise). Terraform ignores the endpoint's deployed models and traffic split, so an apply never undoes a deploy, and `deploy-model` never needs Terraform to run. **Visibility:** the endpoint's settings are in Terraform and its state; `pipewright status` and Fleet View can show each endpoint's network, machine, replicas and traffic from the API, and `doctor` can check a listed endpoint exists. **CLI work for a private endpoint only** (public endpoints keep today's code path): (a) *management calls, expected unchanged, verify before building:* they go to the regional Agent Platform API, which is the same for public and private endpoints (Private Service Connect changes where predictions are served, not how an endpoint is managed): finding the endpoint by name (does the SDK's list return private endpoints), deploying through the SDK's plain `Endpoint` class (the SDK also has a `PrivateEndpoint` class), traffic split and undeploy (canary, promote, roll back), and the v1beta1 scale-to-zero request (if private endpoints do not support it, refuse `scale_to_zero` with `network: psc`); (b) *prediction calls, which break and must change,* because all three send predictions to the public URL (`<region>-aiplatform.googleapis.com/...:predict`), which a private endpoint does not answer: the **BigQuery ML parity check** in `deploy-model` (`bqml/serving.py`, predicts the test rows and compares them with ML.PREDICT; on a private endpoint the predict fails, `deploy-model` reads that as a parity failure and removes the version it just deployed, so this is the dangerous one): send it to the private address when the CLI can reach it (CI inside the VPC), otherwise say plainly that parity could not be checked instead of rolling back; **`pipewright model predict`**: use the private DNS name or address, or say it must run inside the VPC; **the "How to call it" text after a deploy**: print the private DNS name or address and that callers must be in the VPC; (c) *one safety refusal:* when a use case is meant to be private and its Terraform-made endpoint does not exist yet, `deploy-model` refuses ("create it with Terraform first") instead of creating a public one on the first deploy. Each of these applies only when the endpoint is private. Additive and off by default. Wording to be reviewed again with the user before any build | Phase 9 (user, 2026-11-05); the user reviews the wording before it is built | pipewright-starter (`endpoints.tf`), Enterprise blueprint; for private endpoints only: `models/deploy.py` (refusal, how to call), `bqml/serving.py` (parity), `cli/main.py` (`model predict`) |
| FR-93 | **`deploy-model` checks the evaluation is the one approved:** since 1.3.1 (FR-89) each approval records the evaluation it was judged on (record, generation, SHA-256) and `pipewright lineage` checks it, but only when someone runs it. `deploy-model` re-reads the record on file and re-runs the gates before serving (so a record edited to fail is refused), yet never compares it with the approval's digest: a record edited after approval that still passes is deployed with no warning. Found from a reader's follow-up on the 1.3.1 post (2026-11-05: their CI gate reads metrics once and would never notice a later edit). Fix: before serving, `deploy-model` reads the default version's latest `model.approved` event (the evidence dataset's `events` table, as `lineage` does) and compares the record's SHA-256 with the approval's; changed or missing is refused (the message names the approval and both digests, and says to re-approve after review), with `policies.deploy.evaluation_check` (`refuse` or `warn`) in the manifest (default `warn` in 1.x so nothing that deploys today stops; refuse recommended). Approvals from before 1.3.1 (no digest) and environments without the event log deploy as today, with a note. Recorded on the `model.deployed` event (`evaluation_check`: unchanged, changed, missing or not recorded). Also (user, 2026-11-05): **`promote` runs the same check** on the source version before copying (today it checks the copy matches the source, not that the source is still what test approved, so an edited record would be promoted and production's approval would record the edited one); and **a default with no approval is refused**: `deploy-model` (and `promote`) refuse a default version that has no `model.approved` event, because the `default` alias can be moved outside Pipewright (console, gcloud, the API, by anyone with `aiplatform.models.update`) and today only a first version's `approval: pending` label is checked. The inference pipeline also scores with `@default`: the launcher warns on such a run (refusing batch scoring is a policy choice, `policies.inference.unapproved_default` (`warn` or `refuse`)) | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | `models/deploy.py`, `models/lineage.py` (shared reader), manifest schema |
| FR-94 | **What is served is what was approved: model files and serving image by digest:** an approval records the evaluation's digest (FR-89) but not the model files', so a model file replaced after approval (evaluation untouched) goes unnoticed; promotion already computes each file's SHA-256 when it copies. And a version's serving container is registered by tag (the churn example: `.../prediction/pytorch-cpu.2-4:latest`), so the image an approved model runs in can change under it. Fix: (1) the approval records the model folder's files with each file's SHA-256 and generation, `lineage` checks them, and `deploy-model` and `promote` refuse a mismatch (with FR-93's warn or refuse setting); (2) registration resolves the serving image's tag to its digest and registers the digest (the examples' `register.py` and the BigQuery ML registration), recorded in `lineage.json`; `validate` warns on a tag; promotion and the KServe mirror keep the digest. Found in the 2026-11-05 review of the approval chain | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | examples (`register.py`), `models/registry.py`, `models/deploy.py`, `release/promote.py`, `models/kserve.py`, `models/lineage.py` |
| FR-95 | **Show what changed, and who changed it:** when `lineage` (or Fleet View) finds an approved evaluation changed, it says only that. The approval records the record's generation and the artifacts bucket keeps older versions (the starter turns versioning on; Enterprise keeps them 30 days), so `lineage` can read the approved generation and show the difference (e.g. `auc 0.8098 -> 0.9198`), and, where Data Access audit logs are on for the bucket, who wrote the new generation and when. Optional and read-only | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | `models/lineage.py`, `models/gates.py` |
| FR-96 | **Approval and deploy judge by the released rules, not the local checkout:** `pipewright model approve` and `deploy-model` resolve the use case from the person's working copy, so the gates they re-check (`ml.evaluation`) and the approvers they allow (`approvals.approvers`) are whatever that copy says: an approver can lower a gate or add themselves locally and approve a version that fails the gates its release was built with. The manifest's policies (`promote_model`, `min_gates`) still apply. Fix: approve and deploy read the gates and approvers from the release contract that trained the version (its `ml_config` digest is in `lineage.json`) and refuse when the working copy differs in those keys, naming them; a stricter local gate may still apply. Found in the 2026-11-05 review | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | `models/registry.py`, `models/deploy.py`, `release/contract.py`, `cli/main.py` |
| FR-97 | **A decision trail that cannot be forged or quietly rewritten:** lifecycle events are published by the CLI with the caller's own credentials, and every operator and CI identity may publish to the events topic, so an event, its `by` and its digest are what the client says: anyone who can publish can add a `model.approved` that never happened. In the open-source starter the events table is writable only by the Pub/Sub agent (and project owners), but every tenant's runner has `storage.objectAdmin` on the whole artifacts bucket (the starter's own comment says so), so one team's runs can overwrite another team's model files and evaluation records. Fix: (1) `lineage` and Fleet View cross-check each approval with the Cloud Audit Logs Admin Activity entry for the alias move (`MergeVersionAliases`; always on, cannot be turned off), showing the principal the audit log names and flagging an event with no matching entry or another principal; (2) the starter scopes each runner's artifacts grant to its tenant's pipeline-root prefix with an IAM condition; (3) optionally, an approval places a hold or retention on the version's model folder (Cloud Storage object retention) so approved files cannot be overwritten. Found in the 2026-11-05 review | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | `models/lineage.py`, pipewright-starter (`identities.tf`), Enterprise Fleet View |
| FR-98 | **Model serving observability (brainstorm, later phase):** once a model serves, collect operational signals (is the endpoint healthy: requests, errors, latency, saturation, replicas, cold starts with scale to zero) and model signals (is the model doing its job: input drift against training, the prediction distribution, outcomes joined later, quality per slice), per deployed version, so canaries and A/B arms (FR-57) are compared on the same signals. Options raised by the user 2026-11-05: a plugin or Python package (a request and response logger and metric emitter for the serving container and KServe), or collect every trace with OpenTelemetry and let teams stream or export them where they want and build their own monitoring and alerting; Pipewright would own the collection, the names and the joins (request, version, arm, release), not the dashboards. To work out: Agent Platform request-response logging to BigQuery against a sidecar or in-container emitter, sampling and personal data (requests carry features), cost, KServe parity, and how it feeds `monitoring` (batch drift today) and the retrain loop | Phase 11: to be reviewed and prioritised (user, 2026-11-05); brainstorm only, not designed | serving containers, `models/deploy.py`, `models/kserve.py`, monitoring, Enterprise Fleet View |
| FR-99 | **The recommended feature pattern in the reference example:** the `churn` README points to the `features` example up front (a prepared table is fine to start with; feature tables or feature groups are recommended, docs/FEATURES.md), and a `churn` variant on feature tables, so the reference shows the recommended way too. Planned for 1.3.0 (user, 2026-11-01) and not delivered (checked 2026-11-05) | Done: 1.4.0 (2026-11-06), built and tested live (and in a QA round as a bank's governance team would test it) | `examples/churn`, `examples/features`, docs/FEATURES.md |
| FR-100 | **A runner that cannot move a model's default:** found in the 1.4.0 governance QA (G7): each tenant's runner holds `roles/aiplatform.user` project-wide, which includes moving any model's `default` alias and labels, other tenants' too. Caught today (deploy and promote refuse a default with no genuine approval, the audit cross-check, lineage names who moved it) but not prevented. Idea: a custom runner role without alias moves under a manual approval policy (the register step labels versions, so `models.update` needs care) | Phase 11, to review (user) | G7 in the 1.4.0 QA |
| FR-101 | **Know when an approved record changes between deploys, without Fleet View:** asked by the user 2026-11-06, from a reader's question on LinkedIn (their gate judges a run once, and a startup check compares run ids without the metrics; "when a record changes for a version that's already serving, does anything tell you before the next deploy, outside of Fleet View?"). **Today (1.4.0, open source):** a changed evaluation record, model file, serving image, BigQuery ML model or release contract is caught only at the next `deploy-model` or `promote`, or when someone runs `pipewright lineage`; nothing tells anyone in between. The lock prevents an in-place overwrite, but it is off by default (`lock.days: 0`), and on a versioned bucket a new object version can still be put over a locked file. The scoring-run check (`policies.integrity.inference`) only looks for an approval on the default version; it does not check that the files are still the approved ones, so batch scoring could use swapped files. A CI gate on `lineage --json` needs the caller to parse JSON (no exit code). **Proposed (designed with the user before it is built):** (1) *a watch:* on the launcher's existing scheduler tick, verify each use case's default version, and each version serving on an endpoint, against its newest approval (`integrity.verify`: generation then SHA-256, the serving image digest, a BigQuery ML model's etag, the release contract's SHA-256), and record a `model.integrity` event when the result changes (changed, missing, restored), naming records, files and digests only, never data; (2) *an alert:* `alerts sync` adds a log-based metric and a Cloud Monitoring policy on `model.integrity` (changed or missing) and `run.integrity`, to the use case's notification channels; (3) *the scoring-run check verifies:* with `inference: warn` or `refuse`, the launcher runs the full check before a scoring run, not only the approval lookup; (4) *a CI gate with an exit code:* `pipewright integrity check --env <env> [--usecase <uc>] [--json]` exits non-zero when any approval is not unchanged, for a CI step before deploy or on a schedule; (5) *a nudge:* `doctor` warns when a production environment has the lock off or `mode: warn`. **Spike (user, 2026-11-06), run live in pw-ebook and torn down the same day: does a swapped model file reach serving without a deploy?** Method: a serving container that reports the `AIP_STORAGE_URI` it was given, the SHA-256 and generation of the model file it loaded at start-up, and its replica; model A approved, then model B written as a new object version over it (as a versioned artifacts bucket allows). *Agent Platform endpoints: no.* `AIP_STORAGE_URI` is not our bucket but Agent Platform's own copy in a Google-managed tenant bucket (`gs://caip-tenant-.../artifacts`), taken when the model version is uploaded: after the swap, the running replica, a new replica from a scale-out (min replicas 1 to 2) and a fresh deploy of the same version (a new tenant copy, written after B was in our bucket) all served A. *KServe on GKE: yes, confirmed live* (GKE Autopilot, Pipewright's serving container spec: the same `AIP_*` variables, Workload Identity; the KServe controller itself was not installed, it only creates a Deployment with that container): the container downloads from the live model folder at every start, so after the swap the scale-out pod served B while the old pod served A (one service, two models), and once the old pod was replaced (as an eviction or node upgrade does) every pod served B. No deploy, nothing noticed. *Batch scoring: yes, from the code* (the same mechanism as the GKE result): the reference example's `champion.py` (`load_model`, used by `inference_pipeline.py`) downloads `model.pt`, `features.json` and `training_stats.json` from the live folder on every scoring run, and the scoring check only asks whether an approval exists. **What it means for the design:** (a) *KServe and batch scoring must read the approved bytes, not the live folder:* load the approved generations recorded on `model.approved` (`<file>#<generation>`) and check each SHA-256 before use, failing closed (KServe: an init step or a per-approval immutable copy that `AIP_STORAGE_URI` points at; scoring: `load_model` takes the approved generations from the approval); this closes the hole even with the lock off, and is the highest-priority item of FR-101; (b) *Agent Platform is safe after upload, but the check verifies a proxy:* what serves is the copy taken at upload, while the approval and deploy check the bucket; the register step should record the folder's digests at upload (so the approval's files are provably what Agent Platform copied), leaving only the window between training and registration; (c) *the watch (1) and alert (2) stay*, now aimed at the KServe and scoring paths first; a swap on a GKE environment is an incident, not a drift. Spike code and results: the session's scratchpad (not kept). Additive and off by default (`policies.integrity.watch: off | warn`), reads metadata only. **Enterprise:** Fleet View already re-checks every approval's evaluation record, files and BigQuery ML model on each collection and shows it; it does not notify. Follow-up: the alert in (2) covers Enterprise environments too, and EV-06 records when a change was first seen and when the approved bytes were restored. **Also in FR-101 (user, 2026-11-06, from a review as architect, governance, SRE and security):** (a) *every way back is checked like the way forward:* `loop rollback` makes the earlier version the default without `integrity.verify`, and `pipewright rollback` re-checks the old release's images but not that its contract is still the fingerprinted one; both get the same checks as deploy and promote; (b) *the watch compares what each endpoint serves* (its deployed models and traffic split) with the approved default, so a deploy or traffic change made in the console is caught; (c) *the lock lasts while the version is in use:* today it ends `lock.days` after the approval and is never renewed, so a model serving for a year is unprotected after day 30; renew it while the version is a default or serving, release it when the version is retired; (d) *the watch has a heartbeat:* an alert when its checks stop arriving, so a broken watch does not look like "unchanged"; (e) *a response plan:* who is paged (the use case's owner, the platform team), whether scoring is refused or warned when a change is found, the restore command, and an override for `refuse` that is recorded with a name and ends on its own. | **Released in 1.4.1** (2026-11-07; phase 7, design in section 8.18; live-tested on Agent Platform and private GKE, QA round on branch qa) | LinkedIn thread 2026-11-06; `models/integrity.py`, `launcher/app.py` (tick, scoring check), `alerts/sync.py`, `cli/main.py`, `checks/doctor.py`; Enterprise `fleet/collect.py`, `evidence/collect.py` |
| FR-102 | **An audit trail that lasts as long as the models do:** from the same review (user, 2026-11-06). (1) *Audit retention:* the audit cross-check (1.4.0) reads Cloud Audit Logs, which keep Admin Activity for 400 days, so an approval older than that is not cross-checked, and regulated models often serve for years; the starter and the Enterprise blueprint route Admin Activity entries for Agent Platform and the artifacts bucket to a log bucket with locked retention (or a BigQuery sink) for the period the manifest sets, the cross-check reads from there, and `doctor` says when it is missing; (2) *who wrote a file:* `lineage` names who changed an approved record from Cloud Storage Data Access audit logs, which are off by default, so the answer is usually unknown; Terraform turns on DATA_WRITE logs for the artifacts bucket (optional, on by default in the Enterprise blueprint) and `doctor` warns when they are off; (3) *break glass is time-boxed:* `policies.integrity.mode: warn` gets an end date, is recorded (who, why, until) and alerts while it lasts, and restoring approved bytes is recorded as an action with a name; (4) *who can write approved folders:* `doctor` lists the people and identities, other than the runners, that can write or delete in the artifacts bucket of a production environment (Storage Admin, Owner, custom roles), because least privilege prevents what the checks only detect. Additive and off by default | **Released in 1.4.1** (2026-11-07; phase 7) | starter and blueprint logging, `checks/doctor.py`, `models/integrity.py`, `models/lineage.py` |
| FR-103 | **Immutable feature definitions and the complete ML release (epic, section 8.17):** an approved model always uses the feature definitions, transformation logic, schema and serving configuration it was evaluated with. Today the approval pins the model, its evaluation and the release contract, but not the definitions of the feature tables, feature groups and views it reads when it scores. Ten stories from the user (feature contracts, SQL and schema fingerprints, materialisations, validation in the pipeline, feature quality, feature lineage, the release manifest, approval of the complete release, checks at deployment, rollback and drift of definitions). Principle: definitions are immutable, values are not (new transactions are fine under the same contract; reproducing a historical prediction needs the original values or a reproducible source state) | **Released in 1.4.1** (2026-11-07; phase 8, design in section 8.17, live-tested across projects and under CMEK) | section 8.17; `config/data.yaml`, `features`, `models/lineage.py`, `models/integrity.py`, the release contract, the launcher's scoring check |
| FR-104 | **Choose the online feature store, with its cost shown (section 8.19):** online lookup (FR-51) uses Google Cloud's Feature Store online serving on Bigtable, about $781 a month per node in London whether or not it is called, one per tenant in the starter. A live spike (2026-11-06, `docs/evidence/fr104-online-stores/`) measured, at 10 lookups a second: Bigtable 12.5 ms; BigQuery + BI Engine 198 ms end to end through the BigQuery API at about $38 per GiB, 0 bytes billed per cached lookup; Cloud SQL for PostgreSQL 3.3 ms direct, 19 ms through a Cloud Run lookup service, about $61; plain BigQuery billed a full table read per lookup (control). Settled (user, 2026-11-07): `features.online_store.type: bigtable | bigquery | cloudsql | http` per environment, Bigtable the default; `bigquery` with a BI Engine reservation and coverage guards; `cloudsql` first-party (Terraform, IAM login, Pipewright-owned sync through a Parquet export with a manifest and an atomic swap); the `http` contract; the export usable on its own; other databases (Firestore first) as separate open-source plugins | Phase 9, with FR-92 (user, 2026-11-07); the user reviews the design before it is built | section 8.19; `models/online.py`, the serving handler, `checks/doctor.py`, starter `online_features.tf`, the plugin API |

### 4.2 Non-functional requirements

| ID | Requirement | Status | How |
|---|---|---|---|
| NFR-01 | **Reproducible releases:** images, templates and contracts addressed by digest; a published contract never changes | Met | D-41, D-42 |
| NFR-02 | **Idempotency:** retries and redeliveries never duplicate runs, releases or decisions | Met | deterministic run IDs, run keys, create-only writes, generation checks |
| NFR-03 | **Isolation between tenants:** own runner identity, dataset, landing and outputs prefixes, and pipeline root (`pipeline-root/<tenant>/<usecase>`) | Met | release contract, launcher |
| NFR-04 | **Customer-managed keys** applied to pipeline jobs, models, endpoints and BigQuery query results when the manifest names them | Met | D-37, D-44 |
| NFR-07 | **Data residency:** Pub/Sub publishes through the environment's regional endpoint | Met | D-40 |
| NFR-09 | **Auditability:** every change and use of a model is traceable to identity, data, release and approval | Met | events carry `by`; contracts carry provenance |
| NFR-11 | **Fail early, in plain language:** schema, pre-flight and verify gates before anything goes live | Met | `validate`, `doctor`, release verify |
| NFR-12 | **Vendor-neutral observability:** structured logs and events; tools connect through platform configuration | Met | D-30 |
| NFR-13 | **Use-case code does not change between deployment profiles** | Met: Agent Platform Pipelines and Kubeflow Pipelines on private GKE run the same compiled templates | adapters, manifest |
| NFR-14 | **Works behind TLS-inspecting proxies** | Met | D-49 |
| NFR-18 | **Value-free error messages:** errors name the column and rule, never the offending value | Met on `phase-1` (1.1.0): BigQuery and Cloud Storage errors on the data paths (runtime `read_source` and `write_table`, the BigQuery ML steps, the CLI's BigQuery queries) have quoted literals and values taken out (`pipewright.errors.value_free`); the example's contract check reports counts, not values | D-65 |

### 4.3 Constraints

| ID | Constraint |
|---|---|
| C-01 | Google Cloud only. No AWS or Azure work, and no multi-cloud abstractions |
| C-02 | Pipelines are Kubeflow Pipelines v2; `kfp` pinned to the compiler's major.minor (2.17) |
| C-03 | Python 3.11; uv for environments; ruff, line length 120 |
| C-04 | Any Python ML framework: a use case's components install what they need from its `components/requirements.txt`, and its register step names the serving container (an Agent Platform prebuilt or the team's own). The example uses PyTorch |
| C-06 | Agent Platform naming: Gemini Enterprise Agent Platform was formerly Vertex AI (renamed April 2026). APIs, SDK, IAM roles and the orchestrator value `vertex` are unchanged |
| C-07 | Pipewright starts at a `source_uri` (files on Cloud Storage, or BigQuery tables and views); how data gets there is the team's own pipeline (D-17) |
| C-08 | GenAI and LLM fine-tuning are out of scope for v1. Kept for 1.1 (user, 2026-10-24): `validate` and `release` refuse BigQuery's generative AI functions (`AI.*`, `ML.GENERATE_*`) in a use case's SQL |

---

## 5. Personas and ownership

**Rule (D-04):** durable, shared or security-relevant resources belong to the platform team. Use-case runtime
objects belong to the app team. Some services are run by the platform while their contents belong to the team.

| Area | Platform team | App team |
|---|---|---|
| Projects, network, perimeter, keys, organization policies | owns (with infosec) | uses |
| Buckets: landing, artifacts, outputs | owns; lifecycle and retention | writes under its prefixes |
| BigQuery | owns datasets (one per tenant, IAM) | owns tables inside them |
| Identities | owns runner service accounts per tenant, CI identities | runs as them |
| Artifact Registry | owns repositories | pushes images and templates |
| Launcher, Pub/Sub routing, approver groups | owns and operates | triggers runs, decides retrains |
| Agent Platform Pipelines, Model Registry | runs the services; keys and access | owns runs, model versions, approvals, endpoints |
| Environment manifest | writes it | reads it |
| Pipeline code, configuration, gates, thresholds, alert recipients | provides the example and helpers | owns |
| Releases, schedules, rollbacks | sets policy minimums per environment | owns |

**CI identities (D-07):** each environment has an infrastructure identity for the platform, and an MLOps identity
per app repository that can only manage use-case runtime objects (never buckets, datasets, service accounts, IAM or
networks).

---

## 6. Architecture

```
App team --YAML + code--> CI (pipewright ci init) --pipewright release--> registries + write-once contract + live pointer
Upstream systems --files--> landing bucket --GCS notification--> Pub/Sub --push--> launcher /pubsub
pipewright run, pipelines --> run-requests topic --push--> launcher /pubsub/run
Cloud Scheduler --> launcher /run (schedules) and /retraining/tick
launcher --submits PipelineJob (tenant runner; manifest key and network)--> Agent Platform Pipelines
pipelines --> tenant tables, Model Registry; emit_event --> lifecycle events topic
inference drift --> launcher (proposal) --> retrain case in the artifacts bucket --> decision events and email
approvers --> approver group topic --push--> launcher /retraining/decide --> training on quorum
events and logs --> alerts (Cloud Monitoring), the platform's tools, audit
```

**Main flows:**

1. The platform publishes the manifest.
2. Release: tables sync, image and templates by digest, write-once contract, pointer move.
3. Trigger: a file, a schedule or a run request reaches the launcher.
4. Run as the tenant's runner, with the environment's key and network.
5. Pipelines train or score, register, monitor and emit events.
6. Drift or a performance drop opens a retrain case; people decide.

---

## 7. Design decisions

**Status:** Active, Superseded (replacement named), Revisit (open for review).

| ID | Decision | Why | Status |
|---|---|---|---|
| D-01 | Deployment profiles: `poc`, `private-managed`, `private-gke` (named in the manifest's `blueprint`) | Users range from demos to regulated, cluster-only environments | Active |
| D-02 | Two personas with two configurations; app teams assume infrastructure exists | Infrastructure and model changes must not block each other | Active |
| D-03 | App configuration read by the CLI and validated with JSON Schema | One source of truth per side | Active |
| D-04 | Ownership: durable, shared and security resources are the platform's; runtime objects are the team's | Clear accountability | Active |
| D-05 | Onboarding handshake: tenant entry, then manifest, then names in YAML, then `doctor` | Teams never ask for resources ad hoc | Active |
| D-06 | One shared, multi-tenant launcher; releases move a pointer, never redeploy it | Model releases independent of platform changes | Active |
| D-07 | Two kinds of CI identity per environment (infrastructure; MLOps per app repository) | Least privilege | Active |
| D-09 | One project per environment | Isolation, simple IAM | Active |
| D-13 | The package is installable from a private Python repository; images are built in the user's project | Supply chain stays inside the user's organisation | Active |
| D-15 | Models can be promoted between environments (`model` default, `retrain` optional) | Governed path to production | Active (built 2026-10-11: section 8.12, D-84) |
| D-16 | Framework-neutral: teams bring their own ML stack through their component requirements and serving container; the example uses PyTorch | Teams keep the tools they know; Pipewright governs the lifecycle, not the model code | Active (revised 2026-10-19; was "PyTorch first") |
| D-17 | Ingestion is left to ML teams | Scope | Active |
| D-21 | One use-case definition for all environments; a fixed key list may vary | Production is the same model judged the same way | Active |
| D-22 | Schemas live with the use case; `tables sync` alone changes tables, additive only | No surprise breaking changes | Active |
| D-23 | ML lifecycle sections reach pipelines as one `ml_config`; Pipewright checks consistency, code does the ML | Configuration over code | Active |
| D-24 | Several pipelines per use case under contract v3; triggers choose the pipeline | Training and inference released together | Active |
| D-25 | Training never scores its own data | Clean separation | Active |
| D-26 | The registry's default version is the champion | One clear "current best" | Active |
| D-27 | A use case may span files; a section lives in exactly one file | Readable, reviewable | Active |
| D-28 | Compute per step is configuration (`steps`), patched into the compiled template | No code change for compute | Active |
| D-29 | Tables in BigQuery, Parquet on GCS, or both, from one schema file | Users' analytics choice | Active |
| D-30 | Vendor-neutral observability: Pipewright emits logs and events; the platform routes them | No vendor code in pipelines | Active |
| D-31 | **Open core:** Apache-2.0 Pipewright plus Pipewright Enterprise | Adoption plus a commercial line for regulated users | Active |
| D-34 | **Enterprise never patches the core.** It uses published extension points: manifest fields, launcher routes, CLI plugins | One core; enterprise needs become general, tested hooks here | Active |
| D-35 | Google Cloud only | Focus | Active (C-01) |
| D-36 | CLI plugins through the `pipewright.commands` entry point group; `register(subparsers, common)`; a broken plugin is skipped | Add commands without forking the CLI | Active |
| D-37 | The manifest `security` block (`kms_key`, `bigquery_kms_key`, `network`, `reserved_ip_ranges`, `perimeter`) is read **at submit or query time**, not baked into releases | The platform can rotate keys or change networks without every use case re-releasing | Active |
| D-38 | **Run requests over Pub/Sub** (`launcher.run_requests_topic`, launcher `/pubsub/run`) in private environments; `pipewright run` and retrain proposals publish there | An internal-only launcher cannot be called from pipelines or laptops; Pub/Sub push is allowed inside a perimeter | Active |
| D-39 | The launcher can run with internal ingress only; its URL is the deterministic `https://<service>-<project number>.<region>.run.app` | Private by default; the URL is known before deployment | Active |
| D-40 | Publish to Pub/Sub through the **regional endpoint** | Topics that enforce in-region storage in transit refuse the global endpoint | Active |
| D-41 | **Write-once release contracts** (`ifGenerationMatch=0`); the same commit and image is a retry; the same commit with a new image becomes `<commit>-<digest8>` | Audit records point at contracts; a contract must never change | Active |
| D-42 | Component image tag `<release>-<context hash>`, never `latest`; an existing tag is reused | Works with immutable tags; idempotent re-releases | Active |
| D-43 | Tenant isolation on shared buckets by prefix (landing prefix, read prefixes, outputs prefix) | One bucket per purpose, many tenants | Active |
| D-44 | **CMEK on every BigQuery query** when the manifest names `security.bigquery_kms_key` (`bigquery_client` in components, `kms_key=` in the CLI) | An organization policy can refuse queries without a key; a CMEK dataset is not enough | Active |
| D-49 | TLS-inspecting proxies supported: `REQUESTS_CA_BUNDLE`, `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`, `UV_NATIVE_TLS`, `PIPEWRIGHT_BUILD_CA_BUNDLE` (a Docker build secret, never stored in a layer) | Real corporate networks | Active |
| D-52 | Images reach private environments by copying a release by digest into the environment's registry; the copy keeps the digest | The digest that was approved is the digest that runs | Active |
| D-55 | Contracts carry `metadata` (owner, objective, description, contacts, model) | Tools with no repository checkout need it | Active |
| D-65 | **Value-free errors** (column and rule, never the value) | Error text is the most common accidental data leak | Active (built on `phase-1`, 2026-10-24) |
| D-68 | **Drift opens a human decision; no automatic retraining.** Modes `off`, `propose` (the recommended minimum for production), `candidate` (never promotes) | Retraining on the same data does not improve a model; drift has causes needing opposite responses (data bug, seasonality, real change, harmless shift) | Active |
| D-69 | **Email notifies; an authenticated identity approves.** Approval is `pipewright retrain approve`; approvers are verified when they act | Anyone can forward an email; regulated approvals need a named, verified approver | Active |
| D-70 | **Decision events always go on the lifecycle events topic;** delivery to a team's own app is an opt-in, per-tenant filtered subscription (`retraining.decisions.publish`), checked by `doctor` | Audit needs every decision; topics stay platform-owned while teams choose delivery | Active |
| D-71 | **Cases re-open on time, on worsening and on escalation, within a policy cap:** snooze and decline expiry; immediate re-open when drift worsens (worst PSI up by `reopen_when_worse_by`, a new feature drifting, performance below its minimum); escalation when undecided; declines and snoozes need a reason from a short list plus free text | Time alone hides serious worsening and lets cases go stale | Active |
| D-72 | **Pipewright verifies the evaluation gates itself** from the version's evaluation record (`evaluation.json` next to the model artifacts) at `model approve` and `deploy-model`, against the configuration in force now; a missing champion comparison passes only when no earlier version exists | Teams write their own evaluation code; a rewritten step could drop a gate silently. Checking the record makes the freedom safe without constraining the code | Active (2026-10-09) |
| D-73 | **One spelling for configuration files: `.yaml`.** A `.yml` file where Pipewright looks (use case file, included config, per-environment overrides, environment files) is refused with a rename message | A `dev.yml` override that is silently ignored is hard to spot | Active (2026-10-09) |
| D-74 | **One description per key, next to the schema.** `schemas/reference.yaml` holds one line per dotted use case key; it is merged into the schemas the editor reads and into `pipewright explain`, and a test fails when a key has no description or a description has no key | The schemas say what is valid, not what it means; one source keeps hover text, `explain` and the docs from drifting apart | Active (2026-10-09) |
| D-75 | **Editor schemas are files in the MLOps repo**, `.pipewright-schemas/`, written by `pipewright schemas` and `new-usecase`, with a `yaml-language-server` line in each YAML file pointing at its schema by relative path | Works offline, in any editor that uses the YAML language server, and matches the installed Pipewright version; no hosted schema URL to keep in step | Active (2026-10-09) |
| D-76 | **`--from-bq` drafts; it does not decide.** From the table come the columns, types, required columns, the label's 0/1 values, a time split when there is a date column, and non-numeric columns excluded from features. Ranges, allowed values, uniqueness and personal data are left to the team, with a note on columns that look like identifiers | The schema cannot know what a column means; a draft that guessed would be trusted without review | Active (2026-10-09) |
| D-77 | **A Ray cluster per Ray step of a run, made by the launcher from the release.** The step names itself; the launcher checks the caller is the use case's runner, takes the size from the live contract (checked against the platform's limits at release) and the image from the release, and creates the cluster as the runner with the environment's key and network attachment. Requests travel like run requests (Pub/Sub inside a perimeter, with the runner's ID token; HTTP otherwise). Clusters carry run labels and a deadline; the tick deletes clusters of ended runs | Creating a cluster that runs as the runner needs permission to act as it, which the launcher has and tenants do not; sizing from the reviewed release means a request cannot ask for more | Active (2026-10-10) |
| D-78 | **Standalone Ray clusters for development, a cluster per run for pipelines.** Data scientists develop on standalone clusters of their own (console, notebook, Ray Client); every Pipewright run gets its own cluster (D-77). On Agent Platform there is no standing mode (the Google provider has no Ray settings on `google_vertex_ai_persistent_resource`). On private GKE the platform may give a tenant a standing KubeRay cluster (`ray.mode: standing`, section 8.15); a cluster per run stays the default | What is released, gated and audited runs on clusters Pipewright made for it, or on the tenant's cluster the platform keeps; development stays free | Active (2026-10-10; standing on private GKE 2026-10-17) |
| D-79 | **Pipewright consumes features; it never produces them.** Feature tables (entity columns, a timestamp, feature columns), their views and any Feature Store registration belong to the features' owners (D-17). A use case names features; the run's source becomes the entity table (entity IDs, the cut-off time per row, the label); ingest joins the two and everything after ingest is unchanged | One owner per feature; the data contract, validation, training, gates and monitoring keep working as they are | Active (2026-10-10) |
| D-80 | **Feature sources are pluggable; BigQuery feature tables first.** A source is a BigQuery table or view named in the use case. The point-in-time join is SQL Pipewright generates and runs as the runner with CMEK (D-44): for each distinct (entity, cut-off) the newest row dated at or before the cut-off, joined back on (entity, cut-off). A registered Feature Store feature group is a second kind of source (built, D-82) that resolves to the same table and columns; the SDK's Preview fetch is not used. Online serving, when built, uses Bigtable | Google is thinning the managed product (section 8.11); BigQuery is the stable part, so Pipewright depends only on it | Active (2026-10-10) |
| D-81 | **The platform decides where features may come from, in the manifest.** `tenants.<t>.features.datasets` lists a tenant's feature datasets; `features.required` makes `data.features_from` the only source of model features. Both are checked at `validate`, so a use case that breaks them never releases | Access is the platform's decision and reaches teams the same way as every other platform setting; a refusal at validate names what to change | Active (2026-10-10) |
| D-82 | **A feature group is resolved through the Feature Registry at each run, not pinned at release.** The registry is the owners' source of truth: when they move a group to a new table, runs follow it. The event records what each run actually read (group and table), and the tenant's datasets bound where a group may point | Registration is what makes a feature defined once; copying its resolution into each release would let use cases drift from it | Active (2026-10-10) |
| D-83 | **Newer manifest fields are ignored with a warning; policies carry `requires`.** Unknown keys are pruned using the schema itself, then the rest is validated in full; `requires` (a version specifier) makes an older version refuse the manifest when a key must not be skipped | App teams pin their CLI in CI; a platform adding a field must not break every team, and a policy must never be silently ignored. Built before the first release, because the oldest released version is the one that has to tolerate newer manifests | Active (2026-10-10) |
| D-84 | **Promotion keeps the code identical and resolves the configuration for the destination.** Images move by digest; templates are recompiled for the destination and must equal the source's with only the registry address changed; `ml_config` comes from the destination's resolution of the same commit. A model is promoted as its files, checksum-verified, because Model Registry copies only within a project | A byte-identical template cannot point at the destination's own registry, and configuration is legitimately per environment; claiming either is identical would be false. What must not change, the code and the model, is verified | Active (2026-10-11) |
| D-85 | **The closed retrain loop waits for real use.** Each half is built (retrain decisions in one environment; promotion between environments) and a person can carry a case across them with today's commands. The open choices (comparison policy, what counts as validated in test, health checks, ground truth per use case) are best settled by a team running real use cases | Building on guesses would fix policies before anyone has used the halves | Revised 2026-10-22 (user): brought into phase 1 alongside BigQuery ML; the open decisions in section 8.13 are settled first, as the start of the work (was: wait until teams use the halves, 2026-10-11) |
| D-86 | **The first release is 1.0.0 stable, no release candidate.** Nothing was ever published, so no one depends on a 0.x. From 1.0 a breaking change to the stable surface means 2.0: the use case and environment YAML (`pipewright/v1`), the manifest fields the CLI reads, release contract v3, CLI commands, flags and exit codes, lifecycle event types and fields, plugin API v1. Not covered: Python internals, undocumented runtime helpers, message wording, generated workflow contents. Removals are deprecated (a warning) for at least one minor release first. A throwaway pre-release tag rehearses the release workflow before `v1.0.0` | Fluxor's proof of concept with a very large customer gave positive feedback, which is the outside use a release candidate or a 0.9 would have waited for; the roadmap is additive (1.x); regulated buyers read 0.x as not production | Active (2026-10-11, replaces the 1.0.0rc1 plan); done at release time |
| D-87 | **No Iceberg (BigLake) tables.** Row-level deletes at scale are served by BigQuery tables and the feature team's feature tables, following the feature governance guidelines; Parquet outputs keep erasure by rewriting files with generation checks | The Feature Store path covers the need without a second table format | Active (2026-10-11) |
| D-88 | **Early users first, then a public repository.** 1.0.0 shipped on GitHub to early champions and customers first (packages as release files, images in GitHub Container Registry), with Fluxor engagements to help them adopt it; the repository then opens to everyone (October 2026), and 1.0.0 is on PyPI (`pipewright-mlops`) | Real use with support shapes the product before it is public; it is the ideal path for regulated buyers | Active (2026-10-11) |
| D-89 | **Kubernetes: `private-gke` (built 2026-10-17); on premises only on demand.** GKE keeps Google Cloud's data services and is private anyway, so it was the next platform. Plain Kubernetes on premises (no Google services) is not planned: it is built only when a large customer base asks, with one reference stack | No customer asks for on premises today; it would mean a second platform under Pipewright (section 8.14) | Active (2026-10-15) |
| D-104 | **Retraining for a production case runs in test.** Production detects, decides, approves and serves; it never trains | Separation of duties; production keeps only what was tested and approved | Active (2026-10-23, user) |
| D-105 | **Test reads production data through a read-only grant, never a copy.** The grant covers the specific production source tables or views, is approved by named approver groups (for example the data owner and data protection), is declared in the platform file and granted by Enterprise Terraform. Development stays on sample or synthetic data | One source of truth and no uncontrolled copies; access is reviewable and revocable | Active (2026-10-23, user) |
| D-106 | **Comparison policy: no worse than the model production serves,** within `evaluation.champion.min_improvement` (a negative value means "no worse than by this much"), on the latest labelled window; the configured gates still apply. The production model is found through production's `promoted_from`, so test holds identical files (EV-15) | Comparing with test's own champion would judge against the wrong model; reusing the existing key keeps 1.0 configuration valid | Active (2026-10-23, user) |
| D-107 | **Validated in test means a batch scoring run in test** on current data with the candidate, producing a monitoring record within thresholds and no errors; a canary as well only when the use case serves online | A real run on current data is the evidence that matters; a canary needs online traffic | Active (2026-10-23, user) |
| D-108 | **Health checks after deploy:** online, error rate, latency and prediction drift over a set window from Cloud Monitoring, with automatic rollback of a canary and a person deciding for a full rollout; batch, the first production scoring run's monitoring record within thresholds | Automatic rollback is safe for a canary's small share; a full rollout's rollback is a business decision | Active (2026-10-23, user) |
| D-109 | **Ground truth reuses `monitoring.performance.labels`** (table, key, label), with an optional label delay and a minimum labelled volume before performance can open a case | The block exists in 1.0; new keys are optional, so 1.0 use cases are unchanged | Active (2026-10-23, user) |
| D-110 | **A run's data is its ingest snapshot.** The per-run copy the ingest step writes (with `run_id`) is recorded as the run's snapshot, and a replay option reads exactly those rows; every feature query also records its BigQuery time-travel timestamp. The BigQuery ML pipeline writes the same snapshot table | Reproducible beyond time travel's seven days, built once for container and BigQuery ML use cases | Active (2026-10-23, user) |
| D-111 | **An optional label source with a maturity window** (table, key, event time, window in days, anchored on the newest label event): rows whose window has not closed are dropped, 1s and 0s alike, and the anchor is recorded with the run. Off unless declared | Labels that are still maturing are not negatives; anchoring on the newest event, not today, survives late-arriving data. Additive: 1.0 use cases do not change | Active (2026-10-23, user) |
| D-112 | **BigQuery ML is one declaration, `spec.bqml`.** A use case with a `bqml` section gets the training and scoring pipelines Pipewright provides; its own pipelines, components and framework are refused next to it. BigQuery ML is not an orchestrator: the pipelines run on the environment's orchestrator and their steps submit BigQuery jobs | Teams that work in SQL get the whole lifecycle without pipeline code; the platform's choice of orchestrator is unchanged | Active (2026-10-24, user) |
| D-113 | **A BigQuery ML model is registered only after gate 1,** with `ALTER MODEL ... SET OPTIONS (vertex_ai_model_id)`, never at `CREATE MODEL`; approval stays `pipewright model approve`. Each run trains a new BigQuery model (replacing one with the same name replaces its registry version); the version is labelled with its run and BigQuery model, which is how the record is found | One approval path for every engine; nothing that fails a gate reaches the registry | Active (2026-10-24, user; tested live) |
| D-114 | **Binary, multi-class and regression BigQuery ML models together** (with FR-52) | The metrics exist in the core | Active (2026-10-24, user) |
| D-115 | **BigQuery ML models are promoted by retraining** (`model.promotion: retrain`); promotion by files waits until exports are proven | The model stays in BigQuery; environments are separate perimeters | Active (2026-10-24, user) |
| D-116 | **Custom preparation: TRANSFORM or a pinned SQL file.** TRANSFORM is applied by `ML.PREDICT` and by a Model Registry endpoint; a serving path that skips an export's preprocessing model scores wrongly without an error, so Pipewright serves only through paths that run it, with a parity check before any endpoint deploy. A `prepare.sql` is pinned by the release (its SHA-256 in `ml_config`), written into the run's rows, run again when scoring, and recorded with the model; test rows never reach `CREATE MODEL`. Export limits are reported at validate for the configurations that need an export | Parity between evaluation, scoring and serving is a property of the path, not of the engine | Active (2026-10-24, user) |
| D-117 | **Missing values at serving:** a serving path applies the training means as BigQuery ML does (Model Registry deployment or the exported predictor, never a bare booster file), or the contract refuses NULLs | Same scores everywhere | Active (2026-10-24, user) |
| D-118 | **A byte limit on every BigQuery ML job** (`bqml.max_bytes_billed`, default 100 GiB, the platform's `policies.bqml` the ceiling and default) | Tree models bill far more bytes than linear ones | Active (2026-10-24, user) |
| D-119 | **Pipewright's metrics in SQL, its SQL in `.sql` templates.** Evaluation computes Pipewright's own metrics from `ML.PREDICT` on the test rows (defined as in the container path) rather than `ML.EVALUATE`'s set; PSI in SQL against stored baselines rather than `ML.VALIDATE_DATA_DRIFT` (another measure). The SQL a step runs is a template file; a team's SQL is a file in its use case | Same gates and thresholds for every engine; SQL that can be read and reviewed | Active (2026-10-24, user) |
| D-120 | **Each approver group is a GitHub environment.** An approver group (`ml-leads`, `model-risk`) maps to its own GitHub environment with its own required reviewers; after that environment approves, CI publishes the decision as that group (the platform grants the CI identity publish rights on the group's topic) | The group's quorum stays one approval per group, now given in GitHub | Active (2026-10-24, user) |
| D-121 | **With GitHub as the gate, its required reviewers are the control.** Pipewright's own approver list is not checked for decisions CI makes; GitHub's "prevent self-review" stands in for Pipewright's requester-is-not-approver rule. Pipewright records both identities: the CI identity that acted and the GitHub reviewer who approved, with the workflow run | One audit trail, without asking teams to keep two approver lists in step | Active (2026-10-24, user) |
| D-122 | **The CLI keeps working by hand.** `pipewright model approve`, `deploy-model`, `promote --approve` and `retrain` stay available for teams without GitHub and for emergencies; such decisions are recorded as approved outside CI, and Fleet View shows which path each decision took | No team is locked into one CI system | Active (2026-10-24, user) |
| D-123 | **CI polls, the platform never calls GitHub.** A scheduled workflow asks Pipewright what needs a decision (candidate versions, open retrain cases, releases ready to promote) and opens the gated jobs; a private launcher has no route to GitHub and needs none | Private perimeters stay closed to the outside | Active (2026-10-24, user) |
| D-124 | **A failed retrain keeps its approval.** The launcher's tick follows the case's training run and records the outcome on the case (registered, rejected by the gates, or failed with the failed step); transient failures (capacity, quota, preemption, unavailable) retry automatically at most twice, anything else waits for `pipewright retrain retry`, a new run with its own run key, recorded with the person's name | Asking approvers to decide again because a node pool was short of capacity wastes their attention and teaches them to approve blindly; a deterministic error retried automatically only fails again. Found in the 1.1.0 QA | Active (2026-10-25) |
| D-125 | **A use case may name its environments (`spec.run_in`).** Whole-repo commands skip it elsewhere with a note; naming it in another environment, or promoting it there, is refused. Left out, it runs everywhere | A use case that serves on KServe cannot resolve where there is no GKE, and one such use case stopped a whole-repo doctor or release in production. An allow-list in the use case keeps the intent next to the code and needs no platform change | Active (2026-10-25) |
| D-126 | **Alerts sync removes the policies of use cases that are gone.** Only on a whole-repo sync, and only for a use case neither in the repo nor released in the environment | Policies of deleted use cases kept emailing people. Several teams' repos may share an environment, so a use case another repo still releases keeps its policies | Active (2026-10-25) |

---

## 8. Designs in detail

### 8.1 Package layout (`src/pipewright/`)

| Package | Responsibility |
|---|---|
| `config` | Load and merge files, JSON Schema validation, `${...}` references, ML defaults and cross-checks, resolution into `ResolvedUseCase`; `environment_from_manifest` for services with no repository |
| `build` | Compile pipelines, patch `steps` into the compiled template, build the component image (injects `pipewright_runtime.py`) |
| `checks` | `doctor`, the `kfp` and Ray client pin checks |
| `ray_clusters` | Ray clusters for steps, on Agent Platform or KubeRay, per run or standing: the platform's settings and limits and a step's cluster (`settings`), the launcher's create, delete and cleanup (`service`) |
| `release` | Contract v3, pointers, template upload, verify gate, release and rollback, promotion between environments (`promote`) |
| `launcher` | Flask app, pure routing, live contract registry; `pull`, the same routes from Pub/Sub pull subscriptions (the launcher in a private cluster) |
| `adapters` | Orchestrator submit (`vertex`: Agent Platform Pipelines; `kfp`: Kubeflow Pipelines through the Kubernetes API), applying the manifest `security` block |
| `retraining` | Retrain decisions: the case state machine (pure), the decision pack summary, the case store, ID token checks, the launcher's handling, the `retrain` CLI |
| `tables`, `schedules`, `alerts` | Plans and syncs for BigQuery tables, Cloud Scheduler jobs or Kubernetes CronJobs (`kubernetes`, `fire`), log-based alert policies |
| `models`, `monitoring` | Registry list and approve, the gates recomputed from the evaluation record (`gates`), deploy to Agent Platform endpoints with canary or to KServe (`kserve`), `model predict`; `monitor status` |
| `kube`, `pipelines_ui` (modules) | A small Kubernetes client (no kubeconfig); `pipewright pipelines-ui` |
| `observability` | Lifecycle events (regional publish) |
| `data` | Erasure in BigQuery and Parquet |
| `synthetic` | Synthetic data (FR-49): the section's defaults, checks and the platform's policy (`config`), the built-in `synthetic` pipeline and its steps, and `runtime/` (gates, column rules, engines, evaluation, relational helpers), copied into the two synthetic images |
| `runs` | `pipewright run` (Pub/Sub or HTTP) |
| `scaffold`, `testing`, `gcp`, `cli`, `plugins` | New use case and CI workflows; `pipewright test`; REST session, GCS, BigQuery; argument wiring; plugin loading |

### 8.2 Configuration model

**Files:** `environments/<env>.yaml` points at the manifest. `usecases/<name>/usecase.yaml` holds identity, tenant,
pipelines, model and approvals, plus `include: [config/*.yaml]`. The `config/*.yaml` files hold one section each:
triggers, runtime, tables, data, training, evaluation, operations, and `synthetic` when a team switches it on.
Overrides go in
`usecases/<name>/environments/<env>.yaml`.

**Resolution** (`resolve_usecase`):

1. Assemble the document from its files.
2. Validate it against the schema.
3. Deep-merge the environment overrides: maps merge, lists and scalars replace.
4. Take resources from the tenant entry in the manifest.
5. Resolve pipelines, triggers, approvals and tables.
6. Fill `${...}` references.
7. Apply ML defaults and cross-checks, and the platform's policies (approvals, retraining).
8. Resolve the outputs store.

Every problem is collected and reported together.

**Keys that may vary per environment:** `parameters`, `steps`, `resources_per_step`, `triggers`,
`training.hyperparameters`, `training.tuning` (`enabled`, `max_trials`, `parallel_trials`), `model.serving`,
`approvals`, `retraining` (`mode`, `enabled`, `decisions`, `approvals`, `snooze`), `monitoring.alerts`,
`outputs.bigquery.retention_days`.

**`ml_config`** (what pipelines receive): `usecase`, `model_name`, `environment`, `description`, `objective`,
`owner`; `platform` (project, region, launcher URL, dataset, tenant, events topic, run requests topic, `kms_key`,
`bigquery_kms_key`); `outputs`; and the ML sections.

**YAML note:** YAML 1.1 reads unquoted `on`, `off`, `yes` and `no` as booleans. Pipewright names its keys to avoid
them (`retraining.when`, not `on`) and accepts `false` where `off` is meant (`retraining.mode`).

### 8.3 Release contract v3 and pointers

- **Contract:** `gs://<artifacts>/contracts/<usecase>/<release_id>.json`, write-once. It holds identity (release,
  use case, tenant, environment, blueprint, orchestrator, project, region), `component_image` by digest,
  `pipelines{name: template_uri by digest, parameters, static_parameters, experiment}`, `triggers`, and provenance
  (`source_commit`, `created_at`, `labels`, `metadata`).
- **Pointer:** `contracts/<usecase>/current.json`, moved with a generation precondition, keeping 20 entries of
  history.
- **Release ID:** the commit's first 12 characters. Uncommitted work gets `-dirty-<time>`. A changed image on the
  same commit gets `-<digest8>`.
- **Verify gate:** download each template and check the image, parameters and name before the pointer moves.

### 8.4 Launcher

| Route | What it does |
|---|---|
| `POST /pubsub` | GCS file events: routes by the longest matching landing prefix and suffix |
| `POST /run` | Schedules and API runs: `{usecase, schedule?, pipeline?, source_uri?, run_key?}` |
| `POST /pubsub/run` | The same body in a Pub/Sub push envelope; also carries retrain proposals (`kind: retrain.proposal`). Bad request: acknowledged and dropped. No live release: 503, retried then dead-lettered |
| `POST /retraining/propose` | A retrain proposal sent directly: `{usecase, run_id, observation, champion}` |
| `POST /retraining/decide` | A decision: pushed from an approver group's subscription (the message carries the person's ID token), or a direct call by the deciding identity for cases with no required groups |
| `POST /retraining/tick` | Snooze and decline expiry, escalation; with `{"digest": true}` the weekly digest; deletes Ray clusters of ended runs or past their deadline |
| `POST /ray/clusters` | `{action: create or delete, usecase, run_id, step}` from the use case's runner identity: a Ray step's cluster, sized from the live release; also `kind: ray.cluster` on `/pubsub/run`, with the runner's `id_token` |
| `GET /health` | Live use cases and their releases |

- **Idempotent run IDs:** a landing run ID hashes the use case, bucket, object and generation. Schedules use their
  name plus the minute, or a `run_key`. API runs use the source plus the minute, or a `run_key`.
- **Registry:** caches immutable contracts and refreshes pointers and the manifest every 60 seconds. A broken use
  case never stops the others; new tenants appear without a redeploy.
- **Submit:** a `PipelineJob` as the tenant runner, with the key, network and reserved ranges from the manifest.

### 8.5 Component runtime (`pipewright_runtime.py`, injected into every component image)

- `read_source`: CSV, Parquet or `bq://` sources, with the point-in-time feature join (section 8.11).
- `write_table`: store-aware, conforms rows to the schema, uses load jobs.
- `bigquery_client`: queries with the environment's key.
- `log`: labelled JSON lines.
- `emit_event`: a log line plus a regional Pub/Sub publish. It never raises.
- `ray_cluster`, `ray_job`: a Ray step's cluster for the run (asked of the launcher, deleted when the block ends) and a Ray job on it (needs `ray[default]` and `google-cloud-aiplatform[ray]` in the component image).

### 8.6 Events

- `run.submitted`, `run.duplicate`: launcher.
- `release.live`, `release.rolled_back`, `release.promoted`, `model.approved`, `model.deployed`, `data.erased`: CLI,
  each with `by`.
- `data.features.resolved`, `ray.cluster.used`: the component runtime; `ray.cluster.created`: the launcher.
- `synthetic.evaluated`, `synthetic.rejected`, `synthetic.generated`: the synthetic pipeline.
- `training.evaluated`, `model.registered`, `monitoring.recorded`, `monitoring.alert`: components.
- `retrain.proposed`, `retrain.approval`, `retrain.approved`, `retrain.declined`, `retrain.snoozed`,
  `retrain.reopened`, `retrain.escalated`, `retrain.digest`: launcher (data: `case_id`, `state`, `mode`, `summary`,
  `by`, `reason`).

**Shape:** `{event_type, time, usecase, environment, tenant, release, run_id, data}`. Pub/Sub attributes carry
`event_type`, `usecase`, `environment` and `tenant` for filtering; decision events add `case_id` and, when the use
case publishes to its feed, `decision_feed=true`.

### 8.7 Retrain decisions

**Why.** Drift says the inputs changed, not why, and the causes need opposite responses:

| Cause | Right response | What automatic retraining does |
|---|---|---|
| Upstream data bug (schema change, broken join, units, nulls rising) | Fix the data | Bakes the bug into the next model |
| Seasonal or temporary shift | Accept it or wait | Fits the model to a blip |
| Real, lasting change | Retrain on a chosen window, maybe with new features | Helps only if the window and features are right |
| Drift without harm | Note it; maybe adjust the threshold | Wastes compute, adds risk |
| Real degradation once outcomes arrive | Retrain, recalibrate or roll back after diagnosis | Might help, often for the wrong reason |

**Modes:** `off` (alerts only), `propose` (a case opens; nothing trains until approved), `candidate` (a candidate
trains at once and is never promoted without a person; needs `approvals.promote_model: manual`).

**Flow:**

```
inference run: drift or performance fired (retraining.when)
  -> proposal to the launcher (run requests topic, or POST /retraining/propose); the proposer is the contract's
     runner identity, never the message
  -> case opened: gs://<artifacts>/cases/<usecase>/<case_id>.json, current.json points at the open case
  -> retrain.proposed: emailed to decisions.notify, on the events topic, on the tenant's feed if publish is true
  -> approvers: pipewright retrain approve | decline | snooze, a message to their approver group's topic
  -> quorum (one approval per required group): the launcher submits training with the case ID as run key; the
     model still has to pass every gate and the champion comparison; promotion follows approvals.promote_model
  -> /retraining/tick: snoozes and declines expire (re-open), undecided cases escalate, weekly digest
```

**The decision pack** (summary under 1000 characters, no data values): drifted features and PSI, prediction PSI, the
worst PSI over recent runs, live performance and labelled rows, data checks from the latest batch, the champion's
version and age, what retraining would use, the approvers still needed.

**Approvers (FR-41, D-69):**

- The platform registers approver groups in the manifest (`approver_groups`): a Pub/Sub topic per group, whose
  publishers are its members, with a push subscription to the launcher's `/retraining/decide`. Publish access is the
  membership check, so any Google group, user or service account works with no directory lookup.
- The message carries the person's Google-signed ID token; the launcher verifies it and records the decision under
  that identity. The push itself must come from the launcher's own identity.
- One approval per required group; one identity counts once; the proposer cannot decide. Refusals are written to
  the case history.
- The platform can require groups per environment (`policies.retraining.required_approvers`), limit modes
  (`allowed_modes`) and cap snoozes (`max_snooze_days`).

**Case store:** the launcher is the only writer. Writes use generation preconditions with retries, so concurrent
launcher instances cannot overwrite each other. Duplicate proposals for the same run are recorded once. After an
approved retrain, new alerts open no case for `cooldown_days`.

**What the platform side provides:** the approver group topics and subscriptions, a scheduler job for
`/retraining/tick` (hourly) and the digest (weekly), launcher write access to `cases/` in the artifacts bucket, and
optionally a per-tenant decision feed subscription on the events topic filtered on `attributes.tenant` and
`attributes.decision_feed`.

**Residual risks:** a person's gcloud ID token has gcloud's audience, so a service that receives that token could
replay it within the hour; replaying still needs publish access to an approver topic, and approver topics should
keep messages for minutes only. Project-wide roles that include `pubsub.topics.publish` (Owner, Editor, Pub/Sub
roles on the project) make their holders members of every approver group; `doctor` names them.

### 8.8 Private environments

The core has no private-network code paths of its own; it applies what the manifest says:

- `security.kms_key` on pipeline jobs, models and endpoints; `security.bigquery_kms_key` on every query.
- `security.network` and `reserved_ip_ranges` on pipeline jobs (Private Services Access peering).
- `launcher.run_requests_topic`: `pipewright run` and retrain proposals publish there instead of calling the
  launcher, so the launcher can run with internal ingress only.
- Regional Pub/Sub publishing everywhere.

### 8.9 Ray clusters

A step that declares `steps.<step>.ray` gets a Ray on Agent Platform cluster for one run (D-77):

1. **Release:** the step's cluster is resolved and checked against the platform's limits (manifest `ray`, the
   tenant's `ray` over the environment's). `release` builds the use case's Ray image (`<name>-ray`, from
   `ray.Dockerfile`: the platform's Ray base image, the component code on `PYTHONPATH`, `components.ray_requirements`)
   and records `ray {image, ray_version, steps}` in the contract.
2. **Run:** `ray_cluster()` asks the launcher to create the cluster, waits for it to run, yields its name, and asks
   for its deletion when the block ends. Cluster IDs are deterministic (use case, run, step), so retries reuse one.
3. **Launcher:** checks the caller is the runner, builds the persistent resource from the contract (pools, the Ray
   image, the runner identity, CMEK from `security.kms_key`, the PSC interface from `ray.network_attachment`, labels
   with the run and a deadline of `max_minutes` plus 30 minutes) and emits `ray.cluster.created` with what it used.
4. **Cleanup:** the tick deletes Pipewright clusters whose run ended, whose deadline passed, or whose run ID no
   pipeline job has.

**Platform prerequisites:** the launcher's identity needs `aiplatform.persistentResources.create`, `delete`, `get` and `list` (a custom role: `roles/aiplatform.user` only gets and lists); each tenant runner needs, on itself, `roles/iam.serviceAccountUser` (to reach its own cluster's job API) and `roles/iam.serviceAccountOpenIdTokenCreator` (the step signs its own ID token through the IAM Credentials API: the metadata server's identity endpoint on Agent Platform workers refuses tokens on some workers). A Ray head needs at least 16 vCPUs (with 8, the cluster runs but its job API and Ray Client endpoint never answer). The open-source starter grants none of this (it stays unchanged); Enterprise Terraform grants all of it. Found live on 2026-10-10, with two more: a step with its own `cpu` or `memory` (or any `resources` block, even empty) runs where the metadata server issues no ID tokens, so Ray steps take no size and `steps` only writes a `resources` block for cpu, memory or a GPU; and a release reuses a contract only when its images and compiled templates are unchanged.

Without a `ray` block in the manifest, Ray is on with defaults (public endpoint, the runner identity, a prebuilt
image, at most 2 workers on n1-standard-4 or -8, 120 minutes). A private blueprint writes `ray.enabled: false`
unless its platform file turns Ray on. On Agent Platform `ray.mode` is `per_run`; on private GKE a tenant may be
`standing` (D-78, section 8.15).

### 8.10 Plugins

Entry point group `pipewright.commands`; each entry point is a callable `register(subparsers, common)` that adds
subcommands. Plugin API v1. A plugin that fails to load is skipped with a warning; `pipewright plugins` lists what
loaded.

### 8.11 Features from feature tables (built 2026-10-10)

**Where Google's products stand (researched 2026-10-10):**

- The Agent Platform Feature Store (formerly Vertex AI Feature Store) is now a metadata layer over BigQuery: a
  feature group registers a BigQuery table or view (entity columns, `feature_timestamp`), and BigQuery is the
  offline store. Registration is optional, and the training-data fetch (`fetch_historical_feature_values`) is
  still in Preview.
- On 2026-02-17 Google deprecated the legacy Feature Store and Optimized online serving. Both are removed on
  2027-02-17. Bigtable online serving remains, and supports CMEK.
- Google's migration guide for legacy users (updated 2026-10-07) goes to "a Bigtable feature store", BigQuery for
  training and Bigtable for serving, not to the newer Feature Store.
- BigQuery has its own point-in-time functions (`ML.FEATURES_AT_TIME`, `ML.ENTITY_FEATURES_AT_TIME`).
- Cataloguing moved to Knowledge Catalog (Dataplex, renamed 2026-04-10), which indexes BigQuery tables
  automatically.

So the stable part is the pattern (governed feature tables in BigQuery, read point in time, served from
Bigtable). Pipewright depends on that, and treats the managed product as an optional source.

**The use case** (`config/data.yaml`, D-79, D-80):

```yaml
data:
  contract: { ... }                       # unchanged: every column the model and the checks see
  features_from:
    entity: { columns: [customer_id], time_column: snapshot_date }   # in the run's source, the entity table
    sources:
      - table: bq://features.customer_profile          # a BigQuery feature table or view
        features: [tenure_months, monthly_charges]
        # entity_columns: [cust_id]                    # when the table names the entity columns differently
        # time_column: valid_from                      # default feature_timestamp
        # prefix: profile_                             # when two tables have a feature of the same name
```

**How a run reads it** (`pipewright_runtime.read_source`, so teams' own ingest code needs no change):

1. **The source is the entity table**, a `bq://` table or view: the entity columns, the cut-off column and the
   label. `validate` refuses landing-file triggers and `gs://` schedule or retrain sources, since a file cannot be
   one.
2. **One BigQuery query**, as the runner with the environment's key:
   - for each distinct (entity, cut-off), each feature table's newest row dated at or before the cut-off
     (`QUALIFY ROW_NUMBER()`);
   - joined back to every entity row on (entity, cut-off), so duplicates are kept and the result is deterministic.
   - A `date` cut-off is midnight UTC at the start of that day, so values dated that day are not used. A feature
     table should have one row per entity per timestamp; ties are broken arbitrarily.
3. **Scoring** uses the same query. A scoring table usually carries the cut-off column as the scoring date, which
   also makes scores reproducible. A table without it is cut off at the query time and gets the latest values;
   the contract must then mark the column `required: false`, since contract columns are required by default.
4. **Nothing after ingest changes.** The frame has the entity table's columns plus the features, exactly as a
   prepared table would.
5. **The `data.features.resolved` event** records the entity table, the cut-off rule, the query job and time
   (BigQuery time travel can reproduce the read for seven days), and per source the table, the features and how
   many rows found a value. Missing values are also logged as a warning.

**Checks:**

- `validate`: the block's shape, valid column names, every feature in the data contract, no feature in two sources
  without a prefix, no feature that is the label or an entity or cut-off column, a date or timestamp cut-off, one
  table entity column for each entity column.
- `doctor`: each feature table exists, has the entity, time and feature columns, a date or timestamp time column,
  and types the contract accepts.
- `read_source` refuses an entity table without the entity columns, or one that already has a feature's column.

**What the platform decides (manifest, D-81):**

- `tenants.<t>.features.datasets` lists the `project.dataset` feature datasets a tenant's use cases may read.
  `validate` refuses a feature table anywhere else, and names the datasets that are allowed.
- `features.required: true` means every model feature must come from `data.features_from`. `validate` refuses a
  use case without the block, and any model feature taken from the entity table.
- Both keys are optional and appear only when the platform sets them. Without them, a use case may read any table
  its runner can.
- Pipewright Enterprise grants access to the listed datasets (Terraform, nothing created) and checks each run's reads
  (EV-14).

**What stays with the features' owners:** computing features, freshness, erasure in feature tables (`pipewright data
erase` covers the use case's own tables), and Feature Store registration if they use it.

**Feature groups as a source** (D-82). A source may name a group registered in the Agent Platform Feature Store
instead of a table:

```yaml
    sources:
      - feature_group: customer_profile    # registered by the features' owners
        features: [tenure_months, monthly_charges]   # registered feature names
        # project: data-prod                # where the group is registered; default the environment's project
        # region: us-central1               # default the environment's region
```

- **Read at each run:** ingest reads the Feature Registry, which gives the group's BigQuery source, entity columns,
  time column (`feature_timestamp` by default) and each feature's column (`versionColumnName`). The same
  point-in-time query then runs, with each feature under its registered name. If owners point a group at a new
  table, runs follow it; nothing is copied into the release.
- **Same rules as tables:** the resolved table must be in one of the tenant's feature datasets (`read_source`
  refuses it otherwise). The event names the group next to the table it resolved to.
- **Checks:** `validate` checks that a source names a table or a group, not both, and that a group sets no
  `entity_columns` or `time_column`. `doctor` resolves each group through the registry, then checks its table like
  any other, including the tenant's datasets.
- **Permissions:** the runner reads the registry with `roles/aiplatform.user` (which it has). Reading only needs
  `roles/aiplatform.featurestoreResourceViewer`; owners register groups with `roles/aiplatform.featurestoreAdmin`.

**Later:**

- **Online feature lookup (FR-51)**, a toggle: Feature Store Bigtable online serving (which syncs BigQuery to
  Bigtable), with feature views derived from `data.features_from`. The serving handler reads by entity ID. Off unless
  the use case switches it on and the platform allows it, because Bigtable has a standing cost. Until then, online
  requests carry the full feature vector, and batch scoring reads features point in time like training.

**Example:** `examples/features/` (`new-usecase --from features`): the churn sample split into an entity table and
two feature tables, each with rows dated after the cut-off that must never be used. `data/load_tables.sh` loads
them.

**Tested live 2026-10-10** (a private environment with CMEK, VPC-SC and Ray over Private Service Connect):

- **The query itself:** the example's tables loaded into the tenant dataset, then `read_source` run against them as a
  step would. All 5,000 rows are identical to the churn sample the tables were made from, so no value dated after
  a cut-off was used, though every customer has some. Scoring as of the query time matches a pandas reference
  exactly (4,502 of 4,901 rows got the later value; the rest are dated after the query).
- **Training on Agent Platform Pipelines** (`churn-feats`, run `ra0b8c84dcdaf`): the feature join in `ingest-data`,
  then the unchanged pipeline. `churn-feats-classifier` v1 is DEFAULT, AUC 0.810, gates pass. The
  `data.features.resolved` event reached the tenant's `pipewright_events` view.
- **Training with a Ray step** (`churn-feats-ray`, run `rf8507504ebfd`): the same feature tables; the tuning trials
  ran on a per-run Ray cluster. `churn-feats-ray-classifier` v1 is DEFAULT, AUC 0.807, gates pass. Both
  `data.features.resolved` and `ray.cluster.created` were recorded for the run.
- **Batch scoring** (`churn-feats` inference, run `r5fcd02997c4b`): a scoring view dated today. 5,000 customers were
  scored with the features as of today. Drift monitoring flagged exactly the three features the later rows change
  (`tenure_months`, `total_charges`, `num_support_tickets`, maximum PSI 0.55) and raised an alert.
- **Found and fixed on the way:**
  - The example's `record-monitoring` step failed when no monitoring table was set; it now skips the write.
  - The features example now has drift monitoring.
  - A scoring table must carry the contract's required columns: contract columns are required by default, so the
    cut-off column normally doubles as the scoring date.
  - A run requested within 60 seconds of a release can still get the previous release (the launcher caches
    pointers, `POINTER_TTL`); this is by design.

### 8.12 Promotion between environments (FR-29, built 2026-10-11)

**The invariant:** what runs in production is what was tested. The code (the component image, and the Ray image
when there is one) is the same bytes, by digest. With `model.promotion: model` the model is the same files. With
`model.promotion: retrain`, production trains a new candidate that passes production's own gates and approvals. A
promotion is an artifact promotion with policy enforcement, not a registry copy.

**What is identical, and what is not:**

| Part | Across environments | How it is checked |
|---|---|---|
| Component and Ray images | Identical: copied by digest into the destination's registry (D-52) | The destination digest equals the source digest |
| Pipeline templates | Compiled for the destination from the source commit, with the same image digest at the destination's registry address | Must equal the source template with only the registry address rewritten. A template that differs (an environment override that changes the pipeline's shape, such as `training.tuning.parallel_trials`, or different `steps`) is refused, naming what differs, unless the promotion explicitly accepts it (`--accept-template-change`, recorded) |
| Configuration (`ml_config`, parameters) | Resolved for the destination from the source commit: project, datasets, keys, topics and the allowed per-environment overrides | Recorded in the destination contract; the destination's policies are applied to it |
| Model (`model.promotion: model`) | The same files, copied into the destination's pipeline root and registered as a new version in the destination's Model Registry (the registry's copy works only within a project) | Every file's checksum (CRC32C, independent of the encryption key) equals the source's; same serving container |

**The flow** (`pipewright promote --from test --to prod --usecase <name> [--model-version <v>]`):

1. **Allowed source:** the destination environment file names it (`promotion.from`). Any other source is refused.
2. **Promotable:** the source release is the source's live release, or in its pointer history. For model
   promotion, the model version was approved in the source (an approval or a default-version record) and trained
   by that release.
3. **Destination policies, before anything is copied:** the use case resolved for the destination (its manifest's
   policies, `requires`, `features.required`, the tenant's feature datasets, Ray limits); the destination's approval
   policy for promotion; the promoting identity.
4. **Copy and verify:** images by digest, templates (with the equality check), model files with checksums.
5. **Write once:** the destination contract `contracts/<usecase>/<source release id>.json`, with `promoted_from`.
6. **Model:** registered in the destination as a candidate; it becomes the default by the destination's approval
   policy (`promote_model: manual` means `pipewright model approve` there, by an authenticated approver). Model
   Registry makes a model's first version its default, so a first promotion under a manual policy needs
   `--approve` (the promoter's recorded approval); without it the promotion is refused before any copy.
7. **Move the pointer last,** with a precondition on its current generation.

**Failure and repetition:** a missing permission, a failed copy, a digest or checksum mismatch, or a refused policy
stops before step 5, so the live pointer is unchanged. Promotion is idempotent: the destination release ID is the
source release ID, the contract is write-once, and repeating a finished promotion is a no-op. A conflicting
request (the same release ID with different content) is refused.

**Audit:** the destination contract's `promoted_from` and a `release.promoted` event record the source environment,
project, release ID and contract SHA-256, the image and template digests on both sides, the model (source version,
file checksums, destination version), the promoting identity, the policy checks and the approvals. Pipewright
Enterprise's evidence pack checks it (EV-15). **Rollback** is the existing `pipewright rollback`: it moves the
pointer to an earlier contract and never rebuilds.

**Acceptance tests (from the user, 2026-10-11):**

1. Promoting a tested release does not rebuild the image or the pipeline templates.
2. Source and destination digests match after copying.
3. The model is copied unchanged when `model.promotion: model` is selected.
4. Retraining produces a new candidate, which needs the destination's validation and approval.
5. Production policies can reject an otherwise valid test release.
6. Missing permissions, failed copies, digest mismatches or refused policies leave the live pointer unchanged.
7. Repeated promotion requests are idempotent, or a conflicting request is refused safely.
8. The audit record identifies the source release, the artifact digests, the promoting identity, the policy
   decision and the approvals.
9. Rollback restores a previously approved production pointer without rebuilding artifacts.

**Tested live 2026-10-11** (two private environments, each its own project, CMEK and VPC-SC):

| Criterion | Result |
|---|---|
| 1, 2. No rebuild; digests match | The component image digest identical in both projects; each template compiled for the destination equal to the source's with only the registry address rewritten (zero other differences), its digest different for that reason only |
| 3. The model unchanged | The approved source version's 6 files copied and verified, registered as a new version in the destination |
| 4. Retrain is a new lifecycle | Unit-tested (`model.promotion: retrain` copies no model); the destination's gates and approvals apply |
| 5. Destination policies refuse | A use case asking for `promote_model: auto` in production was refused by production's policy before anything was copied |
| 6. Failures leave the pointer | A failed model check stopped the promotion before the contract and the pointer; a refused template likewise |
| 7. Idempotent | Repeating a finished promotion: "already live: nothing to do" |
| 8. Audit record | `promoted_from` names the source environment, project, release, contract SHA-256, digests on both sides, the model (source and destination versions, file checksums), the promoter, the time and every policy check; a `release.promoted` event |
| 9. Rollback without rebuild | Production rolled back from the second promoted release to the first; no image or template was built |

**Found and fixed in the live test:**

- **Tables:** the destination's tables are now synced as a release does.
- **Model files without a stored CRC32C:** files a training step writes through the Cloud Storage FUSE mount have none, so they are compared by the SHA-256 of both copies.
- **A model's first version:** Model Registry makes it the default. Under a manual policy the first promotion now needs `--approve`, recorded as a `model.approved` event. The first live promotion, made before the fix, became the default without an approval, and the evidence pack flagged it (EV-06).
- **Template refusal:** a production-only override (a bigger training machine) made the training template differ. It was refused, naming the difference, and then promoted with `--accept-template-change`, which is recorded.

**Not in FR-29 (later, open decisions; see section 8.13):** the cross-environment retrain loop (drift seen in production, the retrain
run in the training environment, then a promotion), and production data access for the training environment
(which environment trains on production data, whether production may train, whether dev stays on sample data).

### 8.13 The closed retrain loop (FR-48, planned)

Requested as a user story on 2026-10-11 (retraining, candidate against champion, test validation, controlled
promotion, deployment checks, rollback, production feedback, one audit record per loop). Kept on the roadmap
(D-85): teams use the two halves first.

**Where it stands against the story's acceptance criteria:**

| # | Criterion | Today |
|---|---|---|
| 1 | Retraining trigger | Met in one environment (schedules, landings, API, drift or performance cases, quorum, run keys). Gap: a production case cannot start training in test |
| 2 | Data validation | Partial: data contract, point-in-time features, personal data never a feature. Gaps: freshness and label-availability gates; no data snapshot |
| 3 | Reproducible training | Mostly met (contract pins commit, digests, resolved configuration). Gap: data snapshot |
| 4 | Candidate against champion | Met in one environment. Gap: in test the champion is test's own default, not what production serves |
| 5 | Validation in test | Partial: promotion needs a live test release and a test-approved model. Gap: no "ran in test" precondition (batch scoring or canary) |
| 6 | Controlled promotion | Met (gates recomputed, production policies first, approvals, EV-15) |
| 7 | Safe rejection | Met, tested live |
| 8 | Deployment verification | Partial: canary, promote, undeploy. Gap: automatic health checks after deploy |
| 9 | Rollback | Release rollback tested live. Gap: model rollback (default and traffic) as one tested procedure |
| 10 | Production feedback | Partial: drift in production; the decision pack shows labelled outcomes and label delay (R-19). Gap: an agreed ground-truth path per use case |
| 11 | Auditability | Partial: events, contracts, evidence packs, `promoted_from`. Gap: one record per loop; the case ID does not cross environments |
| 12 | Operational readiness | Partial: alerts, dead-letter topics, retries, `mode: off` and snooze. Gap: runbooks and a tested pause |
| 13 | Failure isolation | Met (pointer last, write-once, the champion untouched by a failed run) |

**Intended shape:** production never trains. It detects, decides, approves and serves; training happens in test
on production-eligible data. A case opened in production, once approved, starts a training run in test with the
case ID as run key. The candidate passes the gates and is compared with the version production serves: that
version was promoted from test, so test already holds identical files (EV-15) and finds it through production's
`promoted_from`. A rejected candidate closes the case without touching production. An approved candidate runs in
test, is promoted, approved in production, deployed by canary and checked, and the case closes with the final
production model. The case ID travels in events, model labels and `promoted_from`, so evidence packs and Fleet
View show one loop as one record.

**Today, by hand:** the whole path works with existing commands: production case, training in test on a chosen
window, rejection or approval, a test inference run, `pipewright promote`, production approval, canary, promote,
rollback. A person carries the case across.

**Decisions** (settled 2026-10-23; the loop is in phase 1, D-85; 7 and 8 are shared with the BigQuery ML work):

1. Where retraining for a production case runs: in test (D-104).
2. Production data access for test: a read-only grant approved by named groups, no copies; dev on sample or
   synthetic data (D-105).
3. Comparison policy: no worse than the production model within `evaluation.champion.min_improvement`, on the latest
   labelled window (D-106).
4. Validated in test: a batch scoring run on current data within thresholds; a canary too when served online (D-107).
5. Health checks after deploy: automatic rollback for a canary, a person for a full rollout (D-108).
6. Ground truth: `monitoring.performance.labels`, with an optional label delay and minimum labelled volume (D-109).
7. Data versioning: the ingest snapshot per run, a replay option, time-travel timestamps recorded (D-110). Raised by a
   user on 2026-10-21 (a lead scoring model with a label window).
8. Label maturity window: an optional label source with a window, anchored on the newest label event (D-111).
   Raised with item 7.

**Implementation (1.1.0, built on `phase-1` 2026-10-24):**

- `retraining.loop.enabled` (usually in the production environment file): an approved case is handed to the loop;
  production never submits training (D-104). Off, an approved case trains where it is, as in 1.0.
- `pipewright loop start | advance [--approve] [--steps N] | show | rollback --env <production> --case <id>` drives
  it from outside the perimeters (a private launcher reaches neither GitHub nor the other environment; D-123). Test
  is the production environment's `promotion.from`.
- start: training in test on the case's data, with the run key and `loop` set to the case ID, and
  `champion_version` set to the test version production's default was promoted from (its `source-version` label):
  the comparison is with what production serves (D-106).
- advance, one step when the current one is done (each step is safe to repeat): training ended with no registered
  version: rejected, production untouched; a registered version: candidate; a scoring run in test with
  `model_version` set to it (D-107); that run's monitoring row without an alert: validated. With `--approve` (the
  production approval, a CI gate under FR-56): `pipewright promote --model-version`, `model approve` in production,
  then a scoring run there; its monitoring row without an alert: closed (D-108, batch). Otherwise
  verification-failed, and `loop rollback` makes the version it replaced the default again.
- One loop at a time per use case. Every step is recorded on the production case (`case.loop`, with its history) and
  emitted as `loop.<state>`; the runs carry the case ID in their labels (`pipewright-loop`).
- Run requests may name `champion_version` and `model_version`; a pipeline gets them only if it declares the
  parameter (Pipewright's BigQuery ML pipelines and the 1.1 example do), and a request is refused otherwise.
- Enterprise (D-105): production's platform file grants the test runners read-only access to the tenant's data
  (`tenants.<t>.loop_readers`, with `approved_by`) and lets them in through its perimeter for BigQuery; test's
  (`tenants.<t>.loop_production_project`) lets its runner out to production for BigQuery.
- Phase 2 (built 2026-10-24): with the loop on, a scheduled or landing training run in production opens a case (or
  joins the open one) instead of training there; in `retraining.mode: candidate` the case goes to the loop at once,
  so a person decides only the production approval (the phase 3 shape). `pipewright-decisions` (FR-56) advances
  loops on its schedule; Fleet View shows each loop and its steps. The test runner reading a landed gs:// file in
  production needs a read grant the Enterprise blueprint does not give yet (it grants BigQuery only, D-105).
- Not yet: BigQuery ML use cases (promoted by retraining, D-115, which would train in production: they join with
  promotion by files); the online canary path with automatic rollback (D-108, unit-tested only: the test use cases
  are batch); a loop's features and label window come from test's configuration (only the entity rows are read
  from production).

**Delivery, when started:** phase 1 a manually started loop end to end with rejection and rollback; phase 2
scheduled retraining with run observability and concurrency control; phase 3 automated candidate gating with human
approval for production; phase 4 monitoring-driven retraining with deduplication and safeguards.

**Phase 6 design (user, 2026-11-06):** the four items not yet done, each behind a setting that is off by default, so
an upgrade changes nothing until a setting asks.

1. **BigQuery ML in the loop (D-115).** With `retraining.loop.enabled` on a BigQuery ML use case, the loop trains in
   test with BigQuery ML, compares with the version production serves, validates with a scoring run in test, and
   promotes by files (export, copy, import, parity); production never trains. Linear and logistic regression only:
   `validate` refuses the loop for other model types (their export scores differently), naming why.
2. **The online canary with automatic rollback (D-108).** `retraining.loop.canary` (`percent`, `soak_minutes`,
   `max_error_rate_increase`, `max_latency_increase`) for a model served on an Agent Platform endpoint: after the
   production approval the loop deploys the candidate as a canary, not a full rollout. After the soak window it
   checks the canary's error rate and latency (Cloud Monitoring) against the current version's, and sends the
   validation rows to the canary as a probe: their scores must match what test scored. Healthy: the canary takes all
   traffic and the case closes; otherwise the canary is undeployed and the previous version is the default again,
   automatically. No request or response is logged or stored: the probe compares scores in memory and records only
   counts and the largest difference. KServe stays refused (no canary there).
3. **The loop's data from production.** `retraining.loop.data: production` (default `test`, as before): the training
   run in test reads production's feature tables and label window (production's configuration of `data`), through
   the read-only `loop_readers` grant, instead of test's. The Enterprise blueprint grants the test runner read access
   to production's landing prefix for the tenant when `loop_readers` is set, so a file landed in production can
   train the loop.
4. **Safeguards for monitoring-driven retraining.** `retraining.loop.safeguards`, for loops started without a
   person (`retraining.mode: candidate`): a rate limit (`max_loops`, `per_days`: later alerts join the case as
   observations), a circuit breaker (`pause_after_failures`: after that many automatic loops in a row end rejected,
   failed, validation-failed, verification-failed or rolled back, automatic retraining pauses until a person resumes
   it, with an alert), drift on consecutive runs (`drift_runs`: a drift trigger counts only when that many scoring
   runs in a row alerted), and `pipewright loop pause | resume --env <production> --usecase <u> --reason <text>`
   (recorded with who and why). Performance triggers keep their minimum labelled volume
   (`monitoring.performance.labels`). With `safeguards: {}`, the conservative defaults: 1 loop per 7 days, pause
   after 2 failures, drift on 2 runs.

### 8.14 Running on Kubernetes: private GKE, and on premises on request (design note, 2026-10-15; `private-gke` built: section 8.15)

Two targets, very different in size:

- **`private-gke` (built 2026-10-17, section 8.15):** Kubernetes on Google Cloud. Compute moves into the cluster; Cloud
  Storage, BigQuery, Artifact Registry, IAM, KMS and VPC Service Controls stay. GKE is private anyway, so it serves
  the customers who want Kubernetes without leaving Google Cloud.
- **Plain Kubernetes on premises:** no Google services at all; every building block needs a replacement. Not
  planned: built only when a large customer base asks for it (D-89).

**What stays the same on both:** the use case YAML (`pipewright/v1`), teams' pipeline code (already Kubeflow
Pipelines v2), the release contract, gates and the champion comparison, retrain decisions and approvals,
promotion, the CLI and the evidence questions.

**Building blocks:**

| Today (Google Cloud) | `private-gke` | On premises |
|---|---|---|
| Agent Platform Pipelines | Kubeflow Pipelines in the cluster (`kfp` adapter) | Same |
| Cloud Run launcher | A Deployment in the cluster (same image) | Same |
| Cloud Scheduler | CronJobs | Same |
| Ray on Agent Platform | KubeRay, a RayCluster per run | Same |
| Model Registry and endpoints | Agent Platform, or KServe | MLflow registry (aliases as the champion), KServe |
| Cloud Storage | Stays | S3-compatible store (MinIO, Ceph) with object lock and conditional writes |
| Pub/Sub | Stays | Kafka or NATS; bucket notifications for landings |
| Artifact Registry | Stays | Harbor |
| BigQuery (tables, predictions, monitoring, point-in-time features) | Stays | A warehouse adapter (Trino or Postgres, Parquet on S3); the point-in-time join per SQL dialect. The hardest part |
| IAM, runner service accounts | Workload Identity per tenant namespace | Namespaces, RBAC, ServiceAccounts, store policies; OIDC (for example Keycloak) for people and approvers |
| Cloud KMS | Stays | Vault Transit or storage-level encryption |
| VPC Service Controls, organization policies | Stay, plus NetworkPolicies | NetworkPolicies, egress lock-down, Kyverno or Gatekeeper |
| Logging, Monitoring, email alerts | Stay | Prometheus and Alertmanager, Loki or ELK (OpenTelemetry) |
| Feature Store groups | Stay | Feature tables in the warehouse |

**In the code:** Google Cloud is called from about 35 modules and there is one orchestrator adapter (`vertex`).
`private-gke` adds the orchestrator, Ray and serving ports (`kfp`, KubeRay, KServe) and CronJob schedules;
on premises would add ports for the object store, warehouse, registry, event bus and identity, with the component
runtime (`read_source`, `write_table`) split the same way.

**Enterprise:** the blueprint becomes Terraform plus Helm (D-11); evidence controls read cluster sources
(Kubernetes audit logs, KubeRay, KServe); Fleet View gains a collector per platform; keyless CI reaches the cluster
through Workload Identity.

**Confirmed on `private-gke`:** Ray works under an enforced perimeter. KubeRay runs inside the cluster, so a step
reaches its Ray head directly instead of through Agent Platform's dashboard tunnel, which is what blocks Ray on
Agent Platform under an enforced perimeter (Enterprise R-23).

**If on premises is ever built:** one opinionated reference stack (for example MinIO, Kafka, Harbor, MLflow,
KServe, KubeRay, Trino or Postgres, Keycloak, Vault, Prometheus), shaped by the customer who asks, rather than
pluggable backends; Enterprise only, as `private-gke` is.

### 8.15 Private GKE as built (2026-10-17)

The design in section 8.14, built and used end to end in a private GKE environment under an enforced VPC Service
Controls perimeter. Everything is chosen by the manifest; a use case only says `model.serving.target: kserve` to
serve on the cluster.

| Part | How it works |
|---|---|
| Orchestrator `kfp` (`adapters/kfp.py`) | The launcher creates a run through the Kubernetes API's service proxy to the Kubeflow Pipelines v2beta1 API, from the same compiled template, parameters and pipeline root, as the tenant's Kubernetes service account (bound to its runner by Workload Identity). The deterministic job ID is the run's display name, so a duplicate delivery is refused; labels are compact JSON in the description (Kubeflow keeps 255 characters); tasks inside `dsl.OneOf` branches run uncached (Kubeflow cannot resolve a branch whose task was a cache hit) |
| Kubernetes client (`kube.py`) | The cluster's DNS endpoint (IAM authorized, reachable through the restricted Google APIs) or private endpoint and CA from the manifest, Google access tokens with the `userinfo.email` scope (without it GKE names a caller by a numeric ID and RBAC refuses it), retries; no kubeconfig. `PIPEWRIGHT_KUBERNETES_ENDPOINT` overrides the endpoint inside the cluster |
| Ray on KubeRay (`ray_clusters/`) | The same `ray_cluster()` and `ray_job()`: a RayCluster per run on the Ray node pool, sized from the machine names in `steps.<step>.ray`, created and deleted by the launcher, the job submitted inside the cluster; or the tenant's standing cluster (`ray.mode: standing`, kept by the platform) that steps submit to with their release's code as `working_dir`. The contract records the mode; a mode change makes a new release |
| Serving on KServe (`models/kserve.py`) | `deploy-model` creates or rolls over an InferenceService in the tenant's serving namespace from the registered version's container and Agent Platform environment, so requests and responses match Agent Platform endpoints; an internal load balancer when the manifest says `exposure: internal_lb`; no canary (refused); `model predict` for both targets |
| Schedules as CronJobs (`schedules/kubernetes.py`, `fire.py`) | With `schedules.backend: kubernetes`, `schedules sync` writes a CronJob per schedule; its pod (the launcher image, a numeric non-root user) publishes the run request as Cloud Scheduler would |
| The launcher in the cluster (`launcher/pull.py`) | Pub/Sub cannot push into a private cluster, so a pull worker reads the subscriptions Cloud Run would be pushed from and hands each message to the same route; a pulled decision is trusted as a push from the subscription. `--tick` and `--tick --digest` are the retrain CronJobs. `launcher.audiences` keeps ID tokens valid across a runtime switch |
| Read-only Kubeflow UI (`pipelines_ui.py`) | `pipewright pipelines-ui` serves the UI on localhost, forwarding each request through the DNS endpoint as the caller; the platform's role allows GET only, so changes are refused by Kubernetes |
| `doctor` | The CronJob path and the use case's KServe namespace and InferenceService |

**Found while testing, and fixed:** the `userinfo.email` scope; the 255-character description; OneOf branches on cache
hits; the Ray image's older numeric stack (pin the same versions in `ray-requirements.txt` as in the component image);
a named image user Kubernetes cannot verify as non-root (now numeric); release reuse that ignored a Ray mode change;
`alerts sync` repointing policies when another email channel had the same address (the channel Pipewright made now
wins). The platform side (the cluster, Kubeflow and KubeRay from mirrored images, KServe, finished pods deleted a
minute after they succeed so the logging agent keeps up) is the platform's, not this repository's.

### 8.16 A/B testing of models (FR-57, design note 2026-10-26; roadmap phase 10, user 2026-11-05)

Four ways of comparing models answer four different questions; Pipewright 1.1 has the first three, A/B testing is new:

| | Question | Mechanism | Today |
|---|---|---|---|
| Offline comparison | Better on held-out history? | Candidate and champion score the same test rows | Built: every training run (the champion comparison and margin) |
| Shadow | Sensible on live data? | The candidate scores live data, nothing acts on it | Batch only, by hand (`run --model-version`); no online shadow |
| Canary | Safe to switch? | A share of live requests goes to the candidate, watched, promoted or rolled back | Built on Agent Platform endpoints (`deploy-model --canary`, FR-15); not on KServe |
| A/B test | Better for the business? | Units randomly and stickily assigned to arms; a business outcome measured per arm; a person decides | Not built (this note) |

A canary splits per request (one customer can see both models) and checks safety; it is not an A/B test.

**Shape (additive, off by default).** A use case gains an optional `experiment` section:

```yaml
experiment:
  name: offer-v7-vs-v5                  # salts the assignment; a new name re-randomises
  arms: { control: champion, treatment: { model_version: 7 } }
  allocation: { treatment: 0.1 }        # the rest is control
  unit: customer_id                     # a data contract column; assignment is sticky per unit
  metric: { name: orders_30d, source: <outcomes table>, window_days: 30, direction: higher }
  guardrails: [{ name: basket_value, max_drop: 0.02 }]
  minimum: { units_per_arm: 2000, days: 21 }
```

- **Assignment** is deterministic: `hash(experiment.name, unit) < allocation` puts a unit in the treatment arm,
  with no lookup table, the same in batch and online, repeatable for an audit.
- **Batch use cases:** scoring scores each unit with its arm's model and writes `experiment` and `arm` beside the
  score (the scoring table gains two nullable columns, additive). Outcomes arrive through the label window
  mechanism (D-111) the retraining path already uses.
- **Online use cases:** the serving handler routes by the same hash (both versions deployed on the endpoint); this
  works on Agent Platform and KServe alike, unlike a per-request traffic split. Responses carry the arm.
- **Analysis:** a scheduled step computes the metric and guardrails per arm with confidence intervals. When the
  minimum sample and duration are reached, or a guardrail fails, it opens a decision case (the retrain decision
  machinery: named approvers, quorum, recorded reasons). Approve = the treatment version becomes the default; any
  other outcome ends the experiment and leaves the champion.
- **Events and lineage:** `experiment.started`, `experiment.guardrail_failed`, `experiment.decided`; the decided
  experiment (arms, sample, result, decision) is recorded in the winning version's lineage. Enterprise adds an
  evidence control and a Fleet View page.
- **Refusals:** an experiment needs a candidate that passed its gates; one experiment per use case at a time; not
  during a closed retrain loop on the same use case; forecasts and unsupervised models have no unit outcome and
  are refused.

**Delivery, when started:** phase 5a batch A/B (assignment in scoring, analysis, decision case), on `lapse-risk`
in the book's example; phase 5b online sticky routing and a KServe canary; phase 5c online shadow and stopping
rules (sequential testing). Open decisions: the default statistical test and interval method; whether a
guardrail failure stops the experiment automatically or only opens a case; per-segment analysis.

---

### 8.17 Immutable feature definitions and the complete ML release (FR-103, epic from the user, 2026-11-06; phase 8, released with phase 7)

**Objective (user):** an approved model always uses the same approved feature definitions, transformation logic,
schema and serving configuration that it was evaluated with. Today an approval pins the model version, its evaluation
record and the release contract (FR-93 to FR-97), but not the definitions of the features the model reads when it
scores: a change to a feature table's SQL, a feature view or a transformation can change an approved model's inputs
without a new evaluation or approval.

**The principle (user, verbatim): feature definition immutability is not the same as feature value immutability.**
- Changing the SQL calculation for `average_spend_90d` requires a new feature contract and approval.
- Updating the underlying transactions with new data can be allowed under the existing contract.
- Reproducing a historical prediction requires access to the original feature values or a reproducible historical
  source-data state.

This avoids accidentally freezing live business data while still protecting model governance.

**Business outcome (user):** every model release is a reproducible, traceable artefact holding the model and its
version, the feature contract and its version, the hashes of the transformation SQL and code, the feature schema and
types, source data references and execution metadata, the materialisation reference, the evaluation evidence, and the
approval and deployment metadata.

**What Pipewright already has (so the epic adds, not rebuilds):**

| Epic need | In Pipewright today |
|---|---|
| Model artefact, version, evaluation evidence, approval | Model Registry version, evaluation record, `model.approved` with digests of the evaluation, files, serving image, BigQuery ML model and rules (1.4.0) |
| Release manifest, unique ID, controlled and lockable | The release contract (commit, images and templates by digest, `ml_config` with its SQL digests), locked and fingerprinted on `release.live` (1.4.0) |
| Training sources and rows | `lineage.json` per run (FR-55): entity table, feature tables or groups and their read time, label table and window, the exact rows (the run's snapshot or BigQuery ML rows) |
| Reproducing training | FR-85: rows sorted, a data fingerprint, a replay matches exactly |
| Data contract | `config/data.yaml`: label, split, unique key, the features a use case reads |
| Feature registration | `pipewright features register` (feature groups over the data team's feature tables, FR-75 CLI) and the recommended pattern (FR-99, docs/FEATURES.md) |
| Drift of values | monitoring (feature PSI), retrain cases, the closed loop |

**What is missing (the epic's real scope):**
1. **A feature contract** per feature table or group: each feature's name, type, business definition, source tables,
   the transformation SQL it comes from, a semantic version, and breaking versus non-breaking changes (user story 1).
2. **Definition fingerprints:** the SHA-256 of each feature's SQL and transformation code and of the output schema,
   recorded at training (in `lineage.json`) and on the approval (user stories 2, 6, 8).
3. **The serving side:** a scoring run, an online feature view and an endpoint read features as they are defined
   *now*. Deploy, promote and every scoring run compare the live definition (the table's or view's defining SQL or
   DDL, its schema, a feature view's source and sync settings) with the approved fingerprints, under
   `policies.integrity` (user stories 9 and 10); FR-101's watch and alert cover feature definitions too.
4. **Materialisations for reproducibility, without freezing live data:** training already snapshots its rows; a
   scoring run records what it read (table, snapshot time or BigQuery time travel, row count, fingerprint) so a
   historical prediction can be reproduced within the retention period; approved materialisations cannot be
   overwritten by a normal run (user story 3, scoped by the principle above).
5. **Feature quality before training** (null rates, ranges, freshness, distributions against a baseline, thresholds
   that warn or fail by policy): partly in data validation today; to review against user story 5.
6. **Rollback restores the model and its feature-serving configuration together** (user story 10).

**Target architecture (user):** Git, feature contract, CI validation, BigQuery feature materialisation, Vertex AI
Pipelines, Vertex ML Metadata, Model Registry, approval, deployment workflow, production. In Pipewright the CI is the
generated workflows, the deployment workflow is `deploy-model` and the launcher (no Cloud Workflows needed), and the
release contract plus `lineage.json` and the event log carry what Vertex ML Metadata would; writing the same facts to
Vertex ML Metadata as well is optional, to decide in the design.

**Out of scope (user):** rebuilding the platform, replacing Vertex AI Pipelines, replacing the approval process,
redesigning business feature calculations.

**Stories (user's epic, priorities as given):** 1 version-controlled feature contracts (P0); 2 SQL and transformation
fingerprints (P0); 3 immutable feature materialisation in BigQuery (P0, scoped by the principle); 4 contract validation
as a mandatory pipeline step that blocks training (P0); 5 feature data quality and distribution checks (P1); 6 feature
lineage on training and evaluation (P0); 7 the release manifest includes the feature contract (P0); 8 approval covers
the complete release, and a changed definition needs a new approval (P0); 9 deployment validates the feature contract
against the serving configuration (P0); 10 rollback and production drift detection for feature definitions (P1).
Definition of done and success criteria as in the user's epic: every approved model has a feature contract; every
contract has traceable SQL and schema fingerprints; every model traces to its exact materialisation; a changed approved
definition cannot silently affect production inference; deployment validates the complete release; an approved
release can be restored without rebuilding its feature definitions.

**Rules for the design:** additive and off by default (stable upgrades); a use case without a feature contract works
as today; messages name features, tables, digests and versions, never a value; the user reviews the design before it
is built.

**Design (2026-11-07; the user: "Design is OK to build", toggled; from the live spike in
docs/evidence/fr103-feature-definitions).** What the spike decided:
- A view and a materialized view carry their SQL (`tables.get`, free). A table built by a query (CTAS, a pipeline,
  dbt) carries only its schema, and a rebuild with changed SQL looks exactly like a rebuild with new data. The job
  that wrote it (`INFORMATION_SCHEMA.JOBS`, 180 days) or a scheduled query's transfer config has the SQL.
- etag, lastModifiedTime and the DDL move on labels and descriptions; Feature Store etags move on every read. The
  fingerprint is the SQL with whitespace normalised plus the schema as names, types and modes, never descriptions.
- `FOR SYSTEM_TIME AS OF` on a view silently returns today's values, a view cannot be snapshotted, and a re-created
  view cannot be read from before. A query over a view lists its base tables (`referencedTables`).
- A Feature Store sync of a changed definition reports success but keeps serving the old value unless the row's
  `feature_timestamp` is newer; entities that left the definition keep serving; a breaking change fails the sync
  and serving goes on with the last good values.

The pieces, each additive:
1. **The feature contract** (stories 1, 2, 7): a new use case section `feature_contract` (by convention
   `config/feature_contract.yaml`), one entry per `data.features_from` source (a table or a feature group): its
   BigQuery object, `kind` (view, materialized_view, table, scheduled_query), `sql_sha256` and `schema_sha256`, the
   schema (names, types, modes), each feature's type and description, and the contract's semantic `version`. For a
   table built by a query the SQL is declared (`sql_file`), or `scheduled_query` names its transfer config.
   `pipewright features contract --env <env> --usecase <uc>` writes or updates it from the live objects (a feature
   group is followed to its BigQuery source) and sets the version: a removed or retyped column is a major change,
   changed SQL or an added column minor, descriptions a patch; `--check` compares the contract with the live objects
   (exit 0 same, 1 different, 2 cannot check) for CI. The team commits the file: it is version-controlled with the
   use case, and as a use case section it is in the release contract's `ml_config` (the release manifest) by
   construction. Without the section nothing changes.
2. **The check** (stories 4, 8, 9, 10): `policies.integrity.features.check: on` (default `off`). Then a definition
   that differs is an integrity problem judged by `policies.integrity.mode` (refuse, or warn with `until`), like a
   changed model file: one switch, the same override and the same alerts.
   - Training: the launcher compares the released contract with the live objects before it submits a training run
     (a run the CLI requests goes through the launcher too), and `run.submitted` records the contract version and
     digest (story 6, with lineage.json's sources).
   - Approval records `features`: the contract version and digest and each source's fingerprints as checked live;
     a live definition that differs from the contract the version was trained with refuses the approval (a changed
     definition needs a new contract version, a release and a retrain).
   - `integrity.verify` compares the approval's `features` with the live objects, so deploy-model (endpoint and
     KServe), promote, every scoring run (`policies.integrity.inference`), the watch, `pipewright integrity check`,
     loop rollback and `lineage` all see it; `pipewright rollback` checks the old release's contract.
3. **Built tables** (`policies.integrity.features.built_tables: true`, default false): for a table built by a query,
   the normalised SQL of the newest job that wrote it must match the declared `sql_file`; a scheduled query's
   config must match. Off: such a table is checked by schema only, and `features contract` says so.
4. **Feature Store** (with the watch on): every feature view serving a contract's groups; its newest sync failed
   (serving the last good values) is reported, and the docs say how to resync after a definition change (newer
   `feature_timestamp`, since a sync with the same timestamps changes nothing served).
5. **What scoring read** (story 3, by the principle that values are not frozen): `record_reads: true` records in
   `run.submitted` each source's base tables (a free dry run's `referencedTables`), their last-modified time and row
   count, and the read time, so time travel can rebuild the inputs within the dataset's window (2 to 7 days);
   `fingerprint_rows: true` adds a row fingerprint per source (one full scan each); `snapshot_days: N` takes an
   immutable snapshot of each base table into the use case's dataset, expiring after N days.

| Toggle | Default | Governance | Security | Cost (list prices, europe-west2) |
|---|---|---|---|---|
| the contract (a use case section) | absent | definitions versioned in Git, in the release contract | none new | $0 |
| `features.check` | off | a changed definition cannot reach training, approval, deploy or scoring unnoticed | reads object metadata (`bigquery.tables.get`, already held by readers) | $0: `tables.get`, dry runs and transfer configs are free |
| `features.built_tables` | false | tables built by a query are covered too | needs `bigquery.jobs.listAll` on the project (sees every job's SQL): Terraform grants it to the launcher only when on | 20 MiB minimum a check, about $0.00015; hourly for 100 sources about $11 a month |
| Feature Store sync health | with the watch | stale serving is reported | `aiplatform.featureViews.get` (viewer) | $0 |
| `features.record_reads` | false | which tables and when, for every scoring run | none new | $0 |
| `features.fingerprint_rows` | false | proves the rows read | none new | one scan per source per run: about $0.0076 per GiB (10 MiB minimum) |
| `features.snapshot_days` | 0 | rows kept beyond time travel | the launcher may create snapshots in the use case's dataset (Terraform grants it only when on) | storage of rows that later change, $0.023 per GiB-month logical |

Out of scope, as the epic says: Pipewright never rewrites a team's feature SQL or redefines a feature, and does not
copy definitions to Vertex ML Metadata (the release contract and the event log carry them).

### 8.18 Serving what was approved, and knowing when it changes (FR-101 and FR-102, phase 7; design for review, 2026-11-06)

**Starting point: the spike (FR-101), run live and torn down the same day.** The user chose to design by its findings
(2026-11-06). Where the bytes come from decides where the risk is:

| Path | Where it reads the model files | A file swapped after approval |
|---|---|---|
| Agent Platform endpoint | Agent Platform's own copy, taken when the version is registered (`gs://caip-tenant-.../artifacts`) | Not served (live test: running replica, scale-out, redeploy) |
| KServe on GKE | the live model folder, at every pod start (`AIP_STORAGE_URI` = `artifactUri`) | Served by the next pod that starts (live test) |
| Batch scoring (`champion.py` `load_model`) | the live model folder, on every scoring run | Used by the next run (from the code) |

So the design closes the two live paths first (by reading the approved bytes, which a swap cannot change), then fixes
what the checks compare for Agent Platform, then adds the watch and the alert. A generation in Cloud Storage never
changes its bytes, so "load generation G and check its SHA-256" is safe even with the lock off and on a versioned
bucket where a new version can be put over a locked file.

**Order of work (each item additive; item 0, the cached approval lookup, comes first; toggles, defaults and costs are in the table below):**

1. **Batch scoring reads the approved bytes (P0).** The launcher's scoring check already reads the approval
   (`inference_check`); with `policies.integrity.inference: warn | refuse` it now also runs the full `integrity.verify`
   (files, image, BigQuery ML model, contract) and passes the approved files to the run as a new optional pipeline
   parameter `approved_files` (file, generation, SHA-256 per file; names and digests only). `load_model` in the
   reference example, when given it, downloads each file by its generation, checks the SHA-256 and fails the run if
   one differs or the generation is gone ("model.pt: generation 179...752 is not the approved 179...472"). Without the
   parameter it behaves as today. `pipewright run --pipeline inference` passes the same parameter, so a manual run
   is pinned like a scheduled one. A team's own scoring component reads it the same way (documented, with the
   10-line helper in `pipewright_runtime`: `approved_download(files, dest)`).
2. **KServe serves the approved bytes and checks them at every pod start (P0).** At `deploy-model`, after
   `integrity.verify` passes, Pipewright copies the approved generations into a folder named by the approval's digest,
   `gs://<artifacts>/serving/<usecase>/v<N>-<digest16>/`, and points `AIP_STORAGE_URI` there instead of the live
   folder. Each predictor pod gets an init container that reads that folder, checks every file's SHA-256 against the
   approval (passed in as an environment variable) and exits non-zero on any difference, so a changed copy cannot
   start: the old pods keep serving, the new pod stays not ready, and a `model.integrity` event plus the alert say
   why. Fail closed at pod start is the point: the spike showed the risk is exactly the pod that starts later. The
   init container is the launcher image already mirrored into the environment (it has Python and the Storage client),
   so a private GKE cluster needs no new image. The copy gets the retention lock when `lock.days` is set. Build-time
   check first (an hour, live): whether the prebuilt serving image accepts a local `AIP_STORAGE_URI`; if it does, the
   init container downloads the approved generations into a shared `emptyDir` and no copy is needed at all.
3. **Agent Platform: the check compares what actually serves (P0).** The register step records the model folder's
   files (generation and SHA-256, `integrity.model_files`) at the moment it calls `Model.upload`, in the
   `model.registered` event and in `lineage.json`. `model approve` compares the folder with that record and refuses
   when the files changed between registration and approval ("what Agent Platform serves is the copy taken at
   registration; the folder has changed since"); the approval then records the registered files. For an Agent
   Platform deploy, the bucket check stays (promote and the other paths copy from the bucket), and the message says
   which copy differs. What is left is the short window inside the training run before registration.
4. **The watch (P1).** On the launcher's scheduler tick (`policies.integrity.watch: off | on`, off by default,
   `every: 1h` by default), for each use case: the default version and each version serving (endpoint deployed models
   and traffic split, KServe `InferenceService` annotations), checked against the newest approval with
   `integrity.verify`. A `model.integrity` event only when the result changes (`changed`, `missing`, `restored`,
   `unapproved_serving`), naming records, files, digests, endpoints and versions, never a value. KServe and scoring
   first: on those paths a changed file is `severity: incident`; on Agent Platform it is `severity: drift` (the
   endpoint is not affected until the next promote or a GKE deploy). The endpoint comparison catches a deploy or a
   traffic change made in the console. Reads metadata only, as the launcher's identity.
5. **The alert and the heartbeat (P1).** `alerts sync` adds log-based metrics and Cloud Monitoring policies on
   `model.integrity` (changed, missing, unapproved serving) and `run.integrity`, to the use case's notification
   channels, plus an absence policy: no `model.integrity.check` heartbeat for two intervals means the watch stopped,
   so a broken watch does not look like "unchanged".
6. **A CI gate with an exit code (P1).** `pipewright integrity check --env <env> [--usecase <uc>] [--json]`: the same
   checks as the watch, exit 0 when every approval is unchanged, 1 when any changed or is missing, 2 when it could
   not check. For a CI step before deploy or a scheduled job where the launcher is not used.
7. **Every way back is checked like the way forward (P1).** `loop rollback` runs `integrity.verify` on the version it
   restores; `pipewright rollback` also checks that the old release's contract is still the fingerprinted one. Same
   `enforce` and messages as deploy and promote.
8. **The lock lasts while the version is in use (P2).** Settled with the user 2026-11-06 after a live permission
   spike: lock on activation, plus a daily renewal by a dedicated renewer identity (not the launcher), with
   cross-identity detection and three health alerts. See "Lock renewal, settled" below.
9. **The nudge and the response plan (P2).** `doctor` warns when a production environment has `lock.days: 0`,
   `mode: warn`, `inference: off` or `watch: off`. docs/INTEGRITY.md gains a response plan: who is paged (the use
   case's owners from `approvals`, the platform team), what refuses and what warns, the restore command
   (`pipewright model restore-files <uc> --version N`, new: writes the approved generations back as the live
   version, recorded as `model.restored` with a name), and the time-boxed override from FR-102.

**FR-102 in the same phase:** (1) the starter and the Enterprise blueprint route Admin Activity entries for Agent
Platform and the artifacts bucket to a log bucket with locked retention (`observability.audit_retention_days`, off by
default; the cross-check reads it when set, `doctor` says when it is missing); (2) Terraform turns on DATA_WRITE audit
logs for the artifacts bucket (optional in the starter, on in the blueprint), so `lineage` and the alert can name who
wrote a file; (3) `mode: warn` takes `until:` and `reason:`, is recorded as `policy.override` with who, why and until,
alerts while it lasts and goes back to refuse on its own after the date; (4) `doctor` lists identities other than the
runners that can write or delete in a production artifacts bucket.

**Enterprise:** Fleet View already re-checks every approval; it gains the KServe copy and the registered-files record
(item 3), shows the watch's first-seen and restored times, and EV-06 records them. The alert in item 5 is provisioned
by the blueprint for Enterprise environments.

**Governance, security and cost, item by item (user, 2026-11-06: "people taking decisions should know cost
implications too"; the user accepted the recommendations below and asked for toggles and facts first).**
Prices are Google's list prices from the Cloud Billing Catalog API, USD, europe-west2, read 2026-11-06, before free
tiers unless stated: BigQuery analysis $7.8125 per TiB (first 1 TiB a month free per billing account; at least 10 MiB
billed per table a query references); Cloud Storage Standard London $0.023 per GiB-month, regional Class A $0.000005 and
Class B $0.0000004 per operation; Cloud Run in London is tier 2, request-based $0.0000336 per vCPU-second and
$0.0000035 per GiB-second; Cloud Logging ingestion $0.50 per GiB after 50 GiB per project a month, retention beyond 30
days $0.01 per GiB-month; Cloud Monitoring metric volume $0.258 per MiB after 150 MiB, time series billed $0.0000005
each after 1 million; Cloud Scheduler $0.10 per job a month after 3 free. The catalog listed no per-condition charge
for alerting policies on that date. Same-region reads from Cloud Storage by GKE or a pipeline are free of network
charges (Google's network pricing rule). Facts from the code: the launcher already has an hourly Cloud Scheduler tick
(`17 * * * *`, retraining), runs at 1 vCPU and 1 GiB with no minimum instances, and holds `aiplatform.user`,
`storage.objectViewer` on the artifacts bucket and, with `inference` on, `bigquery.jobUser`; the events table is
partitioned by day on `publish_time`; neither the starter nor the blueprint deletes old object versions by a lifecycle
rule.

**The one finding that decides the cost: the approval lookup.** `integrity.approval_events` runs one BigQuery query per
version with no date filter, so every check scans the whole event log, and the log only grows. A watch built on it
costs (assumptions: 30 events a use case a day at 1.5 KB each, a 12-month-old log, 1.5 versions watched per use case;
replaced by a measurement in the build):

| Fleet | Every 15 min | Every hour | Every 6 hours | Daily |
|---|---|---|---|---|
| 5 use cases, today's lookup | $14.56 / month | $3.64 | $0.61 | $0.15 |
| 25 use cases, today's lookup | $321 | $80 | $13 | $3.35 |
| 100 use cases, today's lookup | $5,015 | $1,254 | $209 | $52 |
| 5 use cases, cached lookup | $0.79 | $0.20 | $0.03 | $0.01 |
| 25 use cases, cached lookup | $2.23 | $0.56 | $0.09 | $0.02 |
| 100 use cases, cached lookup | $7.65 | $1.91 | $0.32 | $0.08 |

Cached lookup: one query per tick that reads only the partitions since the last tick (approvals never change once
written), kept in a small state file the launcher already can write beside its run markers. Its cost is almost all
Cloud Run time. So **the lookup is a P0 fix for the whole phase** (it also serves 1.4.0's deploy and scoring checks:
100 use cases scoring daily with `inference` on already scan about 4.4 TiB a month, about $34, after a year), and with
it the interval is a question of how soon people hear, not of money.

| Item | Toggle (default) | Governance | Security | Cost (list prices above) |
|---|---|---|---|---|
| 0 Cached approval lookup | none (internal) | same answers, faster | no new access | turns the table above from the first three rows to the last three |
| 1 Scoring loads approved bytes | `policies.integrity.pin: on` when `mode: refuse` (the 1.4.0 default), `off` to opt out | prevents a swapped file being scored; the run fails with the file and generation | no new access: reading a generation needs the same `storage.objects.get` | same downloads as today, one cached lookup per run: about $0 |
| 2 KServe serves approved bytes, checked at pod start | the same `pin` key | prevents; a changed file stops the new pod, old pods keep serving (SRE: capacity can fall during a scale-out until restored, by design) | `emptyDir` route: nothing new to protect; copy route: a second folder, writable only by the deployer | `emptyDir`: $0 storage; copy: $0.023 per GiB-month per version kept (0.5 GiB model, 12 versions: $0.14 a month; 20 GiB: $5.52); the check adds seconds of hashing at pod start, and on Autopilot an init container is billed only above the predictor's own requests |
| 3 Register records the files Agent Platform copies | none (a record) | the approval is provably what serves | none | hashing the model files once at registration: Class B reads (fractions of a cent) and seconds of the training step |
| 4 The watch | `policies.integrity.watch: off`; when on, `every: 1h` on the existing tick (no new Scheduler job) | detects and tells; prevention is items 1 and 2; worst-case notice = the interval | reads only, as the launcher; Terraform adds `bigquery.jobUser` when on | cached: $0.01 to $7.65 a month by fleet and interval (table) |
| 5 Alerts and heartbeat | created by `alerts sync` when the watch is on | a broken watch is noticed (absence alert) | none | 3 policies and 3 log-based metrics per environment: within the free metric volume and time series; $0 at the prices read |
| 6 `pipewright integrity check` | on demand | an exit code for CI | the caller's own read access | one cached lookup and a listing per version: under a cent a run |
| 7 Rollbacks checked like deploys | follows `mode` | closes the way back | none | one check per rollback: $0 |
| 8 Lock renewal while in use | `lock.renew: on` when `lock.days > 0` (retention buckets only; hold-based buckets need none) | the preventive control lasts as long as the model is needed, plus `lock.days` | a dedicated renewer identity with `get` + `setRetention` + `update` on model folders (the spike showed `update` is required, and that it also allows releasing holds and changing metadata); the launcher keeps read-only access; detection by a cross-identity check and audit logs (see below) | Class A per file per renewal (100 versions x 5 files daily: $0.08 a month) plus retired models kept up to `lock.days` longer (5 GiB for 30 days: $0.12) |
| 9 `doctor` nudge, restore command, response plan | always | people know when protection is off | none | $0 |
| FR-102 (1) Audit retention | `observability.audit_retention_days: 0` (off) | the cross-check works for models older than 400 days | a locked log bucket cannot be emptied early, by anyone | Admin Activity for Agent Platform and one bucket is megabytes a month: ingestion within 50 GiB free, retention $0.01 per GiB-month (cents a year); a locked bucket's retention cannot be shortened, so it is a commitment |
| FR-102 (2) DATA_WRITE audit logs (switched on per project; a Cloud Logging exclusion keeps only the artifacts bucket's model folders, and excluded entries are not charged) | starter `off`, blueprint `on`; always on when lock renewal is on | names who wrote a file | entries hold object names and the writer's identity (an email is personal data: retention and access to follow the organisation's policy); no data values | scales with objects written (assumption: 300 per run, 1.5 KB each: 3,000 runs a month = 1.3 GiB, inside the 50 GiB free unless GKE or other logs use it up, then $0.50 per GiB); DATA_READ is not proposed (far larger) |
| FR-102 (3) Time-boxed override | `mode: warn` needs `until` and `reason` | no permanent exception | recorded with who and why | $0 |
| FR-102 (4) Who can write approved folders | `doctor` | least privilege made visible | `doctor`'s caller reads IAM policies (already needed) | $0 |

**Decisions (user, 2026-11-06: the recommendations stand, with toggles and their cost shown):** (a) items 1 and 2
are on under `mode: refuse`, with `policies.integrity.pin: off` to opt out; (b) a scoring run whose approved generation
is gone fails; (c) KServe uses the `emptyDir` route if the build-time check passes, else the copy; (d) the watch is off
by default and runs hourly when turned on. **Every toggle's cost goes into docs/INTEGRITY.md as a table like the one
above**, and `doctor` prints each integrity toggle with its state, so whoever turns one on sees the cost and the
protection it buys. Lock renewal and DATA_WRITE logs: settled (below).

**Lock renewal, settled with the user (2026-11-06; design worked through with the user over several rounds, then a
live permission spike).**

*Invariant (for docs/INTEGRITY.md):* when `policies.integrity.lock` is on, every stored artifact needed to verify,
serve, score or roll back an active approved model has effective protection before it becomes active, for as long as
it is needed. "Stored" leaves out Agent Platform's own copy (Google's to hold, not ours to lock) and the event log
(protected by 1.4.0's deletion protection and tenant-scoped writes, and by FR-102).

*Principles:* (1) approval establishes the first protection; (2) operational dependency decides ongoing protection, not
approval status; (3) activation (approve, deploy, promote, rollback) establishes protection synchronously, before it
counts as done; (4) a daily reconciliation renews it; (5) exact generations always (`?generation=` on every call);
(6) rollback eligibility is derived from live sources and the event log, with no second deployment-state store;
(7) retirement stops renewal and never shortens a date; (8) temporary holds stay separate and are released only by an
authorised, recorded command; (9) monitoring has three independent conditions; (10) cleanup is not Pipewright's job
(retired files become deletable when their date passes; neither the starter nor the blueprint deletes model files).

| Concern | Responsibility | Source of truth |
|---|---|---|
| Approval | first retention date | the approval (`integrity.lock`, as today) |
| Operational use | which versions need protection | the live registry (defaults), endpoints (deployed models), KServe services |
| Rollback eligibility | the immediately previous default, for a window | the event log, cross-checked with Admin Activity audit entries |
| Protection | establish and renew retention | Cloud Storage (retention dates on exact generations) |
| Retirement | stop renewing what is no longer needed | live operational state |
| Hold release | release temporary holds | an explicit, authorised command (`pipewright model retire`), recorded with a name |

*Discovery* returns a deduplicated set of protected artifacts, each with bucket, object, generation, type (model file,
evaluation record, release contract, KServe copy) and reason (default, serving, rollback candidate, live contract).
The previous default is rebuilt from the order of every event that made a version the default (`model.approved`
records `previous`; registration with `set_default: on_approval`, the BigQuery ML runtime, loop rollback and the
console do not), cross-checked with the Admin Activity audit log (as `who_made_default` does). *Failing safe means
protecting more, never renewing nothing:* when a link is missing or the sources disagree, every candidate in the window
is protected and the summary is `degraded`. A default that changes during a run needs no retry: the run protects
everything it saw active, the next run settles it. Rollback window: `rollback_deadline = replaced_at +
(lock.rollback_days or lock.days)`, only the most recent previous default; `rollback_days: 0` means no rollback window,
while defaults and serving versions stay protected.

*Reconciliation* per artifact: read the generation's metadata and metageneration; renew only when the remaining
protection is under the threshold `T = max(lock.days / 2, 3 days)`; patch with `ifMetagenerationMatch`; on 412 re-read
and, if the new date is enough, count it done, else retry a bounded number of times with fresh metadata; re-read and
verify after every change; never shorten (in code, and Cloud Storage refuses it anyway). The patch body is only
`{"retention": {...}}`; a unit test fails if any other field appears. A crash between extending and recording is
harmless: the next run finds the date far enough out. `validate` warns when `lock.days` is under 5 and the docs
recommend 7 or more: below that, continuous protection cannot be promised if runs are missed.

*Lock on activation:* approve, deploy, promote and `pipewright rollback` / `loop rollback` establish or refresh the
lock before the action counts as done, as the caller (who already has this access, as `approve` does today). Under
`lock.days > 0` and `mode: refuse` they refuse when protection cannot be established (a change from 1.4.0, where
`lock()` never fails an approval); under `mode: warn` they proceed, say so and record it; with `lock.days: 0` nothing
changes. An expired but existing generation is protected again before activation; a deleted one cannot be, so the
rollback refuses.

**The permission spike (run live in pw-ebook 2026-11-06, torn down the same day; evidence, scripts and results in
`docs/evidence/fr101-retention-permissions/`).** Three identities, each with one custom role on a throwaway bucket
(object retention and versioning on), conditioned to `models/`; each result re-read from the object as an
administrator. Permanent architectural finding:

> Cloud Storage retention renewal requires `storage.objects.update` in addition to `storage.objects.get` and
> `storage.objects.setRetention`. The update permission also permits temporary-hold release and changes to object
> metadata. Consequently, renewal runs under a dedicated service identity, separate from the Pipewright launcher. Its
> IAM scope is restricted to model storage folders, and DATA_WRITE audit logging detects unexpected hold changes.

Verified facts: retention cannot be shortened or removed by the renewer, even with `overrideUnlockedRetention=true`
(that needs `storage.objects.overrideUnlockedRetention`); Unlocked cannot be turned into Locked by it; Locked-mode
retention can be extended; object contents cannot be overwritten or deleted; content type and custom metadata can be
modified; temporary holds can be released; a stale `ifMetagenerationMatch` is refused with 412; generation-specific
updates touch only the approved generation; omitting `generation` targets the live generation instead; nothing outside
the `models/` condition can be read or changed; the conditional bindings took effect within seconds.

*Tested role (the renewer's, custom):* `storage.objects.get`, `storage.objects.setRetention`, `storage.objects.update`,
granted on the artifacts bucket with the condition `resource.name.startsWith("projects/_/buckets/<artifacts>/objects/
<model folders>/")`. *Tested bucket configuration:* uniform bucket-level access, object retention enabled (only at
creation; existing buckets use holds and get no renewer), versioning on.

**Security architecture.** The launcher keeps `roles/storage.objectViewer` only and cannot renew retention or release
holds. Renewal is `pipewright integrity renew --env <env>`, run daily by a dedicated Cloud Run job (the launcher image,
pinned by digest, another command) under its own service account: the role above, `aiplatform.viewer` (not the
launcher's `aiplatform.user`, so it cannot move a default, which keeps FR-100 away from it), read-only access to the
event log, and a read of KServe services (the network path to private GKE is checked in the build; the `model.deployed`
events are the fallback). It does only: discover, read exact-generation metadata, calculate, extend, re-read and verify,
record. No deploy, promote, approve or rollback; no create, overwrite or delete; no IAM or bucket changes; no key files
(the Cloud Run service identity). Discovery stays inside the renewer, so the launcher does not decide what is
protected. Separate identities means separate principals: the launcher's service account has no binding on the
renewer's.

*Threat model.* (1) The renewer can release a hold within its scope, because `update` allows it. It never does so
intentionally, and every exercise is observable, but this is detection, not prevention; legal holds are not claimed to
be technically immutable. Guidance for anyone placing legal holds: keep them in a bucket or folder outside the
renewer's scope (Pipewright itself never places holds on a bucket with object retention). (2) The real boundary for
"no arbitrary command" is who may change the job: only Terraform (the platform team) holds `iam.serviceAccountUser`
on the renewer's account and may update the job; the launcher and the tenant runners hold neither; `doctor` lists who
can act as the renewer and fails when anyone else can. (3) A compromised launcher can move a default (FR-100) and so
change what is renewed: a bounded availability and storage-cost risk, not an integrity bypass; it cannot shorten,
delete or release anything.

*Detection, two independent signals.* (a) Cross-identity check: the launcher's watch (read-only) records the hold state
and metageneration of every protected artifact each day and alerts on a hold that disappeared or a metageneration change
with no matching `model.lock.renewed` event, so a compromised renewer cannot hide its own changes. (b) Audit logs:
Cloud Storage DATA_WRITE logs are switched on per project (they cannot be limited to a bucket or folder at the source),
with a Cloud Logging exclusion that keeps only entries for the artifacts bucket's model folders (excluded entries are
not charged); an alert on the renewer's `storage.objects.update` entries outside its scheduled run, and on any hold
change, to the security and operations channels. The entries name the method and caller, not which fields changed
(to confirm in the alert test), which is why (a) is the primary signal.

*Events and monitoring.* `model.lock.renewed` (per artifact changed: bucket, object, generation, type, reason,
previous and new date, actor) and `model.lock.failed`; no event for unchanged artifacts; one daily
`model.lock.reconciliation` summary (status `healthy | degraded | failed`, protected, renewed and failed counts,
least protection remaining in days, rollback candidates, contracts). A failed run never publishes `healthy`. Three
independent alert conditions: summary missing within the expected interval (renewer or logging down); status not
`healthy`; least protection remaining below two renewal intervals plus the threshold margin. An unhealthy summary does
not suppress the low-protection alert.

*Deployment gate, as code.* Terraform creates the renewer's Scheduler trigger only after the audit config, the
exclusion, the alert policies and the cross-identity check exist; the job ships disabled; `doctor` reports the renewer
unhealthy if any is missing. The live test releases a real hold on a test object and confirms both alerts arrive
before the schedule is enabled in a real environment. DATA_WRITE logs: on whenever renewal is on (part of the gate);
otherwise off in the starter and on in the blueprint.

*Cost (London list prices, 2026-11-06):* the daily job about $0.05 a month (one minute, Cloud Run jobs tier 2); one
more Scheduler job $0.10 a month unless within the billing account's 3 free; renewal operations about $0.08 a month
for 100 versions; retired files kept up to `lock.days` longer (5 GiB for 30 days: $0.12); filtered DATA_WRITE logs
within the free 50 GiB in most cases.

*Build order for renewal:* (1) protected-artifact discovery (defaults, endpoints, KServe, rollback window, contracts,
copies); (2) reconciliation with exact generations, the threshold minimum, metageneration preconditions and bounded
retries; (3) lock on activation; (4) the renewer job, its identity, DATA_WRITE with the exclusion, the security alerts,
the cross-identity check, the events, the summary and the three health alerts, as one stage, shipped disabled and
enabled only after the live gate passes; (5) retirement; (6) the authorised hold-release command. The spike script
becomes an integration test.

*Acceptance tests:* `lock.days: 0`; outside and inside the threshold; Locked mode; an already-extended generation; a
newer generation under the same name; a version removed from serving; a version reactivated after retirement; a failed
storage call; a failed event write; two renewers at once (one patch wins, the other reconciles on fresh metadata);
activation after expiry (protected again first); activation after the generation was deleted (refused); contract
renewal; KServe copy renewal; `lock.days: 2` (threshold 3, `validate` warns); rollback window expiry; a missing
replacement event (candidates over-protected, summary degraded, alert); a default changing during a run (protected,
settled next run); the patch body holds only retention; a hold released on a test object (both alerts fire).

**Live tests (pw-ebook pair and a rebuilt ent with GKE):** the spike's swap repeated against each path: scoring refuses
a swapped file and runs with the approved one; a KServe scale-out after a swap starts no pod with the wrong bytes and
the alert fires; Agent Platform approve refuses a folder changed after registration; the watch reports changed and
restored once each; the heartbeat alert fires when the launcher is stopped; rollback paths refuse a changed version;
lock renewal extends retention on a serving version (with the acceptance tests and the deployment gate above); `integrity check` exit codes in a CI step.

**Rules:** additive, nothing changes for a use case with integrity off; names, digests, generations and versions in
every message, never a data value; the user reviews this design before it is built.

### 8.19 Choosing the online feature store (FR-104, settled with the user 2026-11-07; phase 9 with FR-92)

**Why:** online lookup (FR-51, 1.1.0) uses Google Cloud's managed online store, Feature Store online serving on
Bigtable: $1.07 per node-hour in London, about $781 a month per node whether or not it is called, and the starter
creates one per tenant (FR-75). For a team with a few modest models that can be the largest line on the bill, and many
use cases do not need what Bigtable is built for. The user asked for cheaper choices, with the offline store staying
in BigQuery.

**Evidence:** a live spike in europe-west2 on 2026-11-06, torn down the same day
(`docs/evidence/fr104-online-stores/`): 1M customers x 30 features, lookups by ID at 1, 10 and 50 a second.

| Store | p50 at 10 a second, end to end | Monthly cost (list price) | Notes |
|---|---|---|---|
| Feature Store on Bigtable (today) | 12.5 ms | about $781 per node | steady under load |
| BigQuery + BI Engine | 198 ms through the BigQuery API | about $38 per GiB reserved | 0 bytes billed per lookup once cached; no copy of the data |
| Cloud SQL for PostgreSQL | 3.3 ms direct; 19 ms through a Cloud Run lookup service | about $61 (1 vCPU, 3.75 GB); about $120 with HA | a synced copy of the latest values |
| BigQuery without BI Engine (control) | 289 ms | about $0.0018 per lookup | billed as a full table read every time; never offered for online lookup |
| Firestore (Standard) | 9.8 ms | $0.39 per million reads, $1.17 per million writes | no standing cost; a plugin, not core |

**What is settled (user, 2026-11-07):**

1. *One setting, per environment:* `features.online_store.type: bigtable | bigquery | cloudsql | http`. Bigtable stays
   the default for anyone using it today, so nothing breaks (stable upgrades). `doctor` shows the standing cost of the
   chosen store, and the docs give the comparison above with when to choose each: about
   200 ms acceptable and modest traffic, BigQuery + BI Engine; milliseconds on a budget, Cloud SQL; high traffic,
   tight latency and large data, Bigtable.
2. *`bigquery` (BigQuery + BI Engine):* the serving handler queries the feature table by entity through the BigQuery
   query API (short query mode, no job, result cache off); BigQuery stays the single copy. The environment's BI Engine
   reservation (one per project and region, shared, so set per environment, not per tenant) lists the online feature
   tables as preferred tables, set by Terraform. Guards, because a lookup BI Engine does not serve is billed as a full
   table read: `doctor` refuses `type: bigquery` when the reservation is missing or does not cover the tables and their
   size; the handler samples the acceleration mode and Pipewright alerts when lookups stop being accelerated; the docs
   name the first-minute warm-up. To check in the build: billing while BI Engine reloads a refreshed table.
3. *`cloudsql` (first-party):* the starter's Terraform creates Cloud SQL for PostgreSQL with private IP over the
   Private Services Access peering, CMEK from the manifest, IAM database login (no passwords) and optional HA.
   Pipewright owns the sync: a latest-per-entity snapshot query, a free extract job to Parquet with a manifest (each
   file's generation and SHA-256, row count, schema, entity key, as-of time, and the feature contract version from
   FR-103), a load into a staging table, a check of row count and fingerprints against the manifest, and an atomic
   rename so lookups never see half a load. Freshness is the sync schedule, as with Bigtable today.
4. *Serving path:* callers do not change (they send an entity ID). KServe on GKE connects to Cloud SQL's private IP
   directly (about 3 ms). Agent Platform endpoints run in Google's network, so they call a Cloud Run lookup service
   over HTTPS with an ID token (about 19 ms), which queries Cloud SQL; whether Private Service Connect lets an endpoint
   reach the database directly is checked with FR-92. Batch scoring keeps reading BigQuery point in time.
5. *`http` contract:* `POST /lookup {entity_ids}` returns feature names and values; the handler applies the same
   timeout and missing-value filling (FR-73) and checks returned names and types against the contract. A team-run
   store is outside Pipewright's integrity guarantee: Pipewright proves what it exported and checks what comes back,
   not what happens inside the team's database.
6. *The export as a product:* the Parquet export with its manifest is usable on its own, so teams can load features
   into a database they already run. Other databases (MySQL, SQL Server, Redis, Firestore, MongoDB) are separate
   open-source plugins built on the export, the manifest and the `http` contract, through the plugin API.
   Firestore is the first candidate plugin: no standing cost, cheapest for small idle use cases.
7. *Lessons the reference code carries:* keep HTTPS connections alive; pool database connections with a bound and cap
   instances; check every write when loading a store; load only changed rows where writes are charged one by one.

**Open for the design review (recommendations first):** (a) Pipewright ships the Cloud Run lookup service for
`cloudsql` on Agent Platform endpoints (about 60 lines, pooled, IAM login), rather than each team writing one;
(b) the sync runs on the launcher's existing scheduler, rather than being triggered by the team; (c) Cloud SQL tenancy:
one instance per environment with a schema per tenant (cheapest) or an instance per tenant (strongest isolation);
(d) the request rate one Cloud SQL vCPU sustains (50 a second was untested headroom) and lookups during the rename.

**Rules:** additive and off by default; the stores' costs shown before anyone switches one on; names, digests and
counts in every message and manifest, never a feature value.

## 9. Interfaces and contracts

### 9.1 CLI (`pipewright`)

| Stage | Commands |
|---|---|
| Configure | `new-usecase` (also `--from-bq`), `validate`, `resolve`, `doctor`, `test`, `schemas`, `explain` |
| Release | `tables sync`, `release`, `rollback`, `promote`, `status`, `schedules sync`, `alerts sync`, `ci init` |
| Operate | `run`, `model list`, `model approve`, `deploy-model`, `model predict`, `monitor status`, `data erase`, `synthetic generate`, `synthetic gates`, `pipelines-ui` (private GKE) |
| Decide | `retrain list`, `retrain show`, `retrain approve`, `retrain decline`, `retrain snooze` (with `--env` or `--manifest gs://...`) |
| Extend | `plugins` |

**Common flags:** `--env`, `--usecase` (repeatable where it makes sense), `--root`.

**Environment variables:** `GCLOUD_CONFIG` (refreshable gcloud credentials), `GOOGLE_OAUTH_ACCESS_TOKEN` (a fixed
token), `PIPEWRIGHT_ID_TOKEN` (an ID token for the launcher or a decision), `PIPEWRIGHT_RUN_VIA=http` (call the
launcher directly from inside the network), `PIPEWRIGHT_KUBERNETES_ENDPOINT` (the Kubernetes API inside the
cluster), `PIPEWRIGHT_BUILD_CA_BUNDLE`, and the CA variables in D-49.

### 9.2 Environment manifest (`pipewright/v1`, `manifest.schema.json`)

| Field | Contents |
|---|---|
| Required | `apiVersion`, `environment`, `blueprint`, `orchestrator`, `project`, `region`, `artifact_registry{docker, kfp, python?}`, `artifacts_bucket`, `landing_bucket`, `launcher{service_account, url?, service?, subscription?, run_requests_topic?}`, `tenants{name: {runner_service_account, bigquery_dataset, landing_prefix?, online_serving?, namespace?, outputs_uri?, decision_feed?}}` |
| `policies` | `deploy_model`, `promote_model` (`manual` or `auto`); `retraining{allowed_modes, required_approvers, max_snooze_days}`; `synthetic{allowed, allowed_purposes, allowed_engines, differential_privacy_for, gates, required_approvers}` |
| `approver_groups` | `{name: {topic, subscription, description?}}` |
| `observability` | `events_topic`, `evidence_dataset`, `evidence_uri`, `log_sinks`, `otlp_endpoint` |
| `security` | `kms_key`, `bigquery_kms_key`, `network`, `reserved_ip_ranges`, `perimeter` |
| `features` (optional) | `required`: every model feature from `data.features_from`; per tenant, `tenants.<t>.features.datasets`: the `project.dataset` feature datasets its use cases may read |
| `ray` (optional; also per tenant, `tenants.<t>.ray`) | `enabled`, `mode` (`per_run`, or per tenant on GKE `standing` with `standing{cluster, address, image}`), `ray_version`, `python_version`, `image`, `network_attachment`, `limits{max_workers, machines, accelerators, max_minutes}` |
| `orchestrator`, `kubernetes` (private GKE) | `orchestrator: kfp`; `kubernetes{endpoint, dns_endpoint, cluster, ca_certificate, namespace}`; per tenant `tenants.<t>.kubernetes_service_account` |
| `schedules` (optional) | `backend` (`cloud_scheduler` or `kubernetes`), `namespace`, `service_account`, `image`, `run_as_user` |
| `kserve` (optional) | `mirror`, `service_account`, `namespaces` (per tenant), `exposure` (`cluster` or `internal_lb`) |
| `launcher` (more) | `runtime` (`cloud_run` or `gke`), `namespace`, `audiences` |

**Compatibility rule (R-05, built 2026-10-10):** manifest fields are added, never changed in meaning. A version
that meets keys it does not know (anywhere the schema forbids extra keys) **ignores them and says so**: `validate`
prints a warning, `doctor` lists them, the launcher logs them. Everything it does know is still validated in full.
A key that is a **policy** (one an older version must not silently skip, such as `features.required`) comes with
`requires` in the manifest, a version specifier such as `">=1.1"`: older versions then refuse the manifest with
"this environment needs Pipewright >=1.1", and the launcher keeps its last good manifest. So app teams' pinned CLIs
keep working when the platform adds a field, and never run with a policy they cannot apply.

### 9.3 Use case and environment files

`usecase.schema.json` and `environment.schema.json` in `src/pipewright/config/schemas/`; `reference.yaml` next to
them describes every use case key (D-74). The churn example uses most keys; the minimal example only the required
ones. `pipewright schemas` writes the editor schemas to `.pipewright-schemas/` in the MLOps repo: the use case
schema, one for included files (any top-level sections), one for `environments/<env>.yaml` overrides (only the keys
that may vary), and the environment schema.

### 9.4 Evaluation record

`evaluation.json` in the model version's artifact directory, written by the training pipeline: `metrics` (name to
value), `slices` (column to value to `{rows, <metric>: value}`, for slice gates), `champion` (the champion's metrics on
the same test data and `_version`, or `null`), `decision` (informational). Pipewright recomputes the gates from it
(D-72). When the configuration has no gates, slice gates or champion comparison, no record is needed.

### 9.5 Plugin API v1

Entry point group `pipewright.commands`, signature `register(subparsers, common)`. Helpers a plugin may rely on:
`load_environment`, `environment_from_manifest`, `resolve_usecase`, `Gcp`, `gcs_read`, `gcs_write`,
`observability.events`, `release.service.read_pointer`, `read_contract`.

---

## 10. Testing

| Layer | Command | What it covers |
|---|---|---|
| Unit tests | `uv run pytest` | Configuration, releases, promotion, launcher routing and routes, retrain decisions, features, Ray clusters, the `kfp` adapter, synthetic data, alerts, tables, erasure, plugins; offline, with fakes |
| Lint | `uv run ruff check .` | |
| Example | `uv run --no-project --with-requirements examples/churn/tests/requirements.txt python -m pytest examples/churn/tests` | The example's ML helpers |
| Package | `scripts/check_package.py dist/`, `scripts/smoke_test.sh <pipewright>` | Wheel contents; the installed CLI scaffolds, validates and resolves a use case and writes CI workflows, with no cloud access |
| Product CI | `.github/workflows/ci.yml` | All of the above, plus the launcher image built from the wheel |

Changes that affect behaviour on Google Cloud should also be exercised against a real environment before release.

---

## 11. Known gaps, risks and open questions

| ID | Item | Notes or proposal |
|---|---|---|
| R-04 | The artifacts bucket was shared by tenants | Resolved 2026-10-10: runs write under `pipeline-root/<tenant>/<usecase>`, so a platform can grant each runner its own prefix only (Pipewright Enterprise does). A contract with another pipeline root is not reused for the same commit |
| R-05 | Older versions and newer manifest fields | Resolved 2026-10-10: unknown keys are ignored with a warning; a policy key comes with `requires`, which older versions refuse clearly. Holds from the first release on; images built before it still need new manifest keys rolled out after them |
| R-06 | Cloud Scheduler IAM is project-wide | Jobs should only call the launcher as its identity; `doctor` checks |
| R-07 | Tenant offboarding is undefined | Proposal: block removal until runtime objects are gone |
| R-08 | Erasure in GCS artifacts and trained models | Not covered; retrain to remove influence; review with a data protection specialist |
| R-09 | Manual approval of a first model version is not recorded as `model.approved` | The first version becomes the champion automatically |
| R-18 | Approver identity relies on Google Cloud IAM and ID tokens | See section 8.7 for the residual risks |
| R-19 | Labels arrive late for many use cases | The decision pack shows how many labelled outcomes exist, so drift is not read as degradation |
| R-20 | No Terraform for the platform side in this repository | By design. A separate starter repository (public endpoints, no CMEK or VPC) is shared on request: people ask the maintainers (hello@fluxorsoft.com or GitHub Discussions), which also builds the community. Production blueprints with private networking come with Pipewright Enterprise |
| R-21 | The PyPI name `pipewright` belongs to an unrelated project | Resolved: the distribution is `pipewright-mlops`; the import and the command stay `pipewright` |
| R-22 | Feature governance is BigQuery IAM | The runner reads feature tables directly, so what a tenant may read is decided by grants on the feature datasets (the platform's), and the event records what a run actually read. Pipewright cannot stop a team reading a table its runner may read; Enterprise adds the grants, a policy and evidence |

---

## 12. Working conventions

**Style**

- Plain English for anything a non-specialist reads; explain a term where it first appears.
- No em-dashes in documentation or messages. Use colons, commas or full stops.
- Write code like the surrounding code: naming, comment density, idiom. Line length 120.

**Engineering**

- Enterprise never patches the core (D-34): a need elsewhere becomes a general, tested hook here.
- Configuration changes are schema changes, with defaults and cross-checks; the manifest schema only gains
  optional fields.
- Tests accompany changes and run offline. User-visible behaviour changes update the README, the onboarding guide,
  the example and this file.
- Never commit credentials, project IDs, real email addresses or customer data; use placeholders such as
  `example.com` and `acme-ml-dev`.

### 12.1 Google dependencies (checked 2026-10-10; recheck before each release)

Pipewright builds only on Google services and SDK parts that are not deprecated. Checked against the Agent Platform
deprecations page and the SDK 2.0.1 migration guide:

| Used by Pipewright | State |
|---|---|
| Agent Platform Pipelines (KFP 2.17), Model Registry, endpoints, custom jobs | Supported |
| Persistent resources for Ray (Ray on Agent Platform), the Ray client in `google-cloud-aiplatform[ray]` | Supported; no deprecation notice |
| `google-cloud-aiplatform` 2.x (examples pin 2.3.0) | The classic ML surface is "fully supported"; 2.0.0 must not be installed (the core excludes it) |
| BigQuery, including `ML.FEATURES_AT_TIME` and `ML.ENTITY_FEATURES_AT_TIME` | Supported |
| Feature Store feature groups and the Feature Registry (used since 2026-10-10); planned: Bigtable online serving, Knowledge Catalog (Dataplex renamed, same APIs) | Supported; Bigtable is Google's recommended replacement for Optimized online serving |

| Retiring | Shutdown | Effect on Pipewright |
|---|---|---|
| Feature Store (Legacy) | 2027-02-17 | Never used |
| Feature Store Optimized online serving | 2027-02-17 | Never used; online serving will use Bigtable |
| Vertex Explainable AI | 2027-03-16 | Never used: `explainability` is permutation importance in the use case's own evaluation step |
| SDK generative modules (`vertexai.generative_models` and others) | Removed 2026-06-24 | Never imported |

---

## 13. Roadmap

**Phases** (2026-10-22): phase 1 strengthens the core ML, phase 2 adds real time and trust, phase 3 platform scale and intelligence; phases 1 to 3 ship together as 1.1.0 (user, 2026-10-24), except AIOps (still to analyse). Phase 4 comes after 1.1.0, phase 5 after phase 4. Items marked "when a customer needs it" or "on demand" stay listed and are not phased: they are built when a customer asks. **After Pipewright 1.3.1 and Enterprise 1.3.3** (user, 2026-11-05): **phase 6** (released as 1.4.0, 2026-11-06): FR-93 to FR-97 (what was approved is what runs, and a trail that cannot be forged), the closed retrain loop's phases 2 and 3 (FR-48), `pipewright features register` (FR-75), `pipewright report fleet` (FR-86) and the recommended feature pattern in the reference example (FR-99); **phase 7** (the next release; user, 2026-11-06; phases 7 and 8 ship together as **1.4.1**: the user chose a patch number although they add keys, commands and events, which STABILITY.md reserves for minor versions, because adopters are few for now; revisit when there are many): know when an approved record changes between deploys (FR-101; spikes done, design settled in section 8.18) and an audit trail that lasts (FR-102); **phase 8** (released together with phase 7, user 2026-11-06): immutable feature definitions and the complete ML release (FR-103, section 8.17); **phase 9**: online endpoints in Terraform (FR-92) and the choice of online feature store (FR-104, section 8.19; user, 2026-11-07); **phase 10**: A/B testing (FR-57); **phase 11**: everything else, to be reviewed and prioritised (model serving observability FR-98, OpenTelemetry export, the SmartNoise engine, AIOps, and the items built when a customer needs it). The user reviews each phase's design before it is built.

**Phase 6 decisions** (user, 2026-11-05) (banking and insurance: nothing leaks into logs, and every decision is recorded so that it cannot be changed unnoticed): (1) the integrity checks (FR-93 the evaluation is the one approved, FR-94 the model files and serving image are the approved ones, FR-96 the released rules, a default with no approval) **refuse by default** from the release that ships them; a manifest policy may relax an environment to `warn`. This is the one deliberate exception to "an upgrade changes nothing until a setting asks": a failed check means what runs is not what was approved. Approvals from before 1.3.1, which carry no fingerprint, still deploy, with a note. (2) The trail is tamper-evident (fingerprints, the Cloud Audit Logs cross-check) **and approved model files are locked**: an approval places a retention lock on the version's model folder (Cloud Storage object retention, per object, for a period the platform sets), which needs object retention on the artifacts bucket. (3) FR-48's items are each behind a toggle, so nothing changes on upgrade. (4) No fingerprint, digest check or diff prints a data value: messages name records, digests, metrics and gates, never rows, feature values or slice values.

| Item | Status |
|---|---|
| First open-source release: `1.0.0` stable (D-86), rehearsed with a throwaway pre-release tag; wheel and launcher image; STABILITY.md (the stable surface and the deprecation policy); every 0.4 reference moved to 1.0 | Released 2026-10-17; re-cut 2026-10-18 with synthetic data |
| Manifest forward compatibility (R-05, D-83): unknown keys ignored with a warning, `requires` for policies | Built 2026-10-10 |
| Per-tenant pipeline roots (R-04): `pipeline-root/<tenant>/<usecase>` | Built 2026-10-10 |
| OpenTelemetry export from the launcher | Phase 11: to be reviewed and prioritised (user, 2026-11-05); was phase 5 |
| The recommended feature pattern in the reference example: the `churn` README points to the `features` example up front (a prepared table is fine to start with; feature tables or feature groups are recommended, docs/FEATURES.md), and a `churn` variant on feature tables, so the reference shows the recommended way too | Phase 6, next (user, 2026-11-05) (FR-99); planned for 1.3.0 but not delivered |
| GPU limits for pipeline steps (FR-58): accelerator types and counts per environment for `steps.<name>.gpu`, as Ray already has | Done: 1.2.0 (2026-10-31), tested by unit tests only (no GPU quota) |
| A base image per use case (FR-59): `components.base_image`, within the image allow-list, for example CUDA for GPU training | Done: 1.2.0 (2026-10-31) |
| How to call a deployed model (FR-60): `deploy-model` prints the endpoint ID, the predict URL, the caller's role and a sample request | Done: 1.2.0 (2026-10-31) |
| GPU serving (FR-61): `model.serving.accelerator`, within platform limits per environment | Done: 1.2.0 (2026-10-31), tested by unit tests only (no GPU quota) |
| Scale to zero (FR-62): `replicas.min: 0` where Agent Platform supports it | Done: 1.2.0 (2026-10-31) |
| Serving gated in CI, or automatic (FR-63): `approvals.deploy_model: ci` (a GitHub-approved deploy gate per new version) or `on_register`; `manual` stays the default | Done: 1.2.0 (2026-10-31) |
| Examples that show every setting (FR-64): real YAML, `churn` with every key and its default, `minimal` kept minimal, a README per example, a test against the resolver's defaults | Done: 1.2.0 (2026-10-31) |
| Synthetic data reads labels like training (FR-65): the label window applied to the synthetic source | Done: 1.2.0 (2026-10-31) |
| Tests before a release (FR-66): `tests.on_release`, and the reference wording | Done: 1.2.0 (2026-10-31) |
| A failed run is an event and an alert (FR-67): `run.failed` | Done: 1.2.0 (2026-10-31) |
| The cut-off is never a feature by default (FR-68) | Done: 1.2.0 (2026-10-31) |
| Forecasts evaluated per series (FR-69): metrics, the worst series and gates per series | Done: 1.2.0 (2026-10-31) |
| One order for the contract and preparation (FR-70): the contract checked before `bqml.prepare`, as in the container path | Done: 1.2.0 (2026-10-31) |
| The starter moves to 1.1.0 (FR-71); Ray stays out of the starter, with manual grants documented | Done: 1.2.0 (2026-10-31) |
| Re-check the champion against today's gates (FR-72): `pipewright model check`, champion slices and fairness in the record | Done: 1.2.0 (2026-10-31) |
| Online serving fills missing values as training did (FR-73), or answers 400 with the feature named | Done: 1.2.0 (2026-10-31) |
| `deploy-model` is safe to repeat and to run twice at once (FR-74) | Done: 1.2.0 (2026-10-31) |
| The starter creates an online store per tenant (FR-75) | Done: 1.2.0 (2026-10-31), the starter's store; `pipewright features register`: phase 6 (user, 2026-11-05) |
| A promoted model reads the destination's online store (FR-76, bug) | Done: 1.2.0 (2026-10-31) |
| `alerts sync` removes the email channels it no longer uses (FR-77) | Done: 1.2.0 (2026-10-31) |
| Performance monitoring joins outcomes to the scores they judge (FR-78, bug) | Done: 1.2.0 (2026-10-31) |
| A retrain case records what its training did (FR-79) | Done: 1.2.0 (2026-10-31) |
| `ci init` promotes to `promotion.from` environments; the starter makes the CI identity (FR-80) | Done: 1.2.0 (2026-10-31) |
| Installing a released Pipewright gives the kfp it was tested with (FR-81) | Done: 1.2.0 (2026-10-31) |
| `pipewright test` runs where the components' requirements are installed (FR-82) | Done: 1.2.0 (2026-10-31) |
| A first version is not the champion until a person says so, under a manual policy (FR-83) | Done: 1.2.0 (2026-10-31) |
| The starter keeps the event log; lineage shows the decisions (FR-84) | Done: 1.2.0 (2026-10-31) |
| A replay reproduces the run exactly: rows sorted, a data fingerprint (FR-85) | Done: 1.2.0 (2026-10-31) |
| Machine-readable fleet state; BigQuery ML runs in the metrics table (FR-86) | Done: 1.2.0 (2026-10-31), `--json` and the metrics table; `pipewright report fleet`: phase 6 (user, 2026-11-05) |
| Old images are cleaned up, never the ones a release still needs (FR-87) | Done: 1.2.0 (2026-10-31) |
| An approval keeps the evaluation it was judged on: digest on the event, checked by lineage (FR-89) | Done: 1.3.1 |
| Know when an approved record changes between deploys: a watch, an alert, a full scoring-run check, an exit-code CI gate (FR-101) | Phase 7, the next release (user, 2026-11-06); the user reviews the design before it is built |
| An audit trail that lasts: long audit retention, Data Access logs, time-boxed break glass, who can write approved folders (FR-102) | Phase 7, the next release (user, 2026-11-06) |
| Immutable feature definitions and the complete ML release (FR-103, section 8.17) | Phase 8, released together with phase 7 (user, 2026-11-06) |
| Online endpoints in Terraform: endpoint, IAM and network, deploy-model unchanged (FR-92) | Phase 9 (user, 2026-11-05); the user reviews the wording before it is built |
| Choose the online feature store, with its cost shown: Bigtable, BigQuery + BI Engine, Cloud SQL, http; export and plugins (FR-104, section 8.19) | Phase 9, with FR-92 (user, 2026-11-07) |
| `deploy-model` checks the evaluation is the one approved (FR-93) | Phase 6, next (user, 2026-11-05) |
| What is served is what was approved: model files and serving image by digest (FR-94) | Phase 6, next (user, 2026-11-05) |
| Show what changed in an approved evaluation, and who changed it (FR-95) | Phase 6, next (user, 2026-11-05) |
| Approval and deploy judge by the released rules, not the local checkout (FR-96) | Phase 6, next (user, 2026-11-05) |
| A decision trail that cannot be forged or quietly rewritten (FR-97) | Phase 6, next (user, 2026-11-05) |
| Model serving observability: operational and model signals per version (FR-98) | Phase 11: to be reviewed and prioritised (user, 2026-11-05) |
| A/B testing of models (FR-57, section 8.16): sticky assignment, business metric and guardrails, analysis per arm, a person decides | Phase 10, after FR-92 (user, 2026-11-05); the user reviews the design before it is built |
| Promotion of a release (and its approved model) between environments by digest (FR-29, section 8.12) | Built 2026-10-11 |
| The closed retrain loop across environments (FR-48, section 8.13): phase 1 to 3 deliveries | Phases 1 to 3 released: 1.1.0 (2026-10-26). Phase 6 (user, 2026-11-05): the items section 8.13 lists as not yet done, each behind a toggle: BigQuery ML use cases in the loop (D-115), the online canary path with automatic rollback live (D-108), the loop's data grants (test reading a file landed in production; production's features and label window) and monitoring-driven retraining (delivery phase 4) |
| `kfp` adapter for Kubeflow Pipelines on GKE, with KubeRay, KServe, CronJobs, the pull launcher and the read-only UI (FR-30, sections 8.14 and 8.15) | Built 2026-10-17 |
| `predict` in the generated operate workflow | Built 2026-10-17 |
| Plain Kubernetes on premises (section 8.14) | On demand only (D-89) |
| Synthetic data generation (FR-49): the `synthetic` section, contract-only generation, an engine interface (MOSTLY AI SDK local mode by default, SDV opt-in), evaluation and gates | Built, released in 1.0.0 (2026-10-18); used end to end on Agent Platform and on Kubeflow |
| Synthetic data: relational generation (FR-49 follow-up): the entity table and its child tables together (keys, history per entity, timing against each cut-off), so point-in-time joins and feature tables are tested on synthetic data; the spike showed MOSTLY AI does it (TSTR drop 0.003 on churn) | Built, released in 1.0.0 (2026-10-18); used end to end on Kubeflow (`mode: relational`) |
| Synthetic data: sequential generation (FR-49 follow-up): event histories per entity (transactions, sessions, claims) where order and gaps matter | Built, released in 1.0.0 (2026-10-18); used end to end on Kubeflow (`mode: relational`) |
| Synthetic data: SmartNoise engine (FR-49 follow-up): OpenDP's differentially private generators (MST, AIM; MIT) as a second engine behind the interface, for a formal privacy guarantee where measured risk is not enough | Phase 11: to be reviewed and prioritised (user, 2026-11-05); was phase 5 |
| Synthetic data: AI-assisted authoring (FR-49 follow-up): a drafted `synthetic` section and column rules from a table's schema and profile (Gemini), for a person to review | Built in Pipewright Enterprise (`pipewright synthetic-draft`, 2026-10-18) |
| Value-free error messages (NFR-18, D-65) | Released: 1.1.0 (2026-10-26). Phase 1: built on branch `phase-1` 2026-10-24 |
| Ray on Agent Platform, a cluster per Ray step of a run (FR-45, D-77, section 8.9) | Built 2026-10-10 |
| Standing Ray clusters, a long-lived cluster per tenant (section 8.15): on private GKE, kept by the platform as a KubeRay cluster whose workers scale to zero; a cluster per run stays the default. Not on Agent Platform | Built 2026-10-17 |
| Features from BigQuery feature tables, point in time, for training and batch scoring (FR-46, D-79, D-80, section 8.11) | Built 2026-10-10 |
| Feature Store feature groups as a feature source (D-82) | Built 2026-10-10 |
| Online feature lookup (FR-51): endpoints read features by entity ID from a Bigtable online store, a toggle per use case and environment | Released: 1.1.0 (2026-10-26). Built on branch `phase-1` 2026-10-24 (1.1.0): `model.serving.features: online`, `online_view`, the manifest's `online_store`, the example's handler, `doctor`, `deploy-model` as the runner; Enterprise `tenants.<t>.features.online` (store, views, grants). live-tested in ent 2026-10-24: a prediction by `{customer_id}` on an Agent Platform endpoint read both views (bare IDs and unknown IDs fixed after it). A tenant reads only its own store: the read role is granted on the store's own IAM policy (Enterprise `scripts/online_store_iam.py`, from the blueprint output), because the provider has no IAM resource for online stores and an IAM condition on the store name is not applied to Feature Store permissions (both tested live) |
| BigQuery ML as a training engine (`spec.bqml`, D-112 to D-119; the design and spike are kept with Pipewright Enterprise): built-in training and scoring pipelines for binary, multi-class and regression; later tuning, performance monitoring and retrain proposals from scoring, imported models, promotion by files, k-means and PCA, forecasting exceptions | Released: 1.1.0 (2026-10-26). Phase 1: built on branch `phase-1` 2026-10-24; live-tested end to end on Kubeflow Pipelines on GKE (perimeter enforced) and on Agent Platform Pipelines: train, gate, register, champion comparison, `model approve`, promotion by retrain, scoring, drift. Online serving (phase 2, 2026-10-24): `deploy-model` deploys a BigQuery ML version from its registry entry and checks the endpoint against ML.PREDICT on the run's test rows; live in ent (perimeter enforced): 25 rows, largest difference 6.6e-9, then undeployed |
| Per-run data versioning and a label maturity window (D-110, D-111; `data.snapshot_table`, `pipewright run --replay`, `data.label_source`) | Released: 1.1.0 (2026-10-26). Phase 1: built on branch `phase-1` 2026-10-23; live-tested in ent 2026-10-24 (churn-versions in pipewright-ent-e2e): a 30-day window from renewal events, anchored on the newest one, dropped 409 maturing rows; the snapshot kept 4,591 rows; `pipewright run --replay` read exactly those rows and trained the same model (auc 0.801565 both times). The live test found the launcher refusing `snapshot://` (fixed) |
| Regression and multi-class models (FR-52): metrics, gates, champion comparison and drift beyond binary classification | Released: 1.1.0 (2026-10-26). Phase 1: built on branch `phase-1` 2026-10-23 (`data.label.task`, per-task metrics and directions, example metric helpers, prediction drift on class shares); the BigQuery ML side comes with the BQML build (its SQL in `.sql` files under `examples/`) |
| Fairness metrics and gates (FR-53) | Released: 1.1.0 (2026-10-26). Built on branch `phase-1` 2026-10-24 (1.1.0): `evaluation.fairness`, runtime `fairness_measures`, `models/gates.py`, the example's evaluate step and model card, BigQuery ML `fairness.sql`; binary classification only |
| Approved image allow-list (FR-54): base images and package indexes per environment, refused at validate and release | Released: 1.1.0 (2026-10-26). Built on branch `phase-1` 2026-10-24 (1.1.0): manifest `policies.images`, `build/allowlist.py`, the contract's `images`; Enterprise sets it in the platform file |
| Lineage per run (FR-55): `lineage.json` per run and a lineage view in Fleet View | Released: 1.1.0 (2026-10-26). OSS part built on branch `phase-1` 2026-10-24 (`write_lineage`, the example and BigQuery ML register steps, `pipewright lineage`, which follows promotions); live-tested in ent 2026-10-24 (churn-versions v7: run, release with digests, the 4,591-row snapshot and its read time, feature groups, label window, evaluation); Fleet View shows it (Enterprise) |
| Approval gates in CI (FR-56, D-120 to D-123): gated jobs per decision, the scheduled decision poll, both identities recorded | Released: 1.1.0 (2026-10-26). Built on branch `phase-1` 2026-10-24: the approval record on decision and loop events, `pipewright decisions pending`, the `pipewright-decisions` and `pipewright-gate` workflows from `pipewright ci init`, `PIPEWRIGHT_ID_TOKENS`. Live-tested 2026-10-24 in pipewright-ent-e2e (branch `phase-1-gates`; the two workflow files also on `main`, as GitHub dispatches only workflows on the default branch): the decisions run listed ent's pending approvals and dispatched one gate per item; with no required reviewers (not available for private repositories on GitHub's free plan) every gate refused (`nobody approved this run`) and ran nothing. Approved from the CLI instead, the event records `approval: {via: outside-ci}`. Found: older candidates were offered too (fixed: the newest passing version only). Reviewer approval in GitHub itself is untested until the repository has reviewers |

---

## 14. Glossary

| Term | Meaning |
|---|---|
| Platform team | Engineers who build and run the environments |
| App team, tenant | An ML team; a tenant is its slot in an environment (runner identity, dataset, prefixes) |
| Use case | One ML product: configuration plus pipeline code in one folder |
| Environment manifest | `environment.json`: the names of everything a tenant may use, written by the platform |
| Blueprint, profile | A deployment profile named in the manifest: `poc`, `private-managed`, `private-gke` |
| Release, contract, pointer | A release is a verified, digest-pinned build; its contract is the write-once record; the pointer says which release is live |
| Launcher | The shared service (Cloud Run, or a pull worker in a private GKE cluster) that turns files, schedules and run requests into pipeline runs |
| Runner | A tenant's service account that every pipeline step runs as |
| Champion | The registry's default model version, the current best |
| `ml_config` | The resolved configuration every pipeline receives |
| Lifecycle event | A Pub/Sub message and log line for a key moment (released, registered, drifted, deployed, decided) |
| Retrain decision case | What drift or a performance drop opens instead of retraining: a decision people make and Pipewright records |
| Decision pack | What a reviewer needs to decide on a retrain: is the drift real, is it a data problem, what retraining would use |
| Approver group | A platform-registered group whose members may decide retrains; membership is publish access to its topic |
| Decision feed | An opt-in, per-tenant Pub/Sub subscription carrying that tenant's decision events, for the team's own apps |
| CMEK | Customer-managed encryption keys (Cloud KMS) |
| PSA | Private Services Access: the peering that lets managed pipeline workers reach the environment's network |
| VPC-SC | VPC Service Controls: a data perimeter around a project |
| Agent Platform | Google Cloud's Gemini Enterprise Agent Platform, formerly Vertex AI |
| OSS, Enterprise | Pipewright (Apache-2.0, this repository) and Pipewright Enterprise (Fluxor Software) |
