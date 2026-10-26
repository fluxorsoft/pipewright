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
| OpenTelemetry export | Planned |
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
| FR-48 | **The closed retrain loop across environments:** a case opened in production trains in test on production-eligible data, is compared with the model production serves, is validated in test, promoted, approved and verified in production, and the case closes with the final production model; one case ID end to end | Phase 1 built and live-tested 2026-10-24 (1.1.0) | section 8.13 |
| FR-49 | **Synthetic data generation:** a `synthetic` section in the use case configuration; contract-only generation from the data contract (no real data); learned generation through an engine interface (MOSTLY AI Synthetic Data SDK in local mode by default; SDV as an opt-in adapter that loads only if the team installed SDV, whose Business Source Licence the team accepts directly; never a dependency); evaluation of fidelity, utility (train on synthetic, test on real) and privacy, with gates Pipewright recomputes. Approval, release across environments and evidence are Pipewright Enterprise | Met (released in 1.0.0, 2026-10-18): the section (off unless enabled), the synthetic pipeline on two images, `policies.synthetic`, `synthetic generate` and `synthetic gates`, relational and sequential mode; used end to end on Agent Platform and on Kubeflow in private GKE | `synthetic/`, section 13 |
| FR-51 | **Online feature lookup, a toggle:** online serving fetches a request's features by entity ID from a Feature Store Bigtable online store instead of the caller sending the feature vector. Off unless the use case switches it on (`model.serving.features: online`) and the platform allows it per environment (`policies.online_features`, with the tenant's online store and node limits), because a Bigtable online store has a standing cost. The feature views come from the same `data.features_from` block training and batch scoring read, so online, batch and training share one feature definition (no training-serving skew); the serving handler fetches by ID, then calls the model, on Agent Platform endpoints and KServe; `doctor` checks the views exist and are fresh; events record which view served. The online store and its views belong to the platform or the features' owners (D-79). Enterprise: Terraform for the online store (CMEK, in the perimeter), cost limits in the platform file, an evidence control for online features | Planned (user, 2026-10-22) | section 8.11 |
| FR-52 | **Regression and multi-class models:** evaluation metrics (for example rmse, mae, r2; macro and weighted f1, per-class recall), gates with the right direction, champion comparison, slice gates and prediction drift for continuous and multi-class outputs, for container and BigQuery ML use cases alike. Today `config/ml.py` knows binary classification metrics only (auc, pr_auc, accuracy, f1, precision, recall, log_loss, brier, calibration_error) | Planned (user, 2026-10-22) | `config/ml.py`, `models/gates.py` |
| FR-53 | **Fairness metrics and gates:** bias measures per protected or sensitive attribute (for example demographic parity difference, equal opportunity and equalised odds differences), computed on the test set next to slice metrics, gated like other metrics and recorded in the evaluation record and the model card. The attributes are declared in the data contract; they may be measured without being features | Planned (user, 2026-10-22) | `config/ml.py`, `models/gates.py` |
| FR-54 | **Approved image allow-list:** the platform lists, per environment, the base images and package indexes a use case's images may be built from (component, Ray and synthetic images); `validate` and `release` refuse anything else, and the release contract records the base images by digest. Declared in the manifest like other platform policy (D-81); Pipewright Enterprise sets it in the platform file. Raised with the forecasting route of the BigQuery ML design (approved libraries such as statsmodels and Prophet) | Planned (user, 2026-10-22) | manifest `policies`, `build/images.py` |
| FR-55 | **Lineage per run:** a model version leads to its run, the release it ran (commit, images, templates, `ml_config` with its SQL digests), the exact rows it trained on (the run's snapshot or BigQuery ML rows), the sources those came from (entity table, feature tables or groups and their read time, the label table and window) and its evaluation and approvals: one `lineage.json` per run with the evaluation record, and a lineage section in Fleet View. Upstream lineage of the sources (Data Lineage API) later | Planned: 1.1.0 (user, 2026-10-24) | runtime, bqml steps, Enterprise Fleet View |
| FR-56 | **Approval gates in CI:** every human decision (model approval, deployment, promotion, retrain decisions, the closed loop's steps, synthetic data releases) can be a GitHub Actions job that waits for its environment's required reviewers, then runs the `pipewright` command; a scheduled workflow lists what needs a decision and opens one gated job per item. Pipewright records the GitHub reviewer, the approval time and comment and the workflow run next to the CI identity | Planned: 1.1.0 (user, 2026-10-24) | `pipewright ci init`, events, evidence, Enterprise Fleet View |
| FR-57 | **A/B testing of models (section 8.16):** a controlled experiment between the champion and a candidate, with sticky assignment by a unit key, a business metric and guardrails fixed in advance, a minimum sample and duration, analysis per arm, and a person deciding the winner through a decision case; recorded in the winning version's lineage. Batch first, then online | Planned: phase 5 (design 2026-10-26) |

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

### 8.16 A/B testing of models (FR-57, design note 2026-10-26; roadmap phase 5)

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

**Phases** (2026-10-22): phase 1 strengthens the core ML, phase 2 adds real time and trust, phase 3 platform scale and intelligence; phases 1 to 3 ship together as 1.1.0 (user, 2026-10-24), except AIOps (still to analyse). Phase 4 comes after 1.1.0, phase 5 after phase 4. Items marked "when a customer needs it" or "on demand" stay listed and are not phased: they are built when a customer asks.

| Item | Status |
|---|---|
| First open-source release: `1.0.0` stable (D-86), rehearsed with a throwaway pre-release tag; wheel and launcher image; STABILITY.md (the stable surface and the deprecation policy); every 0.4 reference moved to 1.0 | Released 2026-10-17; re-cut 2026-10-18 with synthetic data |
| Manifest forward compatibility (R-05, D-83): unknown keys ignored with a warning, `requires` for policies | Built 2026-10-10 |
| Per-tenant pipeline roots (R-04): `pipeline-root/<tenant>/<usecase>` | Built 2026-10-10 |
| OpenTelemetry export from the launcher | Planned: phase 4, after 1.1.0 (user, 2026-10-24) |
| A/B testing of models (FR-57, section 8.16): sticky assignment, business metric and guardrails, analysis per arm, a person decides | Planned: phase 5, after phase 4 (user, 2026-10-26) |
| Promotion of a release (and its approved model) between environments by digest (FR-29, section 8.12) | Built 2026-10-11 |
| The closed retrain loop across environments (FR-48, section 8.13): phase 1 to 3 deliveries | Phase 1 (1.1.0): built on branch `phase-1` 2026-10-24 (`retraining.loop`, `pipewright loop`, run requests naming versions, one loop per use case; Enterprise read grant). Live-tested 2026-10-24 between ent (Kubeflow Pipelines on GKE, perimeter enforced) and ent-prod (Agent Platform Pipelines): a drift case on churn-versions in ent-prod, approved over the run requests topic, trained in ent on production's view against the test version production served (v5 for prod v2), candidate v6 validated by a scoring run in ent, the production approval promoted it (v3, the default) and a scoring run verified it: closed; `loop rollback` made v2 the default again. The live test found promotion skipping the model when the release was live already (fixed: the loop promotes its candidate alone) |
| `kfp` adapter for Kubeflow Pipelines on GKE, with KubeRay, KServe, CronJobs, the pull launcher and the read-only UI (FR-30, sections 8.14 and 8.15) | Built 2026-10-17 |
| `predict` in the generated operate workflow | Built 2026-10-17 |
| Plain Kubernetes on premises (section 8.14) | On demand only (D-89) |
| Synthetic data generation (FR-49): the `synthetic` section, contract-only generation, an engine interface (MOSTLY AI SDK local mode by default, SDV opt-in), evaluation and gates | Built, released in 1.0.0 (2026-10-18); used end to end on Agent Platform and on Kubeflow |
| Synthetic data: relational generation (FR-49 follow-up): the entity table and its child tables together (keys, history per entity, timing against each cut-off), so point-in-time joins and feature tables are tested on synthetic data; the spike showed MOSTLY AI does it (TSTR drop 0.003 on churn) | Built, released in 1.0.0 (2026-10-18); used end to end on Kubeflow (`mode: relational`) |
| Synthetic data: sequential generation (FR-49 follow-up): event histories per entity (transactions, sessions, claims) where order and gaps matter | Built, released in 1.0.0 (2026-10-18); used end to end on Kubeflow (`mode: relational`) |
| Synthetic data: SmartNoise engine (FR-49 follow-up): OpenDP's differentially private generators (MST, AIM; MIT) as a second engine behind the interface, for a formal privacy guarantee where measured risk is not enough | Roadmap: phase 4, after 1.1.0 (user to revisit; 2026-10-24) |
| Synthetic data: AI-assisted authoring (FR-49 follow-up): a drafted `synthetic` section and column rules from a table's schema and profile (Gemini), for a person to review | Built in Pipewright Enterprise (`pipewright synthetic-draft`, 2026-10-18) |
| Value-free error messages (NFR-18, D-65) | Phase 1: built on branch `phase-1` 2026-10-24 |
| Ray on Agent Platform, a cluster per Ray step of a run (FR-45, D-77, section 8.9) | Built 2026-10-10 |
| Standing Ray clusters, a long-lived cluster per tenant (section 8.15): on private GKE, kept by the platform as a KubeRay cluster whose workers scale to zero; a cluster per run stays the default. Not on Agent Platform | Built 2026-10-17 |
| Features from BigQuery feature tables, point in time, for training and batch scoring (FR-46, D-79, D-80, section 8.11) | Built 2026-10-10 |
| Feature Store feature groups as a feature source (D-82) | Built 2026-10-10 |
| Online feature lookup (FR-51): endpoints read features by entity ID from a Bigtable online store, a toggle per use case and environment | Built on branch `phase-1` 2026-10-24 (1.1.0): `model.serving.features: online`, `online_view`, the manifest's `online_store`, the example's handler, `doctor`, `deploy-model` as the runner; Enterprise `tenants.<t>.features.online` (store, views, grants). live-tested in ent 2026-10-24: a prediction by `{customer_id}` on an Agent Platform endpoint read both views (bare IDs and unknown IDs fixed after it). A tenant reads only its own store: the read role is granted on the store's own IAM policy (Enterprise `scripts/online_store_iam.py`, from the blueprint output), because the provider has no IAM resource for online stores and an IAM condition on the store name is not applied to Feature Store permissions (both tested live) |
| BigQuery ML as a training engine (`spec.bqml`, D-112 to D-119; the design and spike are kept with Pipewright Enterprise): built-in training and scoring pipelines for binary, multi-class and regression; later tuning, performance monitoring and retrain proposals from scoring, imported models, promotion by files, k-means and PCA, forecasting exceptions | Phase 1: built on branch `phase-1` 2026-10-24; live-tested end to end on Kubeflow Pipelines on GKE (perimeter enforced) and on Agent Platform Pipelines: train, gate, register, champion comparison, `model approve`, promotion by retrain, scoring, drift. Online serving (phase 2, 2026-10-24): `deploy-model` deploys a BigQuery ML version from its registry entry and checks the endpoint against ML.PREDICT on the run's test rows; live in ent (perimeter enforced): 25 rows, largest difference 6.6e-9, then undeployed |
| Per-run data versioning and a label maturity window (D-110, D-111; `data.snapshot_table`, `pipewright run --replay`, `data.label_source`) | Phase 1: built on branch `phase-1` 2026-10-23; live-tested in ent 2026-10-24 (churn-versions in pipewright-ent-e2e): a 30-day window from renewal events, anchored on the newest one, dropped 409 maturing rows; the snapshot kept 4,591 rows; `pipewright run --replay` read exactly those rows and trained the same model (auc 0.801565 both times). The live test found the launcher refusing `snapshot://` (fixed) |
| Regression and multi-class models (FR-52): metrics, gates, champion comparison and drift beyond binary classification | Phase 1: built on branch `phase-1` 2026-10-23 (`data.label.task`, per-task metrics and directions, example metric helpers, prediction drift on class shares); the BigQuery ML side comes with the BQML build (its SQL in `.sql` files under `examples/`) |
| Fairness metrics and gates (FR-53) | Built on branch `phase-1` 2026-10-24 (1.1.0): `evaluation.fairness`, runtime `fairness_measures`, `models/gates.py`, the example's evaluate step and model card, BigQuery ML `fairness.sql`; binary classification only |
| Approved image allow-list (FR-54): base images and package indexes per environment, refused at validate and release | Built on branch `phase-1` 2026-10-24 (1.1.0): manifest `policies.images`, `build/allowlist.py`, the contract's `images`; Enterprise sets it in the platform file |
| Lineage per run (FR-55): `lineage.json` per run and a lineage view in Fleet View | OSS part built on branch `phase-1` 2026-10-24 (`write_lineage`, the example and BigQuery ML register steps, `pipewright lineage`, which follows promotions); live-tested in ent 2026-10-24 (churn-versions v7: run, release with digests, the 4,591-row snapshot and its read time, feature groups, label window, evaluation); Fleet View shows it (Enterprise) |
| Approval gates in CI (FR-56, D-120 to D-123): gated jobs per decision, the scheduled decision poll, both identities recorded | Built on branch `phase-1` 2026-10-24: the approval record on decision and loop events, `pipewright decisions pending`, the `pipewright-decisions` and `pipewright-gate` workflows from `pipewright ci init`, `PIPEWRIGHT_ID_TOKENS`. Live-tested 2026-10-24 in pipewright-ent-e2e (branch `phase-1-gates`; the two workflow files also on `main`, as GitHub dispatches only workflows on the default branch): the decisions run listed ent's pending approvals and dispatched one gate per item; with no required reviewers (not available for private repositories on GitHub's free plan) every gate refused (`nobody approved this run`) and ran nothing. Approved from the CLI instead, the event records `approval: {via: outside-ci}`. Found: older candidates were offered too (fixed: the newest passing version only). Reviewer approval in GitHub itself is untested until the repository has reviewers |

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
