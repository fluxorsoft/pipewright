# Pipewright: context

> **Read this first.** This file is the entry point to Pipewright for people and for AI assistants working on it or
> with it. It states what Pipewright is for (requirements), why it is built the way it is (decisions), how it works
> (design), the interfaces it promises, and how to work on it (conventions). It summarises; the code is the final
> authority.
>
> **Last updated:** 2026-10-08. **Version:** 0.4.0.dev0. **Release contract:** 3. **Configuration and manifest
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
version), and support. Enterprise uses published extension points and never patches this package (D-34).

---

## 2. Status

"Built" means working code with tests. "Used end to end" means exercised on a real Google Cloud project, including a
private environment with customer-managed keys and enforced organization policies.

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
| Promotion between environments, `kfp` adapter (GKE), OpenTelemetry export | Planned |
| Terraform for the platform side | In a separate starter repository, shared on request (R-20) |

---

## 3. Repository layout

```
CONTEXT.md                  this file
README.md                   what Pipewright is, the gaps it closes, getting started
CONTRIBUTING.md, SECURITY.md, CHANGELOG.md, LICENSE, NOTICE
pyproject.toml, uv.lock     the package (hatchling), dev environment (uv)
src/pipewright/             the Python package, one subpackage per responsibility (section 8.1)
  config/schemas/           JSON Schemas: environment, use case, environment manifest (the public contracts)
  build/templates/          components.Dockerfile, pipewright_runtime.py (injected into every component image)
  scaffold/ci_templates/    GitHub Actions workflows written by `pipewright ci init`
examples/churn/             the reference use case copied by `pipewright new-usecase`
images/launcher.Dockerfile  the launcher image, built from the wheel
tests/                      offline unit tests (fakes for Google Cloud)
scripts/                    check_package.py (wheel contents), smoke_test.sh (installed CLI, no cloud)
docs/ONBOARDING.md          how a platform team prepares a tenant and an ML team starts a use case
.github/workflows/          ci.yml (tests, wheel, smoke test, launcher image), release.yml (tagged releases)
```

---

## 4. Requirements

Status: **Met** (built and verified), **Partial**, **Planned**, **Out** (deliberately not done).

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
| FR-29 | Promote a release between environments by digest | Planned | |
| FR-30 | Self-managed pipelines on GKE (Kubeflow Pipelines, KServe) | Planned | `kfp` adapter |
| FR-39 | **Retrain decision case with a decision pack:** drift or a performance drop opens a case holding what is needed to decide (is it real, is it a data problem, what would retraining use, the options) | Met | `retraining/` |
| FR-40 | **Retraining modes per use case:** `off` (alerts only), `propose` (nothing trains until a person approves), `candidate` (trains a candidate, never promotes); the platform can limit the modes per environment | Met | `retraining/`, `config/ml.py` |
| FR-41 | **Approver groups with a quorum:** several groups may be required, the platform can add required groups per environment, the proposer cannot approve, one identity counts once, approvals are authenticated actions by named identities | Met | `retraining/` |
| FR-42 | **Decision events:** always on the lifecycle events topic; an opt-in per-tenant decision feed; email to configured addresses; a weekly digest | Met | `retraining/`, `alerts/sync.py` |

### 4.2 Non-functional requirements

| ID | Requirement | Status | How |
|---|---|---|---|
| NFR-01 | **Reproducible releases:** images, templates and contracts addressed by digest; a published contract never changes | Met | D-41, D-42 |
| NFR-02 | **Idempotency:** retries and redeliveries never duplicate runs, releases or decisions | Met | deterministic run IDs, run keys, create-only writes, generation checks |
| NFR-03 | **Isolation between tenants:** own runner identity, dataset, landing and outputs prefixes | Met (artifacts bucket shared, R-04) | manifest tenants, IAM on the platform side |
| NFR-04 | **Customer-managed keys** applied to pipeline jobs, models, endpoints and BigQuery query results when the manifest names them | Met | D-37, D-44 |
| NFR-07 | **Data residency:** Pub/Sub publishes through the environment's regional endpoint | Met | D-40 |
| NFR-09 | **Auditability:** every change and use of a model is traceable to identity, data, release and approval | Met | events carry `by`; contracts carry provenance |
| NFR-11 | **Fail early, in plain language:** schema, pre-flight and verify gates before anything goes live | Met | `validate`, `doctor`, release verify |
| NFR-12 | **Vendor-neutral observability:** structured logs and events; tools connect through platform configuration | Met | D-30 |
| NFR-13 | **Use-case code does not change between deployment profiles** | Met for managed pipelines | adapters, manifest |
| NFR-14 | **Works behind TLS-inspecting proxies** | Met | D-49 |
| NFR-18 | **Value-free error messages:** errors name the column and rule, never the offending value | Planned | D-65 |

### 4.3 Constraints

| ID | Constraint |
|---|---|
| C-01 | Google Cloud only. No AWS or Azure work, and no multi-cloud abstractions |
| C-02 | Pipelines are Kubeflow Pipelines v2; `kfp` pinned to the compiler's major.minor (2.17) |
| C-03 | Python 3.11; uv for environments; ruff, line length 120 |
| C-04 | PyTorch is the supported framework for the example and serving |
| C-06 | Agent Platform naming: Gemini Enterprise Agent Platform was formerly Vertex AI (renamed April 2026). APIs, SDK, IAM roles and the orchestrator value `vertex` are unchanged |
| C-07 | Ingestion frameworks (dlt, Dataflow, Airflow) are out of scope; Pipewright starts at a `source_uri` |
| C-08 | GenAI and LLM fine-tuning are out of scope for v1 |

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
| D-15 | Models can be promoted between environments (`model` default, `retrain` optional) | Governed path to production | Active (not built) |
| D-16 | PyTorch first | Focus | Active |
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
| D-65 | **Value-free errors** (column and rule, never the value) | Error text is the most common accidental data leak | Active (not built) |
| D-68 | **Drift opens a human decision; no automatic retraining.** Modes `off`, `propose` (the recommended minimum for production), `candidate` (never promotes) | Retraining on the same data does not improve a model; drift has causes needing opposite responses (data bug, seasonality, real change, harmless shift) | Active |
| D-69 | **Email notifies; an authenticated identity approves.** Approval is `pipewright retrain approve`; approvers are verified when they act | Anyone can forward an email; regulated approvals need a named, verified approver | Active |
| D-70 | **Decision events always go on the lifecycle events topic;** delivery to a team's own app is an opt-in, per-tenant filtered subscription (`retraining.decisions.publish`), checked by `doctor` | Audit needs every decision; topics stay platform-owned while teams choose delivery | Active |
| D-71 | **Cases re-open on time, on worsening and on escalation, within a policy cap:** snooze and decline expiry; immediate re-open when drift worsens (worst PSI up by `reopen_when_worse_by`, a new feature drifting, performance below its minimum); escalation when undecided; declines and snoozes need a reason from a short list plus free text | Time alone hides serious worsening and lets cases go stale | Active |

---

## 8. Designs in detail

### 8.1 Package layout (`src/pipewright/`)

| Package | Responsibility |
|---|---|
| `config` | Load and merge files, JSON Schema validation, `${...}` references, ML defaults and cross-checks, resolution into `ResolvedUseCase`; `environment_from_manifest` for services with no repository |
| `build` | Compile pipelines, patch `steps` into the compiled template, build the component image (injects `pipewright_runtime.py`) |
| `checks` | `doctor`, the `kfp` pin check |
| `release` | Contract v3, pointers, template upload, verify gate, release and rollback |
| `launcher` | Flask app, pure routing, live contract registry |
| `adapters` | Orchestrator submit (`vertex`; `kfp` later), applying the manifest `security` block |
| `retraining` | Retrain decisions: the case state machine (pure), the decision pack summary, the case store, ID token checks, the launcher's handling, the `retrain` CLI |
| `tables`, `schedules`, `alerts` | Plans and syncs for BigQuery tables, Cloud Scheduler jobs, log-based alert policies |
| `models`, `monitoring` | Registry list and approve, deploy with canary; `monitor status` |
| `observability` | Lifecycle events (regional publish) |
| `data` | Erasure in BigQuery and Parquet |
| `runs` | `pipewright run` (Pub/Sub or HTTP) |
| `scaffold`, `testing`, `gcp`, `cli`, `plugins` | New use case and CI workflows; `pipewright test`; REST session, GCS, BigQuery; argument wiring; plugin loading |

### 8.2 Configuration model

**Files:** `environments/<env>.yaml` points at the manifest. `usecases/<name>/usecase.yaml` holds identity, tenant,
pipelines, model and approvals, plus `include: [config/*.yaml]`. The `config/*.yaml` files hold one section each:
triggers, runtime, tables, data, training, evaluation, operations. Overrides go in
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
| `POST /retraining/tick` | Snooze and decline expiry, escalation; with `{"digest": true}` the weekly digest |
| `GET /health` | Live use cases and their releases |

- **Idempotent run IDs:** a landing run ID hashes the use case, bucket, object and generation. Schedules use their
  name plus the minute, or a `run_key`. API runs use the source plus the minute, or a `run_key`.
- **Registry:** caches immutable contracts and refreshes pointers and the manifest every 60 seconds. A broken use
  case never stops the others; new tenants appear without a redeploy.
- **Submit:** a `PipelineJob` as the tenant runner, with the key, network and reserved ranges from the manifest.

### 8.5 Component runtime (`pipewright_runtime.py`, injected into every component image)

- `read_source`: CSV, Parquet or `bq://` sources.
- `write_table`: store-aware, conforms rows to the schema, uses load jobs.
- `bigquery_client`: queries with the environment's key.
- `log`: labelled JSON lines.
- `emit_event`: a log line plus a regional Pub/Sub publish. It never raises.

### 8.6 Events

- `run.submitted`, `run.duplicate`: launcher.
- `release.live`, `release.rolled_back`, `model.approved`, `model.deployed`, `data.erased`: CLI, each with `by`.
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

### 8.9 Plugins

Entry point group `pipewright.commands`; each entry point is a callable `register(subparsers, common)` that adds
subcommands. Plugin API v1. A plugin that fails to load is skipped with a warning; `pipewright plugins` lists what
loaded.

---

## 9. Interfaces and contracts

### 9.1 CLI (`pipewright`)

| Stage | Commands |
|---|---|
| Configure | `new-usecase`, `validate`, `resolve`, `doctor`, `test` |
| Release | `tables sync`, `release`, `rollback`, `status`, `schedules sync`, `alerts sync`, `ci init` |
| Operate | `run`, `model list`, `model approve`, `deploy-model`, `monitor status`, `data erase` |
| Decide | `retrain list`, `retrain show`, `retrain approve`, `retrain decline`, `retrain snooze` (with `--env` or `--manifest gs://...`) |
| Extend | `plugins` |

**Common flags:** `--env`, `--usecase` (repeatable where it makes sense), `--root`.

**Environment variables:** `GCLOUD_CONFIG` (refreshable gcloud credentials), `GOOGLE_OAUTH_ACCESS_TOKEN` (a fixed
token), `PIPEWRIGHT_ID_TOKEN` (an ID token for the launcher or a decision), `PIPEWRIGHT_RUN_VIA=http` (call the
launcher directly from inside the network), `PIPEWRIGHT_BUILD_CA_BUNDLE`, and the CA variables in D-49.

### 9.2 Environment manifest (`pipewright/v1`, `manifest.schema.json`)

| Field | Contents |
|---|---|
| Required | `apiVersion`, `environment`, `blueprint`, `orchestrator`, `project`, `region`, `artifact_registry{docker, kfp, python?}`, `artifacts_bucket`, `landing_bucket`, `launcher{service_account, url?, service?, subscription?, run_requests_topic?}`, `tenants{name: {runner_service_account, bigquery_dataset, landing_prefix?, online_serving?, namespace?, outputs_uri?, decision_feed?}}` |
| `policies` | `deploy_model`, `promote_model` (`manual` or `auto`); `retraining{allowed_modes, required_approvers, max_snooze_days}` |
| `approver_groups` | `{name: {topic, subscription, description?}}` |
| `observability` | `events_topic`, `evidence_dataset`, `evidence_uri`, `log_sinks`, `otlp_endpoint` |
| `security` | `kms_key`, `bigquery_kms_key`, `network`, `reserved_ip_ranges`, `perimeter` |

**Compatibility rule:** additive, optional fields only. The schema forbids unknown properties, so the launcher and
CLI must be upgraded **before** a platform publishes new fields (R-05).

### 9.3 Use case and environment files

`usecase.schema.json` and `environment.schema.json` in `src/pipewright/config/schemas/`. Every key carries a
description; the churn example uses most of them.

### 9.4 Plugin API v1

Entry point group `pipewright.commands`, signature `register(subparsers, common)`. Helpers a plugin may rely on:
`load_environment`, `environment_from_manifest`, `resolve_usecase`, `Gcp`, `gcs_read`, `gcs_write`,
`observability.events`, `release.service.read_pointer`, `read_contract`.

---

## 10. Testing

| Layer | Command | What it covers |
|---|---|---|
| Unit tests | `uv run pytest` | Configuration, releases, launcher routing and routes, retrain decisions, alerts, tables, erasure, plugins; offline, with fakes |
| Lint | `uv run ruff check .` | |
| Example | `uv run --no-project --with-requirements examples/churn/tests/requirements.txt python -m pytest examples/churn/tests` | The example's ML helpers |
| Package | `scripts/check_package.py dist/`, `scripts/smoke_test.sh <pipewright>` | Wheel contents; the installed CLI scaffolds, validates and resolves a use case and writes CI workflows, with no cloud access |
| Product CI | `.github/workflows/ci.yml` | All of the above, plus the launcher image built from the wheel |

Changes that affect behaviour on Google Cloud should also be exercised against a real environment before release.

---

## 11. Known gaps, risks and open questions

| ID | Item | Notes or proposal |
|---|---|---|
| R-04 | The artifacts bucket is shared by tenants | Per-tenant pipeline roots (`pipeline-root/<tenant>/<usecase>`) planned |
| R-05 | Older CLIs reject manifests with new fields | Upgrade the launcher and CLI before publishing new fields, or relax the schema for unknown optional keys |
| R-06 | Cloud Scheduler IAM is project-wide | Jobs should only call the launcher as its identity; `doctor` checks |
| R-07 | Tenant offboarding is undefined | Proposal: block removal until runtime objects are gone |
| R-08 | Erasure in GCS artifacts and trained models | Not covered; retrain to remove influence; review with a data protection specialist |
| R-09 | Manual approval of a first model version is not recorded as `model.approved` | The first version becomes the champion automatically |
| R-18 | Approver identity relies on Google Cloud IAM and ID tokens | See section 8.7 for the residual risks |
| R-19 | Labels arrive late for many use cases | The decision pack shows how many labelled outcomes exist, so drift is not read as degradation |
| R-20 | No Terraform for the platform side in this repository | By design. A separate starter repository (public endpoints, no CMEK or VPC) is shared on request: people ask the maintainers (hello@fluxorsoft.com or GitHub Discussions), which also builds the community. Production blueprints with private networking come with Pipewright Enterprise |
| R-21 | The PyPI name `pipewright` belongs to an unrelated project | Resolved: the distribution is `pipewright-mlops`; the import and the command stay `pipewright` |

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

---

## 13. Roadmap

| Item | Status |
|---|---|
| First open-source release (0.4.0), wheel and launcher image | Next |
| Per-tenant pipeline roots (R-04); manifest forward compatibility (R-05); OpenTelemetry export from the launcher | Planned |
| Promotion of a release between environments by digest (FR-29) | Planned |
| `kfp` adapter for Kubeflow Pipelines on GKE (FR-30) | Planned |
| Value-free error messages (NFR-18, D-65) | Planned |
| Iceberg (BigLake) tables for row-level deletes | Design only |
| Feature store integration | Under review |

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
| Launcher | The shared Cloud Run service that turns files, schedules and run requests into pipeline runs |
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
