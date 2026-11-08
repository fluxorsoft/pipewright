# Changelog

All notable changes to Pipewright. The format follows [Keep a Changelog](https://keepachangelog.com/), and versions
follow [semantic versioning](https://semver.org/); [STABILITY.md](STABILITY.md) says what it covers.

## [Unreleased]

Phase 9: choosing the online feature store (FR-104); endpoints in Terraform and private endpoints (FR-92); model
co-hosting on shared pools (FR-105); the lookup gateway (FR-106). Everything is off until it is set: a use case that
sets none of it deploys and serves exactly as in 1.4.1. The guides are [docs/ONLINE_STORES.md](docs/ONLINE_STORES.md)
and [docs/SERVING.md](docs/SERVING.md).

### Added

- **Cloud SQL for PostgreSQL** as an online store: one instance per environment, a schema per team, private IP, SSL
  verified, IAM database login with no password user. Pipewright's sync (`pipeline: online_sync` schedules, run by the
  launcher as the tenant's runner) writes the newest values per entity through a staging table checked against a
  manifest and swapped in one transaction; `sync.mode: changed` writes only changed rows.
- **Pipewright's lookup service** (`python -m pipewright.online.server`, in the launcher image) between Agent
  Platform endpoints and Cloud SQL, created by the launcher at `deploy-model`; or `lookup.provider: own`.
- **BigQuery with BI Engine** as an online store, refused without a reservation; `online.billed` when lookups are
  billed as full table reads.
- **`http`**: a team's own store answering `POST /lookup`, its answers checked against the model.
- **The Parquet export** (`export.enabled`, off by default) with a manifest, for a store a team builds;
  `pipewright features export`.
- Sizes in YAML checked against what Cloud Run and Cloud SQL support (`validate`, Terraform); `doctor` shows each
  store's standing cost; `online.stale` and `online.fresh` from the launcher's tick; `pipewright features sync` and
  `pipewright features online-setup`.
- The churn example's serving handler reads any store through `online_lookup.py`, one lookup per request.
- **The lookup gateway** (`lookup.mode: gateway`, FR-106): `POST /predict` on the use case's lookup service. Callers
  send entity IDs inside the network; the gateway reads the features and calls the endpoint as its own identity,
  because an Agent Platform endpoint cannot reach into the network (found in a live spike). Logs each caller's
  identity, retries a busy endpoint (`lookup.retries`, default 3), keeps the canary split on the endpoint.
  `lookup.callers` names who may call it.
- **The Pipewright custom solution**: the gateway also in front of a team's own lookup service (`type: http` with
  `lookup.mode: gateway`), for stores a team runs inside the network.
- **Endpoints in Terraform, public or private** (FR-92): the starter's `serving.yaml` and Enterprise's `spec.serving`
  create a use case's endpoint; a private one uses Private Service Connect with Google's automation, a service
  connection policy and a private DNS zone. The manifest's `serving.endpoints` tells Pipewright: `deploy-model`
  deploys onto it and never creates one for a use case listed there. `model predict`, "How to call it" and the
  BigQuery ML parity check know a private endpoint answers only inside the network.
- **Model co-hosting on shared pools** (FR-105), only when a use case sets `model.serving.pool`: pools declared by
  the platform (`serving.pools` in the manifest), created and resized in place by `pipewright serving pools plan |
  apply | list` with the environment's CMEK and the tenant's identity (`managed_by: ci`, the default) or by the
  starter's Terraform (`managed_by: terraform`). Checks: Google's prebuilt TensorFlow or PyTorch (Preview) image
  only, the pool's image, no container variables (settings in `serving.json`), one endpoint type per pool, the
  pool's tenant; `policies.serving.pools` (`min_replicas` 2 and no Preview in production by default,
  `allow_preview`, `settle_seconds`). Changes to a pool run one at a time under a lock and are announced
  (`serving.pool.change`). `doctor` shows the endpoint, the pool and the gateway.
- **GKE guideline** ([examples/gke-lookup/](examples/gke-lookup/)): Pipewright's lookup service on GKE pods for KServe.
- **A segment size measure for k-means** (FR-108): `min_segment_share`, the smallest segment's share of the run's
  rows, and each segment's size in the evaluation record. Measured by default for clustering; it gates only when a
  use case sets a gate on it. Found re-running the book: Davies-Bouldin passed a model with segments of 5 and 3
  customers.
- **No 404s after a promote** (FR-109): a version that no longer takes traffic keeps serving for
  `model.serving.drain_seconds` (default 240) before it is undeployed, on a replace, a promote or an abandoned canary.
  After the traffic moves, the endpoint goes on routing requests to the old version for a few minutes; undeploying it
  at once answered every call with 404 for about three minutes (found re-running the book's canary). `0` undeploys
  at once, as before.
- **Sync a Bigtable online store now** (FR-110): `pipewright features sync` starts a sync of each feature view of
  the tenant's Bigtable store and waits for it (it refused this store before). A new view had no sync until its
  06:00 schedule, so it served nothing, and `doctor` said "start a sync" without saying how; it now names the command.
- **Which cut-offs a performance value measures** (FR-111): with `labels.time_column`, the monitoring row records
  `performance_cutoffs` (outcomes per cut-off) and a note. A run scoring a month whose outcomes are not known yet
  reports the version's performance on the earlier months it scored, which read as that month's. In the churn example's
  monitor step (copy it into a use case made from an earlier example) and in BigQuery ML monitoring.
- **Undeploys are recorded** (FR-112): `model.undeployed` is emitted for each version that leaves an endpoint, on
  `deploy-model --undeploy`, a promote, a replace or an abandoned canary, as a decision (who, and how it was
  approved). Lineage, the fleet report and the evidence read it, but nothing emitted it, so a model's story ended at
  "deployed".
- **A CI release names the person behind it** (FR-113): `release.live` records how it was approved (the GitHub
  actor and run), like every other decision. The release pointer had it (`pipewright status`), but the event that
  lineage and the fleet report read named only the CI account.
- **A model trained online is not served to callers who send features** (FR-114): with
  `model.serving.features: request`, `deploy-model` refuses a version that carries an online lookup. The serving
  handler would read each caller's feature values as entity IDs and answer an error for every one; 1.2.0 checked only
  the opposite case.

### Fixed (found re-running the book on 1.4.2)

- A replay that reproduces the champion exactly is not registered (the churn example's register step): since rows
  are sorted, a replay ties its own model, and the champion rule passed the tie.
- BigQuery ML: with `evaluation.champion` off, the champion is no longer measured (a champion whose model was gone
  still failed every run, though the error advised turning it off).
- `model list` no longer shows a former default as `champion`; `run` names the pipeline when `--pipeline` is left
  out.

## [1.4.1] - 2026-11-07

Phase 7: serving what was approved, and knowing when it changes (FR-101, FR-102). A live test showed that KServe and
scoring runs load the model folder at every start, so a file swapped after approval reached them without a deploy;
1.4.1 makes them load the approved bytes, tells people when an approved file changes, and keeps approved files
protected while they are in use. A patch version by the maintainers' choice while adopters are few: it adds keys,
commands and events, all additive. Pinning is on by default under `refuse` (the 1.4.0 default); everything else is
off until set. The guide is [docs/INTEGRITY.md](docs/INTEGRITY.md), with the cost of each setting.

### Upgrading

Nothing to change in a use case. Rebuild a release with 1.4.1 for scoring runs to be pinned (its inference pipeline
then declares `approved_files`); until then the launcher says the run loads the model folder. KServe services are
pinned at their next `deploy-model`. Turn on the watch, lock renewal, DATA_WRITE logs and audit retention in the
manifest and Terraform when you want them; `doctor` lists what is off.

### Added

- **Feature contracts** (FR-103): `pipewright features contract` writes `feature_contract` (per `data.features_from`
  source: kind, SQL and schema fingerprints, features, a semantic version set by what changed); `--check` for CI.
  It travels in the release contract. With `policies.integrity.features.check: on` a changed definition refuses
  training, approval, promotion, deploy, scoring and rollback (or warns under mode warn), and the watch reports it
  and a feature view whose newest sync failed. `built_tables` checks tables built by a query by their last job's SQL;
  `record_reads`, `fingerprint_rows` and `snapshot_days` record what a scoring run read. All off by default.

- **Scoring loads the approved bytes** (`policies.integrity.pin`, on by default): with `inference: warn` or `refuse`
  the launcher runs the full check of the version a scoring run uses and passes its approved files
  (`approved_files`); the reference example's `load_model` downloads exactly those generations and checks each
  SHA-256 (`pipewright_runtime.approved_download`).
- **KServe serves an approved copy, checked at every pod start**: `deploy-model` copies the approved generations next
  to the model folder, locks them, and every predictor pod checks the copy's generations and SHA-256 in an init
  container before the server starts (`kserve.verifier_image` for an image without Python).
- **Agent Platform: approval covers what serves**: the register step records the files Agent Platform copies
  (`model.registered` `files`, `pipewright_runtime.folder_files`), and `model approve` refuses a folder changed since.
- **The watch** (`policies.integrity.watch`, `watch_every`): the launcher re-checks every default and serving version
  and records `model.integrity` when a result changes, with a `model.integrity.check` heartbeat; `/integrity/tick`
  runs it now. It also checks the renewer's work (holds and metadata of protected files).
- **`pipewright integrity check`**: the same check once, exit 0, 1 or 2, for CI.
- **Lock renewal** (`lock.renew`, `lock.rollback_days`): lock on activation in `deploy-model`; `pipewright integrity
  renew`, run daily by a dedicated renewer identity, keeps every artifact an active version needs protected
  (defaults, serving versions, the rollback candidate, KServe copies, live contracts), never shortening, and records
  `model.lock.renewed`, `model.lock.failed` and `model.lock.reconciliation`.
- **`pipewright model restore-files`** (approved generations written back, `model.restored`) and **`pipewright model
  retire`** (`--release-holds`, `model.retired`).
- **A time-boxed override** (`policies.integrity.until`, `reason`): recorded as `policy.override`, back to `refuse` on
  its own after the date.
- **Audit logs that last** (FR-102): `observability.audit_log_bucket` and `audit_retention_days` (the approval
  cross-check reads them), `data_write_logs`; the starter and the blueprint create the log bucket, its sink, DATA_WRITE
  logs with an exclusion that keeps only approved folders and contracts, the renewer and the environment alerts.
- **`doctor`**: an integrity section (protection that is off, an open-ended or expired override, short `lock.days`,
  who else can write the artifacts bucket, audit retention, DATA_WRITE logs, the renewer and who can act as it).
- **`alerts sync`**: `model.integrity` and `run.integrity` for each use case when the watch or the scoring check is on.

### Changed

- Integrity checks read the event log once and then only what is new (`models/approvals.py`) instead of one full scan
  per version, which made a watch affordable (100 use cases hourly: about $1.91 a month instead of about $1,254).
- `loop rollback` checks the version it restores, and `pipewright rollback` checks the old release's contract against
  its `release.live` fingerprint and locks it again, like the way forward.
- An approval of an already protected file keeps its later retention date instead of reporting a failed lock.
- Found in the live test of private GKE: a KServe pod that pulls from the mirror is judged by the image it mirrors
  (the mirror's manifest digest differs from the source's); `PIPEWRIGHT_KUBERNETES_ENDPOINT`, set inside the VPC,
  wins over the cluster's DNS endpoint (an environment with no internet egress cannot reach it: the launcher's watch
  hung until Cloud Run's request timeout); a watch run claims the run before checking, so an overlapping run (a tick
  cut off by the request timeout and the scheduler's retry) records nothing twice; a KServe namespace the watch or the
  renewer cannot read counts as `cannot_check` (exit 2) and makes the renewal degraded, instead of a note;
  `deploy-model` on KServe updates the pods' check when it rewrote a changed copy (it said "already live", and no new
  pod could start); the cross-identity check reports a shortened retention or a metadata change, not a retention
  date moved later (deploy-model's lock on activation was reported as an incident).

## [1.4.0] - 2026-11-06

Phase 6: what runs is what was approved. An approval records what it approved, and nothing is deployed or promoted
unless it is still that. One behaviour changes on upgrade, deliberately: these checks **refuse by default**
(`policies.integrity.mode: warn` relaxes an environment). Everything else is additive and off until set. The guide is
[docs/INTEGRITY.md](docs/INTEGRITY.md).

### Upgrading

Nothing to change in a use case. Approvals made before this release carry no file fingerprints; they deploy as before,
with a note. A default version that has no `model.approved` event (made the default outside Pipewright) is now
refused by `deploy-model` and `promote`: approve it with `pipewright model approve`, or set
`policies.integrity.mode: warn` while you do. `model approve` refuses when your working copy's `ml.evaluation` or
`approvals` differ from the release that trained the version: approve from the release's commit.

- `deploy-model` and `promote` cross-check each approval with Cloud Audit Logs. The identity that deploys or promotes
  needs to read the Admin Activity log (`roles/logging.viewer`); without it the check is a note, never a refusal.
- For a production environment, set `policies.integrity.lock` with `mode: locked`, on an artifacts bucket created with
  object retention (the starter and the Enterprise blueprint create it so when the lock is set from the start).
- The closed retrain loop now checks that the release in test accepts what it sends (`champion_version`,
  `model_version`) and refuses with the reason; a pipeline written before 1.1 needs those parameters for the loop.
- BigQuery ML: a version promoted by files with `bqml.transform` is not served online in the destination (`validate`
  refuses it); serve it where it was trained, or move the expressions to `prepare.sql`.

### Added

- **What an approval records** (FR-94, FR-96): every model file by generation and SHA-256, the serving image's digest,
  a BigQuery ML model's etag, and the release whose rules it was judged by; with `policies.integrity.lock`, the
  approved files are locked with Cloud Storage object retention.
- **Checks before deploy and promote** (FR-93): `deploy-model` (Agent Platform and KServe) and `promote` compare the
  version with its newest approval and refuse a changed evaluation record or model file, a moved serving image tag, a
  replaced BigQuery ML model, a first version nobody approved, and a default with no approval. A promotion's own
  approval is judged by the released approvers and records and locks the copied files. `model.deployed` records the
  result.
- **Approval by the released rules** (FR-96): the gates `model approve` re-checks and the approvers it allows come
  from the release that trained the version; a first version's gates are re-checked too. New releases record their
  approvals in the contract.
- **`pipewright lineage` shows integrity** (FR-95, FR-97): the check and the lock; what changed in an approved
  evaluation (its decision and metrics, never slice values) and who wrote it, where Cloud Storage data access logs are
  on; each approval against Cloud Audit Logs' Admin Activity entry, flagging one with no entry or another principal.
- **A scoring check** (`policies.integrity.inference`: off, warn or refuse): the launcher checks that a scoring run's
  default version was approved, and records `run.integrity`.
- **`validate` warns about a serving image registered by tag**, except Agent Platform's prebuilt images, which it
  refuses to deploy by digest (found live): for those, the digest recorded at approval is checked at deploy.
- **`pipewright features register`** (FR-75): the data team registers feature groups from a spec file.
- **`pipewright report fleet`** (FR-86): every use case in every environment on one read-only HTML page, with each
  champion's integrity.
- **The churn example points to feature tables** (FR-99) and shows the change that moves it onto them.
- **The closed retrain loop, the rest of FR-48** (each off until set under `retraining.loop`): `safeguards` for loops
  started without a person (a rate limit, a circuit breaker that pauses automatic retraining, drift on consecutive
  runs, `pipewright loop pause | resume`); `canary`, an online canary with automatic rollback on an Agent Platform
  endpoint, judged by error rate, latency and a probe; `data: production`, production's feature tables and label
  window for the loop's training in test, accepted only in the production project test's manifest names
  (`tenants.<t>.loop_production_project`, new); BigQuery ML use cases in the loop, promoted by files.
- **Approvals cross-checked with Cloud Audit Logs at deploy and promote** (found testing as a bank's governance team
  would): an approval event with no Admin Activity entry, or naming someone else, is refused like any other check, so
  an event written by hand does not pass.
- **Release contracts locked and fingerprinted:** with a lock configured, each release's contract is locked at
  release and at promotion; `release.live` and `release.promoted` record its SHA-256, and `pipewright lineage` says
  whether the contract on file is still that one.
- **A deploy under `warn` says so:** `model.deployed` records `warned` with the problems found (names and digests,
  never data).

### Fixed

- **BigQuery ML online serving of a version promoted by files** compared the endpoint with a test table that exists
  only where the version was trained, so every such deploy was refused; it now uses the parity rows copied with it.
- **A champion whose BigQuery ML model is gone, or whose registry labels were removed,** failed training with a bare
  404 or an unexplained gate; training now stops naming the version and what to do.
- **A repeated promotion** after a failure registered the model again; it reuses the version it registered.
- **A first version** approved with its promotion is reported as the default, not a candidate.
- **Wording:** a refusal counts only its problems; lineage calls a candidate nobody approved "not approved yet".


## [1.3.1] - 2026-11-04

A fix to the decision trail: a model approval now records the evaluation it was judged on. Additive: nothing to
change, and use cases, manifests and releases behave as in 1.3.0.

### Upgrading from 1.3.0

Nothing to change. Approvals made before 1.3.1 have no evaluation digest; `pipewright lineage` shows them as before.

### Fixed

- **An approval keeps the evaluation it was judged on (FR-89):** the `model.approved` event (first versions too)
  and an approval made with `pipewright promote --approve` now carry `evaluation`: the record's location, its Cloud
  Storage object generation and the SHA-256 of its bytes.
  Before, the trail named who approved which version and how, but not which evaluation record the gates were checked
  against, so a record changed or removed later could not be told apart from the one approved. A record that cannot
  be read never blocks an approval; the event then has no digest.
- **`pipewright lineage` checks it:** under each approval with a digest it shows the record's SHA-256 and generation,
  and whether the record on file now is unchanged, changed or missing (`evaluation_now` in `--json`).

## [1.3.0] - 2026-11-01

AI build assistance: coding agents build and change use cases through Pipewright's tools, as a metadata-only identity,
without ever seeing a row; a person still reviews, merges, approves and deploys. Additive: a 1.2.0 use case, manifest
and release behave as before (the 1.2.0 examples compile to the same templates), every new setting is off until it is
set, and every new check is a warning unless a platform policy asks for more.

### Upgrading from 1.2.0

Nothing to change: the pipeline components and examples are the same as in 1.2.0. The new judgement checks may print
warnings from `validate` for use cases that were silent before; they refuse nothing. To get the review job in pull
requests, run `pipewright ci init` again (or copy the `review` job from the new `pipewright-pr.yml`). Coding agents
need the `mcp` extra: `pip install "pipewright-mlops[gcp,mcp]==1.3.0"`. The guide is [docs/AI_BUILD.md](docs/AI_BUILD.md).

### Added

- **Judgement checks in `validate`:** warnings for configuration that is valid but probably misleads a model: rows
  ordered in time with a split that is not a time split (the model learns from the future), a date or timestamp as
  a feature, features named after the label, which usually describe the outcome itself, and a team with feature
  datasets whose use case reads every feature from its own table (the pattern is feature tables and groups through
  `data.features_from`).
- **Gate minimums per environment:** the manifest's `policies.evaluation.min_gates` sets the least each metric's
  gate may ask (`auc: 0.7`, or `{min, max}`); `validate` refuses a use case without such a gate, or with a weaker one.
- **An agent identity per tenant:** `tenants.<t>.agent_service_account` in the manifest names a service account a coding
  agent acts as, impersonated by the team's engineers, with metadata-only roles; manifest `ai.local_tools: forbidden`
  turns the AI build tools off in an environment. Calls made for an agent carry a `pipewright-mcp` user agent and
  BigQuery job labels, so Cloud Audit Logs show them apart.
- **Context packs for coding agents:** `pipewright context build --env <env>` writes `APP_CONTEXT.md` (the
  environment's teams, datasets, feature datasets, policies and rules, the use cases and where their features come
  from), playbooks in `.pipewright/playbooks/`, and a marked Pipewright block in `AGENTS.md`, `CLAUDE.md` and
  `GEMINI.md` (text outside the block is left alone). The pack is stamped; `pipewright context check` fails when the
  manifest, a use case or Pipewright changed since it was built.
- **`pipewright mcp`, an MCP tool server for coding agents** (the `mcp` extra): runs locally over stdio, as the
  tenant's agent identity when the manifest names one. Read and verify only: `environment`, `validate`, `resolve`,
  `explain`, `doctor`, `plan` (tables, schedules and alerts dry runs), `inspect_table` (schema, row count,
  partitioning; personal columns flagged), `list_features` (Feature Store groups, their features and online views,
  and the tables in the team's feature datasets: definitions, never values), `status`, `runs` (the latest pipeline
  runs, with failed steps and their value-free first error line), `model_list`,
  `monitor_status`, `retrain_list`, `lineage`, `bootstrap_usecase` and `review_bundle`. `sample_rows` and `profile`
  always refuse: rows never reach an agent, and aggregates are not offered. Descriptions written by other people come
  back marked as untrusted data. Every call is logged in `.pipewright/mcp-log.jsonl` (transparency, not evidence).
  `pipewright mcp --print-config` prints the client settings to add.
- **Platform data rules for agents** (manifest `ai.data_classes`): `offline: true` refuses every tool that calls
  Google Cloud; `deny_names` withholds matching table and column names from every tool (and says how many were
  withheld). `ai.certified_agents` and `ai.approved_models` are shown to agents. A tenant's `agent_server` names a
  hosted tool server (Pipewright Enterprise); `--print-config` then prints the client settings for it and the proxy
  command that signs each request as the person.
- **Bootstrap from an entity table and feature sources:** `bootstrap_usecase` (and the drafting behind
  `new-usecase --from-bq`) can take feature groups or feature tables: the draft's contract covers the entity columns
  and the chosen features, `data.features_from` joins them at each row's cut-off, and the landing trigger is off. It
  reads schemas only.
- **`doctor` for an agent:** run through `pipewright mcp` as an agent identity, `doctor` checks that the identity
  cannot release, run, read table data, upload or deploy (by design), and warns if it can, instead of reporting the
  release permissions it rightly lacks as failures.
- **The pull request workflow** from `ci init` writes the review bundle to the run's summary and, once the
  repository has `APP_CONTEXT.md`, checks the context pack is current.
- **`pipewright review`:** the review bundle for a change, Markdown for a pull request: what a release would change
  in Google Cloud, the settings that moved against the base revision, the checks and their warnings, where the
  features come from, the context pack's state, and a section for the author's judgement calls.

## [1.2.0] - 2026-10-31

Additive: a 1.0 or 1.1 use case, manifest and release behave as before (the 1.1 examples compile to the same
templates and resolve to the same configuration), and every new setting is off until it is set. Found by building
the ebook's nineteen chapters on a live environment (FR-58 to FR-87).

### Upgrading from 1.1

Most of 1.2.0 arrives with the package. Some fixes live in the pipeline components a use case owns, which were copied
from the churn example when it was created, so an existing use case gets them only by bringing those files up to
date from `examples/churn/components/` (and retraining, where the fix travels with the model):

| Component | Brings | Until then |
|---|---|---|
| `train.py`, `register.py`, `torchserve_handler.py` | an empty feature filled online as in training (FR-73); a promoted model reading the destination's online store (FR-76) | the endpoint refuses such rows; a model promoted with online features is refused by `deploy-model` and `doctor` |
| `register.py` | a first version held back under a manual policy (FR-83) | the first version becomes the default unapproved; `validate` warns |
| `monitor.py` | outcomes joined to the score made for their cut-off (FR-78) | joined by key only; `validate` warns once `labels.time_column` is set |
| `evaluate.py` | the champion's slices and fairness on the same rows (FR-72) | `model check` has no champion slices to compare |
| `prepare.py` | rows sorted by the contract's unique key before splitting, for exact replays (FR-85) | a replay reads the same rows, but may split them differently |

A re-release after the upgrade gets a new release ID when the resolved configuration changed (a changed default),
so runs use the new settings.

### Added

- **How to call a deployed model** (FR-60): `deploy-model` ends with the endpoint ID, the predict URL, the caller's
  permission and a sample request (the values in the model's feature order); `pipewright model endpoint` prints the
  same, read only. On KServe: the in-cluster URL, the internal load balancer's address and the route.
- **Serving gated in CI, or automatic** (FR-63): `approvals.deploy_model: ci` lists a `deploy-approve` decision for
  each new default version that passes its gates and is not served, deployed by a `pipewright-gate` run once the
  GitHub environment's reviewers approve (recorded on `model.deployed`); `on_register` lets the decisions workflow
  deploy it without a person. Manifest `policies.deploy_model: ci` makes ci the only way in an environment.
- **GPU limits and GPU serving** (FR-58, FR-61): manifest `policies.accelerators` (`steps` and `serving`: `types`,
  `max_count`); `model.serving.accelerator` deploys on a GPU within it.
- **Scale to zero** (FR-62): `model.serving.scale_to_zero` with `replicas.min: 0` (an Agent Platform preview).
- **A base image per use case** (FR-59): `components.base_image`, within the approved base images.
- **Re-check a version against today's gates** (FR-72): `pipewright model check --version <v>`; each evaluation
  also measures the champion's slices and fairness on the same test rows (`_slices`, `_fairness`).
- **Forecasts per series** (FR-69): the evaluation record holds each series' metrics; `evaluation.gates[].per_series`.
- **A failed run is an event** (FR-67): `run.failed`, recorded once per failed or cancelled run by the launcher's
  hourly tick, with the failed steps and their error lines; alertable in `monitoring.alerts.events`.
- **Machine-readable fleet state** (FR-86): `--json` on `status`, `model list`, `monitor status`, `retrain list`;
  `bqml.metrics_table` writes a BigQuery ML run's row in the container examples' metrics table.
- **Image cleanup** (FR-87): `pipewright images prune [--keep n] [--apply]` deletes images no release needs (dry run
  by default); `doctor` reports the image storage.
- **Tests before a release** (FR-66): `tests.on_release`. **Tests with the components' requirements** (FR-82):
  `tests.environment: components`.
- **Promotion in CI** (FR-80): `ci init` writes `pipewright-promote.yml` and an operate `promote` action for an
  environment with `promotion.from`, instead of a release job; `--decisions-cron` (default hourly in working hours);
  `release` warns for such an environment, and `promotion.release: refuse` stops it. A lifecycle event that could
  not be published ends the command with a warning.
- **The kfp CI installs** (FR-81): `ci init` writes `.github/pipewright-constraints.txt` with the tested kfp, and the
  setup action installs with it; a compiler mismatch names the kfp to install.
- **The decision trail** (FR-84): `lineage` shows who approved, promoted and deployed a version, from the event log
  (manifest `observability.evidence_dataset`); the release pointer records the person behind a CI identity;
  `doctor` warns when nothing keeps the events.
- **Exact replays** (FR-85): snapshot events carry a rows fingerprint; the example's prepare step sorts by the
  contract's unique key before splitting.
- **Contract before preparation** (FR-70): `bqml.contract_on: source` checks the data contract on the rows as read.
- **Examples that show every setting** (FR-64): the examples are real YAML with readable sample values; the churn
  example shows each default it leaves out as a `# default` line (a test keeps them true); a README per example.

### Fixed

- **Online serving** (FR-73, FR-76): the endpoint fills an empty feature with its training median, as batch scoring
  does (`model.serving.fill_missing: false` to answer an error instead); a promoted model reads the destination's
  online store, and `deploy-model` and `doctor` refuse one that would read another environment's.
- **deploy-model is safe to repeat** (FR-74): it refuses while the endpoint has an operation in progress, and says
  when the version is already served.
- **Performance monitoring** (FR-78): `monitoring.performance.labels.time_column` joins each outcome to the score
  made for its cut-off (scores carry `scored_for`); `validate` warns where the old join can mismatch.
- **A refused retrain** (FR-79): the case records the gates' reasons, re-opens, and emits `retrain.refused`.
- **A first version under a manual policy** (FR-83): labelled not approved until `pipewright model approve`;
  `model list` and `doctor` say so, `deploy-model` refuses it. Scoring is unchanged.
- **Entity columns as features** (FR-68): BigQuery ML leaves the entity's cut-off column out of the default features
  (a DATE feature failed evaluation); `validate` warns about the key columns, and about both on the container path.
- **Synthetic data reads labels like training** (FR-65) when its source is the entity table.
- **alerts sync** (FR-77) deletes the Pipewright email channels no policy uses any more.
- **kfp held to the tested version:** the package allowed any kfp 2.x, so a fresh install took kfp 2.18 (released
  this week) while the examples pin 2.17, and `validate` refused every new use case. The range is now `<2.18`,
  in step with the kfp Pipewright is tested with; raise both together.
- **A release after a Pipewright upgrade carries the new configuration:** with the same commit and templates, a
  release reused the old contract even when the resolved configuration had changed (an upgraded default), so runs
  kept the old settings. A changed configuration is now a new release ID, as a changed image or template is.

## [1.1.0] - 2026-10-26

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
- **Online feature lookup** (FR-51): `model.serving.features: online` with the tenant's online store from the
  manifest (`tenants.<t>.features.online_store`) and a view per `data.features_from` source (`online_view`, default
  the feature group). The example's serving handler takes entity IDs and reads the views; `doctor` checks the views
  and their last sync; `deploy-model` serves as the tenant's runner. Off unless set.
- **No generative AI in BigQuery ML SQL** (C-08): `prepare.sql` and TRANSFORM calling `AI.*` or `ML.GENERATE_*`
  are refused at validate and release.
- **Fairness metrics and gates** (FR-53): `evaluation.fairness` (attributes, measures, gates, `min_rows`) for
  binary classification: demographic parity, equal opportunity and equalised odds differences on the test set, in
  the evaluation record and the model card, gated in the run and again by Pipewright's gate check.
  `fairness_measures()` and `fairness_problems()` in the component runtime; BigQuery ML computes them in SQL.
- **Approved image allow-list** (FR-54): manifest `policies.images` (`base_images` patterns, `package_indexes`,
  `default_base_image`). `validate` and `release` refuse other base images, indexes and direct references; builds
  use the platform's base and first index; the contract records the base images by digest (`images`, left out
  when no policy is set).
- **Approvals in GitHub** (FR-56, D-120 to D-123): `pipewright decisions pending [--json]` lists model approvals,
  retrain cases and closed loops waiting for a person, with the GitHub environment that gates each.
  `pipewright ci init` adds `pipewright-decisions` (every 15 minutes: advances loops, starts gate runs) and
  `pipewright-gate` (waits for the environment's reviewers, then runs the decision's own command). Decision events
  and retrain decisions record how they were approved: the GitHub reviewers, comment and run
  (`PIPEWRIGHT_APPROVAL`), or outside CI. `PIPEWRIGHT_ID_TOKENS` gives a job ID tokens for several launchers.
- **Lineage per run** (FR-55): `write_lineage()` in the component runtime writes `lineage.json` next to the
  evaluation record (release, `ml_config` and SQL digests, rows, sources, evaluation); the example's register step
  and the BigQuery ML pipeline call it. `pipewright lineage` adds the release contract, the rows' count and read
  time and the registry state, and follows a promoted version to its source.
- **The closed retrain loop** (FR-48, D-104 to D-109): with `retraining.loop.enabled` an approved production case
  is trained in the environment releases come from, compared with the version production serves, validated by a
  scoring run there, promoted, approved and verified in production; `pipewright loop start | advance | show |
  rollback`. Run requests may name `champion_version` and `model_version` for pipelines that declare them
  (`pipewright run --champion-version`, `--model-version`); the example's pipelines and the BigQuery ML pipelines
  do.
  With the loop on, scheduled and landing training runs in production open a case instead of training there,
  and `retraining.mode: candidate` hands the case to the loop at once (only the production approval is a
  person's).
- **BigQuery ML** (`spec.bqml`): a use case trains and scores in BigQuery with pipelines Pipewright provides, on the
  environment's orchestrator. The run's rows (features at each cut-off, the label window, the team's
  `prepare.sql`) with a held-out test set; the data contract in SQL; `CREATE MODEL` on the training rows (with the
  team's TRANSFORM); the model and the champion evaluated on the same test rows with Pipewright's metrics in SQL;
  gate 1; registration in Model Registry only after it (`ALTER MODEL`); scoring into `inference.output_table`;
  drift and performance against observed outcomes into `monitoring.output_table`; retrain proposals to the launcher
  when `retraining.when` matches. Binary, multi-class and regression model types; byte limits per job
  (`bqml.max_bytes_billed`, manifest `policies.bqml`); `examples/bqml` (`pipewright new-usecase --from bqml`).
  `spec.components` is required only without `bqml`.
- **More BigQuery ML model types:** `kmeans` (`data.label.task: clustering`) and `pca` (`decomposition`), measured
  with `ML.EVALUATE`; forecasts (`data.label.task: forecast`, `bqml.forecast`) with `arima_plus` (a backtest on the
  latest period, then a refit on the whole series before registration) or `ai_forecast` (AI.FORECAST, refused
  unless the manifest sets `policies.bqml.ai_forecast`); models trained elsewhere (`bqml.import`: TensorFlow or
  ONNX, with mapped outputs), evaluated and gated like a trained one.
- **BigQuery ML promotion by files:** `model.promotion: model` for `logistic_reg` and `linear_reg`: the model is
  exported, copied and imported, and registered only when it scores the test rows as the source does.
- **BigQuery ML online serving:** `deploy-model` checks the endpoint's scores against `ML.PREDICT` on the run's test
  rows.
- **Failed retrains:** an approved case whose training fails keeps its approval; the launcher's tick records the
  outcome on the case (registered, rejected by the gates, or failed with the failed step and error;
  `retrain.training_failed`), retries transient failures automatically at most twice (`retrain.training_retried`),
  and `pipewright retrain retry --case` starts a new run with its own run key. A failed Kubeflow run names its
  failed step.
- **`spec.run_in`:** the environments a use case runs in. Whole-repo commands skip it elsewhere with a note; naming
  it in another environment, or promoting it to one, is refused.
- **Alerts of use cases that are gone:** a whole-repo `alerts sync` deletes the policies of use cases neither in the
  repo nor released in the environment (`--dry-run` shows them).

### Changed

- **Value-free errors** (NFR-18): errors from BigQuery and Cloud Storage on the data paths (the component runtime's
  `read_source` and `write_table`, the BigQuery ML steps, the CLI's BigQuery queries) no longer quote data values:
  quoted literals and what follows "value:" are replaced with `...`, column and table names stay
  (`pipewright.errors.value_free`). The example's contract check reports how many values break a rule, not which.

### Fixed

- **A run straight after a release ran the previous release:** run requests now name the live release, and a
  launcher holding an older cached pointer reloads it.
- **Promotion** checks the model version (the default, trained by the release promoted) before copying tables and
  images, so a refused promotion leaves nothing behind.
- **Synthetic data pipeline:** the tenant's dataset is read from `ml_config` at run time instead of being compiled
  into the template, so a promoted release compiles the same as the tested one.
- **BigQuery ML registration** waits for Model Registry to serve a new version before labelling it; a model's first
  version is labelled champion, and every version gets a description (run and primary metric), as for container
  models.
- **The CLI** no longer prints the Agent Platform SDK's per-model INFO lines.
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
