# What runs is what was approved

From Pipewright 1.4.0, an approval records what it approved, and nothing is deployed,
promoted or (optionally) scored unless it is still that.

## What an approval records

`pipewright model approve` (and `pipewright promote --approve`, which approves a model's first version in the
destination) writes a `model.approved` event with:

| Field | What it holds |
|---|---|
| `evaluation` | the evaluation record the gates were checked against: its location, Cloud Storage generation and SHA-256 (since 1.3.1) |
| `files` | every file in the version's model folder: name, generation and SHA-256, and one digest over them all |
| `serving_image` | the serving container the version was registered with, and the digest its reference resolved to |
| `bq_model` | for a BigQuery ML version: the model in BigQuery and its etag |
| `rules` | the release whose rules the version was judged by, and digests of its gates and approvals |
| `lock` | when a lock is configured: how many files were locked, until when, and how |

The rules are the released ones. The gates `model approve` re-checks, and the approvers it allows, come from the release
that trained the version, not from your working copy. If your working copy differs in `ml.evaluation` or `approvals`,
the approval is refused: approve from the release's commit, or release the change and retrain. A release from before
1.4.0 records its gates but not its approvers; its approvers then come from the working copy, and the approval says so.

## What is checked, and when

| Before | Checked |
|---|---|
| `pipewright deploy-model` (Agent Platform) and KServe | the default version's newest approval: the evaluation record, every model file, the serving image, a BigQuery ML model; a first version nobody approved; a default that became the default outside Pipewright (no `model.approved` event) |
| `pipewright promote` | the same, for the source version, before anything is copied (a closed retrain loop's candidate is approved by the loop instead) |
| a scoring run (optional) | the full check of the version it will use (`policies.integrity.inference`, 1.4.1; before 1.4.1 only that it had an approval), and with `pin` the run loads exactly the approved generations |
| every KServe pod start (1.4.1) | each file of the approved copy the pod loads: its generation and SHA-256, before the server starts |
| `pipewright model approve` (1.4.1) | that the model folder still holds the files Agent Platform copied at registration |
| `loop rollback`, `pipewright rollback` (1.4.1) | the version or release contract going back into use, like a deploy |
| the launcher's watch (1.4.1, optional) | every default and serving version, hourly by default, with an alert when a result changes |
| `pipewright integrity check` (1.4.1) | the same as the watch, once, with an exit code for CI |
| a training run, an approval, a promotion (1.4.1, with `features.check`) | the feature definitions against the use case's feature contract (the release's); approvals record them, and every check above compares them with the approval ([FEATURES.md](FEATURES.md)) |
| `pipewright lineage` | all of it, shown: the integrity result, the lock, what changed in an approved evaluation and who wrote it, and each approval against Cloud Audit Logs |

A file is compared by its generation first (Cloud Storage gives every write a new one), then, if it was rewritten, by
its SHA-256, so a rewrite with identical bytes still matches. A serving image registered by tag is resolved again: a tag
that now points elsewhere is a change. Register your own serving images by digest (`validate` warns about a tag);
Agent Platform's prebuilt serving images can only be used by tag (it refuses one given by digest), so for those the
approval's recorded digest is what keeps the image the approved one.

Messages name records, files, digests, metrics and gates: never a row, a feature value or a slice value. What changed
in an evaluation record is shown as its decision and top-level metrics; anything else that changed (slices, for
example, which can be named after data values) is counted, not shown.

## Policy (manifest `policies.integrity`)

```yaml
policies:
  integrity:
    mode: refuse                    # the default; warn says what it found and goes on
    until: 2026-11-20               # 1.4.1: with warn, the last day it applies; then refuse again on its own
    reason: incident 4711           # 1.4.1: why (recorded, alerted while it lasts)
    lock: { days: 30, mode: unlocked, renew: true, rollback_days: 30 }
    inference: off                  # off (the default), warn or refuse: the scoring check
    pin: on                         # 1.4.1, the default: scoring and KServe load the approved generations
    watch: off                      # 1.4.1: on: the launcher re-checks what is in use and alerts on a change
    watch_every: 1h                 # 1.4.1: 1h (the default), 6h, 1d
    features:                       # 1.4.1 (FR-103), all off: feature definitions against the feature contract
      check: off                    # on: a changed definition is a problem judged by mode
      built_tables: false           # tables built by a query: by the SQL of the job that wrote them
      record_reads: false           # scoring records the base tables it read and when
      fingerprint_rows: false       # and a row fingerprint per source
      snapshot_days: 0              # and a snapshot of each base table, kept this many days
```

- **mode:** `refuse` is the default. A check that fails means what would run is not what was approved. Set `warn` for an
  environment that needs to keep going while you find out why, never as a standing setting in production.
- **lock:** after an approval, the approved files cannot be destroyed: their approved generations cannot be deleted or
  overwritten, by anyone the lock applies to. On a versioned bucket (the artifacts bucket is one), someone who may
  write objects can still put a new version over a locked file, or delete the live copy: the approved generation then
  becomes noncurrent and stays locked. The lock keeps the approved bytes; the checks above catch the swap and refuse
  to deploy or promote until the approved bytes are back (`gcloud storage cp <file>#<approved generation> <file>`
  restores one; a byte-identical copy passes the check). Tested live on private GKE. On a bucket created with object
  retention (the starter and the Enterprise blueprint create the artifacts bucket with it when a lock is configured
  from the start), each file gets retention until `days` from now: `unlocked` can be shortened or removed by someone
  with `storage.objects.overrideUnlockedRetention`; `locked` cannot be shortened or removed by anyone, including
  project owners. Cloud Storage cannot add object retention to an existing bucket, so on an environment built before
  the lock was configured, an `unlocked` lock is a **temporary hold** on each file instead (no end date: release it
  with `gcloud storage objects update --no-temporary-hold` when the version is retired), and a `locked` lock is not
  possible and is recorded as not done. Terraform ignores the retention setting after the bucket exists, so turning the
  lock on never replaces the bucket.
- **An approval must be in Cloud Audit Logs:** `deploy-model`, KServe deploys and `promote` cross-check the newest
  approval with the Admin Activity entry its change made (the alias moved, the label set, or a promotion's upload),
  which nobody can write. An approval event with no entry, or naming someone else, is refused like any other check
  (`policies.integrity.mode`), so an event written by hand does not pass. A log that cannot be read is a note, never
  a refusal; an approval older than the log keeps (400 days) is not cross-checked.
- **The release contract too:** with a lock configured, each release's contract (what a run pins: images and
  templates by digest) is locked the same way at release and at promotion, so nobody who may write the bucket can
  change what a live release runs. `release.live` and `release.promoted` record the contract's SHA-256, and
  `pipewright lineage` says whether the contract on file is still that one.
- **inference:** with `warn` or `refuse`, the launcher checks before each scoring run that the default version was
  approved, and records `run.integrity` when it was not. The launcher then reads the event log.

## Who approved: Cloud Audit Logs

The event log says who approved, as the CLI reported it. `pipewright lineage` checks each approval against the Admin
Activity entry for the change it made (the `default` alias moved, or a first version's approval label set). Admin
Activity logging is always on and cannot be turned off. An approval with no matching entry, or a different principal,
is shown as such: the event may not be genuine. Reading those entries needs `roles/logging.viewer` (Admin Activity
only).

## Teams that share a project

Set `isolate_tenant_artifacts = true` in the starter so each team's runner writes only under its own pipeline root
(`pipeline-root/<team>/`); one team's runs then cannot overwrite another team's models or evaluation records. The
Enterprise blueprint has always done this.

## A runner that cannot move a model's default (1.4.3)

Every runner holds `roles/aiplatform.user` across the project by default, and that includes uploading model
versions. An upload can move a model's `default` alias, any model's, other teams' too, and no IAM role can stop an
identity that uploads from doing it (model-level IAM cannot scope uploads). Pipewright catches a default nobody
approved at deploy and promote, and the watch names who moved it, but the default setting does not prevent it.

`policies.registration: launcher` prevents it. It is off by default (`runner`, as before 1.4.3):

- Runners get a custom role: `roles/aiplatform.user` without `aiplatform.models.upload`, `update` and `delete`
  (Terraform builds it from the predefined role at each apply).
- The register step asks the launcher (`pipewright_runtime.register_version`). Before it uploads, the launcher checks
  that the request comes from the run it names (the use case's runner, a running job with that run ID), that the
  model folder is under the use case's pipeline root and the evaluation record is the one the step wrote (by
  SHA-256), and recomputes the gates from that record. It moves the default only where the release's policy does
  (`promote_model: auto`). A refusal is a `model.registration_refused` event and fails the run, saying why.
- BigQuery ML registers the same way: the launcher runs the `ALTER MODEL` itself, with model metadata on each team's
  dataset only.
- Operators keep their roles: approving, promoting and retiring are their decisions, recorded with who made them.

Turn it on after every use case is released with 1.4.3 or later: an older register step uploads directly and fails
with 403. `pipewright validate` and `pipewright release` refuse a use case whose components write the registry
themselves, naming each file, so an older step is caught before it runs; `pipewright doctor` fails while a runner can
still upload. The check reads the component files as text: a file that only mentions an upload (a comment, a help
text) can carry the line `# pipewright: not-a-registry-write <reason>`, and `validate` then lists it as a warning with
the reason. The marker changes nothing at run time: the runner still cannot write the registry. Cost: none (one
request to the launcher per training run).

## Where the bytes come from (1.4.1)

A live test (2026-11-06) settled where a swapped file reaches serving without a deploy:

| Path | Where it reads the model files | A file swapped after approval |
|---|---|---|
| Agent Platform endpoint | its own copy, taken when the version is registered | not served (scale-out and redeploy served the approved bytes) |
| KServe on GKE | the model folder, at every pod start | served by the next pod that starts, before 1.4.1 |
| a scoring run | the model folder, on every run | used by the next run, before 1.4.1 |

So 1.4.1 closes the two live paths by reading the approved bytes, which a swap cannot change (a Cloud Storage
generation never changes its bytes):

- **Scoring (`pin`, with `inference: warn` or `refuse`).** The launcher runs the full check, then passes the approved
  files to the run (`approved_files`: each file's generation and SHA-256). The reference example's `load_model`
  downloads exactly those generations and checks each SHA-256; it fails if one differs or is gone. Under `warn` a
  changed file is reported and the run still loads the approved one; under `refuse` the run is not submitted. Your own
  scoring component reads the parameter with `pipewright_runtime.approved_download(approved_files, dest, project)`.
  A pipeline built before 1.4.1 does not declare `approved_files`: the launcher says so and the run loads the folder.
- **KServe (`pin`).** `deploy-model` copies the approved generations next to the model folder
  (`<folder>-approved-v<N>-<digest>/`, inside the team's pipeline root, which the serving identity may read), reads
  them back by SHA-256, locks them like the originals, and points the pods at the copy. Every predictor pod runs an
  init container first that checks each file of the copy (generation and SHA-256) and stops the pod if one differs:
  the pods already running keep serving, the new one does not start, and the watch reports it. The check uses the
  serving image's Python (standard library only); set `kserve.verifier_image` for an image without Python.
- **Agent Platform.** The register step records the folder's files at the moment Agent Platform copies them
  (`model.registered` `files`), and `model approve` refuses a folder that changed since: what serves is that copy.

## The watch, the alert and the CI gate (1.4.1)

With `watch: on` the launcher's hourly tick re-checks, every `watch_every`, each model's default version (what scoring
uses), every version deployed on an endpoint and every KServe service (and its approved copy). It records
`model.integrity` only when a result changes: `changed`, `unapproved` (a version serving without an approval, for
example deployed from the console), `cannot_check`, `restored`; KServe and scoring changes are `incident`, an Agent
Platform endpoint's is `drift` (it serves its own copy until the next promote). Every run records a
`model.integrity.check` heartbeat. `pipewright alerts sync` emails a use case's owners on `model.integrity` and
`run.integrity`; the starter and the Enterprise blueprint alert the platform team (`platform_alert_emails`) on
incidents, on a missing heartbeat (a broken watch must not look like "unchanged") and on an override in force.

`pipewright integrity check --env <env> [--usecase <uc>] [--json]` runs the same check once: exit 0 when every approval
is unchanged, 1 when something changed or is unapproved, 2 when something could not be checked. Use it as a CI step
before a deploy, or on a schedule where the launcher is not used. It reads the event log once and then only what is
new, however many versions it checks.

## Keeping approved files protected while they are in use (1.4.1)

Before 1.4.1 the lock ended `lock.days` after the approval, so a model serving for a year was unprotected after a
month. Now, with `lock.days` set and `lock.renew` on (the default):

- **Lock on activation.** `deploy-model` (Agent Platform and KServe) protects the approved files before the version
  becomes active; under `refuse` it refuses when it cannot.
- **Daily renewal.** The renewer (`pipewright integrity renew`, a Cloud Run job) finds every artifact an active version
  needs: each default's and serving version's approved files, the previous default for `lock.rollback_days` after it
  was replaced (the rollback candidate), KServe's approved copies and the live release contracts. It extends retention
  on the exact approved generation when less than `max(lock.days / 2, 3 days)` is left, never shortens it, and records
  `model.lock.renewed` for each change, `model.lock.failed` for each failure and one `model.lock.reconciliation`
  summary (healthy, degraded or failed; the least protection left). When the history is unclear (a default changed
  outside Pipewright), it protects every candidate and says `degraded`: protecting more, never less.
- **Retirement.** A version no longer in use stops being renewed and keeps its last date; its files can be cleaned up
  when that date passes (Pipewright never deletes model files). `pipewright model retire --release-holds` releases the
  temporary holds of a retired version on a bucket without object retention, as an authorised person, recorded.
- **lock.days under 7.** Renewal still works, but a missed run or two can leave files unprotected; `doctor` warns.

**Security.** Extending retention needs `storage.objects.update` as well as `storage.objects.setRetention` (tested
live: `docs/evidence/fr101-retention-permissions/`), and `update` also allows releasing a hold and changing metadata.
So the renewer is its own identity, never the launcher, with a custom role of exactly `get`, `setRetention` and
`update` on the model folders, contracts and manifest; only Terraform may act as it (`doctor` lists who can). Its work
is watched by another identity: the launcher's watch records each protected file's hold and metageneration and
reports a released hold, a shortened retention, or a metadata change no `model.lock.renewed` explains, as an
incident (a retention date moved later is a lock, whoever made it: approve, deploy-model, promote or the renewer); Cloud Storage DATA_WRITE audit
logs record every write. This is detection, not prevention: keep legal holds in a bucket or folder outside the
renewer's scope. The renewer's schedule exists only with the watch on and DATA_WRITE logs, their exclusion and the
alerts in place, and ships paused (`renewer_schedule`) until you have tested the alerts in that environment.

## What each setting costs (Google list prices, europe-west2, 2026-11-06)

| Setting | Default | Protects | Monthly cost |
|---|---|---|---|
| `pin` | on | scoring and KServe load the approved bytes | about $0; a KServe copy is $0.023 per GiB per version kept |
| `inference: warn` or `refuse` | off | a scoring run is checked in full first | about $0 (one cached event-log read per run) |
| `watch: on`, hourly | off | a change between deploys is reported | about $0.20 (5 use cases) to $1.91 (100 use cases), mostly Cloud Run time; every 15 minutes about 4x |
| `lock.days` with `renew` | off | approved files cannot be deleted while in use | about $0.08 for 100 versions, plus retired files kept up to `lock.days` longer ($0.023 per GiB-month) |
| the renewer job | with renew | the above | about $0.05 (one minute a day), plus $0.10 for a Scheduler job beyond the 3 free |
| DATA_WRITE logs (with the exclusion) | starter off, blueprint on | who wrote a file | usually inside Cloud Logging's free 50 GiB a month; $0.50 per GiB beyond |
| `audit_retention_days` | 0 | approvals older than 400 days are cross-checked | cents a year ($0.01 per GiB-month); a locked bucket's retention cannot be shortened |
| alerts | with `platform_alert_emails` | the platform team is told | $0 at these volumes |
| `features.check: on` (1.4.1, FR-103) | off | a changed feature definition is refused like a changed model file | $0: object metadata and dry runs are free |
| `features.built_tables` | off | tables built by a query, by their last job's SQL | about $0.00015 a check (20 MiB minimum); needs `bigquery.jobs.listAll` |
| `features.record_reads`, `fingerprint_rows`, `snapshot_days` | off | what each scoring run read | $0; a scan per source per run ($0.0076 per GiB); snapshot storage of rows that later change |

## When something changes: the response plan (1.4.1)

1. **Who is told.** The use case's owners (`alerts sync`: `model.integrity`, `run.integrity`) and the platform team
   (the environment's alerts). EV-06 in Enterprise records when the change was first seen and when it was restored.
2. **What refuses and what warns.** Under `refuse`: deploys, promotions and rollbacks of the changed version, and its
   scoring runs; KServe pods with a changed copy do not start. Under `warn`: they go on with the approved bytes where
   pinned, and say so.
3. **Find out.** `pipewright integrity check --env <env>`; `pipewright lineage --version <N>` shows what changed and,
   with DATA_WRITE logs, who wrote it.
4. **Restore.** `pipewright model restore-files --env <env> --usecase <uc> --version <N>` (dry run first, then
   `--confirm`) writes the approved generations back as the live files, checks each SHA-256 and records
   `model.restored` with your name. The watch then reports `restored`. A changed KServe copy (the pods load it, so
   new pods refuse to start while the running ones keep serving): `pipewright deploy-model` rewrites it from the
   approved generations and updates the pods' check, a rolling update.
5. **Override, time-boxed.** If production must keep going first, set `mode: warn` with `until` (a date) and `reason`:
   it is recorded as `policy.override`, alerted while it lasts, and goes back to `refuse` on its own after `until`.

## Audit logs that last (1.4.1)

The approval cross-check reads Cloud Audit Logs, which keep Admin Activity 400 days. With `audit_retention_days` the
starter and the blueprint route Admin Activity entries for Agent Platform and the artifacts bucket (and the DATA_WRITE
entries) to a log bucket kept that long, locked by default (`audit_log_bucket_locked`), and the cross-check reads it
too (`observability.audit_log_bucket`). `doctor` says when it is missing or not locked, whether DATA_WRITE logs are
on, and who other than the runners can write the artifacts bucket.
