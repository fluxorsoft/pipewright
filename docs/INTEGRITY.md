# What runs is what was approved

From the release that ships Pipewright's phase 6, an approval records what it approved, and nothing is deployed,
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
phase 6 records its gates but not its approvers; its approvers then come from the working copy, and the approval says so.

## What is checked, and when

| Before | Checked |
|---|---|
| `pipewright deploy-model` (Agent Platform) and KServe | the default version's newest approval: the evaluation record, every model file, the serving image, a BigQuery ML model; a first version nobody approved; a default that became the default outside Pipewright (no `model.approved` event) |
| `pipewright promote` | the same, for the source version, before anything is copied (a closed retrain loop's candidate is approved by the loop instead) |
| a scoring run (optional) | that the default version has an approval (`policies.integrity.inference`) |
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
    lock: { days: 30, mode: unlocked }
    inference: off                  # off (the default), warn or refuse a scoring run whose default has no approval
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
