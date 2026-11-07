# 1.4.1 QA round (phases 7 and 8) on the qa branches

Branch `qa` in core (8b85360), starter (71cf24b) and Enterprise (f1cb1a5), cut from phase-7 after both phases'
live tests. Static: core 771 tests, Enterprise Python 172, Terraform 49 + 1, ruff and terraform fmt clean.
Open source: pw-ebook (test) and pw-ebook-prod (prod) were built from scratch earlier the same day from the code that
is qa; from the qa checkout `terraform plan` showed no changes in either. Launchers rebuilt from qa; every use case
released again from qa; fresh training. Features policy: test check, built_tables, record_reads, fingerprint_rows,
snapshot_days 1; prod check, built_tables.

## Phase 7 (FR-101, FR-102), open source

| # | Scenario | Result |
|---|---|---|
| L1 | doctor | PASS: protection on, lock.days warning, feature definitions line, writers, audit retention not locked, DATA_WRITE on, renewer |
| L2 | landing-triggered training registers the files | PASS: model.registered 7 files (sha256 f428d4af2977ad98) |
| L3 | approve v1 | PASS: 8 approved files locked 2 days |
| L4 | a file rewritten between registration and approval (churnf v3) | PASS: refused ("model_card.md: sha256 8584e65d8007bf2b when registered, 176bdcba6c5df23f now") |
| L5 | pinned scoring; model.pt swapped under warn | PASS: pinned (8 files); run.integrity warn and still pinned |
| L6 | prod (refuse) scoring with model.pt swapped | PASS: refused; restore-files in prod |
| L7 | watch | PASS: default changed (incident), then restored |
| L8 | integrity check + restore-files | PASS: exit 1; dry run, --confirm, exit 0 |
| L9 | renewer job | PASS: healthy, 26 protected and renewed (with a rollback candidate) |
| L10 | cross-identity | PASS: custom metadata change reported; a hold set is quiet; a hold released is reported |
| L11 | promote + deploy-model on an endpoint | PASS: 8 files copied and verified, 7-day lock; "8 approved files protected for at least 3.5 days" (lock on activation) |
| L12 | a version deployed outside Pipewright | PASS: endpoint:churn-endpoint v2 unapproved |
| L13 | rollback to a release whose contract was rewritten | PASS: refused on the fingerprint; restored bytes: allowed |
| L14 | override: until past / future | PASS: doctor "expired: refuse applies again"; policy.override expired, then active; back to refuse |
| L15 | DATA_WRITE: who swapped model.pt | PASS: the person named (the runner's own writes separate); also in the audit log bucket |
| L16 | BigQuery ML | PASS (full this time): approval records bq_model; integrity check exit 0; scoring runs. Found: the old registry entry churnbq-classifier is an orphan of the destroyed environment (Vertex cannot delete a BQML entry); Pipewright refused clearly; a fresh model name was used |

## Phase 8 (FR-103), open source

| # | Scenario | Result |
|---|---|---|
| F2 | contract --check --built-tables, test and prod | PASS: matches in both (another project) |
| F3 | run.submitted records the contract | PASS (churnf); churn, without a contract, records none |
| F4 | training with a changed built table | PASS: refused by the launcher ("last built by SQL sha256 ...") |
| F6 | scoring with a changed view (warn) | PASS: run.integrity warn; submitted with reads recorded |
| F7 | built table: changed SQL | PASS: --check DIFFERS, integrity check exit 1 |
| F9 | watch + a feature view's failed sync | PASS: "column tenure_months removed; column tenure added"; the sync failure reported; both restored |
| F11 | promotion with prod's view changed | PASS: refused before anything moved (prod pointer unchanged); restored: promoted, "2 sources match in prod" |
| F12 | rollback with the view changed | PASS: refused for the feature definitions |

## Enterprise (ent, private GKE: Kubeflow, KServe, CMEK, VPC, launcher on Cloud Run)

Rebuilt from qa: phase A then phase B (226 resources), 33 images mirrored, launcher and evidence worker built from
qa, seed data, churn-gke with a feature contract (1.1.0: two feature groups over loaded tables; one declared
sql_file, to run the job-history check under CMEK). Features policy: check, built_tables, record_reads,
fingerprint_rows, snapshot_days 1.

| # | Scenario | Result |
|---|---|---|
| E0 | contract and --check --built-tables under CMEK | PASS: the INFORMATION_SCHEMA.JOBS query ran (no CMEK refusal); "no job that wrote it" note for a loaded table; exit 0 |
| E0b | training via the launcher (Kubeflow) | PASS: definitions checked by the launcher (custom role), run.submitted contract 1.1.0 |
| E1 | KServe deploy pins an approved copy; the pod checks it | PASS: "6 approved files checked by SHA-256"; predict 0.636 |
| E2 | copy file swapped, scaled out | PASS: new pod held at its check, old pod serving; watch changed (incident); deploy-model rewrote the copy and rolled the pods; watch restored |
| E3 | renewer (CMEK, VPC) | PASS: healthy, 13 protected and renewed |
| E3b | scoring with record_reads, fingerprints, snapshots under CMEK | PASS after a fix: FOUND+FIXED: the snapshot DDL carried the job's CMEK key ("Cannot set kms key name in jobs with DDL statements"); now without it, and the snapshot is encrypted with the project's key (checked), expiring after 1 day. The per-snapshot error had kept the rest of the record |
| E4 | EV-06 and Fleet View | PASS: EV-06 pass with the feature contract (1.1.0, 2 sources), registered files, the KServe copy, the watch first seen and restored, 12 renewals; Fleet View collected, nothing outstanding |
| E5 | audit configs; plan | PASS: storage ADMIN_READ, DATA_READ, DATA_WRITE; terraform plan from qa: no changes |
| E6 | doctor | PASS: the feature definitions line |

After the CMEK fix (core 5541109 on qa): all suites again (771 core, every one passing in four runs); the open-source
launchers rebuilt and a scoring run with snapshots repeated: snapshots made, no regression.

## Not fully clean, said plainly

- One run of the core suite (89 s, under load) had one failing test; four runs after it all passed, so the test was
  not identified. Worth a look before the release: run the suite a few times in CI.
- Environment leftovers, not product defects: soft-deleted log buckets and the identity pool needed undeleting and
  importing after the earlier teardown (the rebuild guide covers it), and an orphan BigQuery ML registry entry from
  the destroyed environment (Vertex cannot delete it).
