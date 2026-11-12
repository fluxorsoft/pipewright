# AI build assistance

Build and change use cases with the coding agent you already use (Claude Code, Gemini CLI, and others that speak
the Model Context Protocol), safely. The agent writes, Pipewright verifies, a person decides.

```
intent ("score customers daily for churn, email me on drift")
  -> the agent reads the context pack (APP_CONTEXT.md) and a playbook
  -> it inspects and verifies through Pipewright's tools (pipewright mcp): metadata only, never rows
  -> it drafts YAML on a branch; validate, doctor and plan pass
  -> it opens a pull request with the review bundle (pipewright review)
  -> a person approves; CI releases
```

New in Pipewright 1.3.0. Everything here is additive and off until used.

## 1. Give agents their own identity (platform team, recommended)

An agent should not borrow a person's access. Give each team an agent service account with metadata-only roles, and
let the team's engineers impersonate it:

| Grant | On | Why |
|---|---|---|
| `roles/bigquery.metadataViewer` | the team's dataset and its feature datasets | schemas, row counts, partitions; no table data |
| `roles/bigquery.jobUser` | the project | dry runs and job metadata |
| `roles/storage.objectViewer` (conditional: `pipewright/` and `contracts/`) | the artifacts bucket | the manifest and release contracts |
| `roles/aiplatform.viewer` | the project | models, pipeline runs (with their failed steps), feature group and view definitions |
| `roles/cloudscheduler.viewer`, `roles/monitoring.viewer` | the project | the plan of a use case's schedules and alerts |
| a custom role: `storage.buckets.get`, `artifactregistry.repositories.get`, `iam.serviceAccounts.get`, `run.services.get`, `pubsub.topics.get`, `pubsub.subscriptions.get` | the project | `doctor`'s checks of the platform's resources, read as configuration |
| `roles/iam.serviceAccountTokenCreator` for the team's engineers | the agent account | impersonation |

Name it in the manifest:

```json
"tenants": { "retail": { "agent_service_account": "pw-agent-retail@<project>.iam.gserviceaccount.com", ... } },
"ai": { "local_tools": "allowed" }
```

`ai.local_tools: forbidden` turns the tools off in an environment (for example production). The starter's Terraform
creates the account and the grants when `tenants.<t>.agent_identity = true`.

Run for an agent, `doctor` also checks the identity cannot release, run, read table data, upload or deploy, and warns
if it can. Refused attempts and BigQuery queries are always in Cloud Audit Logs; to record every metadata read too,
turn on Data Access audit logs for BigQuery, Agent Platform and Cloud Storage (`agent_audit_logs = true` in the
starter; billed by volume).

Google Cloud's IAM then enforces "metadata only" on every route the agent might take, Pipewright's tools or not, and
Cloud Audit Logs record each call as the agent account acting for the person. Calls through Pipewright's tools also
carry a `pipewright-mcp/<version> agent=<name>` user agent and BigQuery job labels (`pipewright-mcp`,
`pipewright-agent`, `pipewright-session`).

## 2. Build the context pack

```bash
pipewright context build --env test
```

Writes `APP_CONTEXT.md` (teams, datasets, feature datasets, policies, rules, use cases, where their features come
from), the playbooks in `.pipewright/playbooks/`, and a marked Pipewright block in `AGENTS.md`, `CLAUDE.md` and
`GEMINI.md`; text outside the block is yours and is never touched. Commit them. `pipewright context check --env test`
fails when the pack is stale, and the pull request workflow from `ci init` runs it once `APP_CONTEXT.md` exists.

## 3. Connect the agent

```bash
pip install "pipewright-mlops[gcp,mcp]==1.4.3"
pipewright mcp --env test --tenant retail --print-config
```

Add the printed settings to your agent's MCP configuration. The server runs on your machine, over stdio, as the
agent account (or as you, if the manifest names none, and it says so).

| Tool | What it does |
|---|---|
| `environment` | the environment's teams, datasets, policies, and who the tools act as |
| `validate`, `resolve`, `explain` | offline checks (including the judgement checks), the effective configuration, any key's meaning |
| `doctor`, `plan` | live read-only checks; what a release would change (tables, schedules, alerts) |
| `inspect_table` | schema, row count, partitioning; personal columns flagged; descriptions marked untrusted |
| `list_features` | Feature Store feature groups (features, online views) and the team's feature tables: definitions only |
| `status`, `runs`, `model_list`, `monitor_status`, `retrain_list`, `lineage` | what is live, the latest runs (failed steps and their value-free first error line; step logs are not read), and how models are doing |
| `ab_status` | 1.4.3: a use case's A/B test: state, arms, allocation, the newest analysis per arm (results per value of a column are withheld), the decision and the next step. Read only: a person starts, changes and decides a test |
| `serving_health` | 1.4.3: how a served use case is watched: health, capture and online monitoring as resolved, the platform's serving rules, the health alert policies in force and rollback proposals waiting for a person |
| `features_check` | 1.4.3: the feature contract against the live feature sources (definitions and schemas), as `features contract --check` |
| `bootstrap_usecase` | drafts a use case from an entity table and chosen feature sources (schemas only) |
| `review_bundle` | the pull request description for the current branch |
| `sample_rows`, `profile` | always refuse: rows never reach an agent; aggregates are not offered |

There is no integrity tool: `pipewright integrity check` reads model files and audit logs, which an agent identity
must not read. People and CI run it.

The agent account reads `ab-tests/` and `cases/` in the artifacts bucket from the 1.4.3 platform files (starter and
Enterprise); with older platform files `ab_status` and `serving_health` say which grant is missing.

Every call is logged in `.pipewright/mcp-log.jsonl`: transparency for you, not evidence (Cloud Audit Logs on the agent
account are the record).

## 4. The rules agents follow

- Features come from feature tables or Feature Store feature groups in the team's granted datasets
  (`data.features_from`); the use case's own table holds the entity rows and the label. A prepared table is allowed
  to start with, but is not the pattern. A missing feature goes back to the data team.
- Judgement checks in `validate` warn about rows ordered in time with a non-time split, dates as features,
  features named after the label, and a team with feature datasets whose use case takes no feature from them. A
  platform can set gate minimums per environment (`policies.evaluation.min_gates`).
- A platform can tighten what reaches an agent in the manifest's `ai.data_classes`: `offline: true` (no tool calls
  Google Cloud; agents work from the manifest, the repository and the context pack) and `deny_names` (table and
  column names withheld, as patterns such as `*ssn*`).
- Since 1.4.3 the context pack also states the environment's rules for what 1.4 added: integrity, registration
  through the launcher, `policies.ab_test`, `policies.serving` (capture limits, health required, scale to zero), the
  online store and the lookup gateway, and how a served model is watched; playbooks cover preparing an A/B test,
  watching a served model, serving with online features and a changed feature source. Judgement checks warn about an
  A/B test without a business guardrail, shorter than a week, giving the treatment more than half, or with nobody
  named to decide, and about capturing more than half of live calls.
- Agents never merge, release, approve or deploy: their identity cannot, and the playbooks say so. An agent prepares
  an A/B test; a person runs `ab start`, `ab set`, `ab decide` and `ab conclude`.

## 5. Review

```bash
pipewright review --env test --base main > PR.md
```

What a release would change in Google Cloud (since 1.4.3 also an A/B test and who decides it, serving health,
capture, online monitoring, a pool move, scale to zero and a new feature contract), the settings that moved (including
`model.serving`, `ab_test` and `feature_contract`), the checks and their warnings, the feature
sources, and a section for the author's judgement calls. The `ci init` pull request workflow writes it to the run's
summary.

## Open source and Enterprise

Everything above is open source. Pipewright Enterprise adds what makes it provable for regulated organisations:
agent identities provisioned and enforced from the platform file, a hosted tool server inside the perimeter (with
aggregates under platform rules), the audit of every tool call as evidence, certified agent and model
configurations, and platform assistants.
