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
| `roles/storage.objectViewer` (conditional: `pipewright/` and the team's release prefix) | the artifacts bucket | the manifest and release contracts |
| `roles/aiplatform.viewer` | the project | models, pipeline runs, feature group definitions |
| `roles/logging.viewAccessor` on a log view of Pipewright's labelled lines | the project's log bucket | run errors, without other logs |
| `roles/iam.serviceAccountTokenCreator` for the team's engineers | the agent account | impersonation |

Name it in the manifest:

```json
"tenants": { "retail": { "agent_service_account": "pw-agent-retail@<project>.iam.gserviceaccount.com", ... } },
"ai": { "local_tools": "allowed" }
```

`ai.local_tools: forbidden` turns the tools off in an environment (for example production). The starter's Terraform
creates the account and the grants when `tenants.<t>.agent_identity = true`.

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
pip install "pipewright-mlops[gcp,mcp]==1.3.0"
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
| `status`, `model_list`, `monitor_status`, `retrain_list`, `lineage` | what is running and how it is doing |
| `bootstrap_usecase` | drafts a use case from an entity table and chosen feature sources (schemas only) |
| `review_bundle` | the pull request description for the current branch |
| `sample_rows`, `profile` | always refuse: rows never reach an agent; aggregates are not offered |

Every call is logged in `.pipewright/mcp-log.jsonl`: transparency for you, not evidence (Cloud Audit Logs on the agent
account are the record).

## 4. The rules agents follow

- Features come from feature tables or Feature Store feature groups in the team's granted datasets
  (`data.features_from`); the use case's own table holds the entity rows and the label. A prepared table is allowed
  to start with, but is not the pattern. A missing feature goes back to the data team.
- Judgement checks in `validate` warn about rows ordered in time with a non-time split, dates as features, and
  features named after the label. A platform can set gate minimums per environment (`policies.evaluation.min_gates`).
- Agents never merge, release, approve or deploy: their identity cannot, and the playbooks say so.

## 5. Review

```bash
pipewright review --env test --base main > PR.md
```

What a release would change in Google Cloud, the settings that moved, the checks and their warnings, the feature
sources, and a section for the author's judgement calls. The `ci init` pull request workflow writes it to the run's
summary.

## Open source and Enterprise

Everything above is open source. Pipewright Enterprise adds what makes it provable for regulated organisations:
agent identities provisioned and enforced from the platform file, a hosted tool server inside the perimeter (with
aggregates under platform rules), the audit of every tool call as evidence, certified agent and model
configurations, and platform assistants.
