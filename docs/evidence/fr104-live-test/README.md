# FR-104 live test: the online stores, end to end

Run 2026-11-07 on branch `phase-9`, then torn down. Two environments, the churn example (5,000 entities, 8 features
from a feature group over a view and a table built by a query) as two use cases: `churnf` on Cloud SQL through
Pipewright's lookup service, `churnfq` on BigQuery with BI Engine.

- **starter** (`pw-ebook`, europe-west2): the open-source starter with `online_store.yaml`, tenant artifact isolation on.
- **ent** (`pipewright-enterprise`, us-central1): Enterprise's private-managed blueprint: CMEK required by
  organization policy (Cloud SQL included), Cloud Run internal ingress and all-traffic egress required by policy, no
  public IPs or internet egress, Google APIs over Private Service Connect, VPC Service Controls (dry run).

## What was shown

| Step | starter | ent |
|---|---|---|
| One-time setup as the admin IAM user (no password user) | database and 5 grants, 26 s | database and 5 grants, through PSC, CMEK |
| Scheduled sync: Cloud Scheduler, launcher, the tenant's Cloud Run job | Cloud SQL 5,000 rows in 6.5 s, swap 13.6 ms; BigQuery 3.0 s | Cloud SQL 8.6 s, swap 11.3 ms; BigQuery 3.4 s |
| The hourly cron itself | fired at 18:00 for both | fired at 19:00 for both |
| Lookup service created by the launcher at `deploy-model` | IAM-only, the database's CA pinned, one warm instance, CPU boost | the same, internal ingress, all-traffic egress, CMEK |
| Predictions by entity ID through Agent Platform endpoints | both stores, identical scores for the same customers; an unknown ID answered per entity | BigQuery: yes. Cloud SQL: no, by design of the guardrails: internal ingress refuses Agent Platform's public endpoints (FR-92) |
| The lookup service and Cloud SQL from inside the network | | 50 of 50 calls, 150 of 150 entities found, p50 39 ms |
| Latency through the endpoint, from a laptop | Cloud SQL p50 99 ms, BigQuery p50 211 ms (p99 is the laptop's network) | |
| Predictions while a sync swapped the table | 750 of 750 succeeded, swap 17.8 ms | |
| `sync.mode: changed` | exactly the 10 changed rows written, none removed | |
| Export retention (`keep: 2`) | the third generation pruned the first | |
| `online.billed` | fired with BI Engine off: 30 lookups, 300 MiB (10 MiB minimum each) | |
| doctor, Fleet View, EV-17 | costs, instance, service, last sync; the Fleet View cell; EV-17 pass with 4 syncs | doctor; the Fleet View cells (feature contract 1.2.0) |
| VPC Service Controls | | dry run: no would-be violation from the online stores in 3 hours (enforcement not applied) |

## Found live and fixed

See CONTEXT.md section 8.19 "Build and live test", and `starter/grants.md` for every grant with the ones the test found.

## Files

`starter/` and `ent/`: doctor, the sync records and a manifest, the lookup service as created, the prediction runs
(per-second latency and errors), the in-network probe, the EV-17 control, the Fleet View extracts, the grants. Values
are counts, timings, names and digests; no feature values or personal data.
