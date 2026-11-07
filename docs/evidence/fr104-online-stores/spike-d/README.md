# FR-104 spike (d): Cloud SQL capacity, the sync's swap, and BI Engine refreshes

Run live in pw-ebook, europe-west2, on 2026-11-07, and torn down the same day. Synthetic data only: 1M entities x
30 numeric features, two generations exported to Parquet by BigQuery. Results and what the build takes from them are
in CONTEXT.md section 8.19 ("Spike (d) results").

## Setup

- Cloud SQL for PostgreSQL 16, Enterprise edition, `db-custom-1-3840`, zonal, 10 GB SSD, private IP over Private
  Services Access only, CMEK, `cloudsql.iam_authentication=on`, SSL required. No password user was used by any test
  identity; the database and schemas were created by an IAM user given `cloudsqlsuperuser` with
  `gcloud sql users assign-roles`, revoked afterwards.
- A load agent on an e2-standard-4 VM in the same VPC (`agent.py`), as its own IAM database user, polling plans from a
  bucket. Lookups are open-loop at a fixed rate.
- The lookup service (`lookup.py`) on Cloud Run: 1 vCPU, 512 MiB, 1 to 5 instances, concurrency 80, a pool of 8, Direct
  VPC egress to the private IP, internal ingress, IAM invocation with an ID token.

## Runs (`results/`)

| File | What |
|---|---|
| `s01_setup`, `s02_revoke` | schemas per tenant over IAM login; the bootstrap identity's extra membership revoked |
| `s03_load_a`, `s04_load_c` | first loads: 1M rows by `COPY` in 54 s and 51 s |
| `s05_facts` | `max_connections` 100, SSL in use, tenant isolation, the `public` schema catch |
| `s06_rate_direct` | 50 to 1,200 lookups a second direct |
| `s07_swap_rename`, `s08_swap_view` | 200 a second during a load and a swap (rename, view), read in the loading process |
| `s09_rate_http`, `s10_rate_http` | the lookup service at 50 to 800 a second (`s09` starts with 403s while the invoker grant propagated) |
| `s11_swap_rename_http` | 100 a second through the lookup service during a load and a swap |
| `monitoring_*` | database CPU and connections, Cloud Run instances and latencies, per minute |
| `s12_bi_reload`, `s13_bi_steady` | BI Engine acceleration and bytes billed after a reservation and after a refresh |
| `s14_bi_capped`, `s15_bi_capped_warm` | `maximumBytesBilled` refuses even lookups BI Engine would serve |

`runcond2.py` is the Cloud Run IAM condition test (refused even for a matching name; an unconditional control grant
worked). `data.py` made the tables and the exports, `plan.py` sent plans and summarised results, `mon.py` read Cloud
Monitoring, `bireload.py` ran the BI Engine test. They reuse the REST helper `g.py` from the first spike (the same as
`../rest_helper.py`).
