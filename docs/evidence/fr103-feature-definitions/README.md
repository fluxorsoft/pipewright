# Feature definitions on BigQuery and Vertex AI Feature Store, live (FR-103)

Evidence for CONTEXT.md FR-103 and section 8.17 (phase 8). Run live on Google Cloud, europe-west2, 2026-11-07, and
torn down the same day. Synthetic data only: a `transactions` table (20,000 rows, 200 customers) and one feature,
`avg_spend_90d`, built four ways: a view, a materialized view, a table built by a query (`CREATE OR REPLACE TABLE ...
AS`, as a pipeline or dbt does) and a paused scheduled query. Prices: BigQuery on-demand $7.8125 per TiB, at least
10 MiB a query (the list price used in section 8.18). Scripts in `scripts/` (REST only).

## 1. What an object tells about its own definition

| Source of the feature | Defining SQL from the object (`tables.get`) | Where the SQL is otherwise |
|---|---|---|
| View | yes: `view.query` | |
| Materialized view | yes: `materializedView.query` (plus refresh settings) | |
| Table built by a query (CTAS, pipeline, dbt) | **no**: the table holds its schema only; its DDL is `CREATE TABLE (columns)` | the job that wrote it: `INFORMATION_SCHEMA.JOBS` (kept 180 days) has the full statement |
| Table built by a scheduled query | no | the transfer config: `params.query`, linked to the table by dataset and name template only |

## 2. Which signals move (false alarms versus real changes)

| Change | etag / lastModifiedTime | DDL (INFORMATION_SCHEMA) | Schema with descriptions | Schema names and types | Normalised SQL (view, MV) |
|---|---|---|---|---|---|
| Label added | moved | **moved** | same | same | same |
| Table description | moved | **moved** | same | same | same |
| Field description | moved | moved (table only) | moved | same | same |
| View re-created, whitespace only | moved | moved | **moved** (re-create drops field descriptions) | same | same (raw SQL moved) |
| New source rows, table rebuilt with the same SQL | moved | moved | moved | same | n/a |
| View SQL changed (90 to 60 days) | moved | moved | | same | **moved** |
| Table rebuilt with changed SQL | moved | same | same | same | n/a: **invisible on the table** |
| Materialized view refreshed (new data) | moved | same | same | same | same |

So: etag, lastModifiedTime and the DDL move on harmless edits and cannot be the fingerprint. The fingerprint of a
view or materialized view is its SQL with whitespace normalised, plus the schema as names, types and modes (never
descriptions). A table built by a query cannot show a definition change at all: a rebuild with new data and a rebuild
with changed SQL look the same. The job history tells them apart: the normalised SQL of each job that wrote the table
gave `aee2ba069985` for both 90-day rebuilds and `4c77cd1946f1` for the 60-day one. Reading it needs
`bigquery.jobs.listAll` on the project (a permission to grant knowingly) and costs at least 20 MiB a query.

The Admin Activity audit log records every change with who made it: a view's entry carries the new SQL; a table's
rebuild names its job; label and description edits are `PatchTable` entries. A materialized view's refresh writes no
entry.

## 3. Reproducing what a model read

- **`FOR SYSTEM_TIME AS OF` on a view silently returns today's values.** It does not reach the base tables: the view
  read as of before an INSERT returned the value after it (270.8014 both), with no error. Reproducing a feature that
  is a view needs its rows recorded (a snapshot or a copy) or the time travel written into the base tables' reads.
- After a view is re-created, time travel cannot read it from before the re-creation ("Cannot read before ..."): an
  old view definition is gone unless something recorded it.
- Time travel on a table works across `CREATE OR REPLACE TABLE` (the contents before the replace came back), within
  the dataset's window (168 hours here, the default; 48 to 168 can be set).
- A query over a view lists the base tables it read (`referencedTables`), so a scoring run can record them.
- Table snapshots: a table can be snapshotted (also as of a time in the window); **a view cannot** ("Allowed types:
  TABLE, SNAPSHOT"); a snapshot is immutable (an INSERT was refused). Snapshot storage is billed only for data that
  later differs from the base table.
- A row fingerprint (`BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t)))`) scans the whole table: 5,890 and 429,020 bytes
  here, billed at the 10 MiB minimum each, about $0.000075; at 1 GiB about $0.0076.

## 4. Vertex AI Feature Store

- A feature group records only a pointer: `bigQuerySource.inputUri`, the entity ID columns and the timestamp column.
  A feature records only its column (`versionColumnName`). A feature view records its groups, features and sync
  schedule. None holds SQL or a schema: the definition is whatever the BigQuery object is at sync time.
- The `etag` of groups, features and views changes on every read: useless as a signal. `updateTime` is stable and
  did not move when the view's SQL changed.
- **A definition change can be synced "successfully" and still not reach serving.** The view's SQL changed (60 to 5
  days: BigQuery said 3999.75 for c1), the sync reported OK (31 rows), and online serving still returned 270.8014. A
  sync replaces a value only when the row's `feature_timestamp` is newer than the one served: with an unchanged or
  fixed timestamp the old value stays; with `CURRENT_TIMESTAMP()` the next sync served 3999.75.
- An entity that dropped out of the new definition keeps serving its old values (c180 still served).
- A breaking change (a column renamed) fails the sync ("Unrecognized name: avg_spend_90d") and **serving goes on with
  the last good values**, with no error to the caller.

## 5. What detection costs

| Way to notice a definition change | Cost | Sees |
|---|---|---|
| `tables.get` (view and MV SQL, schema) | free (API) | views, materialized views, schema of any table |
| `INFORMATION_SCHEMA.TABLES` | 10 MiB a query, about $0.000075; hourly, about $0.054 a month per query | the same, plus noise from labels and descriptions |
| `INFORMATION_SCHEMA.JOBS` (what wrote a table) | 20 MiB a query, about $0.00015 | tables built by a query, for 180 days; needs `bigquery.jobs.listAll` |
| Data Transfer `transferConfigs.get` | free | scheduled queries |
| Admin Activity audit log | free to read (kept 400 days; longer with the FR-102 log bucket) | every change and who made it |

Spike cost: the Bigtable online store ran about 25 minutes at one node (about $0.45 at $1.07 per node-hour); the
queries stayed within the free tier.
