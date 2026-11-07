# Features: the recommended way

For ML teams writing a use case, and for the people who produce the features they read. How Pipewright reads
features, how a feature table must look, which source to choose, and the mistakes to avoid. The design is in
[CONTEXT.md](../CONTEXT.md), section 8.11.

## Why read features this way

A model is only as trustworthy as the data it trained on, and the most common silent failure is **leakage**:
training on a value that was not yet known at the time the model would have made its prediction. Pipewright reads
features so that this cannot happen, and so that every model can say exactly what it trained on.

| What you get | How |
|---|---|
| Point-in-time correct training data | Each row gets the newest value of each feature dated at or before that row's cut-off, never one from after it |
| Features defined once | Owners publish a feature once (a column of a feature table, or a registered feature); every use case reads that definition instead of writing its own query |
| Training and batch scoring read the same thing | Both run the same query; scoring uses the scoring date as the cut-off |
| A record of what each model read | A `data.features.resolved` event per run: tables (and feature groups), features, cut-off rule, query job, time |
| Access decided by the platform | The platform lists the feature datasets each team may read; `validate` refuses anything else |

## The three ways to give a use case its data

| Way | When to use it | In the YAML |
|---|---|---|
| **A prepared table or view** | A small team, one use case, data nobody else reuses. Fine to start with | the run's `source_uri` points at it; no `features_from` |
| **Feature tables** | Features that other use cases also need, or any time correctness over time matters (it nearly always does) | `data.features_from.sources[].table` |
| **Feature groups** (Agent Platform Feature Store) | Features your organisation registers centrally, so they are named, owned and catalogued | `data.features_from.sources[].feature_group` |

**Recommendation:** use feature groups when your organisation registers features, feature tables otherwise. Keep a
prepared table only for experiments. Your platform may require feature tables (`features.required` in the
environment manifest); `validate` then tells you which model features still come from elsewhere.

## What a feature table looks like

A feature table belongs to whoever produces the features (a data team, or your own team). Pipewright only reads it.

| Rule | Why |
|---|---|
| One or more **entity columns** (for example `customer_id`) | How rows are matched to the entity table |
| A **timestamp column** (`feature_timestamp` by default) saying when each value became true | The cut-off is compared with it |
| **One column per feature** | A feature is a column; the column's type must match the data contract |
| **A new row each time a value changes**; never update a row in place | Updating in place destroys history, so old training rows would see today's value |
| **One row per entity per timestamp** | Two rows with the same timestamp make the choice arbitrary |
| The timestamp is when the value was **known**, not when it was about | A balance computed on 3 March for February is known on 3 March |

The example tables in `examples/features/data/` follow these rules, including rows dated after each cut-off that
must never be used.

## The use case

```yaml
# config/data.yaml
data:
  contract:                     # unchanged: every column the model and the checks see, wherever it comes from
    columns:
      - { name: customer_id,   type: string, required: true }   # entity table
      - { name: snapshot_date, type: date,   required: true }   # entity table: the cut-off
      - { name: churned,       type: integer, allowed: [0, 1] } # entity table: the label
      - { name: tenure_months, type: integer }                  # from a feature group
      - { name: num_support_tickets, type: integer }            # from a feature table
  label: { column: churned }
  split: { strategy: time, time_column: snapshot_date }
  features_from:
    entity: { columns: [customer_id], time_column: snapshot_date }
    sources:
      - feature_group: customer_profile           # registered: table and columns come from the registry
        features: [tenure_months]                 # registered feature names
      - table: bq://features_core.support_activity
        features: [num_support_tickets]
        # entity_columns: [cust_id]               # when the table names the entity column differently
        # time_column: valid_from                 # default feature_timestamp
        # prefix: support_                        # when two sources have a feature of the same name
```

- **The run's source is the entity table:** who, as of when, and the label. Start a run with
  `pipewright run --source-uri bq://<project>.<dataset>.<entity table>` or a schedule. Landing files cannot be an
  entity table, so `validate` refuses a landing trigger with `features_from`.
- **The cut-off:** a `date` cut-off is midnight UTC at the start of that day, so values dated that day are not used.
  Choose the cut-off as the moment the prediction would have been made.
- **Scoring:** give the scoring table the cut-off column as the scoring date. It makes scores reproducible. A table
  without the column is cut off at the query time (the contract must then mark the column `required: false`).
- **Nothing else changes:** validation, splits, training, gates, monitoring and the model card work as before, on
  Agent Platform Pipelines and on Kubeflow Pipelines in private GKE alike.

`pipewright new-usecase --from features` starts from a working example.

## Checks

| When | What is checked |
|---|---|
| `pipewright validate` (offline) | The block against the data contract; a source names a table or a feature group; no feature twice without a prefix; no feature that is the label or an entity or cut-off column; feature tables in the tenant's datasets; with `features.required`, every model feature from `features_from` |
| `pipewright doctor` (online) | Each table exists with the entity, time and feature columns and types the contract accepts; each feature group resolves through the registry and its table is in the tenant's datasets |
| Every run (`read_source`) | The same dataset rule; a group is resolved again, so a group its owners moved is followed; rows with no value are logged |

## Mistakes to avoid

| Mistake | What happens | Instead |
|---|---|---|
| A view that joins the "latest" features | Every training row sees today's values: leakage | Name the feature tables in `features_from` |
| Feature rows updated in place | History is lost; old rows read new values | Append a row per change |
| A timestamp of the period the value describes | Values look known before they were | Use when the value became available |
| The label or the target's outcome as a feature | `validate` refuses it | The label comes from the entity table |
| Copying a feature's SQL into your own table | Two definitions drift apart | Read the owner's feature table or group |
| Scoring without a scoring date | Scores cannot be reproduced | Put the scoring date in the cut-off column |

## What the owners of a feature do

Compute the feature, append a row per change, keep the timestamp honest, keep the table's schema stable (add
columns; do not rename or retype them), register the group if your organisation uses the Feature Store, and handle
erasure requests in their tables (`pipewright data erase` covers only a use case's own tables).

To register a feature table as a feature group (1.4.0), describe it in a file and run
`pipewright features register --env <env> --file feature_groups.yaml` (`--dry-run` first). Run it as the data team,
the identity that owns the tables. It is safe to repeat: a group or feature that exists is left as it is, never
changed or deleted, and messages name groups, tables and features, never values.

```yaml
customer_activity:
  table: pw_retail.customer_activity        # dataset.table in the environment's project, or project.dataset.table
  entity: [customer_id]
  description: Monthly purchase behaviour per customer
  features:
    recency_days: Days since the last purchase
    orders_12m: Orders in the last twelve months
```

## Building feature tables: dbt or Dataform

Pipewright reads feature tables; it does not build them. Any tool works if the table follows the rules above. Two
tools make that easy on BigQuery:

| | Dataform | dbt |
|---|---|---|
| Runs | Managed, inside Google Cloud, in the BigQuery console; nothing to host | dbt Core (free) needs a runner such as Cloud Run jobs, Cloud Composer or CI; dbt Cloud is managed and paid |
| A row only when a value changes | An incremental table with your own MERGE logic | Snapshots, out of the box |
| Tests | Assertions | Tests, plus packages such as dbt-utils |
| Best when | The organisation is Google Cloud only and wants nothing extra to run, including in private environments | The data team already uses dbt, or works across warehouses |

Whichever you choose: partition by `feature_timestamp`, cluster by the entity column, test that (entity,
timestamp) is unique and never null, check freshness, keep the code in the feature team's own repository with
reviews, and put the tables in a dataset the platform lists for the tenant (`tenants.<t>.features.datasets`).
Avoid loose scheduled queries: no tests, no dependencies, no review.

**dbt: a row each time a value changes.** A snapshot records each change; `dbt_valid_from` is when the snapshot saw
it, which is when the value became known.

```sql
-- snapshots/customer_profile_snapshot.sql
{% snapshot customer_profile_snapshot %}
{{ config(target_schema='features_staging', unique_key='customer_id', strategy='check',
          check_cols=['tenure_months', 'monthly_charges', 'contract_length_months']) }}
select customer_id, tenure_months, monthly_charges, contract_length_months
from {{ source('crm', 'customers') }}
{% endsnapshot %}
```

```sql
-- models/features/customer_profile.sql: the feature table Pipewright reads
{{ config(materialized='table', cluster_by=['customer_id'],
          partition_by={'field': 'feature_timestamp', 'data_type': 'timestamp', 'granularity': 'day'}) }}
select customer_id, dbt_valid_from as feature_timestamp, tenure_months, monthly_charges, contract_length_months
from {{ ref('customer_profile_snapshot') }}
```

```yaml
# models/features/customer_profile.yml
models:
  - name: customer_profile
    tests:
      - dbt_utils.unique_combination_of_columns: { combination_of_columns: [customer_id, feature_timestamp] }
    columns:
      - { name: customer_id, tests: [not_null] }
      - { name: feature_timestamp, tests: [not_null] }
```

**Dataform: one row per entity per run.** An incremental table appends each run's values, stamped with the time
they were computed. Run it on a schedule (for example daily).

```sql
-- definitions/features/support_activity.sqlx
config {
  type: "incremental",
  schema: "features",
  bigquery: { partitionBy: "DATE(feature_timestamp)", clusterBy: ["customer_id"] },
  assertions: { uniqueKey: ["customer_id", "feature_timestamp"], nonNull: ["customer_id", "feature_timestamp"] }
}

select
  customer_id,
  current_timestamp() as feature_timestamp,
  countif(created_at >= timestamp_sub(current_timestamp(), interval 30 day)) as num_support_tickets
from ${ref("support_tickets")}
group by customer_id
```

**History before the first run.** A snapshot or incremental table only knows values from its first run onwards.
To train on earlier periods, backfill with a query that computes each past value from the data that was available
at that time, stamped with that time; never stamp backfilled rows with today's date.

## Synthetic copies of feature tables

When a team switches synthetic data on with `mode: relational` (see the README's *Synthetic data*), the engine
learns the entity table together with the feature tables named in `data.features_from` and any event tables in
`synthetic.sequences`, as one model. That keeps what the rules on this page protect:

| What is kept | How |
|---|---|
| Keys that match across tables | Keys are never learned; new keys are made once and used in every table |
| History per entity | A feature table's rows per entity are learned as a sequence, not as independent rows |
| Timing against each cut-off | The gates are measured on the point-in-time join of the synthetic tables, the same join training uses |
| Integrity | A duplicate key or a child row without its entity fails the candidate; rows per entity and the share of history before the cut-off are recorded next to the real tables' |

Each synthetic table goes to its own output table (`synthetic.output_tables`, by source name), so a team can train
on synthetic data with the same `features_from` block pointed at those tables.

## The feature contract: definitions that cannot change under an approved model (1.4.1)

An approval pins the model, its evaluation and the release; the feature contract pins the definitions of the
features it reads. Definitions are immutable, values are not: new rows under the same definition are fine; changing
how a feature is calculated needs a new contract version, a release and a retrain.

**Write it.** `pipewright features contract --env test --usecase churn` reads each `data.features_from` source as
it is now (a feature group is followed to its BigQuery table) and writes `config/feature_contract.yaml`: per source
its kind, the SHA-256 of its SQL and of its schema, the schema itself and each feature's type. Add descriptions;
commit the file. Run it again after a change: it says what changed and sets the version (a removed or retyped column
is major, changed SQL or an added column minor, descriptions a patch). `--check` compares without writing: exit 0
same, 1 different, 2 cannot check, for CI.

**Where the SQL comes from.** A view or materialized view carries its SQL. A table built by a query (a pipeline,
dbt, Dataform, `CREATE TABLE ... AS`) does not: give its `sql_file` (the query, kept with the use case) or its
`scheduled_query` (the transfer config name). With `policies.integrity.features.built_tables: true` the SQL of the
job that last wrote the table must match; without it such a table is checked by schema only. The fingerprint ignores
whitespace, leading comments, the `CREATE TABLE ... AS` prefix and the source's own project (so test and prod
compare equal), and never includes descriptions, labels or etags, which move on harmless edits.

**Turn the check on** (platform team, `policies.integrity.features` in the manifest):

| Key | Default | Then |
|---|---|---|
| `check: on` | off | training is refused on a definition that differs from the release's contract; approval and promotion record the definitions and refuse a difference; deploy, scoring (with `inference` on), the watch, `integrity check` and rollbacks compare with the approval. A difference is judged by `policies.integrity.mode` (refuse, or warn with `until`) |
| `built_tables: true` | false | tables built by a query are checked by their last job's SQL (needs `bigquery.jobs.listAll`; at least 20 MiB a check, about $0.00015) |
| `record_reads: true` | false | `run.submitted` of a scoring run records each source's base tables, their last-modified time and row count, and the read time |
| `fingerprint_rows: true` | false | and a row fingerprint per source (one full scan each: about $0.0076 per GiB, 10 MiB minimum) |
| `snapshot_days: N` | 0 | and an immutable snapshot of each base table in the use case's dataset, expiring after N days |

**Reproducing what a model read.** Time travel works on tables for 2 to 7 days (the dataset's window), even across
`CREATE OR REPLACE TABLE`, but **not through a view**: `FOR SYSTEM_TIME AS OF` on a view returns today's values
without an error, and a view cannot be snapshotted. That is why `record_reads` records a view's base tables, and
snapshots are taken of those. Beyond the window, only snapshots (or your own history tables) can rebuild the inputs.

**Feature Store.** A feature group and a feature view hold only a pointer to the BigQuery table; the definition is
whatever the table is at sync time. Found live: a sync of a changed definition reports success but keeps serving the
old value unless the row's `feature_timestamp` is newer, entities that left the definition keep serving their old
values, and a breaking change fails the sync while serving goes on with the last good values. With the check and
the watch on, a feature view whose newest sync failed is reported (an incident). After a definition change, resync
with rows stamped newer than the last sync (for example `CURRENT_TIMESTAMP()` as `feature_timestamp` in the view).

## Serving features online

The same feature tables serve online lookups. Bigtable (Feature Store) is the default; Cloud SQL and BigQuery with BI
Engine cost far less at modest traffic, and the Parquet export lets a team load a store of its own. Each sync records
the feature contract's version in its manifest, so serving can say which definition it served. Choosing and setting
up a store, costs, performance and the pattern for your own sync: [ONLINE_STORES.md](ONLINE_STORES.md).
