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
- **Nothing else changes:** validation, splits, training, gates, monitoring and the model card work as before.

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
