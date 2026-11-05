# The churn example

The reference example. A PyTorch churn model: trained on a landing file or a schedule, its data checked against a contract, evaluated against gates and the current champion, registered, scored weekly, monitored for drift and performance, with retrain decisions made by a person. It sets every option a use case can set; the keys it leaves out appear as `# default` lines with the value Pipewright fills in.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from churn
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.

## The recommended way to feed it: feature tables

This example reads one prepared file or table with every column already in it. That is fine to start with. For a
model that will run for real, read the features from **feature tables or feature groups** instead
([docs/FEATURES.md](../../docs/FEATURES.md)): each row then gets every feature as it was at its cut-off time, the data
team owns the features, and the same definitions serve training and online scoring. The
[features](../features/) example starts that way (`--from features`).

To move this example onto feature tables, keep its contract and replace the source in `config/data.yaml` with
`data.features_from`: the entity table gives who, as of when, and the label; the feature tables give the rest.

```yaml
data:
  features_from:
    entity: { columns: [customer_id], time_column: snapshot_date }
    sources:
      - table: bq://${resources.bigquery_dataset}.my_churn_features_customer_profile
        features: [tenure_months, monthly_charges, total_charges, contract_length_months, num_products,
                   has_paperless_billing]
      - table: bq://${resources.bigquery_dataset}.my_churn_features_support_activity
        features: [num_support_tickets, avg_monthly_usage_gb]
  split: { strategy: time, time_column: snapshot_date }    # rows ordered in time: train on the past
```

A registered feature group works the same way (`- feature_group: customer_activity` with its `features`). Run
`pipewright validate` after the change: it checks every contract column has a source.
