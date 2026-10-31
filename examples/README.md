# Examples

Each folder is a complete use case. `pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from
<example>` copies one into your MLOps repository's `usecases/<name>/`, with your names in place of the sample ones.

| Example | What it shows |
|---|---|
| [churn](churn/) | The reference: a PyTorch model trained, evaluated, registered, scored and monitored, with every setting a use case can make. Keys it leaves out are shown as `# default` lines with the value Pipewright fills in. |
| [minimal](minimal/) | Only what is required. Start here when you know what you want. |
| [features](features/) | Features read from feature tables, each as it was at the row's cut-off time. |
| [bqml](bqml/) | A model trained and scored in BigQuery ML: no pipeline code. |
| [ray](ray/) | The churn model's tuning trials run in parallel on a Ray cluster that lives for one step. |

## Sample values

The examples are real YAML: they read and highlight on GitHub and are checked against the schema. They use sample
values that `new-usecase` replaces: `my-churn` (the use case's name; `my-churn-bq` and the like in the other
examples), `my-tenant`, `my-team`, `my_churn_` (the table prefix) and `my-churn-classifier` (the model). Each
example's `example.yaml` lists its sample values and the files that use them.

## What you may leave out

Almost everything has a default. A use case needs its name, owner, tenant, pipelines (or a `bqml` section), a data
contract and a label. For the rest:

- `pipewright resolve --env <env> --usecase <name>` shows the use case with every default filled in.
- `pipewright explain <key>` says what a key means, for example `pipewright explain model.serving.replicas`.
- The churn example sets most keys explicitly and shows the others as `# default <key>: <value>` lines; a test
  keeps those values equal to what Pipewright fills in.
