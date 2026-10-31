# The bqml example

A churn model trained and scored in BigQuery ML, on the feature tables example's data. There is no pipeline code: Pipewright provides the pipelines for a use case with a `bqml` section. See the [churn](../churn/) example for every other option.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from bqml
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.
