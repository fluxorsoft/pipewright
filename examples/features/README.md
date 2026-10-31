# The features example

The minimal example with most features read from feature tables (`data.features_from`): a run's source is the entity table (who, as of when, the label) and each row gets every feature as it was at its cut-off time. See the [churn](../churn/) example for every other option.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from features
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.
