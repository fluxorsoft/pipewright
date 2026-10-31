# The ray example

The churn example's tuning trials run in parallel on a Ray on Agent Platform cluster that exists only for that step of a run. Needs Ray enabled in your environment. See the [churn](../churn/) example for every other option.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from ray
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.
