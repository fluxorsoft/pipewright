# The minimal example

Only what Pipewright requires, plus one evaluation gate, on the churn example's training code. Everything else has its default; see the [churn](../churn/) example for every option.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from minimal
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.
