# The churn example

The reference example. A PyTorch churn model: trained on a landing file or a schedule, its data checked against a contract, evaluated against gates and the current champion, registered, scored weekly, monitored for drift and performance, with retrain decisions made by a person. It sets every option a use case can set; the keys it leaves out appear as `# default` lines with the value Pipewright fills in.

Start a use case from it:

```bash
pipewright new-usecase --name <name> --tenant <tenant> --owner <team> --from churn
```

The files use sample values (`my-tenant`, `my-team` and the names in `example.yaml`) that `new-usecase` replaces.
`pipewright resolve --env <env> --usecase <name>` shows the result with every default filled in. This README is not
copied into your use case.
