# Playbook: add a drift alert

Intent: "email <people> when <use case> drifts".

## Steps

1. `resolve` the use case: read `monitoring` and `retraining` as they are now.
2. In `config/operations.yaml`, set `monitoring.drift: { enabled: true, method: psi, threshold: <0.1 to 0.25> }`
   and add the addresses to `monitoring.alerts.notify` with `monitoring.alert` in `monitoring.alerts.events`.
   Add `run.failed` too if the team wants to hear about failed runs.
3. Keep retraining a decision: `retraining.mode: propose` (a person approves); never `candidate` unless asked.
4. `validate`, then `plan`: the plan shows the alert policy `pipewright alerts sync` will create.
5. Open a pull request with `pipewright review`; after the merge the team runs `pipewright alerts sync`.

## Done when

`validate` passes, the review bundle shows the alert policy, and a person has the pull request.
