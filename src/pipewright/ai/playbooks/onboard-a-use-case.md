# Playbook: onboard a use case

Intent: "score <entity> <when> for <outcome>, and tell me when it drifts".

## Steps

1. **Read the context pack** (`APP_CONTEXT.md`): the team (tenant), its dataset, feature datasets and the rules.
2. **Find the entity rows and the label.** `inspect_table` on the table that holds one row per entity and cut-off
   with the outcome (the label). Note its cut-off column (a date or timestamp) and the personal-data columns.
3. **Find the features.** `list_features`: the feature groups and feature tables the team may read. Choose the
   features the model needs. If one is missing, do not compute it: note it for the review, the data team adds it.
4. **Draft the use case** on a branch: `bootstrap_usecase` (or `pipewright new-usecase --from-bq ... --label ...`;
   a hosted tool server returns the files: write them as they are), then edit `config/data.yaml`:
   - the contract covers the entity table's columns; the label; `personal_data_columns`;
   - `split: { strategy: time, time_column: <cut-off> }`;
   - `features_from: { entity: { columns: [<key>], time_column: <cut-off> }, sources: [...] }`.
5. **Set gates and monitoring** at least as the context pack's rules ask: `evaluation.gates`, `monitoring.drift`,
   `monitoring.alerts.notify`, `retraining.mode: propose`.
6. **Verify:** `validate`, then `doctor`, then `plan`. Fix every problem and every judgement-check warning, or say
   in the review why a warning stands.
7. **Rebuild the context pack** (`pipewright context build --env <env>`): it lists the use cases, so CI finds it
   stale until it is rebuilt with the new one.
8. **Open a pull request** with `pipewright review` as its description, and list your judgement calls with reasons.

## Done when

- `validate` passes with no judgement warning you have not explained;
- `doctor` passes for the environment;
- the review bundle lists what changes in Google Cloud and your judgement calls;
- a person has the pull request. You do not merge, release, approve or deploy.
