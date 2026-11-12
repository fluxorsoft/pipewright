# Playbook: serve with online features

Intent: "callers send a <entity> ID and get a score from <use case>".

## Steps

1. `environment`: the team's online store (Teams in `APP_CONTEXT.md`). Without one, stop and say so in the review:
   the platform team chooses the store (Bigtable, Cloud SQL, BigQuery with BI Engine or the team's own) by cost.
2. `list_features`: the model's features must come from feature tables or groups (`data.features_from`), the same
   definitions training reads. The store holds their latest values; never compute features in the serving code.
3. In the use case, `model.serving: { enabled: true, ... }` and its `lookup` block. Where callers or the store are
   inside the network (Cloud SQL, the team's own store, a private endpoint), use `lookup.mode: gateway`: callers send
   IDs to the lookup gateway, which reads the features and calls the endpoint. List the callers in `lookup.callers`.
4. Keep features identical between training and serving: no feature only the serving path has.
5. `validate` (it says what the platform must grant, such as `export_readers` for the team's own store), then `plan`.
6. Open a pull request with `pipewright review`. A person deploys with `pipewright deploy-model` after approval.

## Done when

`validate` passes, the review bundle shows online serving and the store, and a person has the pull request.
