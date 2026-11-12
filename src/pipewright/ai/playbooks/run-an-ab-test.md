# Playbook: prepare an A/B test

Intent: "test whether <candidate version> is better for the business than the champion of <use case>".
An agent prepares the test; a person starts it, changes it and decides it.

## Steps

1. `ab_status` the use case: one test at a time, so there must be no active test. `model_list` shows the champion
   (the control) and the candidate; the candidate must have passed its gates.
2. Read the environment's rules in `APP_CONTEXT.md` (policies.ab_test): the largest treatment share, the shortest
   test, the business guardrails every test declares, the approver groups.
3. Write `config/ab_test.yaml` from the churn example's: `enabled: false`, a `name` used once, the `unit` (a column of
   the data contract), `allocation.treatment_bp` (start small: 500 to 2000), the business `metric` with its
   `source`, `time_column`, `window_days` and `minimum_effect`, at least one business guardrail, and
   `decision.approvals.required`. Decide the metric before the test, never after seeing results.
4. In the environment file that runs it (`environments/<env>.yaml`), set `ab_test.enabled: true` and
   `arms.treatment.model_version`. For a batch test, add `ab_test` and `arm` (STRING, NULLABLE) to the predictions
   table's schema.
5. `validate`, then `plan`. Write in the review's judgement calls why this metric, this share and this length (one
   weekly cycle at least).
6. Open a pull request with `pipewright review`. After the merge and release, a person runs `pipewright ab plan`
   (the sample it needs) and `pipewright ab start`.

## Done when

`validate` passes with no A/B warnings you have not explained, the review bundle shows the test, and a person has the
pull request. Never run `ab start`, `ab set`, `ab decide` or `ab conclude`.
