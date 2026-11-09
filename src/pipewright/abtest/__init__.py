"""A/B testing of models (FR-57, CONTEXT section 8.16).

A controlled test between the champion (control) and a candidate (treatment): units are assigned stickily by a hash of
the test's name and the unit key, the same in batch scoring, in the online gateway and in SQL; a business metric and
guardrails are fixed before the start; the analysis checks the shares first, then the metric per arm with an interval;
a person decides through approver groups. Off unless the use case has an `ab_test` section with `enabled: true`.

    assign   the hash, the threshold and the same expression in BigQuery SQL
    stats    tests and intervals from per-arm aggregates, sample size, SRM, CUPED, sequential (always-valid) p-values
    config   the section's defaults and checks against the data contract and the task
    store    the test record in the artifacts bucket (one writer at a time, generation preconditions)
"""
