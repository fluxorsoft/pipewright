# Playbook: watch a served model

Intent: "tell us when <use case>'s endpoint is unhealthy, and whether it is still right on live traffic".

## Steps

1. `serving_health` the use case: what is on now, the platform's serving rules and the health policies in force.
2. Health, in `config/operations.yaml`: `monitoring.health: { enabled: true, error_rate: { warning, critical },
   latency_p95_ms: { warning, critical }, min_ready_replicas, scaling_minutes }`. Set the latency from what the
   callers can wait, not from today's numbers; production wants `min_ready_replicas: 2`. Set
   `propose_rollback_hours` only if the team wants rollback proposals (a person still decides each).
3. Capture, if the team wants quality on live traffic: `model.serving.capture: { enabled: true, sample_rate,
   retention_days }` within the platform's limits. Captured requests hold features of real people: keep the sample
   small (0.01 to 0.1) and the retention short. On a private endpoint, capture is set when the platform creates it:
   say so in the review.
4. Online monitoring: `spec.pipelines.monitoring` (copy `monitoring_pipeline.py` from the churn example) and
   `monitoring.online: { enabled: true, schedule, window_hours, min_samples }`. It runs the team's own monitor
   steps; do not write new drift code.
5. `validate`, then `plan`: the plan lists the alert policies per deployed version.
6. Open a pull request with `pipewright review`. After the merge, the team runs `pipewright alerts sync` (or the
   next `deploy-model` does), and subscribes to the events: `pipewright events --persona sre` and `--persona ml`.

## Done when

`validate` passes, the review bundle shows the health policies and the capture, and a person has the pull request.
