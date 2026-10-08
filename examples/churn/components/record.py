"""Record: one row per training run in the metrics table, approved or not."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def record_metrics(
    project: str,
    bq_dataset: str,
    metrics_table: str,
    run_id: str,
    source_uri: str,
    model_display_name: str,
    decision: str,
    reasons: str,
    metrics_json: str,
    hyperparameters: dict,
    train_rows: int,
    test_rows: int,
    ml_config: dict,
):
    import json
    from datetime import datetime, timezone

    from pipewright_runtime import emit_event, write_table

    ev = ml_config["evaluation"]
    result = json.loads(metrics_json)
    m, champion = result["metrics"], result.get("champion") or {}
    auc_gate = next((g.get("min") for g in ev["gates"] if g["metric"] == "auc"), None)
    row = {
        "run_id": run_id, "source_uri": source_uri, "model_display_name": model_display_name,
        "auc": m.get("auc"), "accuracy": m.get("accuracy"), "f1": m.get("f1"), "auc_threshold": auc_gate,
        "decision": decision, "train_rows": train_rows, "test_rows": test_rows,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "pr_auc": m.get("pr_auc"), "precision": m.get("precision"), "recall": m.get("recall"),
        "log_loss": m.get("log_loss"), "brier": m.get("brier"), "calibration_error": m.get("calibration_error"),
        "primary_metric": ev["primary_metric"], "primary_value": m.get(ev["primary_metric"]),
        "champion_version": champion.get("_version"), "champion_value": champion.get(ev["primary_metric"]),
        "decision_reasons": reasons, "metrics_json": json.dumps(result),
        "hyperparameters_json": json.dumps(hyperparameters), "environment": ml_config["environment"],
    }
    write_table(metrics_table, [row], ml_config, project, bq_dataset, run_id)
    emit_event("training.evaluated", ml_config, run_id, decision=decision, reasons=json.loads(reasons),
               primary_metric=ev["primary_metric"], primary_value=row["primary_value"],
               champion_version=row["champion_version"], champion_value=row["champion_value"])
