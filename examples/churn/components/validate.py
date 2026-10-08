"""Validate: checks a Dataset against data.contract in usecase.yaml. Fails the run on bad input."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def validate_data(
    data: dsl.Input[dsl.Dataset],
    ml_config: dict,
    mode: str,
    report: dsl.Output[dsl.Artifact],
    metrics: dsl.Output[dsl.Metrics],
) -> str:
    """mode: training (label and min_rows required) or inference. Returns a short JSON summary (rows, null fractions)
    for the retrain decision pack: aggregates only, never values."""
    import json
    import logging

    import pandas as pd
    from mlkit import check_contract

    logging.basicConfig(level=logging.INFO)
    df = pd.read_parquet(data.path)
    errors, summary = check_contract(df, ml_config["data"], mode)
    with open(report.path, "w") as f:
        json.dump(summary, f, indent=2)
    metrics.log_metric("rows", len(df))
    if "label_rate" in summary:
        metrics.log_metric("label_rate", round(summary["label_rate"], 4))
    metrics.log_metric("max_null_fraction", round(max(summary["null_fractions"].values(), default=0.0), 4))
    if errors:
        raise ValueError("data contract failed:\n  " + "\n  ".join(errors))
    logging.info("data contract passed: %d rows", len(df))
    return json.dumps({"rows": len(df), "errors": [],
                       "null_fractions": {k: round(v, 4) for k, v in summary["null_fractions"].items()}})
