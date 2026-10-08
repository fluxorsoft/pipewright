"""Score: new data with the champion model -> inference.output_table (outputs.store) and a scored Dataset."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def score_batch(
    project: str,
    bq_dataset: str,
    run_id: str,
    data: dsl.Input[dsl.Dataset],
    model: dsl.Input[dsl.Model],
    model_version: str,
    model_resource_name: str,
    ml_config: dict,
    scored: dsl.Output[dsl.Dataset],
) -> int:
    """Uses the model's own feature list and training medians, so it scores exactly as it was trained."""
    import json
    import logging
    import os
    from datetime import datetime, timezone

    import pandas as pd
    from churn_net import load, predict
    from mlkit import impute
    from pipewright_runtime import write_table

    logging.basicConfig(level=logging.INFO)
    with open(os.path.join(model.path, "features.json")) as f:
        features = json.load(f)
    stats_path = os.path.join(model.path, "training_stats.json")
    if os.path.exists(stats_path):
        with open(stats_path) as f:
            medians = json.load(f)["medians"]
    else:                                              # models trained before training_stats.json existed
        medians = {}
    df = pd.read_parquet(data.path)
    df = impute(df, features, medians)
    df = df.dropna(subset=features)
    threshold = ml_config["inference"]["decision_threshold"]
    df["churn_probability"] = predict(load(model.path), df[features].to_numpy("float32"))
    df["predicted_churn"] = (df["churn_probability"] >= threshold).astype(int)
    df.to_parquet(scored.path, index=False)

    key = (ml_config["data_protection"].get("erasure") or {}).get("key_column") or "customer_id"
    out = pd.DataFrame({
        key: df[key].astype(str),
        "churn_probability": df["churn_probability"],
        "predicted_churn": df["predicted_churn"],
        "model_resource_name": model_resource_name,
        "model_version_id": model_version,
        "run_id": run_id,
        "scored_at": datetime.now(timezone.utc),
    })
    where = write_table(ml_config["inference"]["output_table"], out, ml_config, project, bq_dataset, run_id)
    logging.info("scored %d rows with version %s -> %s", len(out), model_version, where)
    return len(out)
