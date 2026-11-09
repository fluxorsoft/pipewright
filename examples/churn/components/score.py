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
    ab_test: str = "",
) -> int:
    """Uses the model's own feature list and training medians, so it scores exactly as it was trained. While an A/B
    test runs (ab_test, from the launcher), each unit is scored by its arm's model: the treatment's approved files are
    downloaded and checked by SHA-256, and ab_test and arm are written beside each score (FR-57)."""
    import json
    import logging
    import os
    from datetime import datetime, timezone

    import pandas as pd
    from churn_net import load, predict
    from mlkit import impute
    from pipewright_runtime import ab_arm, ab_spec, approved_download, write_table

    logging.basicConfig(level=logging.INFO)

    def model_files(path):
        with open(os.path.join(path, "features.json")) as f:
            feats = json.load(f)
        stats_path = os.path.join(path, "training_stats.json")
        meds = {}
        if os.path.exists(stats_path):                 # models trained before training_stats.json existed have none
            with open(stats_path) as f:
                meds = json.load(f)["medians"]
        return feats, meds

    threshold = ml_config["inference"]["decision_threshold"]
    key = (ml_config["data_protection"].get("erasure") or {}).get("key_column") or "customer_id"
    df = pd.read_parquet(data.path)
    test = ab_spec(ab_test)
    arms = {"control": (model.path, model_version)}
    if test:
        treatment = "/tmp/pw-treatment"
        approved_download(test["treatment"]["approved_files"], treatment, project)
        arms["treatment"] = (treatment, str(test["treatment"]["version"]))
        unit = test["unit"] if test["unit"] in df.columns else key
        df["arm"] = [ab_arm(test["name"], u, test["treatment_bp"]) for u in df[unit].astype(str)]
    else:
        df["arm"] = "control"
    parts = []
    for name, (path, version) in arms.items():
        part = df[df["arm"] == name].copy()
        if part.empty:
            continue
        features, medians = model_files(path)
        part = impute(part, features, medians).dropna(subset=features)
        part["churn_probability"] = predict(load(path), part[features].to_numpy("float32"))
        part["predicted_churn"] = (part["churn_probability"] >= threshold).astype(int)
        part["model_version_id"] = version
        parts.append(part)
    df = pd.concat(parts) if parts else df.iloc[0:0]
    df.drop(columns=["arm"] if not test else []).to_parquet(scored.path, index=False)
    out = pd.DataFrame({
        key: df[key].astype(str),
        "churn_probability": df["churn_probability"],
        "predicted_churn": df["predicted_churn"],
        "model_resource_name": model_resource_name,
        "model_version_id": df["model_version_id"],
        "run_id": run_id,
        "scored_at": datetime.now(timezone.utc),
    })
    # The cut-off each score is for (Pipewright 1.2.0, FR-78): monitoring.performance joins outcomes to the score made
    # for their cut-off (labels.time_column), never to a later score that already knows the outcome.
    when = (((ml_config["data"].get("features_from") or {}).get("entity") or {}).get("time_column")
            or (ml_config["data"].get("split") or {}).get("time_column"))
    if when and when in df.columns:
        out["scored_for"] = pd.to_datetime(df[when]).dt.date.to_numpy()
    if test:                                           # nullable columns: absent outside a test
        out["ab_test"] = test["name"]
        out["arm"] = df["arm"].to_numpy()
    where = write_table(ml_config["inference"]["output_table"], out, ml_config, project, bq_dataset, run_id)
    logging.info("scored %d rows with version %s%s -> %s", len(out), model_version,
                 f" and treatment v{test['treatment']['version']} ({test['name']})" if test else "", where)
    return len(out)
