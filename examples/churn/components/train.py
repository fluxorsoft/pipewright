"""Train: fits the churn model on train + validation and exports TorchScript, a TorchServe model.mar and
training_stats.json (imputation medians and drift baselines the inference pipeline uses)."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def train_model(
    train_data: dsl.Input[dsl.Dataset],
    validation_data: dsl.Input[dsl.Dataset],
    prep: dsl.Input[dsl.Artifact],
    hyperparameters: dict,
    ml_config: dict,
    model: dsl.Output[dsl.Model],
    metrics: dsl.Output[dsl.Metrics],
):
    """model.path: model.pt, model.mar, features.json, training_stats.json."""
    import json
    import logging
    import os
    import shutil
    import subprocess
    import sys
    import tempfile

    import pandas as pd
    import torch
    from churn_net import fit, predict
    from mlkit import psi_baseline

    logging.basicConfig(level=logging.INFO)
    features, label = ml_config["features"]["include"], ml_config["data"]["label"]["column"]
    seed = ml_config["training"]["seed"]
    df = pd.concat([pd.read_parquet(train_data.path), pd.read_parquet(validation_data.path)], ignore_index=True)
    x = df[features].to_numpy("float32")
    net, loss = fit(x, df[label].to_numpy("float32"), hyperparameters, seed)

    with open(prep.path) as f:
        prep_info = json.load(f)
    stats = {
        "features": features,
        "medians": prep_info["medians"],
        "label_rate": float(df[label].mean()),
        "rows": len(df),
        "hyperparameters": hyperparameters,
        "seed": seed,
        "drift_baseline": {c: psi_baseline(df[c]) for c in features},
        "prediction_baseline": psi_baseline(predict(net, x)),
        "split": prep_info,
    }

    os.makedirs(model.path, exist_ok=True)
    model_pt = os.path.join(model.path, "model.pt")
    torch.jit.script(net).save(model_pt)
    for name, content in (("features.json", features), ("training_stats.json", stats)):
        with open(os.path.join(model.path, name), "w") as f:
            json.dump(content, f, indent=2, default=str)
    extra = [os.path.join(model.path, "features.json")]
    # What the serving handler needs beside the network: the training medians, so an empty feature is filled online
    # as batch scoring fills it (Pipewright 1.2.0, model.serving.fill_missing), and with model.serving.features online
    # (Pipewright 1.1) the views and key, which the serving container's PIPEWRIGHT_ONLINE_FEATURES overrides.
    serving = {"medians": stats["medians"],
               "fill_missing": (ml_config.get("serving") or {}).get("fill_missing", True)}
    if ml_config.get("online_features"):
        serving["online_features"] = ml_config["online_features"]
    with open(os.path.join(model.path, "serving.json"), "w") as f:
        json.dump(serving, f, indent=2, default=str)
    extra.append(os.path.join(model.path, "serving.json"))

    # Agent Platform prebuilt PyTorch serving runs <artifact_uri>/model.mar with TorchServe.
    handler = os.path.join(os.path.dirname(os.path.abspath(__file__)), "torchserve_handler.py")
    with tempfile.TemporaryDirectory() as export_dir, tempfile.TemporaryDirectory() as scratch:
        # The archiver stages and deletes $TMPDIR/<model-name>: a private TMPDIR keeps it away from model.path.
        subprocess.run(
            [sys.executable, "-m", "model_archiver.model_packaging", "--model-name", "model", "--version", "1.0",
             "--serialized-file", model_pt, "--handler", handler,
             "--extra-files", ",".join(extra), "--export-path", export_dir],
            check=True, env={**os.environ, "TMPDIR": scratch, "TEMP": scratch, "TMP": scratch},
        )
        shutil.copy(os.path.join(export_dir, "model.mar"), os.path.join(model.path, "model.mar"))

    metrics.log_metric("final_train_loss", round(loss, 4))
    metrics.log_metric("train_rows", len(df))
    model.metadata.update({"framework": "pytorch", "framework_version": torch.__version__, "features": features,
                           "hyperparameters": hyperparameters})
    logging.info("trained on %d rows, saved to %s", len(df), model.uri)
