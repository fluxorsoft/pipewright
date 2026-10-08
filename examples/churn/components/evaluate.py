"""Evaluate: evaluation.* in usecase.yaml. Metrics, slices, calibration, explainability, gates and the
champion comparison, giving the decision to register or not."""

import os
from typing import NamedTuple

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def evaluate_model(
    model: dsl.Input[dsl.Model],
    champion: dsl.Input[dsl.Model],
    champion_found: str,
    champion_version: str,
    test_data: dsl.Input[dsl.Dataset],
    ml_config: dict,
    metrics: dsl.Output[dsl.Metrics],
    classification_metrics: dsl.Output[dsl.ClassificationMetrics],
    report: dsl.Output[dsl.Artifact],
) -> NamedTuple("Evaluation", [("decision", str), ("reasons", str), ("metrics_json", str)]):
    """decision is "approve" or "reject"; reasons and the full report explain it."""
    import json
    import logging
    from collections import namedtuple

    import numpy as np
    import pandas as pd
    from churn_net import load, predict
    from mlkit import calibration, compute_metrics, decide, permutation_importance, roc_curve, slice_metrics

    logging.basicConfig(level=logging.INFO)
    ev, features = ml_config["evaluation"], ml_config["features"]["include"]
    label, threshold, names = ml_config["data"]["label"]["column"], ev["decision_threshold"], ev["metrics"]
    df = pd.read_parquet(test_data.path)
    y = df[label].to_numpy(int)
    net = load(model.path)
    df["_p"] = predict(net, df[features].to_numpy("float32"))
    scores = compute_metrics(y, df["_p"], threshold, names)
    confusion = scores.pop("_confusion")

    min_rows = min([g.get("min_rows", 1) for g in ev["slice_gates"]] or [1])
    slices = slice_metrics(df, label, "_p", ev["slices"], threshold, names, min_rows)
    ece, reliability = calibration(y, df["_p"].to_numpy())

    champ = None
    if champion_found == "yes":
        with open(f"{champion.path}/features.json") as f:
            champ_features = json.load(f)
        missing = [c for c in champ_features if c not in df.columns]
        if missing:
            logging.warning("champion needs columns the test set lacks %s: comparison skipped", missing)
        else:
            cp = predict(load(champion.path), df[champ_features].to_numpy("float32"))
            champ = compute_metrics(y, cp, threshold, names)
            champ.pop("_confusion")
            champ["_version"] = champion_version

    importance = None
    if ml_config["explainability"]["enabled"]:
        imp = permutation_importance(lambda X: predict(net, X.to_numpy("float32")), df[features], y,
                                     ev["primary_metric"], threshold, seed=ml_config["training"]["seed"])
        importance = dict(list(imp.items())[: ml_config["explainability"]["top_k"]])

    decision, reasons = decide(scores, slices, ev, champ)
    full = {"metrics": scores, "confusion": confusion, "slices": slices,
            "calibration": {"ece": ece, "reliability": reliability}, "champion": champ,
            "feature_importance": importance, "decision": decision, "reasons": reasons, "test_rows": len(df),
            "threshold": threshold}
    with open(report.path, "w") as f:
        json.dump(full, f, indent=2)

    for k, v in scores.items():
        if v is not None:
            metrics.log_metric(k, round(v, 4))
    if champ and champ.get(ev["primary_metric"]) is not None:
        metrics.log_metric(f"champion_{ev['primary_metric']}", round(champ[ev["primary_metric"]], 4))
    metrics.metadata["decision"] = decision
    fpr, tpr, thr = roc_curve(y, df["_p"].to_numpy())
    idx = np.unique(np.linspace(0, len(fpr) - 1, num=min(len(fpr), 50)).astype(int))
    classification_metrics.log_roc_curve(fpr[idx].tolist(), tpr[idx].tolist(), np.clip(thr[idx], 0, 1).tolist())
    classification_metrics.log_confusion_matrix(
        ["negative", "positive"], [[confusion["tn"], confusion["fp"]], [confusion["fn"], confusion["tp"]]])

    logging.info("decision %s: %s", decision, reasons)
    return namedtuple("Evaluation", ["decision", "reasons", "metrics_json"])(
        decision, json.dumps(reasons), json.dumps({"metrics": scores, "champion": champ, "slices": slices}))
