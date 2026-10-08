"""Tuning (training.tuning): random search over search_space, each trial scored on the validation split."""

import os
from typing import List, NamedTuple

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def plan_trials(ml_config: dict) -> NamedTuple("Plan", [("tuning", str), ("trials", list), ("defaults", dict)]):
    """tuning is "on" or "off"; trials are hyperparameter sets (the first is the defaults)."""
    from collections import namedtuple

    from churn_net import DEFAULTS
    from mlkit import sample_trials

    training = ml_config["training"]
    defaults = {**DEFAULTS, **training["hyperparameters"]}
    tuning = training["tuning"]
    trials = sample_trials(tuning, defaults, training["seed"]) if tuning["enabled"] else [defaults]
    return namedtuple("Plan", ["tuning", "trials", "defaults"])("on" if tuning["enabled"] else "off", trials, defaults)


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def tune_trial(
    train_data: dsl.Input[dsl.Dataset],
    validation_data: dsl.Input[dsl.Dataset],
    hyperparameters: dict,
    ml_config: dict,
) -> str:
    """Trains on train, scores tuning.metric on validation. Returns JSON {hyperparameters, score}."""
    import json
    import logging

    import pandas as pd
    from churn_net import fit, predict
    from mlkit import compute_metrics

    logging.basicConfig(level=logging.INFO)
    features, label = ml_config["features"]["include"], ml_config["data"]["label"]["column"]
    metric, threshold = ml_config["training"]["tuning"]["metric"], ml_config["evaluation"]["decision_threshold"]
    train, val = pd.read_parquet(train_data.path), pd.read_parquet(validation_data.path)
    net, _ = fit(train[features].to_numpy("float32"), train[label].to_numpy("float32"), hyperparameters,
                 ml_config["training"]["seed"])
    score = compute_metrics(val[label], predict(net, val[features].to_numpy("float32")), threshold, [metric])[metric]
    logging.info("trial %s -> %s=%s", hyperparameters, metric, score)
    return json.dumps({"hyperparameters": hyperparameters, "score": score})


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def select_best(results: List[str], ml_config: dict, metrics: dsl.Output[dsl.Metrics]) -> dict:
    """The best trial's hyperparameters (tuning.goal decides max or min)."""
    import json
    import logging

    from mlkit import best_trial

    logging.basicConfig(level=logging.INFO)
    tuning = ml_config["training"]["tuning"]
    parsed = [json.loads(r) for r in results]
    best = best_trial(parsed, tuning["metric"], tuning["goal"])
    for i, r in enumerate(sorted(parsed, key=lambda r: r["score"] or 0, reverse=tuning["goal"] != "minimize")):
        logging.info("rank %d: %s=%s %s", i + 1, tuning["metric"], r["score"], r["hyperparameters"])
    metrics.log_metric("trials", len(parsed))
    metrics.log_metric(f"best_validation_{tuning['metric']}", best["score"])
    return best["hyperparameters"]


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def use_defaults(defaults: dict) -> dict:
    """Tuning off: train with training.hyperparameters."""
    return defaults
