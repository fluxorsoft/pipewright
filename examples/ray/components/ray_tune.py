"""Tuning on Ray (training.tuning): the trials run in parallel on a Ray on Agent Platform cluster made for this step of
this run (steps.tune-on-ray.ray), which is deleted when the step ends. Same trials and choice as tune.py."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def tune_on_ray(
    train_data: dsl.Input[dsl.Dataset],
    validation_data: dsl.Input[dsl.Dataset],
    run_id: str,
    ml_config: dict,
    trials: dsl.Output[dsl.Artifact],
    metrics: dsl.Output[dsl.Metrics],
) -> dict:
    """The best trial's hyperparameters. trials holds the job's input (config.json) and every score (results.json)."""
    import json
    import logging

    from churn_net import DEFAULTS
    from google.cloud import storage
    from mlkit import best_trial, sample_trials
    from pipewright_runtime import ray_cluster, ray_job

    logging.basicConfig(level=logging.INFO)

    def blob(uri: str):
        bucket, _, name = uri.removeprefix("gs://").partition("/")
        return storage.Client().bucket(bucket).blob(name)

    training = ml_config["training"]
    tuning = training["tuning"]
    defaults = {**DEFAULTS, **training["hyperparameters"]}
    candidates = sample_trials(tuning, defaults, training["seed"]) if tuning["enabled"] else [defaults]
    job = {"train": train_data.uri, "validation": validation_data.uri, "features": ml_config["features"]["include"],
           "label": ml_config["data"]["label"]["column"], "metric": tuning["metric"],
           "threshold": ml_config["evaluation"]["decision_threshold"], "seed": training["seed"], "trials": candidates}
    blob(f"{trials.uri}/config.json").upload_from_string(json.dumps(job), content_type="application/json")

    with ray_cluster(ml_config, "tune-on-ray", run_id) as cluster:
        ray_job(ml_config, cluster, f"python -m ray_tune_job {trials.uri}", run_id=run_id)

    results = json.loads(blob(f"{trials.uri}/results.json").download_as_text())
    best = best_trial(results, tuning["metric"], tuning["goal"])
    nodes = sorted({r.get("node") for r in results if r.get("node")})
    logging.info("%d trials on %d Ray node(s); best %s=%s %s", len(results), len(nodes), tuning["metric"],
                 best["score"], best["hyperparameters"])
    metrics.log_metric("trials", len(results))
    metrics.log_metric("ray_nodes", len(nodes))
    metrics.log_metric(f"best_validation_{tuning['metric']}", best["score"])
    return best["hyperparameters"]
