"""Churn training pipeline with tuning on Ray (KFP v2 on Agent Platform Pipelines). Behaviour comes from
usecase.yaml via `ml_config`.

    ingest -> validate (data.contract) -> prepare (data.split, features)
           -> tune on Ray: every trial in parallel on a Ray cluster for this run (training.tuning,
              steps.tune-on-ray.ray); the cluster is deleted when the step ends
           -> train (train + validation) -> load champion -> evaluate (evaluation.*, explainability)
           -> record metrics (always)
           -> [approve] register (registry.set_default, model_card)

This example has no inference pipeline (the churn example's works the same way). Component images, and the Ray
image the cluster runs, are pinned at release time.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "components"))

from champion import load_model  # noqa: E402
from evaluate import evaluate_model  # noqa: E402
from ingest import ingest_data  # noqa: E402
from kfp import dsl  # noqa: E402
from prepare import prepare_data  # noqa: E402
from ray_tune import tune_on_ray  # noqa: E402
from record import record_metrics  # noqa: E402
from register import register_model  # noqa: E402
from train import train_model  # noqa: E402
from validate import validate_data  # noqa: E402


@dsl.pipeline(name="churn-ray-training",
              description="Validate, tune on Ray, train, evaluate against the champion, register.")
def churn_ray_training_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    bq_dataset: str = "mlops_poc",
    raw_table: str = "raw_customers",
    metrics_table: str = "model_metrics",
    model_display_name: str = "churn-classifier",
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
):
    ingest = ingest_data(project=project, source_uri=source_uri, bq_dataset=bq_dataset, run_id=run_id,
                        raw_table=raw_table, ml_config=ml_config).set_display_name("ingest-data")
    ingest.set_caching_options(False)            # writes the raw table

    validate = validate_data(data=ingest.outputs["raw_data"], ml_config=ml_config, mode="training")
    validate.set_display_name("validate-data")

    prepare = prepare_data(raw_data=ingest.outputs["raw_data"], ml_config=ml_config).after(validate)
    prepare.set_display_name("prepare-and-split")

    tune = tune_on_ray(train_data=prepare.outputs["train_data"], validation_data=prepare.outputs["validation_data"],
                       run_id=run_id, ml_config=ml_config).set_display_name("tune-on-ray")
    tune.set_caching_options(False)              # a cluster per run: never reuse an old result
    hyperparameters = tune.outputs["Output"]

    train = train_model(train_data=prepare.outputs["train_data"], validation_data=prepare.outputs["validation_data"],
                        prep=prepare.outputs["prep"], hyperparameters=hyperparameters, ml_config=ml_config)
    train.set_display_name("train-model")

    champion = load_model(project=project, region=region, model_display_name=model_display_name, required=False)
    champion.set_display_name("load-champion").set_caching_options(False)     # the registry changes

    evaluate = evaluate_model(model=train.outputs["model"], champion=champion.outputs["model"],
                              champion_found=champion.outputs["found"], champion_version=champion.outputs["version"],
                              test_data=prepare.outputs["test_data"], ml_config=ml_config)
    evaluate.set_display_name("evaluate-model")

    record = record_metrics(project=project, bq_dataset=bq_dataset, metrics_table=metrics_table, run_id=run_id,
                            source_uri=source_uri, model_display_name=model_display_name,
                            decision=evaluate.outputs["decision"], reasons=evaluate.outputs["reasons"],
                            metrics_json=evaluate.outputs["metrics_json"], hyperparameters=hyperparameters,
                            train_rows=prepare.outputs["train_rows"], test_rows=prepare.outputs["test_rows"],
                            ml_config=ml_config)
    record.set_display_name("record-metrics-to-bigquery").set_caching_options(False)

    with dsl.If(evaluate.outputs["decision"] == "approve", name="model-approved"):
        register = register_model(project=project, region=region, model=train.outputs["model"],
                                  report=evaluate.outputs["report"], model_display_name=model_display_name,
                                  run_id=run_id, source_uri=source_uri, ml_config=ml_config)
        register.set_display_name("register-model").set_caching_options(False)
