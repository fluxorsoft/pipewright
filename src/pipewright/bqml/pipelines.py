"""The BigQuery ML pipelines Pipewright provides when a use case has a `bqml` section. They run on the environment's
orchestrator (Agent Platform Pipelines or Kubeflow Pipelines on GKE) like the use case's own would; their steps
submit BigQuery jobs.

    training: rows -> validate -> train -> evaluate (gate 1) -> [approve] register
    scoring:  champion, rows -> validate -> score -> monitor -> [retrain proposal] -> record

Behaviour comes from ml_config (the bqml section and the ML sections); the release pins it.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "steps"))

from bqml_steps import (  # noqa: E402
    bqml_champion,
    bqml_evaluate,
    bqml_monitor,
    bqml_record,
    bqml_register,
    bqml_retrain,
    bqml_rows,
    bqml_score,
    bqml_train,
    bqml_validate,
)
from kfp import dsl  # noqa: E402


@dsl.pipeline(name="bqml-training",
              description="Rows, contract, CREATE MODEL, evaluate against the champion, register.")
def bqml_training_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
    champion_version: str = "",    # a closed retrain loop compares with what production serves
):
    rows = bqml_rows(project=project, source_uri=source_uri, run_id=run_id, mode="training", ml_config=ml_config)
    rows.set_display_name("rows").set_caching_options(False)
    validate = bqml_validate(project=project, rows_table=rows.outputs["rows_table"], run_id=run_id, mode="training",
                             ml_config=ml_config)
    validate.set_display_name("validate-data").set_caching_options(False)
    train = bqml_train(project=project, rows_table=rows.outputs["rows_table"], run_id=run_id,
                       ml_config=ml_config).after(validate)
    train.set_display_name("train-model").set_caching_options(False)
    evaluate = bqml_evaluate(project=project, region=region, rows_table=rows.outputs["rows_table"],
                             model=train.output, run_id=run_id, ml_config=ml_config,
                             champion_version=champion_version)
    evaluate.set_display_name("evaluate-model").set_caching_options(False)
    with dsl.If(evaluate.outputs["decision"] == "approve", name="model-approved"):
        register = bqml_register(project=project, region=region, model=train.output, run_id=run_id,
                                 primary_value=evaluate.outputs["primary_value"], ml_config=ml_config)
        register.set_display_name("register-model").set_caching_options(False)


@dsl.pipeline(name="bqml-scoring", description="Score new rows with the champion in BigQuery; monitor drift.")
def bqml_scoring_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
    model_version: str = "",       # a closed retrain loop validates its candidate before it is the default
):
    champion = bqml_champion(project=project, region=region, ml_config=ml_config, model_version=model_version)
    champion.set_display_name("load-champion").set_caching_options(False)
    rows = bqml_rows(project=project, source_uri=source_uri, run_id=run_id, mode="inference", ml_config=ml_config)
    rows.set_display_name("rows").set_caching_options(False)
    validate = bqml_validate(project=project, rows_table=rows.outputs["rows_table"], run_id=run_id, mode="inference",
                             ml_config=ml_config)
    validate.set_display_name("validate-data").set_caching_options(False)
    score = bqml_score(project=project, rows_table=rows.outputs["rows_table"], model=champion.outputs["model"],
                       model_version=champion.outputs["version"], run_id=run_id, ml_config=ml_config).after(validate)
    score.set_display_name("score-to-bigquery").set_caching_options(False)
    monitor = bqml_monitor(project=project, rows_table=rows.outputs["rows_table"],
                           scored_table=score.outputs["scored_table"], rows=score.outputs["rows"],
                           model_version=champion.outputs["version"], record=champion.outputs["record"],
                           run_id=run_id, ml_config=ml_config)
    monitor.set_display_name("check-drift-and-performance").set_caching_options(False)
    retrain = bqml_retrain(project=project, region=region, run_id=run_id, triggers=monitor.outputs["triggers"],
                           summary=monitor.outputs["summary"], data_checks=validate.output,
                           model_version=champion.outputs["version"], ml_config=ml_config)
    retrain.set_display_name("propose-retraining").set_caching_options(False)
    record = bqml_record(project=project, run_id=run_id, source_uri=source_uri,
                         model_version=champion.outputs["version"], summary=monitor.outputs["summary"],
                         alert=monitor.outputs["alert"], retrain_status=retrain.output, ml_config=ml_config)
    record.set_display_name("record-monitoring").set_caching_options(False)
