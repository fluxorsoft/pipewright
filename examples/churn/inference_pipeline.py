"""Churn inference pipeline: score new data with the champion and monitor it (usecase.yaml: inference,
monitoring, retraining).

    load data (gs:// or bq://) -> validate (data.contract, no label needed) -> load champion
             -> score -> inference.output_table
             -> check drift / prediction drift / performance -> propose a retrain (a person decides)
             -> monitoring table
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "components"))

from champion import load_model  # noqa: E402
from ingest import load_data  # noqa: E402
from kfp import dsl  # noqa: E402
from monitor import check_batch, record_monitoring, request_retraining  # noqa: E402
from score import score_batch  # noqa: E402
from validate import validate_data  # noqa: E402


@dsl.pipeline(name="churn-inference", description="Score new customers with the champion; monitor drift.")
def churn_inference_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    bq_dataset: str = "mlops_poc",
    model_display_name: str = "churn-classifier",
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
    model_version: str = "",       # a closed retrain loop validates its candidate before it is the default
):
    data = load_data(project=project, source_uri=source_uri, ml_config=ml_config,
                     run_id=run_id).set_display_name("load-data")

    validate = validate_data(data=data.outputs["data"], ml_config=ml_config, mode="inference")
    validate.set_display_name("validate-data")

    champion = load_model(project=project, region=region, model_display_name=model_display_name, required=True,
                          version=model_version)
    champion.set_display_name("load-champion").set_caching_options(False)

    score = score_batch(project=project, bq_dataset=bq_dataset, run_id=run_id, data=data.outputs["data"],
                        model=champion.outputs["model"], model_version=champion.outputs["version"],
                        model_resource_name=champion.outputs["resource_name"], ml_config=ml_config).after(validate)
    score.set_display_name("score-to-bigquery").set_caching_options(False)

    check = check_batch(project=project, bq_dataset=bq_dataset, scored=score.outputs["scored"],
                        model=champion.outputs["model"], model_version=champion.outputs["version"],
                        ml_config=ml_config).set_display_name("check-drift-and-performance")
    check.set_caching_options(False)

    retrain = request_retraining(project=project, bq_dataset=bq_dataset, run_id=run_id,
                                 triggers=check.outputs["triggers"], summary=check.outputs["summary"],
                                 data_checks=validate.outputs["Output"], model_version=champion.outputs["version"],
                                 model_resource_name=champion.outputs["resource_name"], ml_config=ml_config)
    retrain.set_display_name("propose-retraining").set_caching_options(False)

    record = record_monitoring(project=project, bq_dataset=bq_dataset, run_id=run_id, source_uri=source_uri,
                               model_version=champion.outputs["version"], summary=check.outputs["summary"],
                               alert=check.outputs["alert"], retrain_status=retrain.output, ml_config=ml_config)
    record.set_display_name("record-monitoring").set_caching_options(False)
