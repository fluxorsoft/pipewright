"""Churn online monitoring (Pipewright 1.4.3, FR-98): the champion's live traffic, checked by the SAME monitor steps as
batch scoring, so batch and online give one answer.

    load champion -> read the online window (capture) -> [enough rows?] check drift -> propose a retrain
                                                                        -> monitoring table

Released with the use case (spec.pipelines.monitoring) and run on monitoring.online.schedule. Below min_samples the
drift check is skipped and the window is recorded as insufficient_data, never as no drift.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "components"))

from attribution import check_attribution  # noqa: E402
from champion import load_model  # noqa: E402
from kfp import dsl  # noqa: E402
from monitor import check_batch, record_monitoring, request_retraining  # noqa: E402
from online_window import read_online_window  # noqa: E402


@dsl.pipeline(name="churn-online-monitoring", description="Monitor the champion's live traffic.")
def churn_monitoring_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    bq_dataset: str = "mlops_poc",
    model_display_name: str = "churn-classifier",
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
    model_version: str = "",
    approved_files: str = "",
):
    champion = load_model(project=project, region=region, model_display_name=model_display_name, required=True,
                          version=model_version, approved_files=approved_files)
    champion.set_display_name("load-champion").set_caching_options(False)

    window = read_online_window(project=project, source_uri=source_uri, run_id=run_id,
                                model=champion.outputs["model"], model_version=champion.outputs["version"],
                                ml_config=ml_config)
    window.set_display_name("read-online-window").set_caching_options(False)

    with dsl.If(window.outputs["Output"] == "yes", name="enough-rows"):
        check = check_batch(project=project, bq_dataset=bq_dataset, scored=window.outputs["scored"],
                            model=champion.outputs["model"], model_version=champion.outputs["version"],
                            ml_config=ml_config, online=True).set_display_name("check-drift-and-performance")
        check.set_caching_options(False)
        attribution = check_attribution(run_id=run_id, scored=window.outputs["scored"],
                                        model=champion.outputs["model"], model_version=champion.outputs["version"],
                                        ml_config=ml_config)
        attribution.set_display_name("check-attribution").set_caching_options(False)
        retrain = request_retraining(project=project, bq_dataset=bq_dataset, run_id=run_id,
                                     triggers=check.outputs["triggers"], summary=check.outputs["summary"],
                                     data_checks="", model_version=champion.outputs["version"],
                                     model_resource_name=champion.outputs["resource_name"], ml_config=ml_config)
        retrain.set_display_name("propose-retraining").set_caching_options(False)
        record = record_monitoring(project=project, bq_dataset=bq_dataset, run_id=run_id, source_uri=source_uri,
                                   model_version=champion.outputs["version"], summary=check.outputs["summary"],
                                   alert=check.outputs["alert"], retrain_status=retrain.output, ml_config=ml_config)
        record.set_display_name("record-monitoring").set_caching_options(False)
