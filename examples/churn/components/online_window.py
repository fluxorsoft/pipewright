"""Online window (Pipewright 1.4.3, FR-98): what the deployed champion received and answered, from capture (Agent
Platform's request-response log, or KServe's records through the log sink), as the same scored Dataset the monitor
step reads after batch scoring. Pipewright reads it (pipewright_runtime.online_window); this step only names the
columns the churn monitor expects.

Below monitoring.online.min_samples the window is insufficient_data: the pipeline skips the drift check (an empty
window is never "no drift": spike 5), and the event monitoring.insufficient_data says so."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def read_online_window(
    project: str,
    source_uri: str,
    run_id: str,
    model: dsl.Input[dsl.Model],
    model_version: str,
    ml_config: dict,
    scored: dsl.Output[dsl.Dataset],
) -> str:
    """Returns "yes" when the window has enough rows to check, "no" otherwise; the summary goes to the log."""
    import json
    import logging

    import pandas as pd
    from pipewright_runtime import emit_event, online_window

    logging.basicConfig(level=logging.INFO)
    features = json.load(open(os.path.join(model.path, "features.json")))
    isvc = None
    if (ml_config.get("platform") or {}).get("orchestrator") == "kfp":       # KServe on private GKE
        isvc = ml_config["usecase"].lower().replace("_", "-").replace(".", "-")[:45].rstrip("-")
    rows, summary = online_window(ml_config, project, source_uri, features, model_version, isvc=isvc)
    # Live quality (stage 3): the predictions with an ID, kept for the outcomes that arrive later
    from pipewright_runtime import record_online_predictions

    summary["recorded_predictions"] = record_online_predictions(ml_config, project, rows, model_version, run_id)
    df = pd.DataFrame(rows, columns=[*features, "prediction"]).rename(columns={"prediction": "churn_probability"})
    if summary["insufficient_data"]:
        df = df.iloc[0:0]
        emit_event("monitoring.insufficient_data", ml_config, run_id, **summary)
    df.to_parquet(scored.path, index=False)
    logging.info("online window: %s", json.dumps(summary))
    return "no" if summary["insufficient_data"] else "yes"
