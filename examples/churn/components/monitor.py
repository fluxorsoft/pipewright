"""Monitor (monitoring.* and retraining.* in usecase.yaml): feature drift and prediction drift against the
training baselines, performance where labels have arrived, a retrain proposal (a person decides), and one row per
run in monitoring.output_table."""

import os
from typing import NamedTuple

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def check_batch(
    project: str,
    bq_dataset: str,
    scored: dsl.Input[dsl.Dataset],
    model: dsl.Input[dsl.Model],
    model_version: str,
    ml_config: dict,
    report: dsl.Output[dsl.Artifact],
    metrics: dsl.Output[dsl.Metrics],
) -> NamedTuple("Monitoring", [("alert", str), ("triggers", list), ("summary", str)]):
    """alert is "yes" or "no"; triggers lists what fired (drift, performance)."""
    import json
    import logging
    import os
    from collections import namedtuple

    import pandas as pd
    from mlkit import LOWER_IS_BETTER, compute_metrics, psi

    logging.basicConfig(level=logging.INFO)
    mon = ml_config["monitoring"]
    df = pd.read_parquet(scored.path)
    stats_path = os.path.join(model.path, "training_stats.json")
    stats = json.load(open(stats_path)) if os.path.exists(stats_path) else None
    result = {"rows": len(df), "model_version": model_version, "notes": []}
    triggers = []

    if mon["drift"]["enabled"] or mon["prediction_drift"]["enabled"]:
        if stats is None:
            result["notes"].append("model has no training_stats.json: drift not checked")
    if mon["drift"]["enabled"] and stats:
        feature_psi = {c: round(psi(stats["drift_baseline"][c], df[c]), 4)
                       for c in mon["drift"]["features"] if c in stats["drift_baseline"] and c in df.columns}
        drifted = sorted(c for c, v in feature_psi.items() if v > mon["drift"]["threshold"])
        result.update({"feature_psi": feature_psi, "drifted_features": drifted})
        if drifted:
            triggers.append("drift")
    if mon["prediction_drift"]["enabled"] and stats:
        result["prediction_psi"] = round(psi(stats["prediction_baseline"], df["churn_probability"]), 4)
        if result["prediction_psi"] > mon["prediction_drift"]["threshold"]:
            triggers.append("drift")

    perf = mon["performance"]
    if perf["enabled"]:
        from google.cloud import bigquery

        labels, key = perf["labels"], perf["labels"]["key"]
        when = labels.get("time_column")
        if when:
            # Pipewright 1.2 (FR-78): each outcome meets the score made for its cut-off, never a later score that
            # already knows the outcome. Outcomes of cut-offs this version has not scored are not counted.
            sql = f"""
            SELECT pr.churn_probability AS prob, CAST(lb.{labels['label']} AS INT64) AS outcome
            FROM `{project}.{bq_dataset}.{ml_config['inference']['output_table']}` AS pr
            JOIN `{project}.{bq_dataset}.{labels['table']}` AS lb
              ON lb.{key} = pr.{key} AND DATE(lb.{when}) = pr.scored_for
            WHERE pr.model_version_id = @version
            QUALIFY ROW_NUMBER() OVER (PARTITION BY pr.{key}, pr.scored_for ORDER BY pr.scored_at DESC) = 1"""
        else:
            # Latest prediction per key from this model version, joined to the observed outcome (1.0 and 1.1). With a
            # cut-off in the data that is wrong once a newer batch is scored, so the monitoring row says so.
            data = ml_config["data"]
            if ((data.get("features_from") or {}).get("entity") or {}).get("time_column") or \
                    (data.get("split") or {}).get("time_column"):
                result["notes"].append("performance joined by key only: set monitoring.performance.labels.time_column "
                                       "so outcomes meet the scores made for their cut-off")
            sql = f"""
            SELECT pr.churn_probability AS prob, CAST(lb.{labels['label']} AS INT64) AS outcome
            FROM `{project}.{bq_dataset}.{ml_config['inference']['output_table']}` AS pr
            JOIN `{project}.{bq_dataset}.{labels['table']}` AS lb USING ({key})
            WHERE pr.model_version_id = @version
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY pr.scored_at DESC) = 1"""
        from pipewright_runtime import bigquery_client

        job = bigquery_client(project, ml_config).query(sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("version", "STRING", model_version)]))
        labelled = job.to_dataframe()
        result["labelled_rows"] = len(labelled)
        if len(labelled) >= 50:
            value = compute_metrics(labelled["outcome"], labelled["prob"], ml_config["inference"]["decision_threshold"],
                                    [perf["metric"]])[perf["metric"]]
            result["performance"] = {perf["metric"]: value}
            if value is not None and perf.get("min") is not None:
                bad = value > perf["min"] if perf["metric"] in LOWER_IS_BETTER else value < perf["min"]
                if bad:
                    triggers.append("performance")
        else:
            result["notes"].append(f"{len(labelled)} labelled predictions so far: performance needs 50")

    triggers = sorted(set(triggers))
    result["triggers"] = triggers
    with open(report.path, "w") as f:
        json.dump(result, f, indent=2)
    metrics.log_metric("rows", len(df))
    if "feature_psi" in result:
        metrics.log_metric("max_feature_psi", max(result["feature_psi"].values(), default=0.0))
    if "prediction_psi" in result:
        metrics.log_metric("prediction_psi", result["prediction_psi"])
    logging.info("monitoring: %s", json.dumps(result))
    return namedtuple("Monitoring", ["alert", "triggers", "summary"])(
        "yes" if triggers else "no", triggers, json.dumps(result))


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def request_retraining(
    project: str,
    bq_dataset: str,
    run_id: str,
    triggers: list,
    summary: str,
    data_checks: str,
    model_version: str,
    model_resource_name: str,
    ml_config: dict,
) -> str:
    """When retraining.when matches a trigger, sends what this run saw to the launcher, which opens or updates a
    retrain decision case (retraining.mode propose or candidate). A person decides; nothing trains from here.
    Over Pub/Sub when the environment has a run requests topic (private perimeters), else POST
    /retraining/propose. Returns what happened; never fails the inference run."""
    import json
    import logging

    logging.basicConfig(level=logging.INFO)
    policy, platform = ml_config["retraining"], ml_config["platform"]
    mode = policy.get("mode") or ("propose" if policy.get("enabled") else "off")
    fired = sorted(set(triggers) & set(policy["when"]))
    if mode == "off":
        return "no case: retraining.mode is off"
    if not fired:
        return "no case: nothing fired"
    try:
        s = json.loads(summary)
        recent = []
        try:  # the trend for the decision pack: earlier runs of this use case
            from pipewright_runtime import bigquery_client

            table = f"{project}.{bq_dataset}.{ml_config['monitoring']['output_table']}"
            sql = (f"SELECT monitored_at, max_feature_psi, prediction_psi, performance_value, alert FROM `{table}` "
                   "ORDER BY monitored_at DESC LIMIT 5")
            recent = [{"at": str(r["monitored_at"]), "max_feature_psi": r["max_feature_psi"],
                       "prediction_psi": r["prediction_psi"], "performance_value": r["performance_value"],
                       "alert": r["alert"]} for r in bigquery_client(project, ml_config).query(sql).result()][::-1]
        except Exception as e:  # noqa: BLE001 - the pack is still useful without the trend
            logging.warning("no monitoring history for the decision pack: %s", e)
        feature_psi = s.get("feature_psi") or {}
        observation = {
            "triggers": fired, "rows": s.get("rows"), "model_version": model_version,
            "feature_psi": feature_psi, "drifted_features": s.get("drifted_features") or [],
            "max_feature_psi": max(feature_psi.values(), default=None), "prediction_psi": s.get("prediction_psi"),
            "performance": s.get("performance"), "labelled_rows": s.get("labelled_rows"),
            "data_checks": json.loads(data_checks) if data_checks else None, "recent": recent,
        }
        body = {"kind": "retrain.proposal", "usecase": ml_config["usecase"], "run_id": run_id,
                "observation": observation,
                "champion": {"version": model_version, "resource_name": model_resource_name}}

        if platform.get("run_requests_topic"):
            from google.cloud import pubsub_v1

            publisher = pubsub_v1.PublisherClient(  # regional: private topics enforce in-region storage in transit
                client_options={"api_endpoint": f"{platform['region']}-pubsub.googleapis.com:443"})
            message_id = publisher.publish(platform["run_requests_topic"], json.dumps(body, default=str).encode(),
                                           usecase=ml_config["usecase"]).result(60)
            # Over Pub/Sub the run cannot know the launcher's answer (a case, or none in a cooldown): it was sent.
            return f"sent ({', '.join(fired)}): message {message_id}; the case says what the launcher did"

        import requests
        from pipewright_runtime import id_token

        url = platform["launcher_url"].rstrip("/")
        token = id_token(url)              # also works on Agent Platform workers (by the account's name)
        r = requests.post(f"{url}/retraining/propose", data=json.dumps(body, default=str),
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          timeout=120)
        if r.status_code != 200:
            return f"failed: launcher returned {r.status_code}: {r.text[:300]}"
        result = r.json()
        if result.get("status") == "no-case":               # for example a cooldown: no case was proposed
            return result.get("detail") or "no case"
        return f"proposed ({', '.join(fired)}): {result.get('detail') or result.get('status')}"
    except Exception as e:  # noqa: BLE001 - reported in the monitoring table, not raised
        logging.exception("retrain proposal failed")
        return f"failed: {type(e).__name__}: {e}"[:500]


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def record_monitoring(
    project: str,
    bq_dataset: str,
    run_id: str,
    source_uri: str,
    model_version: str,
    summary: str,
    alert: str,
    retrain_status: str,
    ml_config: dict,
):
    import json
    from datetime import datetime, timezone

    from pipewright_runtime import emit_event, write_table

    s = json.loads(summary)
    perf = s.get("performance") or {}
    row = {
        "run_id": run_id, "source_uri": source_uri, "model_name": ml_config["model_name"],
        "model_version_id": model_version, "rows_scored": s["rows"],
        "max_feature_psi": max((s.get("feature_psi") or {}).values(), default=None),
        "drifted_features": ",".join(s.get("drifted_features", [])) or None,
        "prediction_psi": s.get("prediction_psi"),
        "performance_metric": next(iter(perf), None), "performance_value": next(iter(perf.values()), None),
        "labelled_rows": s.get("labelled_rows"), "alert": alert == "yes", "triggers": ",".join(s["triggers"]) or None,
        "retrain_status": retrain_status, "details_json": summary,
        "monitored_at": datetime.now(timezone.utc).isoformat(),
    }
    if ml_config["monitoring"]["output_table"]:          # monitoring off: no table, the event still records the run
        write_table(ml_config["monitoring"]["output_table"], [row], ml_config, project, bq_dataset, run_id)
    summary_event = {k: row[k] for k in ("model_version_id", "rows_scored", "max_feature_psi", "drifted_features",
                                         "prediction_psi", "performance_metric", "performance_value", "triggers",
                                         "retrain_status")}
    emit_event("monitoring.recorded", ml_config, run_id, alert=row["alert"], **summary_event)
    if row["alert"]:
        emit_event("monitoring.alert", ml_config, run_id, **summary_event)
