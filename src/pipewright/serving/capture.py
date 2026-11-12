"""Capture (Pipewright 1.4.3, FR-98 stage 2): what a deployed version receives and answers, sampled, in the tenant's
dataset. Off unless model.serving.capture.enabled.

Agent Platform: the endpoint's request-response logging (spike 1: works on public, dedicated, Private Service Connect
and pool endpoints), sample_rate, into <usecase>_online_log in the tenant's dataset. One row per request; its arrays
hold every instance and answer. The table's partitions expire after retention_days. An endpoint that also has a
Model Monitoring v1 job is refused: they share the table. max_per_day is applied where the window is read (Agent
Platform cannot cap; the sample rate bounds the volume).

KServe: the InferenceService's logger (mode all) to the tenant's capture receiver (pipewright.serving.receiver,
deployed by the platform in the serving namespace), with the use case's rate and cap in the URL; the receiver prints
the records and a log sink copies them into the tenant's dataset.
"""

from __future__ import annotations

import re

from pipewright.errors import ConfigError
from pipewright.gcp import error_text

RECEIVER = "pipewright-capture"


def table_name(usecase: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", usecase.lower()) + "_online_log"


# KServe's records reach the tenant's dataset through the platform's log sink, whose table is named after the log
# (container stdout). Found in the 1.4.3 battle test: the online-monitoring schedule read the Agent Platform table.
KSERVE_SINK_TABLE = "stdout"


def source_table(usecase: str, target: str | None) -> str:
    """The table the online window reads: the endpoint's log (Agent Platform) or the log sink's table (KServe)."""
    return KSERVE_SINK_TABLE if target == "kserve" else table_name(usecase)


def log_table(uc, env) -> str:
    dataset = (uc.platform or {}).get("bigquery_dataset")
    if not dataset:
        raise ConfigError(f"model.serving.capture: {uc.name} has no BigQuery dataset in {env.name}")
    return f"{env.project}.{dataset}.{table_name(uc.name)}"


def wanted_logging(uc, env) -> dict:
    cap = (uc.model.get("serving") or {}).get("capture") or {}
    if not cap.get("enabled"):
        return {"enabled": False}
    return {"enabled": True, "samplingRate": float(cap["sample_rate"]),
            "bigqueryDestination": {"outputUri": f"bq://{log_table(uc, env)}"}}


def sync_endpoint(uc, env, gcp, endpoint: str, log=print) -> str:
    """Sets (or turns off) the endpoint's request-response logging to what the use case says. Returns what it did."""
    if "capture" not in (uc.model.get("serving") or {}):
        return "not configured"
    api = f"https://{env.region}-aiplatform.googleapis.com/v1"
    r = gcp.get(f"{api}/{endpoint}")
    if not r.ok:
        raise ConfigError(f"endpoint {endpoint} not readable: {error_text(r)}")
    have = (r.body or {}).get("predictRequestResponseLoggingConfig") or {}
    want = wanted_logging(uc, env)
    if want["enabled"] and (r.body or {}).get("modelDeploymentMonitoringJob"):
        raise ConfigError(f"model.serving.capture: {endpoint} has a Model Monitoring v1 job, which writes the same "
                          "request-response table: remove the job, or leave capture off")
    same = (bool(have.get("enabled")) == want["enabled"] and (not want["enabled"] or (
        abs(float(have.get("samplingRate") or 0) - want["samplingRate"]) < 1e-9
        and (have.get("bigqueryDestination") or {}).get("outputUri") == want["bigqueryDestination"]["outputUri"])))
    body = r.body or {}
    fixed = bool((body.get("privateServiceConnectConfig") or {}).get("enablePrivateServiceConnect")
                 or body.get("dedicatedEndpointEnabled"))
    if not same and fixed:
        # GA QA (found live): Agent Platform sets a private or dedicated endpoint's logging only when it is created
        uc_name = uc.name
        raise ConfigError(f"capture on {endpoint}: a private (or dedicated) endpoint takes request-response logging "
                          "only when it is created. The platform team sets it in Terraform: serving.yaml "
                          f"endpoints.{uc_name}.capture.sample_rate: {want.get('samplingRate', 0):g} (the starter; "
                          "Enterprise: serving.endpoints), which recreates the endpoint (undeploy first)"
                          if want["enabled"] else
                          f"capture off on {endpoint}: a private endpoint's logging is set when it is created; remove "
                          f"endpoints.{uc_name}.capture in the platform's Terraform")
    if same:
        result = "unchanged"
    else:
        p = gcp.patch(f"{api}/{endpoint}", {"predictRequestResponseLoggingConfig": want},
                      updateMask="predict_request_response_logging_config")
        if not p.ok:
            raise ConfigError(f"capture on {endpoint}: {error_text(p)}")
        result = "on" if want["enabled"] else "off"
        log(f"capture {result}: {endpoint}" + (f" -> {want['bigqueryDestination']['outputUri']} at "
                                                f"{want['samplingRate']:g}" if want["enabled"] else ""))
    if want["enabled"]:
        retention(uc, env, gcp, log)
    return result


def retention(uc, env, gcp, log=print) -> None:
    """The online log's partitions expire after retention_days (once Agent Platform has made the table)."""
    days = int(uc.model["serving"]["capture"]["retention_days"])
    project, dataset, table = log_table(uc, env).split(".")
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}"
    r = gcp.get(url)
    if not r.ok:
        return                                  # made at the first logged request; the next deploy or sync sets it
    tp = (r.body or {}).get("timePartitioning") or {}
    ms = str(days * 86400 * 1000)
    if tp and tp.get("expirationMs") != ms:
        p = gcp.patch(url, {"timePartitioning": {**tp, "expirationMs": ms}})
        if p.ok:
            log(f"capture: {table} keeps {days} days")


def kserve_logger(uc, namespace: str) -> dict | None:
    """The InferenceService predictor's logger block, or None when capture is off."""
    cap = (uc.model.get("serving") or {}).get("capture") or {}
    if not cap.get("enabled"):
        return None
    url = (f"http://{RECEIVER}.{namespace}.svc.cluster.local/?rate={float(cap['sample_rate']):g}"
           f"&max={int(cap['max_per_day'])}")
    return {"mode": "all", "url": url}
