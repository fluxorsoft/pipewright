"""TorchServe metrics for KServe (Pipewright 1.4.3, FR-98 stages 1 and 4).

Google's prebuilt PyTorch image writes TorchServe's metrics to its log; Prometheus mode (TS_METRICS_MODE) serves them
on port 8082, where a PodMonitoring lets Managed Prometheus scrape them (spike 4). TorchServe's own latency metrics are
counters (a mean only, spike 8), so Pipewright's handler records PwPredictLatencySeconds, which this metrics config
declares a histogram. Seconds, because TorchServe uses Prometheus's default buckets (5 ms to 10 s).

TorchServe reads the config at startup, before the model archive is unpacked: the container writes it from an
environment variable, then starts as the image does.
"""

from __future__ import annotations

METRICS_FILE = "/tmp/pipewright-metrics.yaml"
HISTOGRAM = "PwPredictLatencySeconds"

# TorchServe 0.12's default metrics.yaml (the image's), plus the histogram.
METRICS_YAML = """dimensions:
  - &model_name "ModelName"
  - &worker_name "WorkerName"
  - &level "Level"
  - &device_id "DeviceId"
  - &hostname "Hostname"
ts_metrics:
  counter:
    - {name: Requests2XX, unit: Count, dimensions: [*level, *hostname]}
    - {name: Requests4XX, unit: Count, dimensions: [*level, *hostname]}
    - {name: Requests5XX, unit: Count, dimensions: [*level, *hostname]}
    - {name: ts_inference_requests_total, unit: Count, dimensions: ["model_name", "model_version", "hostname"]}
    - name: ts_inference_latency_microseconds
      unit: Microseconds
      dimensions: ["model_name", "model_version", "hostname"]
    - name: ts_queue_latency_microseconds
      unit: Microseconds
      dimensions: ["model_name", "model_version", "hostname"]
  gauge:
    - {name: QueueTime, unit: Milliseconds, dimensions: [*level, *hostname]}
    - {name: WorkerThreadTime, unit: Milliseconds, dimensions: [*level, *hostname]}
    - {name: WorkerLoadTime, unit: Milliseconds, dimensions: [*worker_name, *level, *hostname]}
    - {name: CPUUtilization, unit: Percent, dimensions: [*level, *hostname]}
    - {name: MemoryUsed, unit: Megabytes, dimensions: [*level, *hostname]}
    - {name: MemoryAvailable, unit: Megabytes, dimensions: [*level, *hostname]}
    - {name: MemoryUtilization, unit: Percent, dimensions: [*level, *hostname]}
model_metrics:
  gauge:
    - {name: HandlerTime, unit: ms, dimensions: [*model_name, *level]}
    - {name: PredictionTime, unit: ms, dimensions: [*model_name, *level]}
  histogram:
    - {name: PwPredictLatencySeconds, unit: Seconds, dimensions: [*model_name, *level]}
"""


def is_torchserve(image: str) -> bool:
    """Google's prebuilt PyTorch serving images run TorchServe."""
    return "/vertex-ai/prediction/pytorch" in (image or "")


def container_settings(container: dict) -> dict:
    """Prometheus mode, the metrics config with the histogram, and port 8082 named for the PodMonitoring."""
    env = [e for e in container.get("env") or [] if e["name"] not in ("TS_METRICS_MODE", "TS_METRICS_CONFIG",
                                                                       "PIPEWRIGHT_TS_METRICS")]
    env += [{"name": "TS_METRICS_MODE", "value": "prometheus"}, {"name": "TS_METRICS_CONFIG", "value": METRICS_FILE},
            {"name": "PIPEWRIGHT_TS_METRICS", "value": METRICS_YAML}]
    out = {**container, "env": env,
           "ports": [*(container.get("ports") or []), {"name": "ts-metrics", "containerPort": 8082, "protocol": "TCP"}]}
    if not container.get("command"):
        out["command"] = ["sh", "-c", f'printf "%s" "$PIPEWRIGHT_TS_METRICS" > {METRICS_FILE} && exec python '
                                      "entrypoint.py"]
    return out


def pod_monitoring(name: str, namespace: str, isvc: str) -> dict:
    return {"apiVersion": "monitoring.googleapis.com/v1", "kind": "PodMonitoring",
            "metadata": {"name": f"{name}-metrics", "namespace": namespace,
                         "labels": {"app.kubernetes.io/managed-by": "pipewright"}},
            "spec": {"selector": {"matchLabels": {"serving.kserve.io/inferenceservice": isvc}},
                     "endpoints": [{"port": "ts-metrics", "path": "/metrics", "interval": "30s"}]}}
