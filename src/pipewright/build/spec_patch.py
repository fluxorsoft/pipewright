"""Applies a use case's `steps` settings to a compiled pipeline template (KFP v2 IR).

steps:
  train-model: { cpu: 8, memory: 32G, gpu: { type: NVIDIA_L4, count: 1 }, retries: 2, cache: false }

A key matches a task by its task name or display name, including tasks inside loops and conditions.
The result is written exactly as KFP's own set_cpu_limit / set_retry / set_caching_options would write it, so
teams can tune compute without touching pipeline code.
"""

from __future__ import annotations

import re

from pipewright.errors import ConfigError


def _tasks(spec: dict):
    """Yields (task_key, display_name, task dict) for every task in every DAG."""
    dags = [spec["root"].get("dag", {})] + [c.get("dag", {}) for c in spec.get("components", {}).values()]
    for dag in dags:
        for key, task in dag.get("tasks", {}).items():
            yield key, task.get("taskInfo", {}).get("name", key), task


def _memory_gb(value: str) -> float:
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([MG])", value)
    if not m:
        raise ConfigError(f"memory {value!r} must look like 512M or 8G")
    number, unit = float(m.group(1)), m.group(2)
    return number if unit == "G" else number / 1024


def task_names(spec: dict) -> list[str]:
    names = set()
    for key, display, task in _tasks(spec):
        if "componentRef" in task and not task["componentRef"]["name"].startswith("comp-for-loop") \
                and not task["componentRef"]["name"].startswith("comp-condition"):
            names.update({key, display})
    return sorted(names)


def apply_steps(spec: dict, steps: dict) -> list[str]:
    """Mutates spec; returns a description of each change. Unknown step names are an error."""
    changes, unknown = [], []
    executors = spec["deploymentSpec"]["executors"]
    for step, settings in steps.items():
        matched = [(k, d, t) for k, d, t in _tasks(spec) if step in (k, d)]
        if not matched:
            unknown.append(step)
            continue
        for _key, display, task in matched:
            label = spec["components"][task["componentRef"]["name"]].get("executorLabel")
            if label is None:
                raise ConfigError(f"steps.{step}: {display} is a group (loop or condition), not a step")
            container = executors[label]["container"]
            # Only a step that sets cpu, memory or a GPU gets a resources block: Agent Platform runs any step with one
            # (even empty) on its sized-step backend, where the metadata server issues no ID tokens.
            sizing = any(k in settings for k in ("cpu", "memory", "gpu"))
            res = (container.setdefault("resources", {}) or {}) if sizing else (container.get("resources") or {})
            if sizing:
                container["resources"] = res
            if "cpu" in settings:
                res["cpuLimit"] = float(settings["cpu"])
                res["resourceCpuLimit"] = str(settings["cpu"])
            if "memory" in settings:
                res["memoryLimit"] = _memory_gb(settings["memory"])
                res["resourceMemoryLimit"] = settings["memory"]
            if "gpu" in settings:
                g = settings["gpu"]
                res["accelerator"] = {"count": str(g["count"]), "resourceCount": str(g["count"]),
                                      "type": g["type"].upper(), "resourceType": g["type"].upper()}
            if "retries" in settings:
                task["retryPolicy"] = {"backoffDuration": "0s", "backoffFactor": 2.0,
                                       "backoffMaxDuration": "3600s", "maxRetryCount": int(settings["retries"])}
            if "cache" in settings:
                task["cachingOptions"] = {"enableCache": True} if settings["cache"] else {}
            changes.append(f"{display}: " + ", ".join(f"{k}={v}" for k, v in settings.items()))
    if unknown:
        raise ConfigError(f"steps: no task named {', '.join(unknown)} (tasks: {', '.join(task_names(spec))})")
    return changes
