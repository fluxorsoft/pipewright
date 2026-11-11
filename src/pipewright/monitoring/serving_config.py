"""Model serving observability settings (Pipewright 1.4.3, FR-98). All off by default, and resolved only when a use
case sets them, so a use case without them resolves and releases exactly as before.

    model:
      serving:
        capture:                      # record what the deployed version receives and answers
          enabled: true
          sample_rate: 0.05           # of requests (Agent Platform) or calls (KServe)
          max_per_day: 100000         # a cap; above it, records are dropped and counted
          retention_days: 30          # the online log's partition expiry
    monitoring:
      online:                         # the team's monitor steps on live traffic (needs capture)
        enabled: true
        schedule: "0 6 * * *"
        window_hours: 24
        min_samples: 500              # below: insufficient_data, never "no drift"
        attribution:                  # which features drive the answers, against training (stage 4)
          enabled: false
          sample_rows: 500            # up to 5,000
          threshold: 0.10             # total variation distance of the attribution shares
      health:                         # operational health of each deployed version
        enabled: true
        error_rate: { warning: 0.01, critical: 0.05 }
        latency_p95_ms: { warning: 500, critical: 1000 }
        min_ready_replicas: 1
        scaling_minutes: 5            # replicas wanted but not ready this long (a stuck scale-up)
        propose_rollback_hours: 0     # > 0: a critical breach this soon after a promotion proposes a rollback

Platform policy (manifest policies.serving): capture {allowed, max_sample_rate, retention_days}, health {required},
scale_to_zero (allowed | refused).
"""

from __future__ import annotations

import copy

from pipewright.config.merge import deep_merge

CAPTURE_DEFAULTS = {"enabled": False, "sample_rate": 0.05, "max_per_day": 100000, "retention_days": 30}
ATTRIBUTION_DEFAULTS = {"enabled": False, "sample_rows": 500, "threshold": 0.10}
ONLINE_DEFAULTS = {"enabled": False, "schedule": "0 6 * * *", "window_hours": 24, "min_samples": 500,
                   "attribution": ATTRIBUTION_DEFAULTS}
HEALTH_DEFAULTS = {"enabled": False, "error_rate": {"warning": 0.01, "critical": 0.05},
                   "latency_p95_ms": {"warning": 500, "critical": 1000}, "min_ready_replicas": 1,
                   "scaling_minutes": 5, "propose_rollback_hours": 0}
MAX_ATTRIBUTION_ROWS = 5000


def resolve(serving_spec: dict, monitoring_spec: dict, serving: dict, monitoring: dict, policies: dict,
            env_name: str, problems: list[str], warnings: list[str]) -> None:
    """Fills serving["capture"], monitoring["online"] and monitoring["health"] when the use case sets them."""
    pol = (policies or {}).get("serving") or {}
    if "capture" in (serving_spec or {}):
        serving["capture"] = cap = deep_merge(CAPTURE_DEFAULTS, serving_spec["capture"] or {})
        _capture(cap, serving, pol.get("capture") or {}, env_name, problems)
    if "online" in (monitoring_spec or {}):
        monitoring["online"] = online = deep_merge(copy.deepcopy(ONLINE_DEFAULTS), monitoring_spec["online"] or {})
        _online(online, serving, problems)
    if "health" in (monitoring_spec or {}):
        monitoring["health"] = health = deep_merge(copy.deepcopy(HEALTH_DEFAULTS), monitoring_spec["health"] or {})
        _health(health, serving, problems)
    if (pol.get("health") or {}).get("required") and serving.get("enabled") and not (
            (monitoring.get("health") or {}).get("enabled")):
        problems.append(f"monitoring.health: {env_name} requires health policies on every endpoint (platform policy "
                        "policies.serving.health.required): set monitoring.health.enabled: true")
    if serving.get("scale_to_zero"):
        if pol.get("scale_to_zero") == "refused":
            problems.append(f"model.serving.scale_to_zero is refused in {env_name} (platform policy "
                            "policies.serving.scale_to_zero)")
        else:
            warnings.append("model.serving.scale_to_zero (an Agent Platform Preview): the first call after the "
                            "endpoint scaled to zero, and every call during the scale-up, is answered 429 and dropped "
                            "(2 to 3 minutes measured); Cloud Monitoring does not count those 429s. Callers must "
                            "retry; monitoring.health's scaling condition catches a scale-up that never finishes")


def _capture(cap: dict, serving: dict, pol: dict, env_name: str, problems: list[str]) -> None:
    where = "model.serving.capture"
    if not cap["enabled"]:
        return
    if not serving.get("enabled"):
        problems.append(f"{where}: needs model.serving.enabled (there is nothing to capture)")
    if pol.get("allowed") is False:
        problems.append(f"{where}: capture is not allowed in {env_name} (platform policy policies.serving.capture)")
    rate = cap["sample_rate"]
    if not isinstance(rate, (int, float)) or not 0 < rate <= 1:
        problems.append(f"{where}.sample_rate must be above 0 and at most 1")
    elif pol.get("max_sample_rate") is not None and rate > pol["max_sample_rate"]:
        problems.append(f"{where}.sample_rate {rate} is above {env_name}'s maximum {pol['max_sample_rate']} "
                        "(policies.serving.capture.max_sample_rate)")
    if not isinstance(cap["max_per_day"], int) or cap["max_per_day"] < 1:
        problems.append(f"{where}.max_per_day must be a whole number, 1 or above")
    days = cap["retention_days"]
    if not isinstance(days, int) or days < 1:
        problems.append(f"{where}.retention_days must be a whole number, 1 or above")
    elif pol.get("retention_days") is not None and days > pol["retention_days"]:
        problems.append(f"{where}.retention_days {days} is above {env_name}'s maximum {pol['retention_days']} "
                        "(policies.serving.capture.retention_days)")


def _online(online: dict, serving: dict, problems: list[str]) -> None:
    where = "monitoring.online"
    if not online["enabled"]:
        return
    if not (serving.get("capture") or {}).get("enabled"):
        problems.append(f"{where}: needs model.serving.capture.enabled (the online window is what capture records)")
    if not isinstance(online["window_hours"], (int, float)) or online["window_hours"] <= 0:
        problems.append(f"{where}.window_hours must be above 0")
    if not isinstance(online["min_samples"], int) or online["min_samples"] < 1:
        problems.append(f"{where}.min_samples must be a whole number, 1 or above")
    if not isinstance(online["schedule"], str) or len(online["schedule"].split()) != 5:
        problems.append(f"{where}.schedule must be a cron expression (five fields)")
    att = online["attribution"]
    if att["enabled"]:
        if not isinstance(att["sample_rows"], int) or not 1 <= att["sample_rows"] <= MAX_ATTRIBUTION_ROWS:
            problems.append(f"{where}.attribution.sample_rows must be from 1 to {MAX_ATTRIBUTION_ROWS}")
        if not isinstance(att["threshold"], (int, float)) or not 0 < att["threshold"] <= 1:
            problems.append(f"{where}.attribution.threshold must be above 0 and at most 1")


def _health(health: dict, serving: dict, problems: list[str]) -> None:
    where = "monitoring.health"
    if not health["enabled"]:
        return
    if not serving.get("enabled"):
        problems.append(f"{where}: needs model.serving.enabled (health is about a deployed version)")
    for key in ("error_rate", "latency_p95_ms"):
        t = health[key]
        if not all(isinstance(t.get(k), (int, float)) and t[k] > 0 for k in ("warning", "critical")):
            problems.append(f"{where}.{key}: warning and critical must be above 0")
        elif t["warning"] > t["critical"]:
            problems.append(f"{where}.{key}: warning is above critical")
    if any(t > 1 for t in (health["error_rate"].get("warning", 0), health["error_rate"].get("critical", 0))
           if isinstance(t, (int, float))):
        problems.append(f"{where}.error_rate: a share of requests, at most 1")
    for key, low in (("min_ready_replicas", 0), ("scaling_minutes", 1), ("propose_rollback_hours", 0)):
        if not isinstance(health[key], int) or health[key] < low:
            problems.append(f"{where}.{key} must be a whole number, {low} or above")
