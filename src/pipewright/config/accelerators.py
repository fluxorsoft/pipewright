"""Accelerator limits per environment (Pipewright 1.2): GPUs for pipeline steps (FR-58) and for serving (FR-61).

The manifest's policies.accelerators lists what each may ask for:

    policies:
      accelerators:
        steps:   { types: [NVIDIA_TESLA_T4, NVIDIA_L4], max_count: 2 }
        serving: { types: [NVIDIA_L4], max_count: 1 }

Steps: without policies.accelerators.steps a step may ask for any GPU, as in 1.x (quota is the limit). Serving is new
in 1.2: model.serving.accelerator needs the environment to list it. Scale to zero (FR-62) is opt-in as well:
model.serving.scale_to_zero with replicas.min 0; 1.x deploys at least one replica whatever min says.
"""

from __future__ import annotations


def _check(what: str, gpu: dict, policy: dict, where: str) -> list[str]:
    types, most = policy.get("types") or [], policy.get("max_count")
    out = []
    if gpu["type"] not in types:
        out.append(f"{what}.type {gpu['type']}: {where} allows {', '.join(types) or 'no accelerators'} "
                   "(manifest policies.accelerators)")
    if most and int(gpu.get("count", 1)) > int(most):
        out.append(f"{what}.count {gpu['count']}: at most {most} in {where} (manifest policies.accelerators)")
    return out


def problems(steps: dict, serving: dict, policies: dict, env_name: str) -> list[str]:
    pol = policies.get("accelerators") or {}
    out = []
    if "steps" in pol:
        for name, settings in sorted((steps or {}).items()):
            if (settings or {}).get("gpu"):
                out += _check(f"steps.{name}.gpu", settings["gpu"], pol["steps"] or {}, env_name)
    accelerator = (serving or {}).get("accelerator")
    if accelerator:
        if "serving" not in pol:
            out.append(f"model.serving.accelerator: {env_name} lists no serving accelerators (manifest "
                       "policies.accelerators.serving; ask the platform team)")
        else:
            out += _check("model.serving.accelerator", accelerator, pol["serving"] or {}, env_name)
    if (serving or {}).get("scale_to_zero") and int(((serving or {}).get("replicas") or {}).get("min", 1)) != 0:
        out.append("model.serving.scale_to_zero needs replicas.min 0")
    return out


def warnings(serving: dict) -> list[str]:
    replicas = (serving or {}).get("replicas") or {}
    if int(replicas.get("min", 1)) == 0 and not (serving or {}).get("scale_to_zero"):
        return ["model.serving.replicas.min 0 deploys one replica (as in 1.x); set model.serving.scale_to_zero: true "
                "to let an idle endpoint run none (Agent Platform preview)"]
    return []
