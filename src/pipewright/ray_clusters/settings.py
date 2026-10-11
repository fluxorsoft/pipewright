"""Ray settings: the platform's limits and a step's cluster.

The platform sets `ray` in the environment manifest, for the environment and optionally per tenant
(`tenants.<tenant>.ray`, which wins key by key). Without it, every tenant gets DEFAULTS: a cluster per run, running
as the tenant's runner identity, from a prebuilt Ray on Agent Platform image, on a public endpoint, with small limits.
So an environment built before Ray existed needs no change. A private environment (the manifest sets
`security.network`) without a `ray` block has Ray off: public clusters never appear where pipelines are private.

A use case asks for a cluster per step, under `steps.<step>.ray` (so the size may differ per environment):

    steps:
      tune-on-ray: { ray: { head_machine: n1-standard-4, workers: 2 } }

`mode` exists for a later standing (long-lived) cluster per tenant; only `per_run` is accepted now (D-72).
"""

from __future__ import annotations

import copy
import hashlib
import re

from pipewright.config.merge import deep_merge

MODES = ("per_run",)
DEFAULTS = {
    "enabled": True,                 # a private blueprint sets false until its platform file turns Ray on
    "mode": "per_run",
    "ray_version": "2.47",
    "python_version": "3.11",
    "image": None,                   # base image for the use case's Ray image; None: the prebuilt one below
    "network_attachment": None,      # Private Service Connect interface (private environments)
    "limits": {"max_workers": 2, "machines": ["n1-standard-16", "n1-standard-4"], "accelerators": [],
               "max_minutes": 120},
}
# Ray client versions that match each cluster version (google-cloud-aiplatform[ray] supports these).
RAY_RELEASES = {"2.33": "2.33.0", "2.42": "2.42.0", "2.47": "2.47.1"}
STEP_KEYS = ("head_machine", "worker_machine", "workers", "accelerator", "max_minutes")
HEAD_MIN_MEMORY_GB = 18      # Ray on Agent Platform refuses a head node with less ("larger than 18GB")
HEAD_MIN_CPUS = 16           # found live (2026-10-10): with an 8-vCPU head the cluster runs, but its dashboard and
#                              Ray Client endpoint never answer; 16 vCPUs (the console's default) work
# GB of memory per vCPU, by machine family and shape (Compute Engine predefined types).
_MEMORY_PER_CPU = {("n1", "standard"): 3.75, ("n1", "highmem"): 6.5, ("n1", "highcpu"): 0.9,
                   ("n2", "standard"): 4, ("n2", "highmem"): 8, ("n2", "highcpu"): 1,
                   ("n2d", "standard"): 4, ("n2d", "highmem"): 8, ("n2d", "highcpu"): 1,
                   ("e2", "standard"): 4, ("e2", "highmem"): 8, ("e2", "highcpu"): 1,
                   ("c2", "standard"): 4, ("c2d", "standard"): 4, ("c3", "standard"): 4, ("c3", "highmem"): 8,
                   ("a2", "highgpu"): 85 / 12, ("g2", "standard"): 4}


def machine_memory_gb(machine: str) -> float | None:
    """Memory of a predefined machine type (n1-standard-8 -> 30), or None when not known here."""
    parts = machine.split("-")
    if len(parts) == 3 and parts[2].isdigit() and (parts[0], parts[1]) in _MEMORY_PER_CPU:
        return _MEMORY_PER_CPU[(parts[0], parts[1])] * int(parts[2])
    return None


def machine_cpus(machine: str) -> int | None:
    """vCPUs of a predefined machine type (n1-standard-16 -> 16), or None when not known here."""
    parts = machine.split("-")
    return int(parts[2]) if len(parts) == 3 and parts[2].isdigit() and (parts[0], parts[1]) in _MEMORY_PER_CPU else None


def _big_enough(machine: str) -> bool:
    memory, cpus = machine_memory_gb(machine), machine_cpus(machine)
    return (memory is None or memory > HEAD_MIN_MEMORY_GB) and (cpus is None or cpus >= HEAD_MIN_CPUS)


def platform_settings(manifest_ray: dict | None, tenant_ray: dict | None, private: bool = False) -> dict:
    """The effective settings for one tenant: DEFAULTS, then the environment's `ray`, then the tenant's. In a private
    environment (`security.network` set) Ray is off unless the manifest has a `ray` block."""
    base = copy.deepcopy(DEFAULTS)
    if private and manifest_ray is None:
        base["enabled"] = False
    return deep_merge(deep_merge(base, manifest_ray or {}), tenant_ray or {})


def prebuilt_image(region: str, ray_version: str, python_version: str, gpu: bool = False) -> str:
    """The Ray on Agent Platform prebuilt image (the same names the Agent Platform SDK uses)."""
    area = region.split("-")[0] if region.split("-")[0] in ("us", "europe", "asia") else "us"
    kind = "ray-gpu" if gpu else "ray-cpu"
    return (f"{area}-docker.pkg.dev/vertex-ai/training/{kind}.{ray_version.replace('.', '-')}."
            f"py{python_version.replace('.', '')}:latest")


def base_image(settings: dict, region: str, gpu: bool = False) -> str:
    return settings.get("image") or prebuilt_image(region, settings["ray_version"], settings["python_version"], gpu)


def step_cluster(spec: dict, settings: dict) -> dict:
    """A step's `ray` request with the defaults filled in."""
    limits = settings["limits"]
    machines = limits.get("machines") or ["n1-standard-8"]
    head = spec.get("head_machine") or next((m for m in machines if _big_enough(m)), machines[0])
    return {"head_machine": head, "worker_machine": spec.get("worker_machine") or head,
            "workers": int(spec.get("workers", 1)), "accelerator": spec.get("accelerator"),
            "max_minutes": int(spec.get("max_minutes") or limits["max_minutes"])}


def check_step(step: str, cluster: dict, settings: dict, where: str) -> list[str]:
    """Problems with a step's cluster against the platform's limits (empty when it fits)."""
    problems, limits = [], settings["limits"]
    label = f"{where}.{step}.ray"
    if not settings.get("enabled", True):
        return [f"{label}: Ray clusters are not enabled in this environment for this tenant: ask the platform team"]
    if settings["mode"] not in MODES:
        problems.append(f"{label}: Ray mode {settings['mode']!r} is not available; only per_run (a cluster per run) "
                        "is supported (standing clusters are planned)")
    allowed = limits.get("machines") or []
    for key in ("head_machine", "worker_machine"):
        if allowed and cluster[key] not in allowed:
            problems.append(f"{label}.{key} {cluster[key]} is not allowed (the platform allows {', '.join(allowed)})")
    if not _big_enough(cluster["head_machine"]):
        problems.append(f"{label}.head_machine {cluster['head_machine']} has {machine_cpus(cluster['head_machine'])} "
                        f"vCPUs and {machine_memory_gb(cluster['head_machine']):g} GB: a Ray head node needs at least "
                        f"{HEAD_MIN_CPUS} vCPUs and more than {HEAD_MIN_MEMORY_GB} GB (for example n1-standard-16)")
    if cluster["workers"] < 0 or cluster["workers"] > limits["max_workers"]:
        problems.append(f"{label}.workers {cluster['workers']} is outside 0 to {limits['max_workers']} "
                        "(the platform's limit)")
    accelerator = cluster.get("accelerator")
    if accelerator:
        types = limits.get("accelerators") or []
        if accelerator.get("type") not in types:
            problems.append(f"{label}.accelerator {accelerator.get('type')} is not allowed"
                            + (f" (the platform allows {', '.join(types)})" if types else
                               " (the platform allows no accelerators for Ray)"))
    if cluster["max_minutes"] > limits["max_minutes"]:
        problems.append(f"{label}.max_minutes {cluster['max_minutes']} is above the platform's limit of "
                        f"{limits['max_minutes']}")
    if settings["ray_version"] not in RAY_RELEASES:
        problems.append(f"Ray version {settings['ray_version']} is not supported (supported: "
                        f"{', '.join(RAY_RELEASES)}): ask the platform team")
    return problems


def resolve_steps(steps: dict, settings: dict, where: str = "steps") -> tuple[dict, list[str]]:
    """({step: cluster} for every step that asks for Ray, problems)."""
    clusters, problems = {}, []
    for step, cfg in (steps or {}).items():
        if not isinstance(cfg, dict) or "ray" not in cfg:
            continue
        cluster = step_cluster(cfg["ray"] or {}, settings)
        problems += check_step(step, cluster, settings, where)
        sized = [k for k in ("cpu", "memory", "gpu") if k in cfg]
        if sized:
            # A step with its own CPU or memory runs where the metadata server issues no ID tokens, so it could not
            # ask the launcher for its cluster (found on Agent Platform, 2026-10-10).
            problems.append(f"{where}.{step}: a Ray step cannot also set {', '.join(sized)}: the step only submits "
                            "the job and waits, and with its own size it cannot authenticate to the launcher. Size "
                            "the cluster in ray: instead")
        clusters[step] = cluster
    return clusters, problems


# ---------------------------------------------------------------- names (kept in step with pipewright_runtime)

_NAME = re.compile(r"[^a-z0-9-]+")


def cluster_id(usecase: str, run_id: str, step: str) -> str:
    """The deterministic persistent resource ID of a run's cluster for a step: retries reuse it, two runs never
    share one. At most 63 characters, starting with a letter."""
    digest = hashlib.sha256(f"{usecase}|{run_id}|{step}".encode()).hexdigest()[:12]
    stem = _NAME.sub("-", f"{usecase}-{step}".lower()).strip("-")[:44].rstrip("-")
    return f"pw-{stem}-{digest}"


def cluster_name(project: str, region: str, usecase: str, run_id: str, step: str) -> str:
    return f"projects/{project}/locations/{region}/persistentResources/{cluster_id(usecase, run_id, step)}"
