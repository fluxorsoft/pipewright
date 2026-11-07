"""The sizes Google Cloud supports, checked before anything is created (FR-104).

Cloud Run (docs "CPU limits" and "Memory limits", read 2026-11-07) for the lookup service, and Cloud SQL for
PostgreSQL ("Instance settings" and "Machine series") for the environment's instance. Each check returns problems
naming the key and what is allowed. Monthly costs are Google's list prices for europe-west2 (Cloud Billing Catalog,
2026-11-07, before discounts), for `doctor` to show before anyone switches a store on.
"""

from __future__ import annotations

import re

HOURS = 730
SECONDS = HOURS * 3600

# ------------------------------------------------------------------------------------------- Cloud Run --

RUN_CPUS = (1, 2, 4, 6, 8)
# vCPU -> (minimum memory MiB, maximum memory MiB), second-generation runtime
RUN_MEMORY = {1: (512, 4096), 2: (512, 8192), 4: (2048, 16384), 6: (4096, 24576), 8: (4096, 32768)}
LOOKUP_DEFAULTS = {"cpu": 1, "memory": "512Mi", "min_instances": 0, "max_instances": 5, "concurrency": 80,
                   "pool_size": 8, "timeout_ms": 1000, "cpu_boost": True, "billing": "request"}
_MEM = re.compile(r"^([0-9]+)(Mi|Gi)$")


def memory_mib(value) -> int | None:
    m = _MEM.match(str(value or ""))
    if not m:
        return None
    return int(m.group(1)) * (1024 if m.group(2) == "Gi" else 1)


def lookup_problems(lookup: dict, limits: dict | None = None, where: str = "model.serving.online_store.lookup",
                    connection_limit: int | None = None) -> list[str]:
    """The lookup service's sizes against Cloud Run and the platform's limits."""
    out, limits = [], limits or {}
    cpu = lookup.get("cpu", LOOKUP_DEFAULTS["cpu"])
    try:
        cpu_number = float(cpu)
    except (TypeError, ValueError):
        cpu_number = None
    if cpu_number is None or cpu_number not in RUN_CPUS:
        if cpu_number is not None and 0.08 <= cpu_number < 1:
            out.append(f"{where}.cpu {cpu}: fractional CPU makes Cloud Run serve one request at a time, which defeats "
                       "the connection pool; use 1, 2, 4, 6 or 8")
        else:
            out.append(f"{where}.cpu {cpu}: Cloud Run takes 1, 2, 4, 6 or 8")
        cpu_number = None
    mem = lookup.get("memory", LOOKUP_DEFAULTS["memory"])
    mib = memory_mib(mem)
    if mib is None:
        out.append(f"{where}.memory {mem!r}: write it as Mi or Gi, for example 512Mi or 2Gi")
    elif cpu_number is not None:
        lo, hi = RUN_MEMORY[int(cpu_number)]
        if not lo <= mib <= hi:
            out.append(f"{where}.memory {mem}: with {int(cpu_number)} vCPU Cloud Run takes {_mi(lo)} to {_mi(hi)}")
    lo_inst = lookup.get("min_instances", LOOKUP_DEFAULTS["min_instances"])
    hi_inst = lookup.get("max_instances", LOOKUP_DEFAULTS["max_instances"])
    if lo_inst > hi_inst:
        out.append(f"{where}: min_instances {lo_inst} is more than max_instances {hi_inst}")
    conc = lookup.get("concurrency", LOOKUP_DEFAULTS["concurrency"])
    if not 1 <= conc <= 1000:
        out.append(f"{where}.concurrency {conc}: Cloud Run takes 1 to 1000")
    pool = lookup.get("pool_size", LOOKUP_DEFAULTS["pool_size"])
    if not 1 <= pool <= 50:
        out.append(f"{where}.pool_size {pool}: 1 to 50 connections per instance")
    t = lookup.get("timeout_ms", LOOKUP_DEFAULTS["timeout_ms"])
    if not 50 <= t <= 30000:
        out.append(f"{where}.timeout_ms {t}: 50 to 30000")
    if lookup.get("billing", "request") not in ("request", "instance"):
        out.append(f"{where}.billing {lookup.get('billing')!r}: request or instance")
    # the platform's limits (manifest features.online_store.lookup.limits)
    if limits.get("max_cpu") is not None and cpu_number is not None and cpu_number > limits["max_cpu"]:
        out.append(f"{where}.cpu {cpu}: this environment allows at most {limits['max_cpu']}")
    if limits.get("max_memory") and mib is not None and mib > (memory_mib(limits["max_memory"]) or mib):
        out.append(f"{where}.memory {mem}: this environment allows at most {limits['max_memory']}")
    if limits.get("max_instances") is not None and hi_inst > limits["max_instances"]:
        out.append(f"{where}.max_instances {hi_inst}: this environment allows at most {limits['max_instances']}")
    if limits.get("max_min_instances") is not None and lo_inst > limits["max_min_instances"]:
        out.append(f"{where}.min_instances {lo_inst}: this environment allows at most {limits['max_min_instances']} "
                   "warm instances")
    if limits.get("min_min_instances") is not None and lo_inst < limits["min_min_instances"]:
        out.append(f"{where}.min_instances {lo_inst}: this environment keeps at least {limits['min_min_instances']} "
                   "warm (the platform's floor, so callers do not wait for cold starts)")
    if connection_limit and pool * hi_inst > connection_limit:
        out.append(f"{where}: pool_size {pool} x max_instances {hi_inst} = {pool * hi_inst} connections, more than "
                   f"this use case's share of the database ({connection_limit})")
    return out


def _mi(mib: int) -> str:
    return f"{mib // 1024}Gi" if mib % 1024 == 0 and mib >= 1024 else f"{mib}Mi"


# London list prices, USD (Cloud Billing Catalog, 2026-11-07)
RUN_PRICES = {"request_cpu_s": 0.0000336, "request_mem_gib_s": 0.0000035, "idle_cpu_s": 0.0000035,
              "idle_mem_gib_s": 0.0000035, "instance_cpu_s": 0.0000216, "instance_mem_gib_s": 0.0000024}


def lookup_standing_cost(lookup: dict) -> float:
    """What the lookup service costs a month with no traffic: its warm instances (or, with instance billing, the
    whole life of each minimum instance)."""
    cpu = float(lookup.get("cpu", 1))
    gib = (memory_mib(lookup.get("memory", "512Mi")) or 512) / 1024
    n = int(lookup.get("min_instances", 0))
    if lookup.get("billing") == "instance":
        return n * SECONDS * (cpu * RUN_PRICES["instance_cpu_s"] + gib * RUN_PRICES["instance_mem_gib_s"])
    return n * SECONDS * (cpu * RUN_PRICES["idle_cpu_s"] + gib * RUN_PRICES["idle_mem_gib_s"])


# ------------------------------------------------------------------------------------------- Cloud SQL --

_CUSTOM = re.compile(r"^db-custom-([0-9]+)-([0-9]+)$")
_N4 = re.compile(r"^db-custom-N4-([0-9]+)-([0-9]+)$")
_PERF_N = re.compile(r"^db-perf-optimized-N-([0-9]+)$")
_C4A = re.compile(r"^db-c4a-highmem-([0-9]+)$")
_C4 = re.compile(r"^db-perf-optimized-C4-([0-9]+)$")
SHARED = {"db-f1-micro": (1, 0.614), "db-g1-small": (1, 1.7)}
PERF_N = (2, 4, 8, 16, 32, 48, 64, 80, 96, 128)
C4A = (2, 4, 8, 16, 32, 48, 64, 72)
C4 = (2, 4, 8)
INSTANCE_DEFAULTS = {"edition": "enterprise", "tier": "db-custom-1-3840", "postgres_version": "POSTGRES_16",
                     "availability": "zonal", "disk": {"type": "ssd", "size_gb": 10, "autoresize": True},
                     "data_cache": False, "deletion_protection": True}


def tier_shape(tier: str) -> tuple[str, int, float] | None:
    """(series, vCPU, memory GB) of a tier name Cloud SQL for PostgreSQL offers, or None."""
    if tier in SHARED:
        return "shared", SHARED[tier][0], SHARED[tier][1]
    for series, rx in (("custom", _CUSTOM), ("n4", _N4)):
        m = rx.match(tier)
        if m:
            return series, int(m.group(1)), int(m.group(2)) / 1024
    for series, rx, gb in (("perf_n", _PERF_N, 8), ("c4a", _C4A, 8), ("c4", _C4, 7.75)):
        m = rx.match(tier)
        if m:
            n = int(m.group(1))
            return series, n, (864 if series == "perf_n" and n == 128 else n * gb)
    return None


def instance_problems(spec: dict, where: str = "online_store.cloudsql") -> list[str]:
    """The Cloud SQL instance's settings against what Cloud SQL for PostgreSQL supports."""
    out = []
    s = {**INSTANCE_DEFAULTS, **(spec or {})}
    s["disk"] = {**INSTANCE_DEFAULTS["disk"], **((spec or {}).get("disk") or {})}
    edition, tier = s["edition"], str(s["tier"])
    if edition not in ("enterprise", "enterprise_plus"):
        out.append(f"{where}.edition {edition!r}: enterprise or enterprise_plus")
    shape = tier_shape(tier)
    if shape is None:
        out.append(f"{where}.tier {tier!r}: not a Cloud SQL for PostgreSQL machine type (Enterprise: db-f1-micro, "
                   "db-g1-small, db-custom-<vCPU>-<MB>, db-custom-N4-<vCPU>-<MB>; Enterprise Plus: "
                   "db-perf-optimized-N-<n>, db-c4a-highmem-<n>, db-perf-optimized-C4-<n>)")
        return out
    series, cpus, gb = shape
    plus = series in ("perf_n", "c4a", "c4")
    if plus and edition != "enterprise_plus":
        out.append(f"{where}.tier {tier}: an Enterprise Plus machine type; set edition enterprise_plus")
    if not plus and edition == "enterprise_plus":
        out.append(f"{where}.tier {tier}: an Enterprise machine type; Enterprise Plus takes db-perf-optimized-N-<n>, "
                   "db-c4a-highmem-<n> or db-perf-optimized-C4-<n>")
    if series == "custom":
        mb = int(_CUSTOM.match(tier).group(2))
        if not (cpus == 1 or (cpus % 2 == 0 and 2 <= cpus <= 96)):
            out.append(f"{where}.tier {tier}: vCPU must be 1 or an even number from 2 to 96")
        if mb % 256 or mb < 3840 or not 0.9 * 1024 * cpus <= mb <= 6.5 * 1024 * cpus:
            out.append(f"{where}.tier {tier}: memory must be a multiple of 256 MB, at least 3840 MB, and 0.9 to 6.5 GB "
                       f"per vCPU ({int(0.9 * 1024 * cpus)} to {int(6.5 * 1024 * cpus)} MB for {cpus} vCPU)")
    if series == "n4":
        mb = int(_N4.match(tier).group(2))
        if not (cpus % 2 == 0 and 2 <= cpus <= 80):
            out.append(f"{where}.tier {tier}: N4 vCPU must be an even number from 2 to 80")
        if not 2 * 1024 * cpus <= mb <= 8 * 1024 * cpus:
            out.append(f"{where}.tier {tier}: N4 memory must be 2 to 8 GB per vCPU")
    for name, allowed in (("perf_n", PERF_N), ("c4a", C4A), ("c4", C4)):
        if series == name and cpus not in allowed:
            out.append(f"{where}.tier {tier}: vCPU must be one of {', '.join(map(str, allowed))}")
    if s["availability"] not in ("zonal", "regional"):
        out.append(f"{where}.availability {s['availability']!r}: zonal or regional")
    dtype = s["disk"].get("type")
    if dtype not in ("ssd", "hdd", "hyperdisk"):
        out.append(f"{where}.disk.type {dtype!r}: ssd, hdd or hyperdisk")
    if dtype == "hdd" and edition != "enterprise":
        out.append(f"{where}.disk.type hdd: Enterprise edition only")
    if dtype == "hyperdisk" and series not in ("n4", "c4", "c4a"):
        out.append(f"{where}.disk.type hyperdisk: N4, C4 and C4A machine types only")
    size = s["disk"].get("size_gb", 10)
    if not 10 <= size <= 65536:
        out.append(f"{where}.disk.size_gb {size}: 10 to 65536")
    if s.get("data_cache") and edition != "enterprise_plus":
        out.append(f"{where}.data_cache: Enterprise Plus only")
    return out


def instance_warnings(spec: dict, production: bool) -> list[str]:
    tier = str({**INSTANCE_DEFAULTS, **(spec or {})}["tier"])
    if tier in SHARED and production:
        return [f"online_store.cloudsql.tier {tier}: a shared-core machine has no SLA and is meant for tests; "
                "use db-custom-1-3840 or larger in production"]
    return []


# London list prices, USD per hour or per GB-month (Cloud Billing Catalog, 2026-11-07)
SQL_PRICES = {
    ("enterprise", "zonal"): {"vcpu": 0.0496, "gib": 0.0084, "ssd": 0.204, "hdd": 0.108, "hyperdisk": 0.138},
    ("enterprise", "regional"): {"vcpu": 0.0991, "gib": 0.0168, "ssd": 0.408, "hdd": 0.216, "hyperdisk": 0.276},
    ("enterprise_plus", "zonal"): {"vcpu": 0.0644, "gib": 0.0109, "ssd": 0.204, "hyperdisk": 0.138},
    ("enterprise_plus", "regional"): {"vcpu": 0.1289, "gib": 0.0218, "ssd": 0.408, "hyperdisk": 0.276},
}
SHARED_HOURLY = {("db-f1-micro", "zonal"): 0.0126, ("db-f1-micro", "regional"): 0.0252,
                 ("db-g1-small", "zonal"): 0.042, ("db-g1-small", "regional"): 0.084}


def instance_monthly_cost(spec: dict) -> float | None:
    """The instance's list price a month (compute and disk), or None for a tier not priced here."""
    s = {**INSTANCE_DEFAULTS, **(spec or {})}
    disk = {**INSTANCE_DEFAULTS["disk"], **((spec or {}).get("disk") or {})}
    shape = tier_shape(str(s["tier"]))
    prices = SQL_PRICES.get((s["edition"], s["availability"]))
    if shape is None or prices is None:
        return None
    if shape[0] == "shared":
        compute = SHARED_HOURLY[(s["tier"], s["availability"])] * HOURS
    else:
        compute = (shape[1] * prices["vcpu"] + shape[2] * prices["gib"]) * HOURS
    return compute + disk.get("size_gb", 10) * prices.get(disk.get("type", "ssd"), prices["ssd"])


def default_connections(tier: str) -> int | None:
    """Cloud SQL for PostgreSQL's default max_connections by memory (the spike measured 100 on 3.75 GB)."""
    shape = tier_shape(tier)
    if shape is None:
        return None
    gb = shape[2]
    for below, n in ((1, 25), (3.75, 50), (6, 100), (7.5, 200), (15, 400), (30, 500), (60, 600), (120, 800)):
        if gb < below:
            return n
    return 1000
