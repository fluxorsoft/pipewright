"""List prices for what FR-92 and FR-105 add, so `validate`, `doctor` and `serving pools plan` show a cost before a
toggle is switched on. Google's list prices for europe-west2 (London), read from the Cloud Billing Catalog on
2026-11-07, before discounts; other regions differ a little. A price this table does not know is said to be unknown,
never guessed.
"""

from __future__ import annotations

import re

HOURS_A_MONTH = 730
# Agent Platform prediction, N1: a vCPU hour and a GiB hour, the machine plus Agent Platform's management fee.
N1_VCPU_HOUR = 0.047 + 0.00613
N1_GIB_HOUR = 0.006 + 0.000783
N1_GIB_PER_VCPU = {"standard": 3.75, "highmem": 6.5, "highcpu": 0.9}
E2_STANDARD_2_HOUR = 0.1123            # about $82 a month; larger E2 shapes scale with it
PSC_ENDPOINT_HOUR = 0.01               # a Private Service Connect endpoint (forwarding rule), about $7.30 a month
PSC_DATA_GIB = (0.004, 0.01)           # data processed, by monthly volume
DNS_ZONE_MONTH = 0.20                  # a private Cloud DNS zone


def machine_hour(machine: str) -> float | None:
    """The hourly list price of one Agent Platform prediction node of this machine type, or None."""
    m = re.match(r"^n1-(standard|highmem|highcpu)-(\d+)$", machine or "")
    if m:
        vcpu = int(m.group(2))
        return round(vcpu * N1_VCPU_HOUR + vcpu * N1_GIB_PER_VCPU[m.group(1)] * N1_GIB_HOUR, 4)
    m = re.match(r"^e2-standard-(\d+)$", machine or "")
    if m and int(m.group(1)) in (2, 4, 8, 16, 32):
        return round(E2_STANDARD_2_HOUR * int(m.group(1)) / 2, 4)
    return None


def month(hourly: float | None, count: int = 1) -> str:
    if hourly is None:
        return "price unknown to Pipewright (see Agent Platform pricing)"
    return f"about ${hourly * count * HOURS_A_MONTH:,.0f} a month"


def pool_cost(pool: dict) -> str:
    """What a pool costs at its minimum, the floor it never goes below (no scale to zero on pools)."""
    hourly = machine_hour(pool.get("machine", ""))
    n = int(pool.get("min_replicas", 1))
    if hourly is None:
        return f"{n} x {pool.get('machine')}: {month(None)}"
    return f"{n} x {pool['machine']} at ${hourly:.3f} an hour: {month(hourly, n)} at its minimum"


def private_endpoint_cost() -> str:
    return (f"a private endpoint: ${PSC_ENDPOINT_HOUR:.2f} an hour ({month(PSC_ENDPOINT_HOUR)}) plus "
            f"${PSC_DATA_GIB[0]} to ${PSC_DATA_GIB[1]} per GiB processed")
