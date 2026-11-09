"""Sticky assignment: the same arm for a unit in Python (scoring, the gateway, the analysis) and in BigQuery SQL.

    bucket(name, unit) = int(first 7 bytes of SHA-256(name + ":" + unit), big endian)       0 <= bucket < 2^56
    treatment          iff bucket < treatment_bp * 2^56 // 10000                             (whole basis points)

Seven bytes because BigQuery has no unsigned 64-bit integer (the first 7 bytes always fit a positive INT64); an
integer threshold so both sides compare exactly, with no floating point at the boundary. Spike S3 (2026-11-09): the
same bucket for 1,000,004 keys (empty, Unicode and 300-character keys included) and the same arms at five allocations.

Lowering the allocation keeps every treatment unit's bucket below the new threshold only if it was below it before:
units only ever move from treatment to control, never the other way, so a test can be scaled down safely. A new name
re-randomises.
"""

from __future__ import annotations

import hashlib

SPACE = 2**56
CONTROL, TREATMENT = "control", "treatment"


def bucket(name: str, unit) -> int:
    return int.from_bytes(hashlib.sha256(f"{name}:{unit}".encode()).digest()[:7], "big")


def threshold(treatment_bp: int) -> int:
    if not 0 <= int(treatment_bp) <= 10000:
        raise ValueError(f"treatment_bp must be 0 to 10000, not {treatment_bp}")
    return int(treatment_bp) * SPACE // 10000


def arm(name: str, unit, treatment_bp: int) -> str:
    if unit is None or unit == "":
        return CONTROL                       # no key: control, counted as unassigned by the caller
    return TREATMENT if bucket(name, unit) < threshold(treatment_bp) else CONTROL


def sql_bucket(name: str, unit_sql: str) -> str:
    """BigQuery: the same bucket as `bucket()` for the unit expression (cast to STRING as Python formats it)."""
    quoted = name.replace("\\", "\\\\").replace("'", "\\'")
    return (f"CAST(CONCAT('0x', TO_HEX(SUBSTR(SHA256(CONCAT('{quoted}', ':', CAST({unit_sql} AS STRING))), 1, 7)))"
            f" AS INT64)")


def sql_arm(name: str, unit_sql: str, treatment_bp: int) -> str:
    return (f"IF({unit_sql} IS NULL OR CAST({unit_sql} AS STRING) = '', '{CONTROL}', "
            f"IF({sql_bucket(name, unit_sql)} < {threshold(treatment_bp)}, '{TREATMENT}', '{CONTROL}'))")
