"""Sticky assignment: the same arm for a unit in Python (scoring, the gateway, the analysis) and in BigQuery SQL.

    bucket(name, unit) = int(first 7 bytes of SHA-256(name + ":" + unit), big endian)       0 <= bucket < 2^56
    treatment          iff bucket < treatment_bp * 2^56 // 10000                             (whole basis points)

Seven bytes because BigQuery has no unsigned 64-bit integer (the first 7 bytes always fit a positive INT64); an
integer threshold so both sides compare exactly, with no floating point at the boundary. Spike S3 (2026-11-09): the
same bucket for 1,000,004 keys (empty, Unicode and 300-character keys included) and the same arms at five allocations.

With a layer (FR-123), a unit is first placed in the layer by a second hash, `layer/<name>:<unit>`; only units in
the test's range of the layer are in the test (arm None, NULL in SQL, for the others).

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


def in_layer(layer: dict | None, unit) -> bool:
    """FR-123: whether the unit's position in the layer (its own hash, `layer/<name>`) is in the test's range
    [start_bp, end_bp) of 10,000. True without a layer."""
    if not layer:
        return True
    b = bucket(f"layer/{layer['name']}", unit)
    return int(layer["start_bp"]) * SPACE // 10000 <= b < int(layer["end_bp"]) * SPACE // 10000


def arm(name: str, unit, treatment_bp: int, layer: dict | None = None) -> str | None:
    """control or treatment; None for a unit outside the test's range of its layer (FR-123: not in the test, the
    default serves it, never an exposure)."""
    if unit is None or unit == "":
        return CONTROL                       # no key: control, counted as unassigned by the caller
    if not in_layer(layer, unit):
        return None
    return TREATMENT if bucket(name, unit) < threshold(treatment_bp) else CONTROL


def sql_bucket(name: str, unit_sql: str) -> str:
    """BigQuery: the same bucket as `bucket()` for the unit expression (cast to STRING as Python formats it)."""
    quoted = name.replace("\\", "\\\\").replace("'", "\\'")
    return (f"CAST(CONCAT('0x', TO_HEX(SUBSTR(SHA256(CONCAT('{quoted}', ':', CAST({unit_sql} AS STRING))), 1, 7)))"
            f" AS INT64)")


def sql_in_layer(layer: dict | None, unit_sql: str) -> str:
    """SQL: the unit is in the test's range of its layer (TRUE without a layer)."""
    if not layer:
        return "TRUE"
    b = sql_bucket(f"layer/{layer['name']}", unit_sql)
    return (f"({b} >= {int(layer['start_bp']) * SPACE // 10000} AND "
            f"{b} < {int(layer['end_bp']) * SPACE // 10000})")


def sql_arm(name: str, unit_sql: str, treatment_bp: int, layer: dict | None = None) -> str:
    """The same arm in SQL; NULL outside the test's range of its layer."""
    arm_sql = (f"IF({sql_bucket(name, unit_sql)} < {threshold(treatment_bp)}, '{TREATMENT}', '{CONTROL}')")
    if layer:
        b = sql_bucket(f"layer/{layer['name']}", unit_sql)
        lo, hi = int(layer["start_bp"]) * SPACE // 10000, int(layer["end_bp"]) * SPACE // 10000
        arm_sql = f"IF({b} >= {lo} AND {b} < {hi}, {arm_sql}, NULL)"
    return f"IF({unit_sql} IS NULL OR CAST({unit_sql} AS STRING) = '', '{CONTROL}', {arm_sql})"
