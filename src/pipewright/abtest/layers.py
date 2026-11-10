"""Layers (Pipewright FR-123): tests that share a unit kept apart, so a unit is in at most one test of a layer.

    ab_test:
      layer: { name: customers, share_bp: 3000 }      # this test takes 30% of the layer's units

Every unit has one position in a layer, from the layer's own hash (SHA-256 of `layer/<name>:<unit>`, the same seven
bytes as the arms). A test in the layer owns a range of positions, [start_bp, end_bp) of 10,000, claimed when it
starts: units in its range are in the test (and split into its arms by the test's own hash, as without a layer);
units outside it are not in the test at all: the default model serves them, they are never exposures and the
analysis never counts them. The ranges of a layer's running tests never meet, so the tests are independent of each
other, at the price of smaller samples: a test with share_bp 3000 sees 30% of the units.

The registry is one object per layer in the environment's artifacts bucket, ab-tests/_layers/<name>.json, written with
a generation match so two starts cannot claim the same range. A range is free again once its test is no longer
active (stopped, concluded): every claim drops the ranges of ended tests first, so nothing has to release them.
Tests without a layer are as before: their own hash over all units, and policies.ab_test.overlap judges them.
"""

from __future__ import annotations

import json

from pipewright.errors import ConfigError
from pipewright.gcp import PreconditionFailed, gcs_read, gcs_write

PREFIX = "ab-tests/_layers"
WHOLE = 10000
ATTEMPTS = 5


def object_name(layer: str) -> str:
    return f"{PREFIX}/{layer}.json"


def read(gcp, bucket: str, layer: str) -> tuple[dict, int]:
    text, generation = gcs_read(gcp, bucket, object_name(layer))
    return (json.loads(text) if text else {"layer": layer, "unit": None, "tests": []}), generation


def first_gap(taken: list[dict], share_bp: int) -> int | None:
    """The lowest start of share_bp free positions between the taken ranges, or None."""
    at = 0
    for r in sorted(taken, key=lambda r: r["start_bp"]):
        if r["start_bp"] - at >= share_bp:
            return at
        at = max(at, r["end_bp"])
    return at if WHOLE - at >= share_bp else None


def claim(gcp, bucket: str, layer: dict, unit: str, usecase: str, test: str, active) -> dict:
    """The test's range in the layer: {name, start_bp, end_bp, share_bp}. `active(usecase, test)` says whether a
    claimed test still runs (its range is dropped otherwise). Refuses a layer of another unit, or one without room."""
    name, share = layer["name"], int(layer["share_bp"])
    for _ in range(ATTEMPTS):
        record, generation = read(gcp, bucket, name)
        if record.get("unit") and record["unit"] != unit and any(active(t["usecase"], t["ab_test"])
                                                                   for t in record["tests"]):
            raise ConfigError(f"layer {name} splits units by {record['unit']}, and this test's unit is {unit}: a "
                              "layer holds one unit; use another layer name")
        kept = [t for t in record["tests"] if active(t["usecase"], t["ab_test"])
                and (t["usecase"], t["ab_test"]) != (usecase, test)]
        start = first_gap(kept, share)
        if start is None:
            used = sum(t["end_bp"] - t["start_bp"] for t in kept)
            names = ", ".join(f"{t['ab_test']} ({t['usecase']}, {t['share_bp'] / 100:g}%)" for t in kept)
            raise ConfigError(f"layer {name} has no {share / 100:g}% left in one range ({used / 100:g}% taken by "
                              f"{names}): lower share_bp, wait for a test to end, or use another layer")
        mine = {"usecase": usecase, "ab_test": test, "start_bp": start, "end_bp": start + share, "share_bp": share}
        try:
            gcs_write(gcp, bucket, object_name(name),
                      json.dumps({"layer": name, "unit": unit, "tests": [*kept, mine]}, indent=2, sort_keys=True),
                      if_generation_match=generation)
        except PreconditionFailed:
            continue                                     # another start claimed at the same time: look again
        return {"name": name, "start_bp": start, "end_bp": start + share, "share_bp": share}
    raise ConfigError(f"layer {name} kept changing while this test claimed its range: try again")


def release(gcp, bucket: str, layer: str, usecase: str, test: str) -> None:
    """The test's range freed at once (a later claim would drop it anyway)."""
    for _ in range(ATTEMPTS):
        record, generation = read(gcp, bucket, layer)
        kept = [t for t in record["tests"] if (t["usecase"], t["ab_test"]) != (usecase, test)]
        if len(kept) == len(record["tests"]):
            return
        try:
            gcs_write(gcp, bucket, object_name(layer), json.dumps({**record, "tests": kept}, indent=2,
                                                                   sort_keys=True), if_generation_match=generation)
            return
        except PreconditionFailed:
            continue
