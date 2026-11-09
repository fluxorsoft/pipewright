"""The launcher's part in a batch A/B test: what a scoring run of a use case with a running test gets.

A scoring pipeline that declares an `ab_test` parameter gets the test (name, unit, the current allocation) and the
treatment's approved files, checked as the champion's are (the treatment's approval record, every file's generation
and SHA-256). A paused test sends allocation 0, so every unit is scored by the control. When the treatment cannot be
checked under `policies.integrity.mode: refuse`, the run scores everyone with the control and the problem is
recorded: a broken treatment never decides a customer's score.
"""

from __future__ import annotations

import copy
import json

from pipewright.abtest.store import ACTIVE, TestStore


def active_test(gcp, bucket: str, usecase: str) -> dict | None:
    t = TestStore(gcp, bucket).current(usecase)
    return t if t and t.get("state") in ACTIVE and t.get("mode", "ab") == "ab" else None


def current_bp(test: dict) -> int:
    if test.get("state") == "paused":
        return 0
    alloc = test.get("allocation") or []
    return int(alloc[-1]["treatment_bp"]) if alloc else 0


def param(gcp, manifest: dict, contract, gate=None) -> tuple[str | None, dict]:
    """(the run's ab_test parameter as JSON, or None; what to record: {test, treatment, bp} or {problem})."""
    from pipewright.models import integrity

    test = active_test(gcp, manifest["artifacts_bucket"], contract.usecase)
    if test is None:
        return None, {}
    bp = current_bp(test)
    version = str((test.get("arms") or {}).get("treatment", {}).get("version") or "")
    if not version:
        return None, {"problem": f"test {test['name']} names no treatment version"}
    # The treatment is checked whatever policies.integrity.inference says: a test exposes customers to it, so its
    # bytes must be the approved ones (FR-57). The policy's mode decides what a problem does.
    forced = copy.deepcopy(manifest)
    integ = forced.setdefault("policies", {}).setdefault("integrity", {})
    integ["inference"], integ["pin"] = "warn", "on"
    check = (gate or integrity.inference_gate)(gcp, forced, contract, version)
    mode = ((manifest.get("policies") or {}).get("integrity") or {}).get("mode", "refuse")
    if check.get("problem") and (mode == "refuse" or not check.get("approved_files")):
        return None, {"test": test["name"], "problem": f"treatment v{version}: {check['problem']}",
                      "scored": "control only"}
    spec = {"name": test["name"], "unit": test["unit"], "treatment_bp": bp,
            "treatment": {"version": version, "approved_files": check["approved_files"]}}
    record = {"test": test["name"], "treatment": version, "treatment_bp": bp}
    if check.get("problem"):
        record["problem"] = f"treatment v{version} (warn): {check['problem']}"
    return json.dumps(spec, sort_keys=True), record
