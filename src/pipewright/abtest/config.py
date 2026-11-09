"""The `ab_test` section: defaults and checks against the data contract, the task and the rest of the use case.

Off unless `enabled: true`. When off the section is left out of ml_config, so a use case without it resolves, compiles
and releases exactly as before (its ml_config and template digests do not change), as synthetic data does.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re

DEFAULTS = {
    "enabled": False,
    "name": None,
    "mode": "ab",                         # ab: units see their arm's model | shadow: the treatment only scores copies
    "unit": None,                         # a data contract column; default: the entity or erasure key
    "arms": {"control": "champion", "treatment": {"model_version": None}},
    "allocation": {"treatment_bp": 1000},
    "metric": {"name": None, "source": None, "key": None, "time_column": None, "value_column": None,
               "denominator_column": None, "where": None, "window_days": 30, "type": "proportion",
               "direction": "higher", "minimum_effect": None},
    "guardrails": {
        "technical": {"max_error_rate_increase": 0.01, "max_latency_increase": 0.2, "min_requests": 200,
                      "breaker": {"window": 200, "max_error_rate": 0.05}},
        "business": [],
    },
    "duration": {"min_days": 14, "max_days": 60},
    "analysis": {"method": "fixed", "alpha": 0.05, "power": 0.8, "schedule": "0 6 * * *",
                 "cuped": {"enabled": False, "covariate": None, "source": None},
                 "segments": [], "tau": None},
    "decision": {"approvals": {"required": []}},
    "publish": {"events": False, "exposures": False},
    "results_table": None,
}
METRIC_TYPES = ("proportion", "mean", "ratio")
# Tasks with a unit-level outcome. Forecasts and unsupervised models have none (CONTEXT 8.16: refused).
TASKS = ("binary", "multiclass", "regression")
NAME = re.compile(r"^[a-z][a-z0-9-]{1,40}[a-z0-9]$")
# What may not change while a test runs: the config digest at start covers exactly these (pre-registration).
FROZEN = ("name", "unit", "mode", "metric", "guardrails", "analysis", "arms")


def digest(section: dict) -> str:
    """The pre-registration digest: what the test promised before it started. allocation (lower only), duration,
    publish and the decision's approvers may change while it runs; nothing in FROZEN may."""
    frozen = {k: section.get(k) for k in FROZEN}
    return hashlib.sha256(json.dumps(frozen, sort_keys=True, default=str).encode()).hexdigest()


def check(ml: dict, columns: set, label: str | None, known, problems: list[str], task: str = "binary") -> None:
    """Fills defaults into ml["ab_test"] and appends problems. `known(column, where)` checks a contract column."""
    t = ml["ab_test"]
    if not t.get("enabled"):
        return
    where = "ab_test"
    if task not in TASKS:
        problems.append(f"{where}: a {task} model has no unit-level outcome to compare; A/B tests need a binary, "
                        "multiclass or regression model")
    if not t.get("name") or not NAME.match(str(t["name"])):
        problems.append(f"{where}.name: required, 3 to 42 characters, lower case letters, digits and hyphens (it "
                        "salts the assignment: a new name re-randomises)")
    if t["mode"] not in ("ab", "shadow"):
        problems.append(f"{where}.mode: ab or shadow, not {t['mode']!r}")
    if not t.get("unit"):
        erasure = (ml.get("data_protection") or {}).get("erasure") or {}
        entity = (((ml.get("data") or {}).get("features_from") or {}).get("entity") or {}).get("columns") or []
        t["unit"] = erasure.get("key_column") or (entity[0] if entity else None)
    if not t.get("unit"):
        problems.append(f"{where}.unit: required (the column units are assigned by, such as customer_id)")
    elif columns:
        known(t["unit"], f"{where}.unit")
    if t["unit"] and t["unit"] == label:
        problems.append(f"{where}.unit: {t['unit']} is the label")
    treatment = (t.get("arms") or {}).get("treatment") or {}
    if not isinstance(treatment, dict) or not treatment.get("model_version"):
        problems.append(f"{where}.arms.treatment.model_version: required (the candidate version to test)")
    if (t.get("arms") or {}).get("control") != "champion":
        problems.append(f"{where}.arms.control: champion (the default version when the test starts) is the only "
                        "control")
    bp = (t.get("allocation") or {}).get("treatment_bp")
    if not isinstance(bp, int) or not 0 < bp < 10000:
        problems.append(f"{where}.allocation.treatment_bp: whole basis points between 1 and 9999 (2000 is 20%)")
    if t["mode"] == "ab":
        _metric(t["metric"], f"{where}.metric", problems, required=True)
        for i, g in enumerate(t["guardrails"].get("business") or []):
            g.setdefault("type", "mean")
            g.setdefault("direction", "higher")
            g.setdefault("window_days", t["metric"].get("window_days", 30))
            _metric(g, f"{where}.guardrails.business[{i}]", problems, required=False)
            if not isinstance(g.get("max_drop"), (int, float)) or not 0 < g["max_drop"] < 1:
                problems.append(f"{where}.guardrails.business[{i}].max_drop: a relative drop between 0 and 1, "
                                "such as 0.02")
    d = t["duration"]
    if not (isinstance(d.get("min_days"), int) and isinstance(d.get("max_days"), int)
            and 1 <= d["min_days"] <= d["max_days"] <= 365):
        problems.append(f"{where}.duration: 1 <= min_days <= max_days <= 365")
    a = t["analysis"]
    if a["method"] not in ("fixed", "sequential"):
        problems.append(f"{where}.analysis.method: fixed or sequential")
    if not 0 < a["alpha"] < 0.5 or not 0.5 <= a["power"] < 1:
        problems.append(f"{where}.analysis: alpha in (0, 0.5) and power in [0.5, 1)")
    cup = a.get("cuped") or {}
    if cup.get("enabled") and not (cup.get("covariate") and str(cup.get("source") or "").startswith("bq://")):
        problems.append(f"{where}.analysis.cuped: covariate and source (a bq:// feature table with the unit key and "
                        "feature_timestamp) are required with cuped.enabled; the newest value at or before the "
                        "test's start is used")
    if cup.get("enabled") and t["metric"].get("type") == "ratio":
        problems.append(f"{where}.analysis.cuped: not for a ratio metric")
    for s in a.get("segments") or []:
        known(s, f"{where}.analysis.segments")
    pub = t["publish"]
    if pub.get("exposures") and t["mode"] != "ab":
        problems.append(f"{where}.publish.exposures: only an ab test has exposures")


def _metric(m: dict, where: str, problems: list[str], required: bool) -> None:
    if not m.get("name"):
        problems.append(f"{where}.name: required")
    if not str(m.get("source") or "").startswith("bq://"):
        problems.append(f"{where}.source: a bq:// table of outcome events (one row per event, with the unit key "
                        "and a time)")
    if not m.get("time_column"):
        problems.append(f"{where}.time_column: required (outcomes count within window_days of a unit's first "
                        "exposure)")
    if m.get("type") not in METRIC_TYPES:
        problems.append(f"{where}.type: one of {', '.join(METRIC_TYPES)}")
    if m.get("type") in ("mean", "ratio") and not m.get("value_column"):
        problems.append(f"{where}.value_column: required for a {m.get('type')} metric")
    if m.get("type") == "ratio" and not m.get("denominator_column"):
        problems.append(f"{where}.denominator_column: required for a ratio metric")
    if m.get("direction") not in ("higher", "lower"):
        problems.append(f"{where}.direction: higher or lower (which way is better)")
    if not isinstance(m.get("window_days"), int) or not 1 <= m["window_days"] <= 365:
        problems.append(f"{where}.window_days: 1 to 365")
    if required and not (isinstance(m.get("minimum_effect"), (int, float)) and m["minimum_effect"] > 0):
        problems.append(f"{where}.minimum_effect: required, above 0 (the smallest absolute change worth "
                        "detecting; it sizes the test)")


def check_tables(ml: dict, tables: dict, has_inference: bool, serving: bool, problems: list[str]) -> None:
    """Where the arms are recorded: a batch test writes ab_test and arm beside each score in inference.output_table
    (nullable STRING columns the team adds to its schema); an online test needs model.serving."""
    t = ml["ab_test"]
    if not has_inference and not serving:
        problems.append("ab_test: the use case scores nothing (no inference pipeline, model.serving off): a test "
                        "needs batch scoring, online serving or both")
    output = (ml.get("inference") or {}).get("output_table")
    table = tables.get(output) if output else None
    if has_inference and table is not None and table.fields:
        have = {f["name"]: str(f.get("type", "")).upper() for f in table.fields}
        missing = [c for c in ("ab_test", "arm") if c not in have]
        if missing:
            problems.append(f"ab_test: add {' and '.join(missing)} (STRING, NULLABLE) to {output}'s schema: scoring "
                            "records each unit's arm there, and the analysis reads it")
    if t.get("results_table") and t["results_table"] in tables:
        problems.append("ab_test.results_table: the analysis creates and owns it; leave it out of "
                        "outputs.bigquery.tables")


def apply_policy(t: dict, policy: dict, env_name: str, problems: list[str]) -> None:
    """The platform's rules (manifest policies.ab_test): allowed, required approver groups, the most a treatment may
    get, a minimum duration, whether per-unit exposures may leave in the feed."""
    if t["publish"].get("exposures") and not (policy or {}).get("feed_exposures"):
        problems.append(f"ab_test.publish.exposures: per-unit exposures may not leave the tenant's dataset in "
                        f"{env_name} (the platform has not set policies.ab_test.feed_exposures: true)")
    if not policy:
        return
    if policy.get("allowed") is False:
        problems.append(f"ab_test: A/B tests are not allowed in {env_name} (platform policy policies.ab_test.allowed)")
    cap = policy.get("max_treatment_bp")
    if cap is not None and (t.get("allocation") or {}).get("treatment_bp", 0) > cap:
        problems.append(f"ab_test.allocation.treatment_bp {t['allocation']['treatment_bp']} is above {env_name}'s "
                        f"limit of {cap} (policies.ab_test.max_treatment_bp)")
    floor = policy.get("min_days")
    if floor is not None and t["duration"]["min_days"] < floor:
        problems.append(f"ab_test.duration.min_days {t['duration']['min_days']} is below {env_name}'s minimum of "
                        f"{floor} (policies.ab_test.min_days)")
    required = list(dict.fromkeys([*(policy.get("required_approvers") or []),
                                   *t["decision"]["approvals"]["required"]]))
    t["decision"]["approvals"]["required"] = required


def resolved(ml: dict) -> dict | None:
    """The section when on, else None."""
    t = ml.get("ab_test")
    return copy.deepcopy(t) if t and t.get("enabled") else None
