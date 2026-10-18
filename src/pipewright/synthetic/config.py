"""The `synthetic` section: defaults and cross-checks (FR-49). Off unless a use case sets synthetic.enabled: true.

    synthetic:
      enabled: true
      purpose: test-data            # test-data (stays in the organisation) | sharing (leaves it): sets the gates
      tier: learned                 # learned (an engine learns from real data) | contract (rules only, no real data)
      source_uri: bq://...          # learned: what the engine learns from (read point in time like training)
      output_table: synthetic_customers   # learned: a table in outputs.bigquery.tables
      engine: { type: mostlyai, max_training_minutes: 10 }
      columns: { customer_id: { rule: key } }

Personal-data columns are never learned: without a rule they become new keys (if unique) or nulls.
"""

from __future__ import annotations

from pipewright.synthetic.runtime import engines, gates, rules

TIERS = ("learned", "contract")
MAX_CANDIDATES = 8

DEFAULTS = {
    "enabled": False,
    "purpose": "test-data",
    "tier": "learned",
    "source_uri": None,
    "output_table": None,
    "rows": None,                    # default: as many rows as the source has
    "holdout_fraction": 0.2,         # real rows kept back for utility and privacy checks, never learned
    "seed": None,                    # default: training.seed
    "engine": {"type": "mostlyai", "model": None, "max_training_minutes": 10, "differential_privacy": None,
               "sampling_temperature": 1.0, "rare_categories": "constant", "epochs": 300},
    "candidates": [],                # engine variants tried side by side; the best one passing every gate is kept
    "columns": {},                   # column rules (see pipewright.synthetic.runtime.rules)
    "sensitive_columns": [],         # what an inference attack tries to recover, besides the label
    "gates": {},                     # overrides of the purpose's gates (privacy gates: stricter only)
    "evaluation": {"attacks": 1000},  # the spike: 500 attacks gave bounds too unstable to gate on
    "requirements": None,            # extra packages for the synthetic image (e.g. SDV, which the team installs)
}


def check(ml: dict, columns: dict, label: str | None, known, problems: list[str]) -> None:
    """Fills derived values (candidates, column rules, gates, learning exclusions) into ml["synthetic"]."""
    syn = ml["synthetic"]
    if not syn["enabled"]:
        return
    where = "synthetic"
    contract = ml["data"]["contract"]
    if syn["purpose"] not in gates.PURPOSES:
        problems.append(f"{where}.purpose {syn['purpose']!r} is not one of {', '.join(gates.PURPOSES)}")
    if syn["tier"] not in TIERS:
        problems.append(f"{where}.tier {syn['tier']!r} is not one of {', '.join(TIERS)}")
    if not contract:
        problems.append(f"{where}: needs data.contract (the columns to generate)")
        return
    if syn["seed"] is None:
        syn["seed"] = ml["training"]["seed"]
    if syn["rows"] is not None and syn["rows"] < 1:
        problems.append(f"{where}.rows must be at least 1")

    # Column rules: the use case's, then personal data (never learned).
    unique = set(contract.get("unique") or [])
    resolved = {}
    for name, rule in (syn["columns"] or {}).items():
        problems.extend(rules.problems_for(name, rule, columns.get(name)))
        resolved[name] = rule
    personal = list(ml["data_protection"]["personal_data_columns"])
    for name in personal:
        if name not in resolved:
            resolved[name] = {"rule": "key"} if name in unique else {"rule": "drop"}
        if name == label:
            problems.append(f"{where}: the label {name!r} is personal data, so it cannot be learned")
    syn["columns"] = resolved
    # What the engine never sees: personal data and every rule-decided column (rules replace them afterwards).
    syn["exclude"] = sorted(set(personal) | set(resolved))
    for col in syn["sensitive_columns"]:
        known(col, f"{where}.sensitive_columns")
        if col in syn["exclude"]:
            problems.append(f"{where}.sensitive_columns: {col!r} is never learned, so there is nothing to infer")

    if syn["tier"] == "contract":
        return

    # Learned: source, output, engines, gates.
    if not syn["source_uri"]:
        problems.append(f"{where}.source_uri: the data the engine learns from (bq://... or gs://...)")
    if not syn["output_table"]:
        problems.append(f"{where}.output_table: the table in outputs.bigquery.tables that receives the data")
    if not 0 < syn["holdout_fraction"] < 0.5:
        problems.append(f"{where}.holdout_fraction must be above 0 and below 0.5")
    base = dict(syn["engine"])
    variants = syn["candidates"] or default_candidates(base)
    if len(variants) > MAX_CANDIDATES:
        problems.append(f"{where}.candidates: at most {MAX_CANDIDATES}")
    names = [v.get("name") for v in variants]
    if None in names or len(set(names)) != len(names):
        problems.append(f"{where}.candidates: each needs a unique name")
    resolved_candidates = []
    for v in variants:
        c = {**base, **{k: val for k, val in v.items() if k != "name"}}
        c = {"name": v.get("name"), "engine": c.pop("type"), **c}
        at = f"{where}.candidates[{c['name']}]" if syn["candidates"] else f"{where}.engine"
        if c["engine"] not in engines.ENGINES:
            problems.append(f"{at}.type {c['engine']!r} is not one of {', '.join(engines.ENGINES)}")
        elif c["engine"] == "mostlyai" and c["model"] and c["model"] not in engines.MOSTLY_MODELS:
            problems.append(f"{at}.model {c['model']!r} is not one of {', '.join(engines.MOSTLY_MODELS)}")
        elif c["engine"] == "sdv":
            if c["model"] and c["model"] not in engines.SDV_SYNTHESIZERS:
                problems.append(f"{at}.model {c['model']!r} is not one of {', '.join(engines.SDV_SYNTHESIZERS)}")
            if not syn["requirements"]:
                problems.append(f"{at}: SDV is installed by your team, under its own licence (BUSL-1.1): list it in "
                                f"a requirements file and name it in {where}.requirements")
            if c["differential_privacy"]:
                problems.append(f"{at}: differential privacy is not available with SDV")
        dp = c["differential_privacy"]
        if dp is not None and not (isinstance(dp, dict) and dp.get("epsilon", 0) > 0):
            problems.append(f"{at}.differential_privacy: give {{epsilon: <above 0>}}")
        if c["rare_categories"] not in ("constant", "sample"):
            problems.append(f"{at}.rare_categories must be constant or sample")
        if not 0 < c["sampling_temperature"] <= 2:
            problems.append(f"{at}.sampling_temperature must be above 0 and at most 2")
        if c["max_training_minutes"] <= 0:
            problems.append(f"{at}.max_training_minutes must be above 0")
        resolved_candidates.append(c)
    syn["candidates"] = resolved_candidates
    syn["gates"] = gates.effective(syn["purpose"], syn["gates"], problems)
    if not label:
        syn["gates"].pop("tstr_drop", None)           # utility needs a label to train on
    if syn["evaluation"]["attacks"] < 100:
        problems.append(f"{where}.evaluation.attacks: at least 100 (fewer gives confidence bounds too wide to gate)")


def default_candidates(engine: dict) -> list[dict]:
    """Without synthetic.candidates: the engine as given and, for MOSTLY AI at temperature 1, a wider sampling
    temperature too. In the spike, temperature 1.3 passed the sharing gates on real census records (singling out
    0.13) with fidelity 0.918 and no utility loss, where temperature 1 failed them; both run in parallel."""
    if engine.get("type", "mostlyai") == "mostlyai" and engine.get("sampling_temperature", 1.0) == 1.0 \
            and not engine.get("differential_privacy"):
        return [{"name": "default"}, {"name": "spread", "sampling_temperature": 1.3}]
    return [{"name": "default"}]


def output_problems(syn: dict, contract: dict, tables: dict) -> list[str]:
    """The output table must exist and hold every contract column (checked once tables are loaded)."""
    if not syn["enabled"] or syn["tier"] != "learned" or not syn["output_table"]:
        return []
    name = syn["output_table"]
    if name not in tables:
        return [f"synthetic.output_table: {name!r} is not in outputs.bigquery.tables"]
    have = {f["name"] for f in tables[name].fields}
    missing = [c["name"] for c in contract["columns"] if c["name"] not in have]
    out = []
    if missing:
        out.append(f"synthetic.output_table {name!r}: its schema lacks contract columns {', '.join(missing)}")
    if "run_id" not in have:
        out.append(f"synthetic.output_table {name!r}: add a run_id column (STRING): every synthetic row records the "
                   "run that made it, so a release can be traced or removed")
    return out
