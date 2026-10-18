"""The `synthetic` section: defaults and cross-checks (FR-49). Off unless a use case sets synthetic.enabled: true.

    synthetic:
      enabled: true
      purpose: test-data            # test-data (stays in the organisation) | sharing (leaves it): sets the gates
      tier: learned                 # learned (an engine learns from real data) | contract (rules only, no real data)
      source_uri: bq://...          # learned: what the engine learns from (read point in time like training)
      output_table: synthetic_customers   # learned: a table in outputs.bigquery.tables
      engine: { type: mostlyai, max_training_minutes: 10 }
      columns: { customer_id: { rule: key } }

Never learned: personal-data columns (without a rule they become new keys if unique, else nulls) and keys (unique
columns and the entity columns of feature tables get new keys).

mode: relational learns the entity table and its child tables together, one model: the feature tables of
data.features_from (history per entity) and synthetic.sequences (events per entity, in order). Each generated
table goes to its own output table (output_table for the entity table, output_tables for each child); the
evaluation joins the synthetic tables point in time, as training does, and checks their integrity.
"""

from __future__ import annotations

import re

from pipewright.synthetic.runtime import engines, gates, rules

TIERS = ("learned", "contract")
MODES = ("flat", "relational")
MAX_CANDIDATES = 8
_BQ = re.compile(r"^bq://(?:[a-z][a-z0-9-]{4,61}[a-z0-9]\.)?[A-Za-z_][A-Za-z0-9_]{0,1023}\.[A-Za-z0-9_$-]{1,1024}$")

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
    "mode": "flat",                  # flat (one table) | relational (the entity table and its child tables)
    "output_tables": {},             # relational: child table name -> table in outputs.bigquery.tables
    "sequences": [],                 # relational: event tables per entity [{name, source_uri, key, order_by}]
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
    # Keys are never learned either: unique columns and the entity columns of feature tables get new keys.
    entity = list(((ml["data"].get("features_from") or {}).get("entity") or {}).get("columns") or [])
    for name in dict.fromkeys([*unique, *entity]):
        if name in columns and name not in resolved:
            resolved[name] = {"rule": "key"}
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
    if syn["mode"] not in MODES:
        problems.append(f"{where}.mode {syn['mode']!r} is not one of {', '.join(MODES)}")
    elif syn["mode"] == "relational":
        _relational(syn, ml, columns, problems)
    elif syn["sequences"] or syn["output_tables"]:
        problems.append(f"{where}: sequences and output_tables need mode: relational")
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


def apply_policy(syn: dict, policy: dict, env_name: str, approver_groups: dict | None, problems: list[str]) -> None:
    """The platform's limits (manifest policies.synthetic): whether synthetic data is allowed here, for which
    purposes and engines, where differential privacy is required, gate floors (a looser use case gate is raised to
    the floor), and the approver groups a release needs (recorded in synthetic.approvers for Enterprise)."""
    where = f"(platform policy in {env_name})"
    if policy.get("allowed") is False:
        problems.append(f"synthetic: synthetic data is not allowed in {env_name} {where}")
        return
    if policy.get("allowed_purposes") and syn["purpose"] not in policy["allowed_purposes"]:
        problems.append(f"synthetic.purpose {syn['purpose']} is not allowed in {env_name} {where}: "
                        f"{', '.join(policy['allowed_purposes'])}")
    for c in syn.get("candidates") or []:
        if policy.get("allowed_engines") and c["engine"] not in policy["allowed_engines"]:
            problems.append(f"synthetic candidate {c['name']}: engine {c['engine']} is not allowed in {env_name} "
                            f"{where}: {', '.join(policy['allowed_engines'])}")
        if syn["purpose"] in (policy.get("differential_privacy_for") or []) and not c.get("differential_privacy"):
            problems.append(f"synthetic candidate {c['name']}: purpose {syn['purpose']} requires differential "
                            f"privacy in {env_name} {where}: set differential_privacy: {{epsilon: ...}}")
    for metric, floor in (policy.get("gates") or {}).items() if syn["tier"] == "learned" else ():
        if metric not in syn["gates"] or gates.GATES[metric][0] not in floor:
            continue                       # (unknown gates and tstr_drop without a label: nothing to raise)
        direction = gates.GATES[metric][0]
        current = syn["gates"][metric][direction]
        syn["gates"][metric] = {direction: (max if direction == "min" else min)(current, floor[direction])}
    approvers = list(policy.get("required_approvers") or [])
    if approvers and approver_groups is not None:
        for group in approvers:
            if group not in approver_groups:
                problems.append(f"policies.synthetic.required_approvers: {group!r} is not an approver group in "
                                f"{env_name}")
    syn["approvers"] = approvers


def default_candidates(engine: dict) -> list[dict]:
    """Without synthetic.candidates: the engine as given and, for MOSTLY AI at temperature 1, a wider sampling
    temperature too. In the spike, temperature 1.3 passed the sharing gates on real census records (singling out
    0.13) with fidelity 0.918 and no utility loss, where temperature 1 failed them; both run in parallel."""
    if engine.get("type", "mostlyai") == "mostlyai" and engine.get("sampling_temperature", 1.0) == 1.0 \
            and not engine.get("differential_privacy"):
        return [{"name": "default"}, {"name": "spread", "sampling_temperature": 1.3}]
    return [{"name": "default"}]


def _relational(syn: dict, ml: dict, columns: dict, problems: list[str]) -> None:
    """mode: relational. Resolves synthetic.relational = {key, time_column, parent_columns, children}: the entity
    table's key (one column) and cut-off, the contract columns it holds, and every child table (the feature tables
    of data.features_from, then synthetic.sequences) with its key, order and output table."""
    from pipewright.config.ml import feature_columns

    where = "synthetic"
    ff = ml["data"].get("features_from")
    contract = ml["data"]["contract"] or {"columns": []}
    keys = (ff or {}).get("entity", {}).get("columns") or list(contract.get("unique") or [])
    if len(keys) != 1:
        problems.append(f"{where}.mode relational: the entity table needs exactly one key column "
                        "(data.features_from.entity.columns, or data.contract.unique)")
        return
    key = keys[0]
    from_features = feature_columns(ff)
    children, names = [], set()
    for i, src in enumerate((ff or {}).get("sources") or []):
        name = src.get("feature_group") or src["table"].rsplit(".", 1)[-1]
        names.add(name)
        children.append({"name": name, "kind": "features", "index": i, "key": (src.get("entity_columns") or keys)[0],
                         "order_by": src.get("time_column", "feature_timestamp"),
                         "features": {src.get("prefix", "") + f: f for f in src["features"]},
                         "output_table": syn["output_tables"].get(name)})
    for j, seq in enumerate(syn["sequences"]):
        at = f"{where}.sequences[{j}]"
        missing = [k for k in ("name", "source_uri", "key", "order_by") if not seq.get(k)]
        if missing:
            problems.append(f"{at}: give {', '.join(missing)}")
            continue
        if seq["name"] in names:
            problems.append(f"{at}: the name {seq['name']!r} is taken (a feature table or another sequence)")
        names.add(seq["name"])
        if "${" not in seq["source_uri"] and not _BQ.match(seq["source_uri"]):
            problems.append(f"{at}.source_uri: a bq://project.dataset.table (or bq://dataset.table)")
        children.append({"name": seq["name"], "kind": "sequence", "source_uri": seq["source_uri"],
                         "key": seq["key"], "order_by": seq["order_by"], "columns": seq.get("columns"),
                         "output_table": syn["output_tables"].get(seq["name"])})
    if not children:
        problems.append(f"{where}.mode relational: no child tables (data.features_from or synthetic.sequences)")
    for c in children:
        if not c["output_table"]:
            problems.append(f"{where}.output_tables.{c['name']}: the table in outputs.bigquery.tables that receives "
                            f"the synthetic {c['name']} rows")
    for name in syn["output_tables"]:
        if name not in names:
            problems.append(f"{where}.output_tables.{name}: no child table of that name "
                            f"(children: {', '.join(sorted(names)) or 'none'})")
    syn["relational"] = {"key": key, "time_column": (ff or {}).get("entity", {}).get("time_column"),
                         "parent_columns": [c for c in columns if c not in from_features], "children": children}
    # The key is the entity table's primary key: learned as a key (never as values) and replaced by the key rule.
    syn["exclude"] = [c for c in syn["exclude"] if c != key]


def output_problems(syn: dict, contract: dict, tables: dict) -> list[str]:
    """The output tables must exist and hold what goes into them, plus run_id (checked once tables are loaded):
    flat, every contract column; relational, the entity table's columns, and each child's key."""
    if not syn["enabled"] or syn["tier"] != "learned" or not syn["output_table"]:
        return []
    rel = syn.get("relational")
    wanted = {syn["output_table"]: (rel["parent_columns"] if rel else [c["name"] for c in contract["columns"]])}
    for child in (rel or {}).get("children", []):
        if child["output_table"]:
            wanted[child["output_table"]] = [child["key"]]
    out = []
    for name, need in wanted.items():
        where = f"synthetic output table {name!r}"
        if name not in tables:
            out.append(f"{where} is not in outputs.bigquery.tables")
            continue
        have = {f["name"] for f in tables[name].fields}
        missing = [c for c in need if c not in have]
        if missing:
            out.append(f"{where}: its schema lacks {', '.join(missing)}")
        if "run_id" not in have:
            out.append(f"{where}: add a run_id column (STRING): every synthetic row records the run that made it, "
                       "so a release can be traced or removed")
    return out
