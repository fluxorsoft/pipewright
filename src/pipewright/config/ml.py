"""ML lifecycle sections: defaults and cross-checks.

data · features · training · evaluation · explainability · registry · model_card · inference · monitoring ·
retraining · data_protection

Pipewright checks that the sections agree with each other and with the data contract; the pipeline code does
the ML. The resolved sections reach the pipeline as one `ml_config` parameter.
"""

from __future__ import annotations

import copy
import re

from pipewright.config.merge import deep_merge

METRICS = {"auc", "pr_auc", "accuracy", "f1", "precision", "recall", "log_loss", "brier", "calibration_error"}
LOWER_IS_BETTER = {"log_loss", "brier", "calibration_error"}
TIME_TYPES = {"date", "timestamp"}
RETRAINING_MODES = ("off", "propose", "candidate")

DEFAULTS = {
    "data": {"contract": None, "label": None,
             "split": {"strategy": "stratified", "time_column": None, "test_fraction": 0.2,
                       "validation_fraction": 0.0},
             "features_from": None},
    "features": {"include": None, "exclude": []},
    "training": {"hyperparameters": {}, "seed": 42, "experiment": {"name": None},
                 "tuning": {"enabled": False, "metric": None, "goal": "maximize", "max_trials": 8,
                            "parallel_trials": 4, "search_space": {}}},
    "evaluation": {"primary_metric": "auc", "decision_threshold": 0.5,
                   "metrics": ["auc", "pr_auc", "accuracy", "f1", "precision", "recall", "log_loss",
                               "calibration_error"],
                   "gates": [], "champion": {"enabled": True, "metric": None, "min_improvement": 0.0},
                   "slices": [], "slice_gates": []},
    "explainability": {"enabled": False, "method": "permutation", "top_k": 10},
    "registry": {"set_default": None},            # None: follows approvals.promote_model (see resolve.py)
    "model_card": {},
    "inference": {"output_table": None, "decision_threshold": None},
    "monitoring": {"drift": {"enabled": False, "method": "psi", "threshold": 0.2, "features": None},
                   "prediction_drift": {"enabled": False, "threshold": 0.2},
                   "performance": {"enabled": False, "labels": None, "metric": None, "min": None},
                   "output_table": None,
                   "alerts": {"notify": [], "events": ["monitoring.alert"], "severity": "WARNING"}},
    # mode: off (alerts only) | propose (a person decides) | candidate (trains, never promotes). The older
    # `enabled: true` means propose; `enabled: false` means off. See pipewright.retraining.
    "retraining": {"mode": None, "enabled": None, "when": ["schedule"], "cooldown_days": 7, "source_uri": None,
                   "decisions": {"notify": [], "publish": False, "escalate_after_days": 3, "escalate_notify": [],
                                 "digest": True},
                   "approvals": {"required": [], "separate_from_requester": True},
                   "snooze": {"max_days": 30, "reopen_when_worse_by": 0.1}},
    "data_protection": {"personal_data_columns": [], "erasure": None},
}
SECTIONS = tuple(DEFAULTS)


def resolve_ml(spec: dict, usecase: str, has_inference: bool, table_names: set[str],
               problems: list[str]) -> dict:
    ml = {name: deep_merge(DEFAULTS[name], spec.get(name) or {}) for name in SECTIONS}
    data, features, training, evaluation = ml["data"], ml["features"], ml["training"], ml["evaluation"]
    monitoring, retraining, protection = ml["monitoring"], ml["retraining"], ml["data_protection"]

    contract = data["contract"]
    columns = {c["name"]: c for c in (contract or {}).get("columns", [])}
    if contract and len(columns) != len(contract["columns"]):
        problems.append("data.contract.columns: column names must be unique")

    def known(col: str, where: str) -> bool:
        if columns and col not in columns:
            problems.append(f"{where}: column {col!r} is not in data.contract.columns")
            return False
        return True

    # ---- label, split
    label = (data.get("label") or {}).get("column")
    if label:
        known(label, "data.label.column")
    for col in (contract or {}).get("unique", []):
        known(col, "data.contract.unique")
    split = data["split"]
    split.setdefault("seed", training["seed"])
    if split["strategy"] == "time":
        tc = split.get("time_column")
        if not tc:
            problems.append("data.split: strategy time needs time_column")
        elif known(tc, "data.split.time_column") and columns and columns[tc]["type"] not in TIME_TYPES:
            problems.append(f"data.split.time_column {tc!r} must be a date or timestamp column")
    if split["strategy"] == "stratified" and not label and contract:
        problems.append("data.split: strategy stratified needs data.label.column")
    if split["test_fraction"] + split.get("validation_fraction", 0) >= 1:
        problems.append("data.split: test_fraction + validation_fraction must be below 1")

    _features_from(data.get("features_from"), columns, label, known, problems)

    # ---- features
    personal = set(protection["personal_data_columns"])
    for col in personal:
        known(col, "data_protection.personal_data_columns")
    if features["include"] is None and columns:
        skip = {label, split.get("time_column"), *personal, *features["exclude"]}
        features["include"] = [c for c in columns if c not in skip]
    for col in features["include"] or []:
        known(col, "features.include")
        if col in personal:
            problems.append(f"features.include: {col!r} is personal data (data_protection.personal_data_columns)")
        if col == label:
            problems.append(f"features.include: {col!r} is the label")
        if col in features["exclude"]:
            problems.append(f"features.include: {col!r} is also in features.exclude")

    # ---- training: experiment, tuning
    if training["experiment"].get("name") is None:
        training["experiment"]["name"] = usecase
    tuning = training["tuning"]
    tuning["metric"] = tuning["metric"] or evaluation["primary_metric"]
    if tuning["metric"] not in METRICS:
        problems.append(f"training.tuning.metric {tuning['metric']!r} is not one of {sorted(METRICS)}")
    if tuning["enabled"]:
        if not tuning["search_space"]:
            problems.append("training.tuning: enabled but search_space is empty")
        if tuning["parallel_trials"] > tuning["max_trials"]:
            problems.append("training.tuning: parallel_trials is greater than max_trials")
        if not data["split"].get("validation_fraction"):
            problems.append("training.tuning: trials are scored on a validation split; set "
                            "data.split.validation_fraction")
    for name, dim in tuning["search_space"].items():
        where = f"training.tuning.search_space.{name}"
        if name not in training["hyperparameters"]:
            problems.append(f"{where}: give a default in training.hyperparameters.{name}")
        if dim["type"] in ("double", "integer"):
            if "min" not in dim or "max" not in dim or dim["min"] >= dim["max"]:
                problems.append(f"{where}: needs min < max")
            elif dim.get("scale") == "log" and dim["min"] <= 0:
                problems.append(f"{where}: log scale needs min > 0")
        elif not dim.get("values"):
            problems.append(f"{where}: type {dim['type']} needs values")

    # ---- evaluation
    if evaluation["primary_metric"] not in METRICS:
        problems.append(f"evaluation.primary_metric {evaluation['primary_metric']!r} is not one of {sorted(METRICS)}")
    for m in evaluation["metrics"]:
        if m not in METRICS:
            problems.append(f"evaluation.metrics: {m!r} is not one of {sorted(METRICS)}")
    for g in evaluation["gates"]:
        if g["metric"] not in METRICS:
            problems.append(f"evaluation.gates: {g['metric']!r} is not one of {sorted(METRICS)}")
        if "min" not in g and "max" not in g:
            problems.append(f"evaluation.gates: {g['metric']} needs min or max")
    # Every metric a gate or the champion comparison uses must be computed.
    needed = {evaluation["primary_metric"], *(g["metric"] for g in evaluation["gates"]),
              *(g["metric"] for g in evaluation["slice_gates"])}
    champion = evaluation["champion"]
    champion["metric"] = champion["metric"] or evaluation["primary_metric"]
    needed.add(champion["metric"])
    evaluation["metrics"] = sorted(set(evaluation["metrics"]) | (needed & METRICS))
    evaluation["lower_is_better"] = sorted(m for m in evaluation["metrics"] if m in LOWER_IS_BETTER)
    for col in evaluation["slices"]:
        known(col, "evaluation.slices")
    if evaluation["slice_gates"] and not evaluation["slices"]:
        problems.append("evaluation.slice_gates: set evaluation.slices first")

    # ---- inference, monitoring, retraining
    inference = ml["inference"]
    inference["decision_threshold"] = inference["decision_threshold"] or evaluation["decision_threshold"]
    drift = monitoring["drift"]
    if drift["features"] is None:
        drift["features"] = list(features["include"] or [])
    for col in drift["features"]:
        if features["include"] is not None and col not in features["include"]:
            problems.append(f"monitoring.drift.features: {col!r} is not a model feature")
    perf = monitoring["performance"]
    perf["metric"] = perf["metric"] or evaluation["primary_metric"]
    if perf["enabled"] and not perf.get("labels"):
        problems.append("monitoring.performance: enabled but labels (table, key, label) are not set")
    monitoring_on = drift["enabled"] or monitoring["prediction_drift"]["enabled"] or perf["enabled"]
    if (monitoring_on or inference["output_table"]) and not has_inference:
        problems.append("inference and monitoring need pipelines.inference")
    for key, where in ((inference["output_table"], "inference.output_table"),
                       (monitoring["output_table"], "monitoring.output_table")):
        if key and table_names and key not in table_names:
            problems.append(f"{where}: {key!r} is not in outputs.bigquery.tables")
    if monitoring_on and not monitoring["output_table"]:
        problems.append("monitoring: set monitoring.output_table so results are stored")
    # Mode: `enabled` is the older switch. enabled: false always wins (environment files switch retraining off).
    if retraining["mode"] is False:          # YAML 1.1 reads an unquoted `off` as false
        retraining["mode"] = "off"
    if retraining["enabled"] is False:
        retraining["mode"], retraining["mode_source"] = "off", "enabled"
    elif retraining["mode"] is None:
        retraining["mode"] = "propose" if retraining["enabled"] else "off"
        retraining["mode_source"] = "enabled" if retraining["enabled"] else "default"
    else:
        retraining["mode_source"] = "mode"
    retraining["enabled"] = retraining["mode"] != "off"     # components released before modes read this
    if retraining["enabled"]:
        if "drift" in retraining["when"] and not (drift["enabled"] or monitoring["prediction_drift"]["enabled"]):
            problems.append("retraining.when drift needs monitoring.drift or monitoring.prediction_drift enabled")
        if "performance" in retraining["when"] and not perf["enabled"]:
            problems.append("retraining.when performance needs monitoring.performance enabled")
        if {"drift", "performance"} & set(retraining["when"]) and not retraining["source_uri"]:
            problems.append("retraining: set retraining.source_uri, the training data a requested retrain uses")

    # ---- data protection
    erasure = protection.get("erasure")
    if erasure:
        known(erasure["key_column"], "data_protection.erasure.key_column")

    return copy.deepcopy(ml)


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
FEATURE_GROUP = re.compile(r"^[a-z_][a-z0-9_]{0,59}$")
FEATURE_TABLE = re.compile(r"^bq://(?:[a-z][a-z0-9-]{4,61}[a-z0-9]\.)?[A-Za-z_][A-Za-z0-9_]{0,1023}\.[A-Za-z0-9_$-]{1,1024}$")


def feature_columns(features_from: dict | None) -> dict[str, tuple[int, str]]:
    """The columns feature tables add to the run's data: output name -> (source index, column in that table)."""
    out: dict[str, tuple[int, str]] = {}
    for i, src in enumerate((features_from or {}).get("sources") or []):
        for col in src.get("features") or []:
            out.setdefault(src.get("prefix", "") + col, (i, col))
    return out


def _features_from(spec: dict | None, columns: dict, label: str | None, known, problems: list[str]) -> None:
    """data.features_from: the run's source is the entity table; each source adds the latest values at or before
    the entity row's cut-off time (see pipewright_runtime.read_source)."""
    if not spec:
        return
    where = "data.features_from"
    entity = spec["entity"]
    for col in [*entity["columns"], entity["time_column"]]:
        if not IDENTIFIER.match(col):
            problems.append(f"{where}.entity: {col!r} is not a column name")
    for col in entity["columns"]:
        known(col, f"{where}.entity.columns")
    tc = entity["time_column"]
    if known(tc, f"{where}.entity.time_column") and columns and columns[tc]["type"] not in TIME_TYPES:
        problems.append(f"{where}.entity.time_column {tc!r} must be a date or timestamp column")
    taken = set(entity["columns"]) | {tc}
    seen: dict[str, int] = {}
    for i, src in enumerate(spec["sources"]):
        at = f"{where}.sources[{i}]"
        if ("table" in src) == ("feature_group" in src):
            problems.append(f"{at}: name either a table or a feature_group")
            continue
        if "feature_group" in src:
            # The registry gives the table, entity and time columns at run time; `doctor` checks them now.
            for key in ("entity_columns", "time_column"):
                if key in src:
                    problems.append(f"{at}.{key}: is read from the feature group's registration; leave it out")
            if not FEATURE_GROUP.match(src["feature_group"]):
                problems.append(f"{at}.feature_group {src['feature_group']!r} is not a feature group ID")
            names = list(src["features"])
        else:
            if not FEATURE_TABLE.match(src["table"]):
                problems.append(f"{at}.table {src['table']!r} is not bq://project.dataset.table (or bq://dataset.table)")
            keys = src.get("entity_columns") or entity["columns"]
            if len(keys) != len(entity["columns"]):
                problems.append(f"{at}.entity_columns: give one column for each of data.features_from.entity.columns")
            names = [*keys, src.get("time_column", "feature_timestamp"), *src["features"]]
        for col in names:
            if not IDENTIFIER.match(col):
                problems.append(f"{at}: {col!r} is not a column name")
        prefix = src.get("prefix", "")
        if prefix and not IDENTIFIER.match(prefix + "x"):
            problems.append(f"{at}.prefix {prefix!r} cannot start a column name")
        for col in src["features"]:
            name = prefix + col
            if name in seen:
                problems.append(f"{at}: feature {name!r} also comes from sources[{seen[name]}]; set a prefix")
                continue
            seen[name] = i
            if name in taken:
                problems.append(f"{at}: feature {name!r} is an entity or time column of the entity table")
            if name == label:
                problems.append(f"{at}: feature {name!r} is the label; the label comes from the entity table")
            known(name, f"{at}.features")
