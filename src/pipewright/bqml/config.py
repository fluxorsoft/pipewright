"""The `bqml` section: checked against the ML sections and the platform's limits, then carried in ml_config["bqml"].

The team's SQL files (bqml.transform, bqml.prepare) are read here and carried with their SHA-256: ml_config is a
static parameter of the release contract, so the SQL a model trained with is pinned by the release, and the model
records the digest (decision 6, Enterprise docs/BQML.md).
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from pipewright.bqml import (
    DEFAULT_MAX_BYTES_BILLED,
    MIN_BYTES_BILLED,
    MODEL_TYPES,
    RESERVED_OPTIONS,
    ROWS_PLACEHOLDER,
)

# The scoring output table: these columns, and any of the scored rows' columns the table also has.
OUTPUT_COLUMNS = {"run_id": "STRING", "model_version_id": "STRING", "scored_at": "TIMESTAMP"}
TASK_OUTPUT_COLUMNS = {
    "binary": {"predicted_label": "STRING", "score": "FLOAT64"},      # score: probability of the positive class
    "multiclass": {"predicted_label": "STRING", "score": "FLOAT64"},  # score: probability of predicted_label
    "regression": {"prediction": "FLOAT64"},
}
_FLOAT = {"FLOAT", "FLOAT64"}
_OPTION = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
# C-08, kept for 1.1: no generative AI in a use case's SQL. BigQuery's AI.* functions (AI.GENERATE, AI.FORECAST, ...)
# and ML.GENERATE_* call Google's generative and foundation models row by row: outputs that may change between calls,
# rows sent to a model, and a cost the byte limit does not cover.
_GENERATIVE = re.compile(r"\b(AI\s*\.\s*[A-Z_]+|ML\s*\.\s*GENERATE_[A-Z_]+)\s*\(", re.I)


def resolve(spec: dict, ml: dict, directory: Path, policy: dict, tables: dict, has_inference: bool,
            env_name: str, problems: list[str]) -> dict:
    """The resolved bqml section. Adds to `problems`; never raises."""
    where = "bqml"
    data = ml["data"]
    label = data.get("label") or {}
    task = label.get("task", "binary")
    model_type = str(spec["model_type"]).lower()
    if model_type not in MODEL_TYPES.get(task, ()):
        fits = ", ".join(MODEL_TYPES.get(task, ())) or "none"
        problems.append(f"{where}.model_type {model_type!r} does not fit data.label.task {task} (use: {fits})")
    allowed = policy.get("allowed_model_types")
    if allowed and model_type not in allowed:
        problems.append(f"{where}.model_type {model_type} is not allowed in {env_name} (platform policy): "
                        f"{', '.join(allowed)}")
    if not label.get("column"):
        problems.append(f"{where}: needs data.label.column (the label CREATE MODEL learns)")
    if not data.get("contract"):
        problems.append(f"{where}: needs data.contract (the columns checked before training)")

    options = {}
    for key, value in (spec.get("options") or {}).items():
        k = str(key).lower()
        if k in RESERVED_OPTIONS:
            problems.append(f"{where}.options.{key}: Pipewright sets it"
                            + (" (use bqml.explain)" if k == "enable_global_explain" else ""))
        elif not _OPTION.match(k):
            problems.append(f"{where}.options.{key}: not a CREATE MODEL option name")
        elif isinstance(value, (dict, list)) and not (isinstance(value, list) and all(
                isinstance(v, (str, int, float, bool)) for v in value)):
            problems.append(f"{where}.options.{key}: a value or a list of values")
        else:
            options[k] = value

    # Pipewright's own steps: no tuning trials, no validation split (BigQuery ML has its own early stopping).
    if ml["training"]["tuning"]["enabled"]:
        problems.append("training.tuning: not available with bqml yet (BigQuery ML hyperparameter tuning comes "
                        "later); set the values in bqml.options")
    if data["split"].get("validation_fraction"):
        problems.append("data.split.validation_fraction: bqml trains on the training rows only; remove it")

    ceiling = policy.get("max_bytes_billed")
    limit = spec.get("max_bytes_billed") or ceiling or DEFAULT_MAX_BYTES_BILLED
    if limit < MIN_BYTES_BILLED:
        problems.append(f"{where}.max_bytes_billed: at least {MIN_BYTES_BILLED} (BigQuery bills 10 MB a job)")
    if ceiling and limit > ceiling:
        problems.append(f"{where}.max_bytes_billed {limit} is above {env_name}'s limit {ceiling} (platform "
                        "policy policies.bqml.max_bytes_billed)")

    resolved = {"model_type": model_type, "options": options,
                "explain": bool(spec.get("explain", ml["explainability"]["enabled"])),
                "max_bytes_billed": int(limit),
                "transform": _sql_file(spec.get("transform"), directory, f"{where}.transform", problems),
                "prepare": _sql_file(spec.get("prepare"), directory, f"{where}.prepare", problems)}
    if resolved["prepare"] and ROWS_PLACEHOLDER not in resolved["prepare"]["sql"]:
        problems.append(f"{where}.prepare: {spec['prepare']} must read the run's rows FROM {ROWS_PLACEHOLDER}")
    if resolved["transform"] and re.search(r"\bTRANSFORM\s*\(", resolved["transform"]["sql"], re.I):
        problems.append(f"{where}.transform: {spec['transform']} holds the TRANSFORM clause's expressions only, "
                        "without TRANSFORM ( ... )")

    output = ml["inference"].get("output_table")
    if has_inference and not output:
        problems.append("inference.output_table: bqml scoring writes its predictions there")
    if output and output in tables:
        _output_problems(tables[output], task, problems)
        # Performance joins the latest score of each key with its observed outcome.
        perf = ml["monitoring"]["performance"]
        key = (perf.get("labels") or {}).get("key")
        if perf["enabled"] and key and key not in {f["name"] for f in tables[output].fields}:
            problems.append(f"monitoring.performance.labels.key {key!r} is not a column of inference.output_table "
                            f"{output}: bqml scoring keeps the input columns the table has")
    return resolved


def _sql_file(name: str | None, directory: Path, where: str, problems: list[str]) -> dict | None:
    if not name:
        return None
    path = (directory / name).resolve()
    if not path.is_file():
        problems.append(f"{where} not found: {name}")
        return None
    sql = path.read_text(encoding="utf-8").strip().rstrip(";").strip()
    if not sql:
        problems.append(f"{where}: {name} is empty")
    elif ";" in _strip_comments(sql):
        problems.append(f"{where}: {name} holds one statement; remove the semicolons")
    called = sorted({re.sub(r"\s+", "", m.group(1)).upper()
                     for m in _GENERATIVE.finditer(_strip_comments(sql, identifiers=True))})
    if called:
        problems.append(f"{where}: {name} calls {', '.join(called)}: generative AI functions are out of scope "
                        "for Pipewright (C-08); compute such values outside the use case, or remove the call")
    return {"file": name, "sql": sql, "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest()}


def _strip_comments(sql: str, identifiers: bool = False) -> str:
    """The SQL without comments and string literals (with identifiers, also without quoted names)."""
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
    if identifiers:
        sql = re.sub(r'"(?:[^"\\]|\\.)*"', '""', sql)
        sql = re.sub(r"`[^`]*`", "``", sql)
    return sql


def _output_problems(table, task: str, problems: list[str]) -> None:
    have = {f["name"]: f["type"].upper() for f in table.fields}
    for name, kind in {**OUTPUT_COLUMNS, **TASK_OUTPUT_COLUMNS.get(task, {})}.items():
        got = have.get(name)
        if got is None:
            problems.append(f"inference.output_table {table.name}: add a {name} column ({kind}) for bqml scoring")
        elif got != kind and not (kind in _FLOAT and got in _FLOAT):
            problems.append(f"inference.output_table {table.name}: {name} is {got}, bqml scoring writes {kind}")
