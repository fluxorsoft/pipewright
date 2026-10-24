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
    AI_FORECAST_MODELS,
    DEFAULT_MAX_BYTES_BILLED,
    FORECAST_ENGINES,
    MAX_HORIZON,
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
    "clustering": {"predicted_label": "STRING", "score": "FLOAT64"},  # the cluster; distance to its centroid
    "decomposition": {},                                             # keeps the principal_component_* it has
    "forecast": {"forecast_timestamp": "TIMESTAMP", "forecast_value": "FLOAT64"},  # and the series' ID columns
}
_FLOAT = {"FLOAT", "FLOAT64"}
_OPTION = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
# C-08, kept for 1.1: no generative AI in a use case's SQL. BigQuery's AI.* functions (AI.GENERATE, AI.FORECAST, ...)
# and ML.GENERATE_* call Google's generative and foundation models row by row: outputs that may change between calls,
# rows sent to a model, and a cost the byte limit does not cover. The one exception is AI.FORECAST through
# bqml.forecast.engine ai_forecast, where the platform allows it (policies.bqml.ai_forecast): Pipewright's own SQL
# calls it with the settings a person approved, never a team's SQL file.
_GENERATIVE = re.compile(r"\b(AI\s*\.\s*[A-Z_]+|ML\s*\.\s*GENERATE_[A-Z_]+)\s*\(", re.I)


def resolve(spec: dict, ml: dict, directory: Path, policy: dict, tables: dict, has_inference: bool,
            env_name: str, problems: list[str]) -> dict:
    """The resolved bqml section. Adds to `problems`; never raises."""
    where = "bqml"
    data = ml["data"]
    label = data.get("label") or {}
    task = label.get("task", "binary")
    imported = _imported(spec, task, problems)
    forecast = _forecast(spec, ml, task, policy, env_name, problems)
    model_type = "imported" if imported else str(spec.get("model_type") or "").lower()
    if forecast and forecast["engine"] == "ai_forecast":
        if model_type:
            problems.append(f"{where}.model_type: AI.FORECAST trains no model; remove model_type next to "
                            "bqml.forecast.engine ai_forecast")
        model_type = "ai_forecast"
    elif forecast and not model_type:
        model_type = "arima_plus"
    if imported or model_type == "ai_forecast":
        pass
    elif not model_type:
        problems.append(f"{where}: give model_type (BigQuery ML trains) or import (a model trained elsewhere)")
    elif model_type not in MODEL_TYPES.get(task, ()):
        fits = ", ".join(MODEL_TYPES.get(task, ())) or "none"
        problems.append(f"{where}.model_type {model_type!r} does not fit data.label.task {task} (use: {fits})")
    allowed = policy.get("allowed_model_types")
    if allowed and model_type not in allowed and model_type != "ai_forecast":    # it has its own switch
        problems.append(f"{where}.model_type {model_type} is not allowed in {env_name} (platform policy): "
                        f"{', '.join(allowed)}")
    if not label.get("column") and task not in ("clustering", "decomposition"):
        problems.append(f"{where}: needs data.label.column (the label CREATE MODEL learns)")
    if not data.get("contract"):
        problems.append(f"{where}: needs data.contract (the columns checked before training)")

    options = {}
    for key, value in (spec.get("options") or {}).items():
        k = str(key).lower()
        if k in RESERVED_OPTIONS:
            problems.append(f"{where}.options.{key}: Pipewright sets it"
                            + (" (use bqml.explain)" if k == "enable_global_explain" else ""))
        elif model_type == "ai_forecast":
            problems.append(f"{where}.options.{key}: AI.FORECAST takes its settings from bqml.forecast")
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

    if task in ("clustering", "decomposition", "forecast") and spec.get("explain"):
        problems.append(f"{where}.explain: global explanations are for supervised models, not {task}")
    if task == "forecast" and spec.get("transform"):
        problems.append(f"{where}.transform: a forecast has no features to transform; shape the series in "
                        "bqml.prepare")
    resolved = {"model_type": model_type, "options": options, **({"import": imported} if imported else {}),
                **({"forecast": forecast} if forecast else {}),
                "explain": False if imported or task in ("clustering", "decomposition", "forecast")
                else bool(spec.get("explain", ml["explainability"]["enabled"])),
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


def _forecast(spec: dict, ml: dict, task: str, policy: dict, env_name: str, problems: list[str]) -> dict | None:
    """bqml.forecast (Pipewright 1.1): the series (data.label.column over data.split.time_column, one per
    id_columns value), the horizon scoring forecasts, and the engine: ARIMA_PLUS, or AI.FORECAST where allowed."""
    given = spec.get("forecast")
    if task != "forecast":
        if given:
            problems.append(f"bqml.forecast: for data.label.task forecast; this use case is {task}")
        return None
    given = dict(given or {})
    if not given.get("horizon"):
        problems.append("bqml.forecast.horizon: how many time points scoring forecasts")
    engine = given.get("engine", "arima_plus")
    if engine not in FORECAST_ENGINES:
        problems.append(f"bqml.forecast.engine {engine!r} is not one of {', '.join(FORECAST_ENGINES)}")
    out = {"engine": engine, "horizon": min(int(given.get("horizon") or 1), MAX_HORIZON),
           "id_columns": list(given.get("id_columns") or []),
           "confidence_level": float(given.get("confidence_level", 0.95))}
    columns = {c["name"] for c in (ml["data"].get("contract") or {}).get("columns", [])}
    for col in out["id_columns"]:
        if columns and col not in columns:
            problems.append(f"bqml.forecast.id_columns: column {col!r} is not in data.contract.columns")
    if engine == "ai_forecast":
        if policy.get("ai_forecast") is not True:
            problems.append(f"bqml.forecast.engine ai_forecast: AI.FORECAST sends the series to Google's TimesFM "
                            f"model, an exception to C-08 that {env_name} has not switched on (platform policy "
                            "policies.bqml.ai_forecast)")
        out["model"] = given.get("model", AI_FORECAST_MODELS[0])
        if out["model"] not in AI_FORECAST_MODELS:
            problems.append(f"bqml.forecast.model {out['model']!r}: use {', '.join(AI_FORECAST_MODELS)}")
        if given.get("context_window"):
            out["context_window"] = int(given["context_window"])
    else:
        for key in ("model", "context_window"):
            if key in given:
                problems.append(f"bqml.forecast.{key}: for engine ai_forecast; ARIMA_PLUS takes bqml.options")
        if ml["evaluation"]["champion"]["enabled"]:
            problems.append("evaluation.champion: ARIMA_PLUS forecasts from the end of its own training data, so the "
                            "champion cannot be measured on this run's held-out period; gate with "
                            "evaluation.gates")
    return out


IMPORT_OUTPUT = {"binary": "score", "multiclass": "label", "regression": "prediction"}


def _imported(spec: dict, task: str, problems: list[str]) -> dict | None:
    """bqml.import (phase 2): a TensorFlow or ONNX model trained elsewhere, imported, then evaluated and gated."""
    imp = spec.get("import")
    if not imp:
        return None
    for key in ("model_type", "options", "transform", "explain"):
        if spec.get(key):
            problems.append(f"bqml.{key}: an imported model is not trained in BigQuery ML; remove it next to "
                            "bqml.import")
    need = IMPORT_OUTPUT.get(task)
    outputs = dict(imp.get("outputs") or {})
    if need and not outputs.get(need):
        problems.append(f"bqml.import.outputs.{need}: name the model's output column that holds it ({task})")
    return {"format": str(imp.get("format", "")).lower(), "path": imp.get("path"), "outputs": outputs}


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
