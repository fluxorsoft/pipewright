"""Helpers of the BigQuery ML steps: the SQL templates (sql/), BigQuery jobs, the data contract, metrics, PSI and
Model Registry. No KFP here: unit-tested locally (tests/test_bqml_runtime.py) and copied into the steps image.

Jobs: a plain query (SELECT) names the environment's key for its temporary results (the CMEK organization policy);
DDL, CREATE MODEL and ALTER MODEL refuse a job key and take the dataset's default key. Every job has a byte limit
(bqml.max_bytes_billed) and the run's labels.
"""

from __future__ import annotations

import json
import math
import re
import time
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent / "sql"
_PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
EPS = 1e-4                                   # as the container path's PSI (mlkit)
KEEP_DAYS = 30                               # test predictions kept for a look; the run's rows are kept
# As config.ml.LOWER_IS_BETTER (a test keeps them in step).
LOWER_IS_BETTER = {"log_loss", "brier", "calibration_error", "rmse", "mae", "mape", "median_absolute_error",
                   "davies_bouldin_index", "mean_squared_distance"}
UNSUPERVISED = ("clustering", "decomposition")
MIN_LABELLED = 50                            # performance needs this many labelled predictions, as the container path


# ------------------------------------------------------------------- SQL text --

def render(name: str, **values) -> str:
    """A template from sql/ with every {placeholder} filled in one pass: values (a team's SQL among them) are never
    read as placeholders themselves. A placeholder without a value is an error, not SQL."""
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    missing = sorted(set(_PLACEHOLDER.findall(text)) - set(values))
    if missing:
        raise ValueError(f"{name}: no value for {', '.join(missing)}")
    return _PLACEHOLDER.sub(lambda m: str(values[m.group(1)]), text).strip()


def ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"{name!r} is not a BigQuery column name")
    return name


def q(name: str) -> str:
    return f"`{ident(name)}`"


def literal(value) -> str:
    """A CREATE MODEL option value as SQL."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(literal(v) for v in value) + "]"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def string(value) -> str:
    return literal(str(value))


def label_pairs(labels: dict) -> str:
    """BigQuery OPTIONS (labels = [...]) from the run's labels (keys and values cleaned as labels need)."""
    def clean(v: str) -> str:
        return re.sub(r"[^a-z0-9_-]", "-", str(v).lower())[:63]

    return ", ".join(f"({string(clean(k))}, {string(clean(v))})" for k, v in labels.items() if v not in (None, ""))


def is_query(sql: str) -> bool:
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--")).lstrip().upper()
    return body.startswith(("SELECT", "WITH"))


# ----------------------------------------------------------------------- names --

def base_name(usecase: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", usecase.lower())


def run_part(run_id: str) -> str:
    part = re.sub(r"[^a-z0-9_]", "_", run_id.lower())
    if not part:
        raise ValueError(f"{run_id!r} is not a run ID")
    return part


def table_ref(project: str, ml_config: dict, run_id: str, kind: str) -> str:
    """project.dataset.<usecase>_bqml_<kind>_<run>: a run's tables in the tenant's dataset."""
    dataset = (ml_config.get("platform") or {}).get("bigquery_dataset")
    return f"{project}.{dataset}.{base_name(ml_config['usecase'])}_bqml_{kind}_{run_part(run_id)}"


def model_ref(project: str, ml_config: dict, run_id: str) -> str:
    """Every run trains a new model: replacing a model with the same name would replace its registry version."""
    dataset = (ml_config.get("platform") or {}).get("bigquery_dataset")
    return f"{project}.{dataset}.{base_name(ml_config['usecase'])}_bqml_{run_part(run_id)}"


def record_uri(ml_config: dict, run_id: str) -> str:
    """Where a run's evaluation record is: `model approve`, `deploy-model` and evidence read it from there."""
    return f"{ml_config['bqml']['records']}{run_part(run_id)}/"


# ------------------------------------------------------------------------ jobs --

class Jobs:
    """BigQuery jobs of one step, with the byte limit, the labels and the key handling."""

    def __init__(self, project: str, ml_config: dict, run_id: str, labels: dict, client=None):
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(project=project)
        self.client, self.project, self.ml_config, self.run_id = client, project, ml_config, run_id
        self.labels = {k: re.sub(r"[^a-z0-9_-]", "-", str(v).lower())[:63] for k, v in labels.items() if v}
        self.key = (ml_config.get("platform") or {}).get("bigquery_kms_key")
        self.max_bytes = int(ml_config["bqml"]["max_bytes_billed"])
        self.billed = 0

    def config(self, sql: str):
        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes, labels=self.labels)
        if self.key and is_query(sql):
            config.destination_encryption_configuration = bigquery.EncryptionConfiguration(kms_key_name=self.key)
        return config

    def run(self, sql: str, what: str) -> list[dict]:
        """Runs one statement; returns its rows (a query's) as dicts. Raises with BigQuery's message."""
        started = time.time()
        job = self.client.query(sql, job_config=self.config(sql))
        try:
            rows = [dict(r.items()) for r in job.result()]
        except Exception as e:  # noqa: BLE001 - BigQuery's message, with its data values taken out (NFR-18)
            from pipewright_runtime import value_free

            raise RuntimeError(f"{what} failed (job {job.job_id}): {value_free(getattr(e, 'message', e))}") from None
        self.billed += int(job.total_bytes_billed or 0)
        print(json.dumps({"severity": "INFO", "message": f"{what}: job {job.job_id}, "
                          f"{int(job.total_bytes_billed or 0)} bytes billed, {time.time() - started:.1f} s"}),
              flush=True)
        return rows


# ------------------------------------------------------------------- the rows --

def split_flag(ml_config: dict, keys: list[str]) -> str:
    """The held-out test flag: a hash of the key (random and stratified), or the latest rows (time). With time, no
    time value is split across the sets, as in the container path."""
    split = ml_config["data"]["split"]
    if split["strategy"] == "time":
        quantile = max(0, min(999, int(round((1 - split["test_fraction"]) * 1000))))
        return render("split_time.sql", time_column=ident(split["time_column"]), quantile=quantile)
    key = ", ".join(q(k) for k in keys) if keys else "prepared"
    return render("split_random.sql", seed=int(split.get("seed", 42)), keys=key,
                  test_millionths=int(round(split["test_fraction"] * 1_000_000)))


def split_keys(ml_config: dict) -> list[str]:
    data = ml_config["data"]
    unique = list((data.get("contract") or {}).get("unique") or [])
    entity = list(((data.get("features_from") or {}).get("entity") or {}).get("columns") or [])
    return unique or entity


def prepared_sql(ml_config: dict, source_sql: str) -> str:
    prepare = ml_config["bqml"].get("prepare")
    if not prepare:
        return f"SELECT * FROM ({source_sql})"
    return prepare["sql"].replace("{rows}", f"({source_sql})")


def contract_checks(ml_config: dict, columns: set[str], mode: str) -> tuple[str, list[tuple[str, str, object]]]:
    """The contract's checks as COUNTIF expressions over the run's rows, and (alias, rule, limit) for each, as the
    container path's validate step checks them (mlkit.validate_frame)."""
    data = ml_config["data"]
    contract = data.get("contract") or {}
    label = (data.get("label") or {}).get("column")
    exprs, rules = [], []

    def add(alias: str, expr: str, rule: str, limit=0):
        exprs.append(f",\n  {expr} AS {alias}")
        rules.append((alias, rule, limit))

    for i, col in enumerate(contract.get("columns") or []):
        name = col["name"]
        if name == label and mode == "inference":
            continue
        if name not in columns:
            if col.get("required", True):
                rules.append((f"_missing_{i}", f"missing column {name}", None))
            continue
        c = q(name)
        limit = col.get("max_null_fraction", contract.get("max_null_fraction", 1.0))
        add(f"_nulls_{i}", f"COUNTIF({c} IS NULL)", f"{name}: nulls above {limit:.1%}", ("fraction", limit))
        if "min" in col:
            add(f"_min_{i}", f"COUNTIF({c} < {literal(col['min'])})", f"{name}: values below {col['min']}")
        if "max" in col:
            add(f"_max_{i}", f"COUNTIF({c} > {literal(col['max'])})", f"{name}: values above {col['max']}")
        if "allowed" in col:
            allowed = ", ".join(string(v) for v in col["allowed"])
            add(f"_allowed_{i}", f"COUNTIF({c} IS NOT NULL AND CAST({c} AS STRING) NOT IN ({allowed}))",
                f"{name}: values outside {col['allowed']}")
    unique = [c for c in contract.get("unique") or [] if c in columns]
    if unique:
        key = ", ".join(q(c) for c in unique)
        add("_duplicates", f"COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(STRUCT({key})))",
            f"duplicate rows for key {unique}")
    values = (data.get("label") or {}).get("values")
    if mode == "training" and label in columns and values:
        allowed = ", ".join(string(v) for v in values)
        add("_label_values", f"COUNTIF({q(label)} IS NOT NULL AND CAST({q(label)} AS STRING) NOT IN ({allowed}))",
            f"label {label} has values outside {values}")
    return "".join(exprs), rules


def contract_errors(row: dict, rules: list, contract: dict, mode: str) -> list[str]:
    errors = []
    n = int(row.get("_rows") or 0)
    if mode == "training" and n < contract.get("min_rows", 0):
        errors.append(f"only {n} rows, need at least {contract['min_rows']}")
    for alias, rule, limit in rules:
        if limit is None:
            errors.append(rule)
            continue
        count = int(row.get(alias) or 0)
        if isinstance(limit, tuple):                       # a fraction of the rows
            if n and count / n > limit[1]:
                errors.append(f"{rule} ({count / n:.1%})")
        elif count > limit:
            errors.append(f"{rule} ({count} rows)")
    return errors


# ------------------------------------------------------------------ the model --

def create_model_sql(ml_config: dict, model: str, rows_table: str, columns: list[str]) -> str:
    """CREATE MODEL on the training rows: the features and the label, or every column with a TRANSFORM clause
    (BigQuery ML takes what the clause names)."""
    section, data = ml_config["bqml"], ml_config["data"]
    label = (data.get("label") or {}).get("column")
    if data["label"].get("task") in UNSUPERVISED:                     # k-means, PCA: no label
        options = {"model_type": section["model_type"], **section["options"]}
        selected = "* EXCEPT (_pw_test)" if section.get("transform") else ", ".join(
            q(c) for c in feature_columns(ml_config, columns))
        return render("create_model.sql", model=model, rows_table=rows_table, columns=selected,
                      transform=f"TRANSFORM (\n{section['transform']['sql']}\n)" if section.get("transform") else "",
                      options=", ".join(f"{k} = {literal(v)}" for k, v in options.items()))
    options = {"model_type": section["model_type"], "input_label_cols": [label], "data_split_method": "NO_SPLIT",
               **({"enable_global_explain": True} if section.get("explain") else {}), **section["options"]}
    transform = section.get("transform")
    if transform:
        selected = "* EXCEPT (_pw_test)"
    else:
        selected = ", ".join(q(c) for c in [*feature_columns(ml_config, columns), label])
    return render("create_model.sql", model=model, rows_table=rows_table, columns=selected,
                  transform=f"TRANSFORM (\n{transform['sql']}\n)" if transform else "",
                  options=", ".join(f"{k} = {literal(v)}" for k, v in options.items()))


def feature_columns(ml_config: dict, columns: list[str]) -> list[str]:
    """The model's features: features.include without prepare.sql; with it, the prepared columns other than the
    label, the keys, the cut-off and split columns, personal data and features.exclude."""
    data, features = ml_config["data"], ml_config["features"]
    if not ml_config["bqml"].get("prepare"):
        return [c for c in features.get("include") or [] if c in columns]
    label = (data.get("label") or {}).get("column")
    skip = {label, "_pw_test", data["split"].get("time_column"), *split_keys(ml_config),
            *(ml_config.get("data_protection") or {}).get("personal_data_columns", []), *features.get("exclude", []),
            ((data.get("features_from") or {}).get("entity") or {}).get("time_column")}
    return [c for c in columns if c not in skip]


def model_parts(jobs, model: str) -> dict | None:
    """An imported model (promotion by files): {"transform": its transform model or None}; None for a model
    BigQuery ML trained here. The transform model, when TRANSFORM was used, is <model>_transform."""
    if str(jobs.client.get_model(model).model_type or "").upper() not in ("TENSORFLOW", "ONNX"):
        return None
    try:
        jobs.client.get_model(f"{model}_transform")
        return {"transform": f"{model}_transform"}
    except Exception:  # noqa: BLE001 - no TRANSFORM in the source model
        return {"transform": None}


def imported_outputs(ml_config: dict, evaluation: bool = False) -> str:
    """The _pw_* outputs from an imported model's columns ({label}_probs as numbers, {label}_values as classes)."""
    data = ml_config["data"]
    label, task = ident(data["label"]["column"]), data["label"].get("task", "binary")
    positive = str(data["label"].get("positive", 1))
    threshold = float(ml_config["inference"].get("decision_threshold") or 0.5)
    mapped = ((ml_config.get("bqml") or {}).get("import") or {}).get("outputs")
    if mapped:                              # bqml.import: the team names its model's output columns
        values = [str(v) for v in data["label"].get("values") or []]
        negative = next((v for v in values if v != positive), "0")
        names = {k: ident(v) for k, v in mapped.items() if k in ("score", "label", "prediction")}
        if "score" in names:                # a scalar column, or one element of an array (score_index)
            index = mapped.get("score_index")
            names["score"] = (f"CAST(p.`{names['score']}`" + (f"[OFFSET({int(index)})]" if index is not None else "")
                              + " AS FLOAT64)")
        return render(f"outputs_mapped_{task}.sql", **{**{k: "_" for k in ("score", "label", "prediction")}, **names},
                      threshold=threshold, positive=positive, negative=negative)
    outputs = render(f"outputs_imported_{task}.sql", label=label, positive=positive, threshold=threshold)
    if task == "multiclass" and evaluation:
        outputs += ",\n" + render("outputs_imported_multiclass_true.sql", label=label)
    return outputs


def imported_inner(rows_sql: str, transform: str | None) -> str:
    """The rows an imported model scores: each row also as one STRUCT (_pw_raw), through the transform model first."""
    rows = f"SELECT r AS _pw_raw, r.* FROM ({rows_sql}) AS r"
    return f"SELECT * FROM ML.PREDICT(MODEL `{transform}`, ({rows}))" if transform else rows


def predict_sql(ml_config: dict, model: str, rows_table: str, out_table: str, where: str, labels: dict,
                evaluation: bool, parts: dict | None = None) -> str:
    """parts: model_parts() of an imported model (promotion by files); None for a model trained here."""
    if parts is not None:
        rows = f"SELECT * EXCEPT (_pw_test) FROM `{rows_table}` WHERE {where}"
        return render("predict_imported.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                      outputs=imported_outputs(ml_config, evaluation), model=model,
                      inner=imported_inner(rows, parts.get("transform")))
    data = ml_config["data"]
    task = data["label"].get("task", "binary")
    if task == "decomposition":
        return render("predict_plain.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                      model=model, rows_table=rows_table, where=where)
    if task == "clustering":
        return render("predict.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                      predicted_columns="CENTROID_ID, NEAREST_CENTROIDS_DISTANCE",
                      outputs=render("outputs_clustering.sql"), model=model, rows_table=rows_table, where=where,
                      threshold="")
    label = data["label"]["column"]
    positive = str(data["label"].get("positive", 1))
    outputs = render(f"outputs_{task}.sql", label=ident(label), positive=positive)
    if task == "multiclass" and evaluation:
        outputs += ",\n" + render("outputs_multiclass_true.sql", label=ident(label))
    predicted = [q(f"predicted_{label}")] + ([] if task == "regression" else [q(f"predicted_{label}_probs")])
    threshold = (f", STRUCT({float(ml_config['inference'].get('decision_threshold') or 0.5)} AS threshold)"
                 if task == "binary" else "")
    return render("predict.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                  predicted_columns=", ".join(predicted), outputs=outputs, model=model, rows_table=rows_table,
                  where=where, threshold=threshold)


# ------------------------------------------------------------------ forecasts --

def forecast_ids(ml_config: dict) -> list[str]:
    return [ident(c) for c in ml_config["bqml"]["forecast"]["id_columns"]]


def forecast_steps(jobs: Jobs, ml_config: dict, rows_table: str) -> int:
    """The held-out period's length: the most time points one series has in the test rows."""
    ids = forecast_ids(ml_config)
    row = jobs.run(render("forecast_steps.sql", time_column=ident(ml_config["data"]["split"]["time_column"]),
                          rows_table=rows_table,
                          group_by=f" GROUP BY {', '.join(q(c) for c in ids)}" if ids else ""), "held-out period")
    steps = int(row[0]["steps"]) if row else 0
    if not steps:
        raise ValueError("a forecast needs held-out rows: the test rows (the latest period) are empty")
    return steps


def forecast_create_sql(ml_config: dict, model: str, rows_table: str, steps: int) -> str:
    """CREATE MODEL (ARIMA_PLUS) on the series up to the held-out period. Its horizon covers both that period
    (evaluation) and bqml.forecast.horizon (scoring): ML.FORECAST forecasts no further than it."""
    section, data = ml_config["bqml"], ml_config["data"]
    settings, ids = section["forecast"], forecast_ids(ml_config)
    time_column, label = ident(data["split"]["time_column"]), ident(data["label"]["column"])
    options = {"model_type": section["model_type"], "time_series_timestamp_col": time_column,
               "time_series_data_col": label,
               **({"time_series_id_col": ids if len(ids) > 1 else ids[0]} if ids else {}),
               "horizon": max(int(settings["horizon"]), steps), **section["options"]}
    return render("create_model.sql", model=model, rows_table=rows_table, transform="",
                  columns=", ".join(q(c) for c in [*ids, time_column, label]),
                  options=", ".join(f"{k} = {literal(v)}" for k, v in options.items()))


def forecast_sql(ml_config: dict, settings: dict, model: str, rows_table: str, where: str, horizon: int) -> str:
    """The forecast: ML.FORECAST of an ARIMA_PLUS model, or AI.FORECAST over the rows (where) with the settings."""
    ids = forecast_ids(ml_config)
    selected = "".join(f"{q(c)}, " for c in ids)
    confidence = float(settings.get("confidence_level", 0.95))
    if settings["engine"] != "ai_forecast":
        return render("forecast_arima.sql", ids=selected, model=model, horizon=int(horizon),
                      confidence_level=confidence)
    data = ml_config["data"]
    time_column, label = ident(data["split"]["time_column"]), ident(data["label"]["column"])
    history = (f"SELECT {selected}{q(time_column)}, {q(label)} FROM `{rows_table}` WHERE {where}")
    window = settings.get("context_window")
    return render("forecast_ai.sql", ids=selected, history=history, label=label, time_column=time_column,
                  model=settings["model"].replace("'", ""), horizon=int(horizon), confidence_level=confidence,
                  id_cols=f" id_cols => [{', '.join(string(c) for c in ids)}]," if ids else "",
                  context_window=f", context_window => {int(window)}" if window else "")


def forecast_table_sql(ml_config: dict, forecast: str, out_table: str, labels: dict,
                       rows_table: str | None = None) -> str:
    """The forecast into a table; with rows_table (evaluation), joined to the held-out actual values."""
    if not rows_table:
        return render("forecast_table.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                      forecast=forecast, actual="", join="")
    data, ids = ml_config["data"], forecast_ids(ml_config)
    label = ident(data["label"]["column"])
    join = render("forecast_actuals.sql", ids="".join(f"{q(c)}, " for c in ids),
                  time_column=ident(data["split"]["time_column"]), label=label, rows_table=rows_table,
                  on="".join(f" AND a.{q(c)} = f.{q(c)}" for c in ids))
    return render("forecast_table.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                  forecast=forecast, actual=f", a.{q(label)}", join="\n" + join)


def forecast_failures(jobs: Jobs, table: str) -> None:
    """AI.FORECAST reports a series it could not forecast in ai_forecast_status: fail rather than score part."""
    row = jobs.run(f"SELECT COUNTIF(_pw_status != '') AS failed, ANY_VALUE(NULLIF(_pw_status, '')) AS status "
                   f"FROM `{table}`", "forecast status")[0]
    if int(row["failed"] or 0):
        raise RuntimeError(f"the forecast failed for {row['failed']} rows: {row['status']}")


def evaluate_forecast(jobs: Jobs, ml_config: dict, settings: dict, model: str, rows_table: str,
                      out_table: str) -> dict:
    """{metrics, slices, rows, horizon}: the forecast over the held-out latest period against the actual values,
    measured as regression is (the same definitions as the container path)."""
    steps = forecast_steps(jobs, ml_config, rows_table)
    jobs.run(forecast_table_sql(ml_config, forecast_sql(ml_config, settings, model, rows_table, "NOT _pw_test",
                                                        steps), out_table, jobs.labels, rows_table),
             "forecast of the held-out period")
    forecast_failures(jobs, out_table)
    result = evaluate_rows(jobs, ml_config, out_table)
    if not result["rows"]:
        raise ValueError("the forecast matched none of the held-out rows: is data.split.time_column a regular series "
                         "(one row per series and time point)?")
    return {**result, "horizon": steps}


def version_record(project: str, ml_config: dict, version: str, session=None) -> dict | None:
    """A registry version's evaluation record, from its run label."""
    platform = ml_config["platform"]
    found = default_version(project, platform["region"], ml_config["bqml"]["registry_id"], session=session,
                            version=version)
    run = ((found or {}).get("labels") or {}).get("pipewright-run")
    return read_record(project, f"{ml_config['bqml']['records']}{run}/") if run else None


# --------------------------------------------------------------------- metrics --

def metrics_sql(ml_config: dict, predictions: str, group: str | None = None) -> str:
    data = ml_config["data"]
    task = data["label"].get("task", "binary")
    task = "regression" if task == "forecast" else task       # a forecast is measured as regression is
    threshold = float(ml_config["evaluation"].get("decision_threshold") or 0.5)
    return render(f"metrics_{task}.sql", predictions=predictions, label=ident(data["label"]["column"]),
                  group=f"CAST({q(group)} AS STRING)" if group else "'overall'",
                  positive=str(data["label"].get("positive", 1)), threshold=threshold)


def shape_metrics(row: dict, names: list[str]) -> dict:
    """One metrics row as the record has it: the configured metrics, rounded as the container path rounds."""
    out = {}
    for n in names:
        v = row.get(n)
        out[n] = None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 6)
    return out


def evaluate_unsupervised(jobs: Jobs, ml_config: dict, model: str, rows_table: str) -> dict:
    """{metrics, rows, slices}: ML.EVALUATE of a k-means model on the test rows, or of a PCA model."""
    names = ml_config["evaluation"]["metrics"]
    task = ml_config["data"]["label"].get("task")
    rows = (f", (SELECT * EXCEPT (_pw_test) FROM `{rows_table}` WHERE _pw_test)" if task == "clustering" else "")
    found = (jobs.run(render("evaluate_unsupervised.sql", model=model, rows=rows), "ML.EVALUATE") or [{}])[0]
    n = int(jobs.run(f"SELECT COUNTIF(_pw_test) AS n FROM `{rows_table}`", "test rows")[0]["n"])
    return {"metrics": shape_metrics(found, names), "rows": n, "slices": {}}


def evaluate_rows(jobs: Jobs, ml_config: dict, predictions: str) -> dict:
    """{metrics, slices, rows, [per_class], [_confusion]} for one model's predictions on the test rows."""
    names = ml_config["evaluation"]["metrics"]
    overall = jobs.run(metrics_sql(ml_config, predictions), "metrics")
    if not overall:
        return {"metrics": {n: None for n in names}, "rows": 0, "slices": {}}
    row = overall[0]
    out = {"metrics": shape_metrics(row, names), "rows": int(row["row_count"])}
    if "per_class" in row:
        out["per_class"] = {c["class"]: {k: (round(float(c[k]), 6) if k != "support" else int(c[k]))
                                         for k in ("precision", "recall", "f1", "support")}
                            for c in row["per_class"]}
    if "tp" in row:
        out["_confusion"] = {k: int(row[k]) for k in ("tp", "fp", "fn", "tn")}
    slices = {}
    for col in ml_config["evaluation"].get("slices") or []:
        slices[col] = {}
        for r in jobs.run(metrics_sql(ml_config, predictions, col), f"metrics by {col}"):
            slices[col][str(r["_pw_group"])] = {"rows": int(r["row_count"]), **shape_metrics(r, names)}
    out["slices"] = slices
    return out


def fairness(jobs: Jobs, ml_config: dict, predictions: str) -> dict:
    """evaluation.fairness on the test predictions: each attribute's groups and their gaps, shaped as the container
    path records them (pipewright_runtime.fairness_measures); {} without evaluation.fairness."""
    from pipewright_runtime import fairness_gaps

    fair = ml_config["evaluation"].get("fairness")
    if not fair:
        return {}
    data = ml_config["data"]
    min_rows = int(fair.get("min_rows") or 30)
    out = {}
    for attr in fair["attributes"]:
        rows = jobs.run(render("fairness.sql", predictions=predictions, attribute=q(attr),
                               label=ident(data["label"]["column"]), positive=str(data["label"].get("positive", 1)),
                               threshold=float(ml_config["evaluation"].get("decision_threshold") or 0.5)),
                        f"fairness by {attr}")
        groups, rates = {}, []
        for r in rows:
            entry = {"rows": int(r["row_count"])}
            if entry["rows"] >= min_rows:
                entry.update({k: None if r[k] is None else float(r[k]) for k in ("selection_rate", "tpr", "fpr")})
                rates.append(entry)
            groups[str(r["_pw_group"])] = entry
        out[attr] = {"groups": groups, **fairness_gaps(rates, fair["metrics"])}
    return out


# ------------------------------------------------------------------------- PSI --

def baseline(profile: dict, bins: int = 10) -> list[float]:
    """Bin edges as the container path makes them (mlkit.psi_baseline): midpoints between the values when there
    are few, else the deciles (BigQuery's approximate ones)."""
    distinct = int(profile.get("distinct_values") or 0)
    if distinct == 0:
        return []
    if distinct <= bins:
        values = [float(v) for v in profile["smallest"]]
        return [(a + b) / 2 for a, b in zip(values[:-1], values[1:], strict=True)]
    quantiles = [float(v) for v in profile["quantiles"][1:-1]]
    return sorted(set(quantiles))


def fractions(counts: list[dict], edges: list[float]) -> list[float]:
    total = sum(int(c["n"]) for c in counts) or 1
    out = [0.0] * (len(edges) + 1)
    for c in counts:
        out[int(c["bucket"])] += int(c["n"]) / total
    return [round(v, 6) for v in out]


def psi(expected: list[float], actual: list[float]) -> float:
    e = [max(v, EPS) for v in expected]
    a = [max(v, EPS) for v in actual]
    return float(sum((x - y) * math.log(x / y) for x, y in zip(a, e, strict=True)))


def column_baseline(jobs: Jobs, table: str, column: str, where: str) -> dict:
    profile = jobs.run(render("profile.sql", column=ident(column), table=table, where=where), f"profile {column}")
    edges = baseline(profile[0] if profile else {})
    return {"edges": edges, "fractions": column_fractions(jobs, table, column, where, edges)}


def column_fractions(jobs: Jobs, table: str, column: str, where: str, edges: list[float]) -> list[float]:
    counts = jobs.run(render("bins.sql", column=ident(column), table=table, where=where,
                             edges=", ".join(repr(float(e)) for e in edges)), f"bins {column}")
    return fractions(counts, edges)


def class_shares(jobs: Jobs, table: str) -> dict:
    rows = jobs.run(render("class_shares.sql", table=table), "class shares")
    total = sum(int(r["n"]) for r in rows) or 1
    return {str(r["class"]): round(int(r["n"]) / total, 6) for r in rows}


def class_psi(expected: dict, actual: dict) -> float:
    """PSI over predicted classes; classes training never predicted count in one bucket of their own."""
    classes = sorted(expected)
    e = [expected[c] for c in classes] + [0.0]
    a = [actual.get(c, 0.0) for c in classes] + [sum(v for c, v in actual.items() if c not in expected)]
    return psi(e, a)


# --------------------------------------------------------------- Model Registry --

def _session():
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


def registry_url(project: str, region: str, registry_id: str) -> str:
    return f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/models/{registry_id}"


def default_version(project: str, region: str, registry_id: str, session=None, version: str = "") -> dict | None:
    """The `default` version of the use case's model, or the given one (its labels name the BigQuery model), or
    None. A closed retrain loop names the version: what production serves, or the candidate it validates."""
    session = session or _session()
    r = session.get(registry_url(project, region, registry_id) + f"@{version or 'default'}", timeout=60)
    if r.status_code == 404:
        return None
    if r.status_code >= 300:
        raise RuntimeError(f"Model Registry: {r.status_code} {r.text[:300]}")
    return r.json()


def registered_version(jobs: Jobs, model: str, wait_s: int = 300) -> str:
    """The registry version ALTER MODEL created: BigQuery records it on the model once the registry has it."""
    deadline = time.time() + wait_s
    while True:
        bq_model = jobs.client.get_model(model)
        runs = (bq_model._properties.get("trainingRuns") or [])
        version = next((r.get("vertexAiModelVersion") for r in reversed(runs) if r.get("vertexAiModelVersion")),
                       None)
        if version:
            return str(version)
        if time.time() > deadline:
            raise RuntimeError(f"{model} was registered, but no registry version appeared within {wait_s} s")
        time.sleep(10)


def label_version(project: str, region: str, registry_id: str, version: str, labels: dict, session=None) -> None:
    session = session or _session()
    url = registry_url(project, region, registry_id) + f"@{version}"
    current = session.get(url, timeout=60).json().get("labels") or {}
    r = session.patch(url, params={"updateMask": "labels"}, json={"labels": {**current, **labels}}, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"labelling v{version}: {r.status_code} {r.text[:300]}")


def make_default(project: str, region: str, registry_id: str, version: str, session=None) -> None:
    session = session or _session()
    url = registry_url(project, region, registry_id) + f"@{version}:mergeVersionAliases"
    r = session.post(url, json={"versionAliases": ["default"]}, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"making v{version} the default: {r.status_code} {r.text[:300]}")


def upload_version(project: str, region: str, registry_id: str, labels: dict, description: str,
                   kms_key: str | None = None, session=None, wait_s: int = 300) -> str:
    """A registry version without model files (AI.FORECAST: the approved settings are the version, in its
    evaluation record). Creates the model on its first version. Returns the version ID."""
    session = session or _session()
    base = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}"
    parent = f"projects/{project}/locations/{region}/models/{registry_id}"
    exists = session.get(f"{base}/models/{registry_id}", timeout=60).status_code == 200
    model = {"displayName": registry_id, "description": description, "labels": labels,
             **({"encryptionSpec": {"kmsKeyName": kms_key}} if kms_key else {})}
    r = session.post(f"{base}/models:upload", timeout=60,
                     json={"model": model, **({"parentModel": parent} if exists else {"modelId": registry_id})})
    if r.status_code >= 300:
        raise RuntimeError(f"Model Registry upload: {r.status_code} {r.text[:300]}")
    operation, deadline = r.json()["name"], time.time() + wait_s
    while True:
        op = session.get(f"https://{region}-aiplatform.googleapis.com/v1/{operation}", timeout=60).json()
        if op.get("done"):
            if op.get("error"):
                raise RuntimeError(f"Model Registry upload: {op['error']}")
            return str(op["response"]["modelVersionId"])
        if time.time() > deadline:
            raise RuntimeError(f"Model Registry upload did not finish within {wait_s} s ({operation})")
        time.sleep(5)


def write_record(project: str, uri: str, record: dict) -> str:
    from google.cloud import storage

    bucket, _, prefix = uri.removeprefix("gs://").partition("/")
    blob = storage.Client(project=project).bucket(bucket).blob(f"{prefix.rstrip('/')}/evaluation.json")
    blob.upload_from_string(json.dumps(record, indent=2, default=str), content_type="application/json")
    return f"gs://{bucket}/{blob.name}"


def read_record(project: str, uri: str) -> dict | None:
    from google.cloud import storage

    bucket, _, prefix = uri.removeprefix("gs://").partition("/")
    blob = storage.Client(project=project).bucket(bucket).blob(f"{prefix.rstrip('/')}/evaluation.json")
    return json.loads(blob.download_as_text()) if blob.exists() else None


# ------------------------------------------------------------- performance --

PERFORMANCE_OUTPUTS = {
    "binary": "pr.predicted_label AS _pw_label, pr.score AS _pw_score",
    "multiclass": "pr.predicted_label AS _pw_label, pr.score AS _pw_score, CAST(NULL AS FLOAT64) AS _pw_true_prob",
    "regression": "pr.prediction AS _pw_prediction",
}


def performance_sql(ml_config: dict, project: str, model_version: str, out_table: str, labels: dict) -> str:
    """The scored rows of this model version that have an observed outcome, shaped for the metrics templates."""
    data, perf = ml_config["data"], ml_config["monitoring"]["performance"]
    task = data["label"].get("task", "binary")
    dataset = ml_config["platform"]["bigquery_dataset"]
    source = perf["labels"]
    return render("performance.sql", out_table=out_table, labels=label_pairs(labels), keep_days=KEEP_DAYS,
                  outcome=ident(source["label"]), label=ident(data["label"]["column"]),
                  outputs=PERFORMANCE_OUTPUTS[task], key=ident(source["key"]),
                  output_table=f"{project}.{dataset}.{ml_config['inference']['output_table']}",
                  labels_table=f"{project}.{dataset}.{source['table']}", model_version=str(int(model_version)))


def performance_trigger(value: float | None, metric: str, minimum: float | None) -> bool:
    """Whether the value is worse than the limit (monitoring.performance.min: a minimum, or a maximum for errors)."""
    if value is None or minimum is None:
        return False
    return value > minimum if metric in LOWER_IS_BETTER else value < minimum


# --------------------------------------------------------- retrain proposals --

def publish(topic: str, region: str, body: dict, attributes: dict, session=None) -> str:
    """One message on a Pub/Sub topic (regional endpoint: private topics keep messages in the region)."""
    import base64

    session = session or _session()
    message = {"data": base64.b64encode(json.dumps(body, default=str).encode()).decode(), "attributes": attributes}
    r = session.post(f"https://{region}-pubsub.googleapis.com/v1/{topic}:publish", json={"messages": [message]},
                     timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"publish to {topic}: {r.status_code} {r.text[:300]}")
    return r.json()["messageIds"][0]


def proposal(ml_config: dict, run_id: str, fired: list[str], summary: dict, data_checks: dict | None,
             model_version: str, resource: str, recent: list[dict]) -> dict:
    """The retrain proposal the launcher opens or updates a decision case with, as the container path sends it."""
    feature_psi = summary.get("feature_psi") or {}
    observation = {
        "triggers": fired, "rows": summary.get("rows"), "model_version": model_version,
        "feature_psi": feature_psi, "drifted_features": summary.get("drifted_features") or [],
        "max_feature_psi": max(feature_psi.values(), default=None), "prediction_psi": summary.get("prediction_psi"),
        "performance": summary.get("performance"), "labelled_rows": summary.get("labelled_rows"),
        "data_checks": data_checks, "recent": recent, "engine": "bqml",
    }
    return {"kind": "retrain.proposal", "usecase": ml_config["usecase"], "run_id": run_id,
            "observation": observation, "champion": {"version": model_version, "resource_name": resource}}
