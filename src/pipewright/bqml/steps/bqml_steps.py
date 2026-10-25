"""The steps of the BigQuery ML pipelines (KFP components on the use case's BQML image). Each step submits BigQuery
jobs and records what it did; the data and the model stay in BigQuery.

    training: rows -> validate -> train -> evaluate (gate 1) -> [approve] register
    scoring:  champion, rows -> validate -> score -> monitor -> [retrain proposal] -> record
"""

import os
from typing import NamedTuple

from kfp import dsl

# Pinned by digest at release time.
COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "pipewright-bqml:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_rows(project: str, source_uri: str, run_id: str, mode: str, ml_config: dict) -> NamedTuple(
        "Rows", [("rows_table", str), ("rows", int), ("test_rows", int)]):
    """The run's rows in a table: the source (the entity table, its features at each row's cut-off, the label
    window in training), the team's prepare.sql, and the held-out test flag (training)."""
    from collections import namedtuple

    import bqml_runtime as rt
    from pipewright_runtime import (
        _check_feature_datasets,
        _labelled_entities,
        bigquery_client,
        bq_table,
        emit_event,
        features_query,
        labels,
        resolve_feature_groups,
    )

    if not source_uri.startswith("bq://"):
        raise ValueError(f"{source_uri!r}: a bqml use case reads a bq:// table or view")
    data = ml_config["data"]
    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-rows"}))
    ref = named = bq_table(source_uri, project)
    if mode == "training" and data.get("label_source"):
        ref = _labelled_entities(bigquery_client(project, ml_config), ref, project, data["label_source"], data,
                                 ml_config, run_id)
    source = f"SELECT * FROM `{ref}`"
    features_from = data.get("features_from")
    if features_from:
        features_from = resolve_feature_groups(features_from, project, ml_config["platform"].get("region"))
        _check_feature_datasets(features_from, project, ml_config)
        entity_columns = [f.name for f in jobs.client.get_table(ref).schema]
        staged = rt.table_ref(project, ml_config, run_id, "source" if mode == "training" else "score_source")
        jobs.run(f"CREATE OR REPLACE TABLE `{staged}` OPTIONS (expiration_timestamp = "
                 f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS\n"
                 f"{features_query(ref, entity_columns, features_from, project)}",
                 "features at each row's cut-off")
        hits = ", ".join(f"COUNTIF(__pw_hit_{i}) AS hit_{i}" for i in range(len(features_from["sources"])))
        counted = jobs.run(f"SELECT COUNT(*) AS n, {hits} FROM `{staged}`", "feature matches")[0]
        tc = features_from["entity"]["time_column"]
        emit_event("data.features.resolved", ml_config, run_id, entity_table=named,
                   **({"labelled": True} if named != ref else {}),
                   cutoff=tc if tc in entity_columns else "query time", table=staged, sources=[
                       {"table": bq_table(s["table"], project),
                        "time_column": s.get("time_column", "feature_timestamp"),
                        "features": [s.get("prefix", "") + c for c in s["features"]],
                        **({"feature_group": s["feature_group"]} if s.get("feature_group") else {}),
                        "rows_matched": int(counted[f"hit_{i}"]), "rows": int(counted["n"])}
                       for i, s in enumerate(features_from["sources"])])
        drop = ", ".join(f"__pw_hit_{i}" for i in range(len(features_from["sources"])))
        source = f"SELECT * EXCEPT ({drop}) FROM `{staged}`"

    rows_table = rt.table_ref(project, ml_config, run_id, "rows" if mode == "training" else "score")
    test_flag = rt.split_flag(ml_config, rt.split_keys(ml_config)) if mode == "training" else "FALSE"
    jobs.run(rt.render("rows.sql", rows_table=rows_table, labels=rt.label_pairs(jobs.labels), run_id=run_id,
                       prepared=rt.prepared_sql(ml_config, source), test_flag=test_flag), "the run's rows")
    counts = jobs.run(f"SELECT COUNT(*) AS n, COUNTIF(_pw_test) AS test FROM `{rows_table}`", "row counts")[0]
    columns = [f.name for f in jobs.client.get_table(rows_table).schema]
    label = (data.get("label") or {}).get("column")
    if mode == "training" and label and label not in columns:
        raise ValueError(f"the run's rows have no label column {label!r}: bqml.prepare must keep it")
    emit_event("data.snapshot.written", ml_config, run_id, table=rows_table, rows=int(counts["n"]),
               test_rows=int(counts["test"]), source_uri=source_uri, mode=mode,
               **({"prepare_sha256": ml_config["bqml"]["prepare"]["sha256"]} if ml_config["bqml"].get("prepare")
                  else {}))
    return namedtuple("Rows", ["rows_table", "rows", "test_rows"])(rows_table, int(counts["n"]), int(counts["test"]))


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_validate(project: str, rows_table: str, run_id: str, mode: str, ml_config: dict) -> str:
    """The data contract, checked in BigQuery as the container path checks it. Fails the run on any error."""
    import json

    import bqml_runtime as rt
    from pipewright_runtime import labels

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-validate"}))
    columns = {f.name for f in jobs.client.get_table(rows_table).schema}
    checks, rules = rt.contract_checks(ml_config, columns, mode)
    row = jobs.run(rt.render("contract.sql", checks=checks, rows_table=rows_table), "data contract")[0]
    errors = rt.contract_errors(row, rules, ml_config["data"].get("contract") or {}, mode)
    if errors:
        raise ValueError("data contract failed:\n  " + "\n  ".join(errors))
    return json.dumps({"rows": int(row["_rows"]), "errors": [], "passed": True})


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_train(project: str, rows_table: str, run_id: str, ml_config: dict) -> str:
    """CREATE MODEL on the training rows. Returns the model (project.dataset.model)."""
    import bqml_runtime as rt
    from pipewright_runtime import labels

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-train"}))
    model = rt.model_ref(project, ml_config, run_id)
    forecast = ml_config["bqml"].get("forecast")
    if forecast and forecast["engine"] == "ai_forecast":     # AI.FORECAST trains nothing: evaluation measures it
        return ""
    if forecast:
        steps = rt.forecast_steps(jobs, ml_config, rows_table)
        jobs.run(rt.forecast_create_sql(ml_config, model, rows_table, steps), f"CREATE MODEL {model}")
        return model
    imported = ml_config["bqml"].get("import")
    if imported:                               # bqml.import: a model trained elsewhere
        jobs.run(rt.render("import_any_model.sql", model=model, format=imported["format"].upper(),
                           path=imported["path"]), f"import {imported['path']}")
        return model
    columns = [f.name for f in jobs.client.get_table(rows_table).schema]
    jobs.run(rt.create_model_sql(ml_config, model, rows_table, columns), f"CREATE MODEL {model}")
    return model


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_evaluate(project: str, region: str, rows_table: str, model: str, run_id: str, ml_config: dict,
                  champion_version: str = "") -> NamedTuple(
        "Evaluation", [("decision", str), ("reasons", list), ("record", str), ("primary_value", float)]):
    """The model and the champion on the same test rows, Pipewright's metrics in SQL, slices, gate 1 by the same
    check `pipewright model approve` makes, the PSI baselines and the evaluation record."""
    from collections import namedtuple

    import bqml_runtime as rt
    import pipewright_gates
    from pipewright_runtime import emit_event, labels

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-evaluate"}))
    evaluation, section = ml_config["evaluation"], ml_config["bqml"]
    task = ml_config["data"]["label"].get("task", "binary")
    predictions = rt.table_ref(project, ml_config, run_id, "test")
    forecast = section.get("forecast")
    unsupervised = task in rt.UNSUPERVISED
    if forecast:                       # the held-out latest period, forecast and measured against what happened
        result = rt.evaluate_forecast(jobs, ml_config, forecast, model, rows_table, predictions)
    else:
        jobs.run(rt.predict_sql(ml_config, model, rows_table, predictions, "_pw_test", jobs.labels, True,
                                parts=rt.model_parts(jobs, model) if section.get("import") else None),
                 "test predictions")
        result = (rt.evaluate_unsupervised(jobs, ml_config, model, rows_table) if unsupervised
                  else rt.evaluate_rows(jobs, ml_config, predictions))
    record = {"metrics": result["metrics"], "slices": result["slices"], "rows": {"test": result["rows"]},
              "task": task, "engine": "bqml",
              "model": {"bigquery_model": model or None, "model_type": section["model_type"],
                        "options": section["options"], **({"forecast": forecast} if forecast else {})},
              **({"horizon_evaluated": result["horizon"]} if forecast else {}),
              "sql": {k: section[k]["sha256"] for k in ("transform", "prepare") if section.get(k)},
              **({"per_class": result["per_class"]} if "per_class" in result else {}),
              **({"confusion": result["_confusion"]} if "_confusion" in result else {})}
    if ml_config["evaluation"].get("fairness"):            # FR-53: gated by pipewright_gates.check below
        record["fairness"] = rt.fairness(jobs, ml_config, predictions)

    # The champion: the default version, or the one a closed retrain loop names (what production serves).
    champion = rt.default_version(project, region, section["registry_id"], version=champion_version)
    if champion_version and champion is None:
        raise ValueError(f"champion_version {champion_version}: {section['registry_id']} has no such version")
    champion_model = ((champion or {}).get("labels") or {}).get("pipewright-bq-model")
    if forecast:
        # ARIMA_PLUS forecasts from its own training end, so it has no champion (config refuses one); an AI.FORECAST
        # champion is its approved settings, run on this run's history.
        champion_model = None
        if forecast["engine"] == "ai_forecast" and evaluation["champion"]["enabled"] and champion:
            run = (champion.get("labels") or {}).get("pipewright-run")
            approved = ((rt.read_record(project, f"{section['records']}{run}/") or {}).get("model") or {}).get(
                "forecast")
            if approved:
                champion_table = rt.table_ref(project, ml_config, run_id, "champion")
                got = rt.evaluate_forecast(jobs, ml_config, approved, "", rows_table, champion_table)
                record["champion"] = {**got["metrics"], "_version": champion.get("versionId"), "_settings": approved}
                champion_model = "ai_forecast"
    elif champion_model:
        dataset = ml_config["platform"]["bigquery_dataset"]
        champion_ref = f"{project}.{dataset}.{champion_model}"
        champion_predictions = rt.table_ref(project, ml_config, run_id, "champion")
        jobs.run(rt.predict_sql(ml_config, champion_ref, rows_table, champion_predictions, "_pw_test", jobs.labels,
                                True, parts=rt.model_parts(jobs, champion_ref)), "champion predictions")
        champion_metrics = (rt.evaluate_unsupervised(jobs, ml_config, champion_ref, rows_table) if unsupervised
                            else rt.evaluate_rows(jobs, ml_config, champion_predictions))["metrics"]
        record["champion"] = {**champion_metrics,
                              "_version": champion.get("versionId"), "_model": champion_ref}

    # PSI baselines for scoring: the drift features on the training rows, the predictions on the test rows.
    columns = {f.name for f in jobs.client.get_table(rows_table).schema}
    drift = [c for c in ml_config["monitoring"]["drift"]["features"] or [] if c in columns]
    stats = {"drift_baseline": {c: rt.column_baseline(jobs, rows_table, c, "NOT _pw_test") for c in drift}}
    if task in ("multiclass", "clustering"):                 # class shares: the clusters' for k-means
        stats["prediction_baseline"] = {"classes": rt.class_shares(jobs, predictions)}
    elif task == "decomposition":
        stats["prediction_baseline"] = {}
    else:
        stats["prediction_baseline"] = rt.column_baseline(
            jobs, predictions, "_pw_prediction" if task in ("regression", "forecast") else "_pw_score", "TRUE")
    stats["rows"] = int(jobs.run(f"SELECT COUNTIF(NOT _pw_test) AS n FROM `{rows_table}`", "training rows")[0]["n"])
    record["training_stats"] = stats
    if section.get("explain"):
        top = jobs.run(rt.render("explain.sql", model=model), "global explanations")
        k = int(ml_config["explainability"].get("top_k") or 10)
        record["explanations"] = sorted(({"feature": r["feature"], "attribution": float(r["attribution"])}
                                         for r in top), key=lambda r: -abs(r["attribution"]))[:k]

    problems = pipewright_gates.check(record, evaluation, champion_expected=bool(champion_model))
    decision = "reject" if problems else "approve"
    primary = evaluation["primary_metric"]
    reasons = list(problems)
    if record.get("champion") and not problems:
        reasons.append(f"champion: {primary} {record['metrics'].get(primary)} vs {record['champion'].get(primary)} "
                       f"(champion version {record['champion']['_version']})")
    record.update({"decision": decision, "reasons": reasons, "run_id": run_id, "bytes_billed": jobs.billed})
    uri = rt.write_record(project, rt.record_uri(ml_config, run_id), record)
    emit_event("training.evaluated", ml_config, run_id, decision=decision, reasons=reasons, primary_metric=primary,
               primary_value=record["metrics"].get(primary), record=uri,
               champion_value=(record.get("champion") or {}).get(primary),
               champion_version=(record.get("champion") or {}).get("_version"))
    value = record["metrics"].get(primary)
    return namedtuple("Evaluation", ["decision", "reasons", "record", "primary_value"])(
        decision, reasons, uri, float(value) if value is not None else float("nan"))


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_register(project: str, region: str, model: str, run_id: str, primary_value: float, ml_config: dict,
                  source_uri: str = "", rows_table: str = "") -> str:
    """After gate 1 only: registers the model as a new version of the use case's model (ALTER MODEL), labels it
    with the run and the BigQuery model, makes it the default when registry.set_default is on_approval, and writes
    its lineage next to the evaluation record."""
    import bqml_runtime as rt
    from pipewright_runtime import emit_event, labels, lineage_record, write_lineage

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-register"}))
    registry_id = ml_config["bqml"]["registry_id"]
    forecast = ml_config["bqml"].get("forecast") or {}
    tags = {"pipewright-usecase": rt.base_name(ml_config["usecase"]).replace("_", "-"),
            "pipewright-run": rt.run_part(run_id), "status": "candidate"}
    if forecast.get("engine") == "ai_forecast":
        # No model to register: a version without files, whose evaluation record holds the settings it was measured
        # with; scoring runs AI.FORECAST with those once a person approves it.
        version = rt.upload_version(
            project, region, registry_id, {**tags, "pipewright-engine": "ai-forecast"},
            f"AI.FORECAST {forecast['model']}, horizon {forecast['horizon']} (no model files: the settings in its "
            "evaluation record are the version)", ml_config["platform"].get("kms_key"))
    else:
        if forecast:     # ARIMA_PLUS: the measured options refit on the whole series, so it forecasts from today
            measured, model = model, f"{model}_refit"
            jobs.run(rt.forecast_create_sql(ml_config, model, rows_table, int(forecast["horizon"]), refit=True),
                     f"CREATE MODEL {model} (refit on the whole series)")
            record = rt.read_record(project, rt.record_uri(ml_config, run_id)) or {}
            record["model"] = {**record.get("model", {}), "bigquery_model": model, "measured_model": measured}
            rt.write_record(project, rt.record_uri(ml_config, run_id), record)
        jobs.run(rt.render("register.sql", model=model, registry_id=registry_id), "registration")
        version = rt.registered_version(jobs, model)
        primary = ml_config["evaluation"]["primary_metric"]
        rt.label_version(project, region, registry_id, version, {
            **tags, "pipewright-bq-model": model.rsplit(".", 1)[1], "pipewright-engine": "bqml"},
            description=f"run {run_id}, {primary}={primary_value:.4f}" if primary_value == primary_value  # not NaN
            else f"run {run_id}")
    default = ml_config["registry"].get("set_default") == "on_approval"
    if default:
        rt.make_default(project, region, registry_id, version)
    primary = ml_config["evaluation"]["primary_metric"]
    emit_event("model.registered", ml_config, run_id, model=registry_id, version=version, default=default,
               primary_metric=primary, primary_value=primary_value, bigquery_model=model or None)
    folder = rt.record_uri(ml_config, run_id)
    write_lineage(ml_config, run_id, folder, lineage_record(
        ml_config, run_id, source_uri, {"name": registry_id, "version": str(version),
                                        **({"bigquery_model": model} if model else {"engine": "ai_forecast"})},
        rt.read_record(project, folder) or {}, f"{folder}evaluation.json", rows_table or None))
    return version


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_champion(project: str, region: str, ml_config: dict, model_version: str = "") -> NamedTuple(
        "Champion", [("model", str), ("version", str), ("record", str)]):
    """The default version of the use case's model, or the one a closed retrain loop validates (model_version):
    the BigQuery model it names and its evaluation record."""
    from collections import namedtuple

    import bqml_runtime as rt

    section = ml_config["bqml"]
    version = rt.default_version(project, region, section["registry_id"], version=model_version)
    labels = (version or {}).get("labels") or {}
    if labels.get("pipewright-engine") == "ai-forecast":     # AI.FORECAST: no BigQuery model, its record's settings
        record = f"{section['records']}{labels.get('pipewright-run', '')}/"
        return namedtuple("Champion", ["model", "version", "record"])("ai_forecast", str(version["versionId"]), record)
    if not labels.get("pipewright-bq-model"):
        which = f"version {model_version}" if model_version else "default version"
        raise ValueError(f"{section['registry_id']} has no {which} from a bqml training run: train and approve one "
                         "first (`pipewright model approve`)")
    model = f"{project}.{ml_config['platform']['bigquery_dataset']}.{labels['pipewright-bq-model']}"
    record = f"{section['records']}{labels.get('pipewright-run', '')}/"
    return namedtuple("Champion", ["model", "version", "record"])(model, str(version["versionId"]), record)


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_score(project: str, rows_table: str, model: str, model_version: str, run_id: str,
               ml_config: dict) -> NamedTuple("Scores", [("scored_table", str), ("rows", int)]):
    """ML.PREDICT with the champion on the run's rows, into inference.output_table."""
    from collections import namedtuple

    import bqml_runtime as rt
    from pipewright_runtime import labels

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-score"}))
    scored = rt.table_ref(project, ml_config, run_id, "scored")
    if ml_config["bqml"].get("forecast"):
        # The settings the champion was approved with (its record), else the release's; AI.FORECAST reads the
        # history in this run's rows, ARIMA_PLUS forecasts from the end of its training data.
        settings = ((rt.version_record(project, ml_config, model_version) or {}).get("model") or {}).get(
            "forecast") or ml_config["bqml"]["forecast"]
        if settings["engine"] == "ai_forecast" and ml_config["data"]["label"]["column"] not in {
                f.name for f in jobs.client.get_table(rows_table).schema}:
            raise ValueError(f"the rows have no {ml_config['data']['label']['column']} column: AI.FORECAST "
                             "forecasts from the series' history")
        jobs.run(rt.forecast_table_sql(ml_config, rt.forecast_sql(ml_config, settings, model, rows_table, "TRUE",
                                                                   settings["horizon"]), scored, jobs.labels),
                 "forecast")
        rt.forecast_failures(jobs, scored)
    else:
        jobs.run(rt.predict_sql(ml_config, model, rows_table, scored, "TRUE", jobs.labels, False,
                                parts=rt.model_parts(jobs, model)), "predictions")
    output = ml_config["inference"]["output_table"]
    target = f"{project}.{ml_config['platform']['bigquery_dataset']}.{output}"
    table_columns = [f.name for f in jobs.client.get_table(target).schema]
    have = {f.name for f in jobs.client.get_table(scored).schema}
    fixed = {"run_id": rt.string(run_id), "model_version_id": rt.string(model_version),
             "scored_at": "CURRENT_TIMESTAMP()", "predicted_label": "_pw_label", "score": "_pw_score",
             "prediction": "_pw_prediction"}
    chosen = [(c, fixed[c]) for c in table_columns
              if c in fixed and (not fixed[c].startswith("_pw") or fixed[c] in have)]
    chosen += [(c, rt.q(c)) for c in table_columns if c not in fixed and c in have]
    jobs.run(rt.render("score.sql", output_table=target, columns=", ".join(rt.q(c) for c, _ in chosen),
                       values=", ".join(v for _, v in chosen), scored_table=scored), f"scores into {output}")
    n = int(jobs.run(f"SELECT COUNT(*) AS n FROM `{scored}`", "scored rows")[0]["n"])
    return namedtuple("Scores", ["scored_table", "rows"])(scored, n)


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_monitor(project: str, rows_table: str, scored_table: str, rows: int, model_version: str, record: str,
                 run_id: str, ml_config: dict) -> NamedTuple(
        "Monitoring", [("alert", str), ("triggers", list), ("summary", str)]):
    """Feature drift and prediction drift (PSI against the champion's training baselines) and performance against
    observed outcomes (monitoring.performance.labels), as the container path checks them."""
    import json
    from collections import namedtuple

    import bqml_runtime as rt
    from pipewright_runtime import labels

    jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-monitor"}))
    mon = ml_config["monitoring"]
    stats = (rt.read_record(project, record) or {}).get("training_stats")
    result = {"rows": rows, "model_version": model_version, "notes": []}
    triggers = []
    if (mon["drift"]["enabled"] or mon["prediction_drift"]["enabled"]) and not stats:
        result["notes"].append(f"no training baselines in {record}evaluation.json: drift not checked")
    if mon["drift"]["enabled"] and stats:
        feature_psi = {
            c: round(rt.psi(b["fractions"], rt.column_fractions(jobs, rows_table, c, "TRUE", b["edges"])), 4)
            for c, b in stats["drift_baseline"].items() if c in mon["drift"]["features"]}
        drifted = sorted(c for c, v in feature_psi.items() if v > mon["drift"]["threshold"])
        result.update({"feature_psi": feature_psi, "drifted_features": drifted})
        if drifted:
            triggers.append("drift")
    if mon["prediction_drift"]["enabled"] and stats:
        base = stats["prediction_baseline"]
        if "classes" in base:
            value = rt.class_psi(base["classes"], rt.class_shares(jobs, scored_table))
        else:
            column = ("_pw_prediction" if ml_config["data"]["label"].get("task") in ("regression", "forecast")
                      else "_pw_score")
            value = rt.psi(base["fractions"], rt.column_fractions(jobs, scored_table, column, "TRUE", base["edges"]))
        result["prediction_psi"] = round(value, 4)
        if value > mon["prediction_drift"]["threshold"]:
            triggers.append("drift")

    perf = mon["performance"]
    if perf["enabled"]:
        # The latest prediction per key from this model version, with its observed outcome: Pipewright's metrics
        # in SQL, the same definitions as evaluation.
        labelled = rt.table_ref(project, ml_config, run_id, "performance")
        jobs.run(rt.performance_sql(ml_config, project, model_version, labelled, jobs.labels), "labelled predictions")
        row = (jobs.run(rt.metrics_sql(ml_config, labelled), "performance") or [{}])[0]
        result["labelled_rows"] = int(row.get("row_count") or 0)
        if result["labelled_rows"] >= rt.MIN_LABELLED:
            value = rt.shape_metrics(row, [perf["metric"]])[perf["metric"]]
            result["performance"] = {perf["metric"]: value}
            if rt.performance_trigger(value, perf["metric"], perf.get("min")):
                triggers.append("performance")
        else:
            result["notes"].append(f"{result['labelled_rows']} labelled predictions so far: performance needs "
                                   f"{rt.MIN_LABELLED}")
    triggers = sorted(set(triggers))
    result["triggers"] = triggers
    return namedtuple("Monitoring", ["alert", "triggers", "summary"])(
        "yes" if triggers else "no", triggers, json.dumps(result, default=str))


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_retrain(project: str, region: str, run_id: str, triggers: list, summary: str, data_checks: str,
                 model_version: str, ml_config: dict) -> str:
    """When retraining.when matches a trigger, sends what this run saw to the launcher, which opens or updates a
    retrain decision case (retraining.mode propose or candidate). A person decides; nothing trains from here. Over
    Pub/Sub when the environment has a run requests topic (private perimeters), else POST /retraining/propose.
    Returns what happened; never fails the scoring run."""
    import json

    import bqml_runtime as rt
    from pipewright_runtime import labels, log

    policy, platform = ml_config["retraining"], ml_config["platform"]
    mode = policy.get("mode") or ("propose" if policy.get("enabled") else "off")
    fired = sorted(set(triggers) & set(policy["when"]))
    if mode == "off":
        return "no case: retraining.mode is off"
    if not fired:
        return "no case: nothing fired"
    try:
        recent = []
        table = ml_config["monitoring"].get("output_table")
        if table:
            jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-retrain"}))
            try:      # the trend for the decision pack: earlier runs of this use case
                rows = jobs.run(rt.render("history.sql", table=f"{project}.{platform['bigquery_dataset']}.{table}"),
                                "monitoring history")
                recent = [{"at": str(r["monitored_at"]), "max_feature_psi": r["max_feature_psi"],
                           "prediction_psi": r["prediction_psi"], "performance_value": r["performance_value"],
                           "alert": r["alert"]} for r in rows][::-1]
            except Exception as e:  # noqa: BLE001 - the pack is still useful without the trend
                log(f"no monitoring history for the decision pack: {e}", ml_config, "WARNING", run_id)
        resource = f"projects/{project}/locations/{region}/models/{ml_config['bqml']['registry_id']}"
        body = rt.proposal(ml_config, run_id, fired, json.loads(summary),
                           json.loads(data_checks) if data_checks else None, model_version, resource, recent)
        if platform.get("run_requests_topic"):
            message = rt.publish(platform["run_requests_topic"], platform["region"], body,
                                 {"usecase": ml_config["usecase"]})
            return f"proposed ({', '.join(fired)}): message {message}"
        import requests
        from pipewright_runtime import id_token

        url = platform["launcher_url"].rstrip("/")
        r = requests.post(f"{url}/retraining/propose", data=json.dumps(body, default=str),
                          headers={"Authorization": f"Bearer {id_token(url)}", "Content-Type": "application/json"},
                          timeout=120)
        if r.status_code != 200:
            return f"failed: launcher returned {r.status_code}: {r.text[:300]}"
        result = r.json()
        return f"proposed ({', '.join(fired)}): {result.get('detail') or result.get('status')}"
    except Exception as e:  # noqa: BLE001 - reported in the monitoring table, not raised
        log(f"retrain proposal failed: {type(e).__name__}: {e}", ml_config, "ERROR", run_id)
        return f"failed: {type(e).__name__}: {e}"[:500]


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def bqml_record(project: str, run_id: str, source_uri: str, model_version: str, summary: str, alert: str,
                retrain_status: str, ml_config: dict):
    """The monitoring row (monitoring.output_table) and the monitoring events, as the container path records them."""
    import json
    from datetime import datetime, timezone

    import bqml_runtime as rt
    from pipewright_runtime import emit_event, labels

    s = json.loads(summary)
    perf = s.get("performance") or {}
    row = {"run_id": run_id, "source_uri": source_uri, "model_name": ml_config["model_name"],
           "model_version_id": model_version, "rows_scored": s["rows"],
           "max_feature_psi": max((s.get("feature_psi") or {}).values(), default=None),
           "drifted_features": ",".join(s.get("drifted_features", [])) or None,
           "prediction_psi": s.get("prediction_psi"),
           "performance_metric": next(iter(perf), None), "performance_value": next(iter(perf.values()), None),
           "labelled_rows": s.get("labelled_rows"), "alert": alert == "yes",
           "triggers": ",".join(s["triggers"]) or None, "retrain_status": retrain_status, "details_json": summary,
           "monitored_at": datetime.now(timezone.utc).isoformat()}
    table = ml_config["monitoring"]["output_table"]
    if table:              # monitoring off: no table, the event still records the run
        jobs = rt.Jobs(project, ml_config, run_id, labels(ml_config, run_id, **{"pipewright-step": "bqml-record"}))
        target = f"{project}.{ml_config['platform']['bigquery_dataset']}.{table}"
        kinds = {f.name: f.field_type for f in jobs.client.get_table(target).schema}
        cells = [(c, v) for c, v in row.items() if c in kinds and v is not None]
        values = [(f"TIMESTAMP {rt.string(v)}" if kinds[c] == "TIMESTAMP" else rt.literal(v)) for c, v in cells]
        jobs.run(rt.render("monitoring.sql", table=target, columns=", ".join(rt.q(c) for c, _ in cells),
                           values=", ".join(values)), "monitoring row")
    event = {k: row[k] for k in ("model_version_id", "rows_scored", "max_feature_psi", "drifted_features",
                                 "prediction_psi", "performance_metric", "performance_value", "triggers",
                                 "retrain_status")}
    emit_event("monitoring.recorded", ml_config, run_id, alert=row["alert"], **event)
    if row["alert"]:
        emit_event("monitoring.alert", ml_config, run_id, **event)
