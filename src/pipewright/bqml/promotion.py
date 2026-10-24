"""Promotion by files for BigQuery ML (phase 2): `pipewright promote` with model.promotion model on a bqml use case.

In the source environment the approved version's BigQuery model is exported (EXPORT MODEL: a TensorFlow SavedModel,
and with TRANSFORM a transform/ model beside it) into its run's records folder, with parity.json: the source's test
rows ML.PREDICT scored, every row with a missing value among the model's inputs and up to PARITY_SAMPLE others. The
folder (the evaluation record and lineage.json with it) is copied to the destination's records, checksums checked;
the files are imported there (CREATE MODEL ... MODEL_TYPE TENSORFLOW, the transform model as <model>_transform); the
parity rows are scored by the imported models; only when every score matches within TOLERANCE is the model
registered (ALTER MODEL) as a candidate version, labelled with where it came from. Nothing reads across the two
environments: the copy is the only bridge, as for container models.

Linear and logistic regression only: they export as TensorFlow and score identically when re-imported (the spike,
Enterprise docs/BQML.md section 13). Boosted trees and random forests export an XGBoost booster that fills missing
values differently: they are promoted by retraining. An exported model fills missing values differently from
BigQuery ML too; the parity rows include every test row with one, so such a model fails parity unless the data
contract makes the inputs required or prepare.sql or TRANSFORM fills them.
"""

from __future__ import annotations

import json
import time

from pipewright.errors import ConfigError

EXPORTABLE = ("linear_reg", "logistic_reg")
PARITY_SAMPLE = 200
TOLERANCE = 1e-6
_SCORE = {"binary": "_pw_score", "regression": "_pw_prediction", "multiclass": "_pw_label"}
# The tables API names legacy types; CAST takes the standard SQL names.
_STANDARD = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "RECORD": "STRUCT"}


def _rt():
    from pipewright.bqml.steps import bqml_runtime

    return bqml_runtime


def _query(gcp, env, sql: str, select: bool) -> list[dict]:
    """A statement in env's project: a SELECT names the environment's BigQuery key; DDL takes the dataset's."""
    from pipewright.gcp.bigquery import query, rows

    return rows(query(gcp, env.project, sql, [], kms_key=env.security.get("bigquery_kms_key") if select else None))


def _json_rows(gcp, env, sql: str) -> list[dict]:
    return [json.loads(r["j"]) for r in _query(gcp, env, f"SELECT TO_JSON_STRING(t) AS j FROM ({sql}) AS t", True)]


def _model_api(env, ref: str) -> str:
    project, dataset, model = ref.split(".")
    return f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/models/{model}"


def _objects_exist(gcp, uri: str) -> bool:
    bucket, _, prefix = uri.removeprefix("gs://").partition("/")
    r = gcp.get(f"https://storage.googleapis.com/storage/v1/b/{bucket}/o", prefix=prefix, maxResults=1)
    return bool(r.ok and (r.body or {}).get("items"))


def _write_json(gcp, uri: str, doc: dict) -> None:
    from pipewright.gcp import gcs_write

    bucket, _, name = uri.removeprefix("gs://").partition("/")
    gcs_write(gcp, bucket, name, json.dumps(doc, indent=1, default=str))


def _read_json(gcp, uri: str) -> dict | None:
    from pipewright.gcp import gcs_read

    bucket, _, name = uri.removeprefix("gs://").partition("/")
    text, _ = gcs_read(gcp, bucket, name)
    return json.loads(text) if text else None


def check_exportable(uc) -> str | None:
    """Why a bqml use case cannot be promoted by files, or None."""
    model_type = str(((uc.ml or {}).get("bqml") or {}).get("model_type", "")).lower()
    if model_type not in EXPORTABLE:
        return (f"bqml.model_type {model_type} is promoted by retraining: promotion by files (model.promotion model) "
                f"supports {', '.join(EXPORTABLE)}, which export as TensorFlow and score identically when imported")
    return None


def parity_doc(gcp, env, uc, run: str, model_ref: str) -> dict:
    """The source's test rows with ML.PREDICT's scores (see the module docstring)."""
    rt = _rt()
    ml = uc.ml_config()
    task = ml["data"]["label"].get("task", "binary")
    test = rt.table_ref(env.project, ml, run, "test")
    features = [c["name"] for c in (gcp.get(_model_api(env, model_ref)).body or {}).get("featureColumns", [])]
    table = test.split(".")
    schema = gcp.get(f"https://bigquery.googleapis.com/bigquery/v2/projects/{table[0]}/datasets/{table[1]}/tables/"
                     f"{table[2]}")
    if not schema.ok:
        raise ConfigError(f"the test predictions of run {run} ({test}) are gone (kept {rt.KEEP_DAYS} days): retrain "
                          "in the source environment, approve, then promote")
    types = {f["name"]: f["type"] for f in schema.body["schema"]["fields"]}
    expected = _SCORE[task]
    sql = rt.render("parity_rows.sql", columns=", ".join(rt.q(c) for c in features), expected=expected,
                    any_null=" OR ".join(f"{rt.q(c)} IS NULL" for c in features) or "FALSE", test_table=test,
                    sample=PARITY_SAMPLE)
    found = _query(gcp, env, sql, True)
    if not found:
        raise ConfigError(f"no scored test rows in {test}: nothing to check the promoted model against")
    return {"task": task, "expected": expected, "columns": {c: types.get(c, "STRING") for c in features},
            "rows": [{"instance": json.loads(r["instance"]), "expected": r["expected"], "has_null": r["has_null"]}
                     for r in found]}


def parity_sql(uc, doc: dict, model_ref: str, transform: str | None) -> str:
    rt = _rt()
    casts = ", ".join(f"SAFE_CAST(JSON_VALUE(j, '$.{c}') AS {_STANDARD.get(t, t)}) AS {rt.q(c)}"
                      for c, t in doc["columns"].items())
    rows = ", ".join(rt.string(json.dumps(r["instance"])) for r in doc["rows"])
    inner = rt.imported_inner("SELECT * FROM given", transform)
    return rt.render("parity_predict.sql", casts=casts, rows=rows, model=model_ref, inner=inner,
                     outputs=rt.imported_outputs(uc.ml_config()))


def compare(doc: dict, got: list[dict]) -> dict:
    """{rows, max_difference, failed, failed_with_missing}: scores within TOLERANCE, multi-class labels equal."""
    by_i = {int(r["i"]): r for r in got}
    failed, worst = [], 0.0
    for i, row in enumerate(doc["rows"]):
        value = (by_i.get(i) or {}).get(doc["expected"])
        if doc["task"] == "multiclass":
            bad = str(value) != str(row["expected"])
        else:
            diff = abs(float(value) - float(row["expected"])) if value is not None else float("inf")
            worst, bad = max(worst, diff), diff > TOLERANCE
        if bad:
            failed.append(i)
    return {"rows": len(doc["rows"]), "max_difference": worst, "failed": len(failed),
            "failed_with_missing": sum(1 for i in failed if doc["rows"][i]["has_null"])}


def promote_bqml_model(gcp, src_env, dst_env, uc_src, uc_dst, release_id: str, version: str | None, log=print,
                       loop_case: str | None = None) -> dict:
    """The source's approved BigQuery ML version, as a candidate version in the destination (see the docstring)."""
    from pipewright.release import publish
    from pipewright.release.promote import _find_model, _versions, copy_model_files

    rt = _rt()
    if why := check_exportable(uc_dst):
        raise ConfigError(why)
    registry_id = uc_dst.model["name"]
    src_model = _find_model(gcp, src_env, registry_id)
    if src_model is None:
        raise ConfigError(f"{registry_id} has no versions in {src_env.name}")
    versions = {v["versionId"]: v for v in _versions(gcp, src_env, src_model)}
    if version is None:
        version = next((k for k, v in versions.items() if "default" in v.get("versionAliases", [])), None)
    chosen = versions.get(str(version))
    if chosen is None:
        raise ConfigError(f"{registry_id} has no version {version} in {src_env.name}")
    if "default" not in chosen.get("versionAliases", []) and not loop_case:
        raise ConfigError(f"v{version} of {registry_id} is not the default (approved) version in {src_env.name}: "
                          "approve it there first (`pipewright model approve`)")
    labels = chosen.get("labels") or {}
    if labels.get("pipewright-engine") != "bqml" or not labels.get("pipewright-bq-model"):
        raise ConfigError(f"v{version} of {registry_id} in {src_env.name} is not a BigQuery ML version")
    if labels.get("pipewright-bq-imported") == "true":
        raise ConfigError(f"v{version} of {registry_id} was itself promoted by files: promote from where it was "
                          "trained")
    run, bq_model = labels["pipewright-run"], labels["pipewright-bq-model"]
    src_ref = f"{src_env.project}.{uc_src.resources['bigquery_dataset']}.{bq_model}"
    dst_ref = f"{dst_env.project}.{uc_dst.resources['bigquery_dataset']}.{bq_model}"
    src_records = f"{uc_src.ml['bqml']['records']}{run}/"
    dst_records = f"{uc_dst.ml['bqml']['records']}{run}/"

    # 1. Source: export and the parity rows (each once: a repeated promotion reuses them).
    if not _objects_exist(gcp, f"{src_records}export/"):
        log(f"exporting {src_ref} (EXPORT MODEL) ...")
        _query(gcp, src_env, rt.render("export_model.sql", model=src_ref, uri=f"{src_records}export/"), False)
    doc = _read_json(gcp, f"{src_records}parity.json")
    if doc is None:
        doc = parity_doc(gcp, src_env, uc_src, run, src_ref)
        _write_json(gcp, f"{src_records}parity.json", doc)
    transform = _objects_exist(gcp, f"{src_records}export/transform/")

    # 2. The files, checksums checked; then the import in the destination.
    files = copy_model_files(gcp, src_records, dst_records, log)
    log(f"importing {dst_ref} in {dst_env.name} ...")
    _query(gcp, dst_env, rt.render("import_model.sql", model=dst_ref, path=f"{dst_records}export/*"), False)
    dst_transform = f"{dst_ref}_transform" if transform else None
    if dst_transform:
        _query(gcp, dst_env, rt.render("import_model.sql", model=dst_transform,
                                       path=f"{dst_records}export/transform/*"), False)

    # 3. Parity: the destination scores the source's test rows as the source did, or nothing is registered.
    result = compare(doc, _json_rows(gcp, dst_env, parity_sql(uc_dst, doc, dst_ref, dst_transform)))
    log(f"parity: {result['rows']} test rows, {result['failed']} differ (largest difference "
        f"{result['max_difference']:.2e})")
    if result["failed"]:
        for ref in [dst_ref, dst_transform]:
            if ref:
                _query(gcp, dst_env, f"DROP MODEL IF EXISTS `{ref}`", False)
        hint = (" All of them have a missing input: an exported model fills missing values differently from BigQuery "
                "ML. Make those columns required in the data contract, or fill them in prepare.sql or TRANSFORM, "
                "then retrain." if result["failed"] == result["failed_with_missing"] else "")
        raise ConfigError(f"v{version} of {registry_id} does not score in {dst_env.name} as it did in "
                          f"{src_env.name}: {result['failed']} of {result['rows']} test rows differ; nothing was "
                          f"registered.{hint}")

    # 4. Register as a candidate version, labelled with where it came from.
    dst_model = _find_model(gcp, dst_env, registry_id)
    before = {v["versionId"] for v in _versions(gcp, dst_env, dst_model)} if dst_model else set()
    _query(gcp, dst_env, rt.render("register.sql", model=dst_ref, registry_id=registry_id), False)
    new = None
    for _ in range(60):
        dst_model = dst_model or _find_model(gcp, dst_env, registry_id)
        now = {v["versionId"]: v for v in _versions(gcp, dst_env, dst_model)} if dst_model else {}
        new = next((k for k in now if k not in before), None)
        if new:
            break
        time.sleep(5)
    if new is None:
        raise ConfigError(f"{dst_ref} was registered but no new version of {registry_id} appeared in "
                          f"{dst_env.name}")
    tag = publish._tag
    new_labels = {"pipewright-usecase": rt.base_name(uc_dst.name).replace("_", "-"), "pipewright-run": run,
                  "pipewright-bq-model": bq_model, "pipewright-engine": "bqml", "pipewright-bq-imported": "true",
                  "status": "candidate", "release": tag(release_id)[:63], "promoted-from": src_env.name[:63],
                  "source-version": str(version), **({"pipewright-loop": tag(loop_case)[:63]} if loop_case else {})}
    rt.label_version(dst_env.project, dst_env.region, registry_id, new, new_labels, session=gcp.session)
    log(f"model: {registry_id} v{version} ({src_env.name}) -> v{new} ({dst_env.name}), a candidate (imported, "
        "parity checked)")
    return {"name": registry_id, "source_version": str(version), "source_artifact_uri": src_records,
            "artifact_uri": dst_records, "version": str(new), "files": files, "parity": result,
            "bigquery_model": dst_ref, "transform": dst_transform}
