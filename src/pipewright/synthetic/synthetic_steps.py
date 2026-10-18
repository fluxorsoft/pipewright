"""The synthetic pipeline's steps: KFP components on the use case's synthetic images (FR-49).

    prepare   read the source point in time, drop what is never learned, keep a real holdout
    generate  one engine candidate: learn from the training rows, generate (in parallel per candidate)
    evaluate  score the candidate against the real data: fidelity, utility, privacy
    select    recompute the gates and keep the best candidate that passes every one
    publish   apply the column rules (new keys, made-up personal data) and write the output table

mode relational: the datasets are folders of tables (the entity table and each child, see
pipewright_synthetic.relational); the holdout is whole entities; evaluation adds integrity (keys, orphans, rows
per entity, history before the cut-off) and measures the gates on the point-in-time join of the synthetic tables;
publish writes every table, with new keys the same in the entity table and its children.

Every step reads its settings from ml_config["synthetic"]; engines and evaluation come from the images'
pipewright_synthetic package, platform helpers from pipewright_runtime.
"""

import os
from typing import NamedTuple

from kfp import dsl

# The images run this file without Pipewright installed: no pipewright imports at module level. The names are
# pipewright.synthetic.IMAGE_ENV and EVALUATION_IMAGE_ENV (kept in step by a test). generate runs on the engine
# image, every other step on the evaluation image.
ENGINE_IMAGE = os.environ.get("PIPEWRIGHT_SYNTHETIC_IMAGE", "pipewright-synthetic:dev")
EVALUATION_IMAGE = os.environ.get("PIPEWRIGHT_SYNTHETIC_EVALUATION_IMAGE", "pipewright-synthetic-evaluation:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=EVALUATION_IMAGE)
def prepare_synthetic(project: str, source_uri: str, run_id: str, ml_config: dict,
                      train: dsl.Output[dsl.Dataset], holdout: dsl.Output[dsl.Dataset]) -> int:
    """Returns the number of rows to generate (synthetic.rows, or as many as the source has). An empty source_uri
    means synthetic.source_uri."""
    import pipewright_runtime as rt

    syn = ml_config["synthetic"]
    if syn.get("mode") == "relational":
        return _prepare_tables(project, source_uri or syn["source_uri"], run_id, ml_config, train.path, holdout.path)
    df = rt.read_source(source_uri or syn["source_uri"], project, ml_config, run_id)
    contract = [c["name"] for c in ml_config["data"]["contract"]["columns"]]
    missing = [c for c in contract if c not in df.columns]
    if missing:
        raise ValueError(f"the source lacks contract columns: {', '.join(missing)}")
    learn = df[[c for c in contract if c not in set(syn["exclude"])]]      # never personal data or rule columns
    held = learn.sample(frac=syn["holdout_fraction"], random_state=syn["seed"])
    learn.drop(held.index).reset_index(drop=True).to_parquet(train.path, index=False)
    held.reset_index(drop=True).to_parquet(holdout.path, index=False)
    rt.log(f"synthetic source: {len(df)} rows, {len(held)} held out, learning {list(learn.columns)}", ml_config,
           run_id=run_id, rows=len(df), holdout=len(held))
    return int(syn["rows"] or len(df))


@dsl.component(base_image=BASE_IMAGE, target_image=ENGINE_IMAGE)
def generate_synthetic(train: dsl.Input[dsl.Dataset], rows: int, candidate: str, run_id: str, ml_config: dict,
                       synthetic: dsl.Output[dsl.Dataset]) -> str:
    """One candidate (its settings as JSON). A failing engine is recorded, not raised: the other candidates still
    count."""
    import json
    import tempfile
    import time
    import traceback

    import pandas as pd
    import pipewright_runtime as rt
    from pipewright_synthetic import engines

    settings = engines.Settings.from_dict(json.loads(candidate))
    t0 = time.time()
    relational = ml_config["synthetic"].get("mode") == "relational"
    try:
        table = ml_config["usecase"].replace("-", "_")
        if relational:
            from pipewright_synthetic import relational as rel

            parent, children, spec = rel.load(train.path)
            out, facts = engines.generate_tables(parent, spec["key"], children, rows, settings,
                                                 tempfile.mkdtemp(prefix="engine-"), table=table)
            rel.save(synthetic.path, out["parent"], {c["name"]: (out[c["name"]], c["key"]) for c in spec["children"]},
                     spec)
            n = len(out["parent"])
        else:
            data = pd.read_parquet(train.path)
            out, facts = engines.generate(data, rows, settings, tempfile.mkdtemp(prefix="engine-"), table=table)
            out.to_parquet(synthetic.path, index=False)
            n = len(out)
        result = {"name": settings.name, "rows": n, "seconds": round(time.time() - t0, 1), **facts}
    except Exception as e:  # noqa: BLE001 - recorded in the evaluation record
        if not relational:
            pd.DataFrame().to_parquet(synthetic.path)
        result = {"name": settings.name, "error": f"{type(e).__name__}: {e}"[:1000],
                  "seconds": round(time.time() - t0, 1)}
        rt.log(f"candidate {settings.name} failed: {traceback.format_exc()[-2000:]}", ml_config, "ERROR", run_id)
    rt.log(f"candidate {settings.name}: {json.dumps(result, default=str)}", ml_config, run_id=run_id)
    return json.dumps(result, default=str)


@dsl.component(base_image=BASE_IMAGE, target_image=EVALUATION_IMAGE)
def evaluate_synthetic(train: dsl.Input[dsl.Dataset], holdout: dsl.Input[dsl.Dataset],
                       synthetic: dsl.Input[dsl.Dataset], generated: str, run_id: str, ml_config: dict) -> str:
    """The candidate's evaluation: {name, uri, metrics, attacks, engine, errors}."""
    import json

    import pandas as pd
    import pipewright_runtime as rt
    from pipewright_synthetic import evaluation

    syn, data = ml_config["synthetic"], ml_config["data"]
    facts = json.loads(generated)
    result = {"name": facts["name"], "uri": synthetic.uri, "engine": facts, "errors": []}
    if facts.get("error"):
        result["errors"] = [f"generation failed: {facts['error']}"]
        return json.dumps(result, default=str)
    label = (data.get("label") or {}).get("column")
    dates = [c["name"] for c in data["contract"]["columns"] if c["type"] in ("date", "timestamp")]
    try:
        extra, drop = {}, ()
        if syn.get("mode") == "relational":
            from pipewright_synthetic import relational as rel

            tr_parent, tr_children, spec = rel.load(train.path)
            ho_parent, ho_children, _ = rel.load(holdout.path)
            sy_parent, sy_children, _ = rel.load(synthetic.path)
            specs = {c["name"]: {"key": c["key"], "time_column": c["order_by"] if c["kind"] == "features" else None}
                     for c in spec["children"]}
            extra, problems = rel.integrity(sy_parent, {n: d for n, (d, _k) in sy_children.items()}, spec["key"],
                                            spec["time_column"], tr_parent,
                                            {n: d for n, (d, _k) in tr_children.items()}, specs)
            if problems:
                result["errors"] = [f"integrity: {p}" for p in problems]
                result["metrics"] = extra
                return json.dumps(result, default=str)
            real, real_held = rel.flat(tr_parent, tr_children, spec), rel.flat(ho_parent, ho_children, spec)
            synthetic_flat, drop = rel.flat(sy_parent, sy_children, spec), (spec["key"],)
            columns = [c for c in real.columns if c in synthetic_flat.columns]
            real, real_held, synthetic_flat = real[columns], real_held[columns], synthetic_flat[columns]
        else:
            real, real_held = pd.read_parquet(train.path), pd.read_parquet(holdout.path)
            synthetic_flat = pd.read_parquet(synthetic.path)
        metrics = evaluation.evaluate(real, real_held, synthetic_flat, label, (data.get("label") or {}).get("positive"),
                                      dates=dates, drop=drop, secrets=syn["sensitive_columns"],
                                      n_attacks=syn["evaluation"]["attacks"])
        metrics.update(extra)
    except Exception as e:  # noqa: BLE001
        result["errors"] = [f"evaluation failed: {type(e).__name__}: {e}"[:1000]]
        return json.dumps(result, default=str)
    result["attacks"] = metrics.pop("attacks", {})
    result["metrics"] = metrics
    rt.log(f"candidate {facts['name']} evaluated", ml_config, run_id=run_id, metrics=metrics)
    return json.dumps(result, default=str)


@dsl.component(base_image=BASE_IMAGE, target_image=EVALUATION_IMAGE)
def select_synthetic(results: list, run_id: str, ml_config: dict, record: dsl.Output[dsl.Artifact]
                     ) -> NamedTuple("Selection", [("decision", str), ("selection", str)]):
    """Pipewright recomputes the gates (the purpose's, with the use case's overrides) on every candidate and keeps
    the best that passes all of them. decision: release or rejected; selection: {chosen, uri, metrics, record}.
    Writes the evaluation record."""
    import json
    from collections import namedtuple

    import pipewright_runtime as rt
    from pipewright_synthetic import gates

    syn = ml_config["synthetic"]
    candidates = [json.loads(r) if isinstance(r, str) else r for r in results]
    chosen, verdicts = gates.select(candidates, syn["gates"])
    decision = "release" if chosen is not None else "rejected"
    doc = {"usecase": ml_config["usecase"], "environment": ml_config["environment"], "run_id": run_id,
           "purpose": syn["purpose"], "gates": syn["gates"], "decision": decision,
           "chosen": candidates[chosen]["name"] if chosen is not None else None,
           "seed": syn["seed"], "source_uri": syn["source_uri"], "excluded_columns": syn["exclude"],
           "candidates": [{**c, "verdict": v} for c, v in zip(candidates, verdicts, strict=True)]}
    with open(record.path, "w") as f:
        json.dump(doc, f, indent=2, default=str)
    summary = {v["name"]: ("passed" if v["passed"] else v["reasons"]) for v in verdicts}
    rt.emit_event("synthetic.evaluated", ml_config, run_id, decision=decision, chosen=doc["chosen"],
                  purpose=syn["purpose"], candidates=summary, record=record.uri)
    if decision == "rejected":
        rt.emit_event("synthetic.rejected", ml_config, run_id, purpose=syn["purpose"], candidates=summary,
                      record=record.uri)
    selection = json.dumps({"chosen": doc["chosen"], "uri": candidates[chosen]["uri"] if chosen is not None else "",
                            "metrics": candidates[chosen].get("metrics") if chosen is not None else None,
                            "record": record.uri}, default=str)
    return namedtuple("Selection", ["decision", "selection"])(decision, selection)


@dsl.component(base_image=BASE_IMAGE, target_image=EVALUATION_IMAGE)
def publish_synthetic(selection: str, project: str, bq_dataset: str, run_id: str, ml_config: dict) -> str:
    """The chosen candidate, with the column rules applied, appended to synthetic.output_table (run_id on every
    row). Returns where it went."""
    import io
    import json

    import pandas as pd
    import pipewright_runtime as rt
    from google.cloud import storage
    from pipewright_synthetic import rules

    syn, contract = ml_config["synthetic"], ml_config["data"]["contract"]
    chosen = json.loads(selection)
    if syn.get("mode") == "relational":
        return _publish_tables(chosen, project, bq_dataset, run_id, ml_config)
    bucket, _, name = chosen["uri"].removeprefix("gs://").partition("/")
    df = pd.read_parquet(io.BytesIO(storage.Client(project=project).bucket(bucket).blob(name).download_as_bytes()))
    records = rules.apply_rules(df.to_dict("records"), contract, syn["columns"], seed=syn["seed"])
    out = pd.DataFrame(records)
    out["run_id"] = run_id
    written = rt.write_table(syn["output_table"], out, ml_config, project, bq_dataset, run_id)
    rt.emit_event("synthetic.generated", ml_config, run_id, table=syn["output_table"], rows=len(out),
                  locations=written, chosen=chosen["chosen"], purpose=syn["purpose"], metrics=chosen["metrics"],
                  record=chosen["record"])
    return json.dumps({"rows": len(out), "locations": written})


# ------------------------------------------------------------------- relational helpers --
# Plain functions next to the components: the images import this file, so the components call them at run time.

def _prepare_tables(project: str, source_uri: str, run_id: str, ml_config: dict, train_dir: str,
                    holdout_dir: str) -> int:
    """mode relational: the entity table as it is (no join) and each child table, only rows of its entities;
    timestamps without a time zone (as the engine reads them); whole entities held out."""
    import pipewright_runtime as rt
    from pipewright_synthetic import relational as rel

    syn = ml_config["synthetic"]
    spec = dict(syn["relational"])
    client = rt.bigquery_client(project, ml_config)
    parent_ref = rt.bq_table(source_uri, project)
    parent = client.query(f"SELECT * FROM `{parent_ref}`").to_dataframe(create_bqstorage_client=False)
    learn = [c for c in spec["parent_columns"] if c not in set(syn["exclude"])]
    missing = [c for c in learn if c not in parent.columns]
    if missing:
        raise ValueError(f"the entity table {parent_ref} lacks {', '.join(missing)}")
    parent = _plain(parent[learn])
    ff = (ml_config["data"].get("features_from") or {})
    if ff:
        ff = rt.resolve_feature_groups(ff, project, (ml_config.get("platform") or {}).get("region"))
        rt._check_feature_datasets(ff, project, ml_config)
    key = spec["key"]
    children, resolved = {}, []
    for c in spec["children"]:
        c = dict(c)
        if c["kind"] == "features":
            src = ff["sources"][c["index"]]
            table = rt.bq_table(src["table"], project)
            names = src.get("columns") or {}
            c["key"] = (src.get("entity_columns") or ff["entity"]["columns"])[0]
            c["order_by"] = src.get("time_column", "feature_timestamp")
            c["features"] = {out: names.get(f, f) for out, f in c["features"].items()}
            select = ", ".join(f"`{x}`" for x in dict.fromkeys([c["key"], c["order_by"], *c["features"].values()]))
        else:
            table = rt.bq_table(c["source_uri"], project)
            select = ", ".join(f"`{x}`" for x in c["columns"]) if c.get("columns") else "*"
        df = client.query(f"SELECT {select} FROM `{table}` WHERE `{c['key']}` IN (SELECT `{key}` FROM `{parent_ref}`)"
                          ).to_dataframe(create_bqstorage_client=False)
        df = _plain(df).sort_values([c["key"], c["order_by"]]).reset_index(drop=True)
        children[c["name"]] = (df, c["key"])
        resolved.append(c)
    spec["children"] = resolved
    tr, tr_c, ho, ho_c = rel.split(parent, children, key, syn["holdout_fraction"], syn["seed"])
    rel.save(train_dir, tr, {n: (d, children[n][1]) for n, d in tr_c.items()}, spec)
    rel.save(holdout_dir, ho, {n: (d, children[n][1]) for n, d in ho_c.items()}, spec)
    rt.log(f"synthetic source (relational): {len(parent)} entities, {len(ho)} held out; children "
           f"{ {n: len(d) for n, (d, _k) in children.items()} }", ml_config, run_id=run_id, entities=len(parent))
    return int(syn["rows"] or len(parent))


def _plain(df):
    """BigQuery types the engines and Parquet read plainly: DATE as ISO text, TIMESTAMP without a time zone."""
    import pandas as pd

    df = df.copy()
    for col in df.columns:
        kind = str(df[col].dtype)
        if kind == "dbdate":
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")
        elif kind.startswith("datetime64") and "UTC" in kind:
            df[col] = df[col].dt.tz_convert(None)
    return df


def _publish_tables(chosen: dict, project: str, bq_dataset: str, run_id: str, ml_config: dict) -> str:
    """mode relational: new keys (the key rule) the same in the entity table and every child, the other column
    rules on the entity table, run_id on every row; each table appended to its output table."""
    import json
    import tempfile

    import pandas as pd
    import pipewright_runtime as rt
    from google.cloud import storage
    from pipewright_synthetic import relational as rel
    from pipewright_synthetic import rules

    syn, contract = ml_config["synthetic"], ml_config["data"]["contract"]
    bucket, _, prefix = chosen["uri"].removeprefix("gs://").partition("/")
    local = tempfile.mkdtemp(prefix="synthetic-")
    for blob in storage.Client(project=project).bucket(bucket).list_blobs(prefix=prefix.rstrip("/") + "/"):
        blob.download_to_filename(f"{local}/{blob.name.rsplit('/', 1)[-1]}")
    parent, children, spec = rel.load(local)
    key = spec["key"]
    rule = {k: v for k, v in (syn["columns"].get(key) or {"rule": "key"}).items() if k != "rule"}
    parent, renamed = rel.remap_keys(parent, children, key, rule, seed=syn["seed"])
    others = {n: r for n, r in syn["columns"].items() if n != key}
    parent = pd.DataFrame(rules.apply_rules(parent.to_dict("records"), contract, others, seed=syn["seed"]))
    written, rows = {}, {"parent": len(parent)}
    parent["run_id"] = run_id
    written[syn["output_table"]] = rt.write_table(syn["output_table"], parent, ml_config, project, bq_dataset, run_id)
    for c in spec["children"]:
        df = renamed[c["name"]].assign(run_id=run_id)
        written[c["output_table"]] = rt.write_table(c["output_table"], df, ml_config, project, bq_dataset, run_id)
        rows[c["name"]] = len(df)
    rt.emit_event("synthetic.generated", ml_config, run_id, table=syn["output_table"], rows=len(parent),
                  tables=rows, locations=written, chosen=chosen["chosen"], purpose=syn["purpose"],
                  metrics=chosen["metrics"], record=chosen["record"], mode="relational")
    return json.dumps({"rows": rows, "locations": written})
