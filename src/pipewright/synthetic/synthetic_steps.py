"""The synthetic pipeline's steps: KFP components on the use case's synthetic image (FR-49).

    prepare   read the source point in time, drop what is never learned, keep a real holdout
    generate  one engine candidate: learn from the training rows, generate (in parallel per candidate)
    evaluate  score the candidate against the real data: fidelity, utility, privacy
    select    recompute the gates and keep the best candidate that passes every one
    publish   apply the column rules (new keys, made-up personal data) and write the output table

Every step reads its settings from ml_config["synthetic"]; engines and evaluation come from the image's
pipewright_synthetic package, platform helpers from pipewright_runtime.
"""

import os
from typing import NamedTuple

from kfp import dsl

# The image runs this file without Pipewright installed: no pipewright imports at module level. The name is
# pipewright.synthetic.IMAGE_ENV (kept in step by a test).
IMAGE = os.environ.get("PIPEWRIGHT_SYNTHETIC_IMAGE", "pipewright-synthetic:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=IMAGE)
def prepare_synthetic(project: str, source_uri: str, run_id: str, ml_config: dict,
                      train: dsl.Output[dsl.Dataset], holdout: dsl.Output[dsl.Dataset]) -> int:
    """Returns the number of rows to generate (synthetic.rows, or as many as the source has). An empty source_uri
    means synthetic.source_uri."""
    import pipewright_runtime as rt

    syn = ml_config["synthetic"]
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


@dsl.component(base_image=BASE_IMAGE, target_image=IMAGE)
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
    try:
        data = pd.read_parquet(train.path)
        out, facts = engines.generate(data, rows, settings, tempfile.mkdtemp(prefix="engine-"),
                                      table=ml_config["usecase"].replace("-", "_"))
        out.to_parquet(synthetic.path, index=False)
        result = {"name": settings.name, "rows": len(out), "seconds": round(time.time() - t0, 1), **facts}
    except Exception as e:  # noqa: BLE001 - recorded in the evaluation record
        pd.DataFrame().to_parquet(synthetic.path)
        result = {"name": settings.name, "error": f"{type(e).__name__}: {e}"[:1000],
                  "seconds": round(time.time() - t0, 1)}
        rt.log(f"candidate {settings.name} failed: {traceback.format_exc()[-2000:]}", ml_config, "ERROR", run_id)
    rt.log(f"candidate {settings.name}: {json.dumps(result, default=str)}", ml_config, run_id=run_id)
    return json.dumps(result, default=str)


@dsl.component(base_image=BASE_IMAGE, target_image=IMAGE)
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
        metrics = evaluation.evaluate(pd.read_parquet(train.path), pd.read_parquet(holdout.path),
                                      pd.read_parquet(synthetic.path), label, (data.get("label") or {}).get("positive"),
                                      dates=dates, secrets=syn["sensitive_columns"],
                                      n_attacks=syn["evaluation"]["attacks"])
    except Exception as e:  # noqa: BLE001
        result["errors"] = [f"evaluation failed: {type(e).__name__}: {e}"[:1000]]
        return json.dumps(result, default=str)
    result["attacks"] = metrics.pop("attacks", {})
    result["metrics"] = metrics
    rt.log(f"candidate {facts['name']} evaluated", ml_config, run_id=run_id, metrics=metrics)
    return json.dumps(result, default=str)


@dsl.component(base_image=BASE_IMAGE, target_image=IMAGE)
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


@dsl.component(base_image=BASE_IMAGE, target_image=IMAGE)
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
