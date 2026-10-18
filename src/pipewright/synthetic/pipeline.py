"""The synthetic pipeline Pipewright provides when a use case sets synthetic.enabled (learned tier).

    prepare -> [generate -> evaluate] per candidate, in parallel -> select (gates) -> [release] publish

Behaviour comes from ml_config["synthetic"]; the candidates are fixed when the release compiles it (like tuning
trials). Runs like any other pipeline of the use case: `pipewright run --pipeline synthetic`, or a schedule with
`pipeline: synthetic`.
"""

import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from kfp import dsl  # noqa: E402
from synthetic_steps import (  # noqa: E402
    evaluate_synthetic,
    generate_synthetic,
    prepare_synthetic,
    publish_synthetic,
    select_synthetic,
)

_CONFIG = json.loads(os.environ.get("PIPEWRIGHT_ML_CONFIG") or "{}")
CANDIDATES = (_CONFIG.get("synthetic") or {}).get("candidates") or [{"name": "default", "engine": "mostlyai"}]
# Loop items as JSON text: KFP gives dict items an attribute per key, and a key called `name` breaks it.
CANDIDATE_ITEMS = [json.dumps(c, sort_keys=True) for c in CANDIDATES]


@dsl.pipeline(name="synthetic", description="Learn from real data in this environment, generate, gate, publish.")
def synthetic_pipeline(
    project: str,
    region: str,
    source_uri: str,
    run_id: str,
    ml_config: dict = {},  # noqa: B006 - KFP needs a literal default; Pipewright always passes it
):
    prepare = prepare_synthetic(project=project, source_uri=source_uri, run_id=run_id, ml_config=ml_config)
    prepare.set_display_name("prepare-synthetic").set_caching_options(False)

    with dsl.ParallelFor(CANDIDATE_ITEMS, parallelism=len(CANDIDATE_ITEMS), name="candidates") as candidate:
        generate = generate_synthetic(train=prepare.outputs["train"], rows=prepare.outputs["Output"],
                                      candidate=candidate, run_id=run_id, ml_config=ml_config)
        generate.set_display_name("generate-synthetic").set_caching_options(False)
        evaluate = evaluate_synthetic(train=prepare.outputs["train"], holdout=prepare.outputs["holdout"],
                                      synthetic=generate.outputs["synthetic"], generated=generate.outputs["Output"],
                                      run_id=run_id, ml_config=ml_config)
        evaluate.set_display_name("evaluate-synthetic").set_caching_options(False)

    select = select_synthetic(results=dsl.Collected(evaluate.output), run_id=run_id, ml_config=ml_config)
    select.set_display_name("select-synthetic").set_caching_options(False)

    with dsl.If(select.outputs["decision"] == "release", name="gates-passed"):
        publish = publish_synthetic(selection=select.outputs["selection"], project=project,
                                    bq_dataset=str((_CONFIG.get("platform") or {}).get("bigquery_dataset") or ""),
                                    run_id=run_id, ml_config=ml_config)
        publish.set_display_name("publish-synthetic").set_caching_options(False)
