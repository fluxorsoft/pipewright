"""The closed retrain loop (FR-48, D-104 to D-109): run requests that name versions, and the loop's own steps."""
from dataclasses import replace

import pytest

from pipewright.launcher import routing
from pipewright.launcher.routing import BadRequest
from tests.test_release_parts import INFERENCE, TRAINING, V3

# A release whose pipelines declare the loop's parameters (Pipewright's BigQuery ML pipelines and the 1.1 example).
LOOPING = replace(V3, pipelines={
    "training": {**TRAINING, "parameters": [*TRAINING["parameters"], "champion_version"]},
    "inference": {**INFERENCE, "parameters": [*INFERENCE["parameters"], "model_version"]}})


def test_run_overrides_are_the_same_everywhere():
    from pipewright.build import compile as build
    from pipewright.release import contract

    assert build.RUN_OVERRIDES == contract.RUN_OVERRIDES and build.LAUNCHER_PARAMETERS == contract.LAUNCHER_PARAMETERS


def test_a_run_names_the_champion_or_the_model_version():
    run = routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "champion_version": "4", "loop": "rc-20261024-ab12cd",
                                        "run_key": "rc-20261024-ab12cd"})
    assert dict(run.overrides) == {"champion_version": "4"} and run.loop == "rc-20261024-ab12cd"
    assert run.trigger == "loop"
    target = LOOPING.pipeline("training")
    params = routing.parameters_for(LOOPING, target, run.source_uri, run.run_id, dict(run.overrides))
    assert params["champion_version"] == "4"
    labels = routing.labels_for(LOOPING, run.trigger, run.run_id, "training", run.loop)
    assert labels["pipewright-loop"] == "rc-20261024-ab12cd" and labels["trigger"] == "loop"
    run = routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "pipeline": "inference", "model_version": "5"})
    assert dict(run.overrides) == {"model_version": "5"} and run.loop is None and run.trigger == "api"


def test_versions_are_refused_where_the_pipeline_does_not_take_them():
    with pytest.raises(BadRequest, match="takes no champion_version parameter"):
        routing.resolve_run(V3, {"source_uri": "bq://d.t", "champion_version": "4"})      # a 1.0 template
    with pytest.raises(BadRequest, match="not a model version number"):
        routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "champion_version": "4; drop"})
    with pytest.raises(BadRequest, match="not a case ID"):
        routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "loop": "Bad Case"})
    plain = routing.resolve_run(V3, {"source_uri": "bq://d.t"})                          # 1.0 requests: unchanged
    assert plain.overrides == () and plain.loop is None
    assert "pipewright-loop" not in routing.labels_for(V3, plain.trigger, plain.run_id)
