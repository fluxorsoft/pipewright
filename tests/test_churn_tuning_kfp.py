"""FR-128 (found in the 1.4.3 battle test): the churn example's tuning trials travel as JSON text, because Kubeflow
Pipelines 2.17.2 hands a ParallelFor item over as a string and refused a step declaring a dict."""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
CHURN = ROOT / "examples" / "churn"


def test_a_trial_is_text_in_the_compiled_pipeline(tmp_path, monkeypatch):
    from kfp import compiler

    monkeypatch.syspath_prepend(str(ROOT / "src/pipewright/build/templates"))
    monkeypatch.syspath_prepend(str(CHURN / "components"))
    monkeypatch.syspath_prepend(str(CHURN))
    monkeypatch.setenv("PIPEWRIGHT_ML_CONFIG", json.dumps({"training": {"tuning": {"parallel_trials": 2}}}))
    for name in ("training_pipeline", "tune"):
        sys.modules.pop(name, None)
    import training_pipeline

    out = tmp_path / "t.yaml"
    compiler.Compiler().compile(training_pipeline.churn_training_pipeline, str(out))
    spec = yaml.safe_load(out.read_text())
    assert spec["components"]["comp-tune-trial"]["inputDefinitions"]["parameters"]["hyperparameters"] == \
        {"parameterType": "STRING"}


def test_trials_are_planned_as_json_and_read_back(monkeypatch):
    monkeypatch.syspath_prepend(str(CHURN / "components"))
    sys.modules.pop("tune", None)
    import tune

    ml = {"training": {"seed": 7, "hyperparameters": {"epochs": 3},
                       "tuning": {"enabled": True, "max_trials": 3, "search_space": {
                           "learning_rate": {"type": "float", "min": 0.001, "max": 0.01}}}}}
    plan = tune.plan_trials.python_func(ml_config=ml)
    assert plan.tuning == "on" and all(isinstance(t, str) for t in plan.trials)
    first = json.loads(plan.trials[0])
    assert first["epochs"] == 3                                   # the first trial is the defaults
