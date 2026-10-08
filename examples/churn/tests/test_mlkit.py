"""Unit tests for the ML helpers (run by `pipewright test`, i.e. tests.command in usecase.yaml)."""

import pathlib
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "components"))

import mlkit  # noqa: E402

DATA = {
    "contract": {
        "columns": [
            {"name": "customer_id", "type": "string", "required": True, "max_null_fraction": 0},
            {"name": "snapshot_date", "type": "date", "required": True},
            {"name": "tenure_months", "type": "integer", "min": 0},
            {"name": "plan", "type": "integer", "allowed": [1, 12]},
            {"name": "churned", "type": "integer", "allowed": [0, 1]},
        ],
        "min_rows": 3,
        "max_null_fraction": 0.5,
        "unique": ["customer_id"],
        "allow_extra_columns": False,
    },
    "label": {"column": "churned", "values": [0, 1]},
}


def frame(n=20, seed=0):
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "customer_id": [f"C{i}" for i in range(n)],
        "snapshot_date": pd.date_range("2026-01-31", periods=n, freq="7D").strftime("%Y-%m-%d"),
        "tenure_months": rng.randint(0, 50, n),
        "plan": rng.choice([1, 12], n),
        "churned": rng.randint(0, 2, n),
    })


def test_contract_passes_and_reports():
    errors, summary = mlkit.check_contract(frame(), DATA)
    assert errors == [] and summary["rows"] == 20 and 0 <= summary["label_rate"] <= 1


def test_contract_catches_problems():
    df = frame()
    df.loc[0, "tenure_months"] = -1
    df.loc[1, "plan"] = 6
    df.loc[2, "customer_id"] = "C3"
    df.loc[4, "snapshot_date"] = "not a date"
    df["extra"] = 1
    errors, _ = mlkit.check_contract(df, DATA)
    text = "\n".join(errors)
    assert "tenure_months: 1 values below 0" in text
    assert "plan: values outside [1, 12]" in text
    assert "duplicate rows for key ['customer_id']" in text
    assert "snapshot_date: 1 values are not date" in text
    assert "unexpected columns: ['extra']" in text


def test_inference_mode_does_not_need_the_label():
    df = frame().drop(columns=["churned"]).head(2)
    errors, _ = mlkit.check_contract(df, DATA, mode="inference")
    assert errors == []
    errors, _ = mlkit.check_contract(df, DATA, mode="training")
    assert any("missing column churned" in e for e in errors) and any("only 2 rows" in e for e in errors)


def test_time_split_has_no_leakage():
    df = frame(100)
    split = {"strategy": "time", "time_column": "snapshot_date", "test_fraction": 0.2, "validation_fraction": 0.1}
    train, val, test = mlkit.split_frame(df, split, "churned")
    assert len(train) + len(val) + len(test) == 100
    assert train["snapshot_date"].max() < val["snapshot_date"].min() <= val["snapshot_date"].max() \
        < test["snapshot_date"].min()
    assert abs(len(test) - 20) <= 1


def test_stratified_split_keeps_label_rate():
    df = frame(400, seed=3)
    split = {"strategy": "stratified", "test_fraction": 0.25, "validation_fraction": 0.0, "seed": 1}
    train, val, test = mlkit.split_frame(df, split, "churned")
    assert val.empty and len(test) == 100
    assert abs(test["churned"].mean() - df["churned"].mean()) < 0.02


def test_metrics_on_a_perfect_and_a_random_model():
    y = np.array([0, 0, 1, 1, 0, 1])
    perfect = mlkit.compute_metrics(y, np.array([0.1, 0.2, 0.9, 0.8, 0.3, 0.7]), 0.5,
                                    ["auc", "pr_auc", "accuracy", "f1", "log_loss", "brier", "calibration_error"])
    assert perfect["auc"] == 1.0 and perfect["pr_auc"] == 1.0 and perfect["accuracy"] == 1.0 and perfect["f1"] == 1.0
    assert perfect["_confusion"] == {"tp": 3, "fp": 0, "fn": 0, "tn": 3}
    flat = mlkit.compute_metrics(y, np.full(6, 0.5), 0.5, ["auc", "brier"])
    assert flat["auc"] == 0.5 and flat["brier"] == 0.25
    assert mlkit.compute_metrics(np.zeros(4), np.full(4, 0.3), 0.5, ["auc"])["auc"] is None  # one class


def test_decide_gates_slices_and_champion():
    ev = {"gates": [{"metric": "auc", "min": 0.75}, {"metric": "log_loss", "max": 0.5}],
          "slice_gates": [{"metric": "auc", "max_gap": 0.1, "min_rows": 10}],
          "champion": {"enabled": True, "metric": "auc", "min_improvement": 0.01}}
    good = {"auc": 0.82, "log_loss": 0.4}
    slices = {"plan": {"1": {"rows": 50, "auc": 0.80}, "12": {"rows": 5, "auc": 0.5}}}   # 12 is too small
    assert mlkit.decide(good, slices, ev, None) == ("approve", [])
    decision, reasons = mlkit.decide(good, slices, ev, {"auc": 0.815, "_version": "3"})
    assert decision == "reject" and "does not beat 0.8150 by 0.01" in reasons[0]
    decision, reasons = mlkit.decide({"auc": 0.7, "log_loss": 0.6}, {"plan": {"1": {"rows": 50, "auc": 0.55}}}, ev,
                                     None)
    assert decision == "reject" and len(reasons) == 3


def test_psi_is_small_for_the_same_population_and_large_for_a_shift():
    rng = np.random.RandomState(0)
    base = mlkit.psi_baseline(rng.normal(0, 1, 5000))
    assert mlkit.psi(base, rng.normal(0, 1, 2000)) < 0.05
    assert mlkit.psi(base, rng.normal(1, 1, 2000)) > 0.2
    discrete = mlkit.psi_baseline([1, 1, 12, 12, 24])
    assert discrete["edges"] == [6.5, 18.0] and mlkit.psi(discrete, [1, 12, 24, 1, 12]) < 0.1


def test_permutation_importance_finds_the_signal():
    rng = np.random.RandomState(0)
    X = pd.DataFrame({"signal": rng.normal(size=500), "noise": rng.normal(size=500)})
    y = (X["signal"] > 0).astype(int).to_numpy()
    imp = mlkit.permutation_importance(lambda d: 1 / (1 + np.exp(-3 * d["signal"].to_numpy())), X, y, "auc", 0.5)
    assert list(imp) == ["signal", "noise"] and imp["signal"] > 0.3 and abs(imp["noise"]) < 1e-9


def test_sample_trials_respects_the_space():
    space = {"learning_rate": {"type": "double", "min": 0.001, "max": 0.1, "scale": "log"},
             "hidden_units": {"type": "discrete", "values": [16, 32]},
             "epochs": {"type": "integer", "min": 5, "max": 10}}
    trials = mlkit.sample_trials({"max_trials": 5, "search_space": space},
                                 {"learning_rate": 0.003, "hidden_units": 32, "epochs": 8}, seed=1)
    assert len(trials) == 5 and trials[0] == {"learning_rate": 0.003, "hidden_units": 32, "epochs": 8}
    for t in trials[1:]:
        assert 0.001 <= t["learning_rate"] <= 0.1 and t["hidden_units"] in (16, 32) and 5 <= t["epochs"] <= 10
    assert mlkit.best_trial([{"score": 0.7}, {"score": 0.8}, {"score": None}], "auc", "maximize") == {"score": 0.8}
    with pytest.raises(ValueError):
        mlkit.best_trial([{"score": None}], "auc", "maximize")


def test_slice_keys_are_readable():
    df = pd.DataFrame({"plan": [1.0, 1.0, 12.0, 12.0], "y": [0, 1, 0, 1], "p": [0.2, 0.8, 0.3, 0.6]})
    out = mlkit.slice_metrics(df, "y", "p", ["plan"], 0.5, ["auc"])
    assert set(out["plan"]) == {"1", "12"} and out["plan"]["1"] == {"rows": 2, "auc": 1.0}
