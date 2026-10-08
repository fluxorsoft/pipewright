"""Plain ML helpers used by the components: no KFP, no Google clients, so they are unit-tested locally
(tests/test_mlkit.py) and run unchanged inside the component image.

Every function takes the relevant part of `ml_config` (the resolved ML sections of usecase.yaml).
"""

from __future__ import annotations

import math
import random

import numpy as np
import pandas as pd

LOWER_IS_BETTER = {"log_loss", "brier", "calibration_error"}
EPS = 1e-4


# ------------------------------------------------------------------ data contract --

def _is_type(series: pd.Series, kind: str) -> pd.Series:
    """True where a non-null value has the contract type."""
    values = series.dropna()
    if kind in ("integer", "float"):
        num = pd.to_numeric(values, errors="coerce")
        ok = num.notna()
        if kind == "integer":
            ok &= (num.fillna(0) % 1 == 0)
        return ok
    if kind == "boolean":
        return values.isin([True, False, 0, 1, "true", "false", "True", "False"])
    if kind in ("date", "timestamp"):
        return pd.to_datetime(values, errors="coerce").notna()
    return pd.Series(True, index=values.index)          # string: anything


def check_contract(df: pd.DataFrame, data_cfg: dict, mode: str = "training",
                   ignore_columns: tuple = ()) -> tuple[list[str], dict]:
    """Returns (errors, summary). mode=inference does not need the label or min_rows."""
    contract = data_cfg.get("contract") or {}
    label = (data_cfg.get("label") or {}).get("column")
    columns = {c["name"]: c for c in contract.get("columns", [])}
    errors: list[str] = []

    for name, col in columns.items():
        if name == label and mode == "inference":
            continue
        if name not in df.columns and col.get("required", True):
            errors.append(f"missing column {name}")
    extra = sorted(set(df.columns) - set(columns) - set(ignore_columns))
    if extra and contract.get("allow_extra_columns") is False:
        errors.append(f"unexpected columns: {extra}")
    if mode == "training" and len(df) < contract.get("min_rows", 0):
        errors.append(f"only {len(df)} rows, need at least {contract['min_rows']}")

    nulls = {}
    for name, col in columns.items():
        if name not in df.columns or (name == label and mode == "inference"):
            continue
        s = df[name]
        nulls[name] = float(s.isna().mean()) if len(s) else 0.0
        limit = col.get("max_null_fraction", contract.get("max_null_fraction", 1.0))
        if nulls[name] > limit:
            errors.append(f"{name}: {nulls[name]:.1%} nulls exceeds {limit:.1%}")
        bad = int((~_is_type(s, col["type"])).sum())
        if bad:
            errors.append(f"{name}: {bad} values are not {col['type']}")
            continue
        values = s.dropna()
        if col["type"] in ("integer", "float"):
            values = pd.to_numeric(values)
        if "min" in col and (values < col["min"]).any():
            errors.append(f"{name}: {int((values < col['min']).sum())} values below {col['min']}")
        if "max" in col and (values > col["max"]).any():
            errors.append(f"{name}: {int((values > col['max']).sum())} values above {col['max']}")
        if "allowed" in col and not values.isin(col["allowed"]).all():
            errors.append(f"{name}: values outside {col['allowed']}: "
                          f"{sorted(map(str, set(values[~values.isin(col['allowed'])])))[:5]}")

    unique = [c for c in contract.get("unique", []) if c in df.columns]
    if unique and df.duplicated(subset=unique).any():
        errors.append(f"{int(df.duplicated(subset=unique).sum())} duplicate rows for key {unique}")

    if label and label in df.columns and mode == "training":
        allowed = (data_cfg.get("label") or {}).get("values")
        if allowed and not df[label].dropna().isin(allowed).all():
            errors.append(f"label {label} has values outside {allowed}")
    summary = {"rows": len(df), "null_fractions": nulls, "errors": errors, "passed": not errors}
    if label and label in df.columns and mode == "training":
        summary["label_rate"] = float(pd.to_numeric(df[label], errors="coerce").mean())
    return errors, summary


# ------------------------------------------------------------------------ split --

def split_frame(df: pd.DataFrame, split: dict, label: str | None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """(train, validation, test). time: test is the latest period, validation the period before it; no row of
    a time value is split across sets, so nothing from the future leaks into training."""
    test_f, val_f, seed = split["test_fraction"], split.get("validation_fraction") or 0.0, split.get("seed", 42)
    strategy = split["strategy"]
    if strategy == "time":
        t = pd.to_datetime(df[split["time_column"]])
        times = np.sort(t.unique())
        order = np.sort(t.to_numpy())
        n = len(df)

        def cutoff(fraction: float):
            if fraction <= 0:
                return None
            value = order[max(0, min(n - 1, int(round(n * (1 - fraction)))))]
            later = times[times >= value]
            return later[0] if len(later) else None

        test_from = cutoff(test_f)
        val_from = cutoff(test_f + val_f) if val_f else None
        test = df[t >= test_from] if test_from is not None else df.iloc[0:0]
        val = df[(t >= val_from) & (t < test_from)] if val_from is not None else df.iloc[0:0]
        train = df[t < (val_from if val_from is not None else test_from)]
        if train.empty or test.empty:
            raise ValueError(f"time split left an empty set: {len(train)} train / {len(test)} test rows; "
                             f"the data covers {len(times)} distinct {split['time_column']} values")
        return train, val, test
    rng = np.random.RandomState(seed)
    if strategy == "stratified" and label:
        test_idx, val_idx = [], []
        for _, group in df.groupby(label):
            idx = group.index.to_numpy().copy()
            rng.shuffle(idx)
            n_test, n_val = int(round(len(idx) * test_f)), int(round(len(idx) * val_f))
            test_idx += list(idx[:n_test])
            val_idx += list(idx[n_test:n_test + n_val])
    else:
        idx = df.index.to_numpy().copy()
        rng.shuffle(idx)
        n_test, n_val = int(round(len(idx) * test_f)), int(round(len(idx) * val_f))
        test_idx, val_idx = list(idx[:n_test]), list(idx[n_test:n_test + n_val])
    test, val = df.loc[test_idx], df.loc[val_idx]
    train = df.drop(index=test_idx + val_idx).sample(frac=1.0, random_state=seed)
    return train, val, test


def numeric_features(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    out = df[features].apply(pd.to_numeric, errors="coerce")
    bad = [c for c in features if out[c].isna().all() and df[c].notna().any()]
    if bad:
        raise ValueError(f"features must be numeric for this model: {bad}")
    return out.astype("float64")


def impute(df: pd.DataFrame, features: list[str], medians: dict) -> pd.DataFrame:
    out = df.copy()
    out[features] = numeric_features(out, features).fillna({k: v for k, v in medians.items() if k in features})
    return out


# ---------------------------------------------------------------------- metrics --

def roc_curve(y, p):
    order = np.argsort(-p, kind="mergesort")
    p, y = p[order], y[order]
    distinct = np.r_[np.where(np.diff(p))[0], len(p) - 1]
    tps = np.cumsum(y)[distinct]
    fps = (distinct + 1) - tps
    tpr = np.r_[0.0, tps / max(tps[-1], 1)]
    fpr = np.r_[0.0, fps / max(fps[-1], 1)]
    return fpr, tpr, np.r_[1.0, p[distinct]]


def _auc(y, p):
    if len(set(y.tolist())) < 2:
        return None
    fpr, tpr, _ = roc_curve(y, p)
    return float(np.trapezoid(tpr, fpr))


def _pr_auc(y, p):
    """Average precision."""
    if y.sum() == 0:
        return None
    order = np.argsort(-p, kind="mergesort")
    y = y[order]
    tp = np.cumsum(y)
    precision = tp / np.arange(1, len(y) + 1)
    return float((precision * y).sum() / y.sum())


def calibration(y, p, bins: int = 10) -> tuple[float, list[dict]]:
    """Expected calibration error and the reliability curve (mean predicted vs observed per bin)."""
    edges = np.linspace(0, 1, bins + 1)
    which = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    ece, curve = 0.0, []
    for b in range(bins):
        mask = which == b
        if not mask.any():
            continue
        pred, obs = float(p[mask].mean()), float(y[mask].mean())
        ece += mask.mean() * abs(pred - obs)
        curve.append({"bin": b, "predicted": round(pred, 4), "observed": round(obs, 4), "rows": int(mask.sum())})
    return float(ece), curve


def compute_metrics(y, p, threshold: float, names) -> dict:
    y, p = np.asarray(y, dtype=int), np.asarray(p, dtype=float)
    pred = (p >= threshold).astype(int)
    tp, fp = int(((pred == 1) & (y == 1)).sum()), int(((pred == 1) & (y == 0)).sum())
    fn, tn = int(((pred == 0) & (y == 1)).sum()), int(((pred == 0) & (y == 0)).sum())
    clipped = np.clip(p, 1e-7, 1 - 1e-7)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    every = {
        "auc": lambda: _auc(y, p),
        "pr_auc": lambda: _pr_auc(y, p),
        "accuracy": lambda: (tp + tn) / len(y) if len(y) else None,
        "precision": lambda: precision,
        "recall": lambda: recall,
        "f1": lambda: 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
        "log_loss": lambda: float(-np.mean(y * np.log(clipped) + (1 - y) * np.log(1 - clipped))),
        "brier": lambda: float(np.mean((p - y) ** 2)),
        "calibration_error": lambda: calibration(y, p)[0],
    }
    out = {}
    for n in names:
        v = every[n]()
        out[n] = None if v is None else round(float(v), 6)
    out["_confusion"] = {"tp": tp, "fp": fp, "fn": fn, "tn": tn}
    return out


def slice_metrics(df: pd.DataFrame, y_col: str, p_col: str, slices: list[str], threshold: float, names,
                  min_rows: int = 1) -> dict:
    """{column: {value: {rows, metrics...}}}; slices smaller than min_rows are listed with metrics None."""
    out = {}
    for col in slices:
        out[col] = {}
        for value, group in df.groupby(col):
            entry = {"rows": len(group)}
            if len(group) >= min_rows:
                m = compute_metrics(group[y_col], group[p_col], threshold, names)
                m.pop("_confusion")
                entry.update(m)
            key = int(value) if isinstance(value, float) and value.is_integer() else value
            out[col][str(key)] = entry
    return out


# ------------------------------------------------------------------- decisions --

def better(metric: str, a: float, b: float, margin: float = 0.0) -> bool:
    """Is a better than b by more than margin?"""
    return (b - a > margin) if metric in LOWER_IS_BETTER else (a - b > margin)


def decide(metrics: dict, slices: dict, evaluation: dict, champion: dict | None) -> tuple[str, list[str]]:
    """Returns ("approve" | "reject", reasons). Reasons explain every failed check (and the champion result)."""
    reasons = []
    for gate in evaluation.get("gates", []):
        value = metrics.get(gate["metric"])
        if value is None:
            reasons.append(f"gate {gate['metric']}: not computable on the test set")
            continue
        if "min" in gate and value < gate["min"]:
            reasons.append(f"gate {gate['metric']}={value:.4f} below min {gate['min']}")
        if "max" in gate and value > gate["max"]:
            reasons.append(f"gate {gate['metric']}={value:.4f} above max {gate['max']}")
    for sg in evaluation.get("slice_gates", []):
        m, overall = sg["metric"], metrics.get(sg["metric"])
        if overall is None:
            continue
        for col, values in slices.items():
            for value, entry in values.items():
                v = entry.get(m)
                if v is None or entry["rows"] < sg.get("min_rows", 1):
                    continue
                gap = (v - overall) if m in LOWER_IS_BETTER else (overall - v)
                if gap > sg["max_gap"]:
                    reasons.append(f"slice {col}={value}: {m}={v:.4f} is {gap:.4f} worse than overall "
                                   f"(max gap {sg['max_gap']})")
    failed = bool(reasons)
    champ_cfg = evaluation.get("champion", {})
    if champ_cfg.get("enabled") and champion:
        m, margin = champ_cfg["metric"], champ_cfg.get("min_improvement", 0.0)
        new, old = metrics.get(m), champion.get(m)
        if new is not None and old is not None:
            if better(m, new, old, margin) or (margin == 0 and new == old):
                reasons.append(f"champion: {m} {new:.4f} vs {old:.4f} (champion version "
                               f"{champion.get('_version', '?')})")
            else:
                failed = True
                reasons.append(f"champion: {m} {new:.4f} does not beat {old:.4f} by {margin} "
                               f"(difference {new - old:+.6f}; champion version {champion.get('_version', '?')})")
    return ("reject" if failed else "approve"), reasons


# ------------------------------------------------------------------- explainers --

def permutation_importance(predict, X: pd.DataFrame, y, metric: str, threshold: float, repeats: int = 3,
                           seed: int = 42) -> dict:
    """Drop in `metric` when one feature is shuffled (positive = the feature matters)."""
    rng = np.random.RandomState(seed)
    base = compute_metrics(y, predict(X), threshold, [metric])[metric]
    out = {}
    for col in X.columns:
        drops = []
        for _ in range(repeats):
            shuffled = X.copy()
            shuffled[col] = rng.permutation(shuffled[col].to_numpy())
            value = compute_metrics(y, predict(shuffled), threshold, [metric])[metric]
            drops.append((value - base) if metric in LOWER_IS_BETTER else (base - value))
        out[col] = round(float(np.mean(drops)), 6)
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


# --------------------------------------------------------------------- drift --

def psi_baseline(values, bins: int = 10) -> dict:
    """Bin edges (training quantiles) and the training fraction per bin."""
    values = pd.to_numeric(pd.Series(values), errors="coerce").dropna().to_numpy()
    distinct = np.unique(values)
    if len(distinct) <= bins:                         # discrete: one bin per value
        edges = ((distinct[1:] + distinct[:-1]) / 2).tolist()
    else:
        edges = np.unique(np.quantile(values, np.linspace(0, 1, bins + 1)[1:-1])).tolist()
    return {"edges": edges, "fractions": _fractions(values, edges)}


def _fractions(values, edges) -> list[float]:
    counts = np.bincount(np.searchsorted(np.asarray(edges), values, side="right"), minlength=len(edges) + 1)
    return (counts / max(counts.sum(), 1)).round(6).tolist()


def psi(baseline: dict, values) -> float:
    """Population stability index of `values` against a training baseline. 0.1 = moderate, 0.2+ = large."""
    values = pd.to_numeric(pd.Series(values), errors="coerce").dropna().to_numpy()
    if len(values) == 0:
        return 0.0
    expected = np.clip(np.asarray(baseline["fractions"]), EPS, None)
    actual = np.clip(np.asarray(_fractions(values, baseline["edges"])), EPS, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


# --------------------------------------------------------------------- tuning --

def sample_trials(tuning: dict, defaults: dict, seed: int) -> list[dict]:
    """Random search: max_trials hyperparameter sets (the first is always the defaults)."""
    rng = random.Random(seed)
    trials = [dict(defaults)]
    space = tuning.get("search_space", {})
    for _ in range(max(0, tuning["max_trials"] - 1)):
        trial = dict(defaults)
        for name, dim in space.items():
            kind = dim["type"]
            if kind in ("discrete", "categorical"):
                trial[name] = rng.choice(dim["values"])
            elif dim.get("scale") == "log":
                v = math.exp(rng.uniform(math.log(dim["min"]), math.log(dim["max"])))
                trial[name] = round(v) if kind == "integer" else round(v, 8)
            else:
                v = rng.uniform(dim["min"], dim["max"])
                trial[name] = int(round(v)) if kind == "integer" else round(v, 8)
        trials.append(trial)
    return trials


def best_trial(results: list[dict], metric: str, goal: str) -> dict:
    scored = [r for r in results if r.get("score") is not None]
    if not scored:
        raise ValueError("no tuning trial produced a score")
    pick = min if goal == "minimize" else max
    return pick(scored, key=lambda r: r["score"])
