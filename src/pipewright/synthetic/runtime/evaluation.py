"""Scores a synthetic table against the real data it learned from, the same way for every engine.

Standalone (no Pipewright imports). Runs in the synthetic evaluation image: pandas, SciPy, scikit-learn and
Anonymeter (BSD) are imported when used. Nothing from the SDV stack: SDMetrics would bring copulas (BUSL-1.1), so
fidelity is computed here with SDMetrics' definitions. The metric names are the gate names in gates.py.

    fidelity        quality score (column shapes and column-pair trends, as SDMetrics defines them), against the
                    training rows
    tstr_drop       AUC of a model trained on real training rows (TRTR) minus one trained on synthetic rows (TSTR),
                    both tested on the real holdout
    exact_copies    share of synthetic rows identical to a training row
    dcr_ratio       median distance from synthetic rows to their nearest training row, over the same for holdout rows
    closer_than_p5  share of synthetic rows nearer to a training row than 95% of holdout rows are (about 5% for
                    independent data)
    singling_out, linkability, inference
                    Anonymeter risks against the holdout as control, upper confidence bounds (the worst secret for
                    inference)
"""

from __future__ import annotations

import warnings


def prepare(df, like=None, dates=(), drop=()):
    """The columns everything is judged on: no keys or personal data, dates as days, text as categories, numbers as
    plain float64 (pandas' nullable types break Anonymeter's numba code)."""
    import pandas as pd

    out = df.drop(columns=[c for c in drop if c in df], errors="ignore").copy()
    for c in dates:
        if c in out:
            out[c] = (pd.to_datetime(out[c], utc=True).dt.tz_localize(None) - pd.Timestamp("1970-01-01")).dt.days
    ref = like if like is not None else out
    for c in ref.columns:
        if c not in out:
            out[c] = None
        if ref[c].dtype == object or str(ref[c].dtype) in ("category", "bool", "boolean"):
            out[c] = out[c].astype(str).astype("category")
        else:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    return out[list(ref.columns)]


def _categorical(df, c, label) -> bool:
    return str(df[c].dtype) == "category" or c == label or df[c].nunique() <= 10


def _frequencies(values) -> dict:
    counts = values.astype(str).value_counts(normalize=True)
    return counts.to_dict()


def _tv_complement(real, syn) -> float:
    """1 minus the total variation distance between two categorical distributions."""
    p, q = _frequencies(real), _frequencies(syn)
    return 1 - 0.5 * sum(abs(p.get(k, 0) - q.get(k, 0)) for k in set(p) | set(q))


def _ks_complement(real, syn) -> float:
    """1 minus the two-sample Kolmogorov-Smirnov statistic of two numeric distributions."""
    from scipy.stats import ks_2samp

    a, b = real.dropna(), syn.dropna()
    if a.empty or b.empty:
        return 0.0
    return 1 - float(ks_2samp(a, b).statistic)


def _bins(values):
    """A numeric column in 10 equal-width bins of its own range, as SDMetrics' ColumnPairTrends does it (each table
    is binned by its own range: kept, so the gates mean what they meant in the spike)."""
    import numpy as np
    import pandas as pd

    clean = values.dropna()
    if clean.empty:
        return values.astype(str)
    return pd.Series(np.digitize(values, bins=np.histogram_bin_edges(clean)), index=values.index).astype(str)


def fidelity(train, syn, label: str | None) -> dict:
    """The quality score's two parts, defined as in SDMetrics' QualityReport (computed here, so evaluation needs no
    package from the SDV stack): column shapes (KS complement for numbers, TV complement for categories) and
    column-pair trends (correlation similarity for two numbers, contingency similarity otherwise, numbers in 10
    bins). Each from 0 to 1; fidelity is their mean. Checked against SDMetrics 0.32 on the spike's census outputs:
    within 0.005 for MOSTLY AI (with and without differential privacy, temperature 1.6), 0.036 higher for SDV's
    Gaussian copula."""
    import itertools

    cols = list(train.columns)
    cat = {c: _categorical(train, c, label) for c in cols}
    shapes = [(_tv_complement(train[c], syn[c]) if cat[c] else _ks_complement(train[c], syn[c])) for c in cols]
    pairs = []
    for a, b in itertools.combinations(cols, 2):
        if not cat[a] and not cat[b]:
            r_real, r_syn = train[a].corr(train[b]), syn[a].corr(syn[b])
            if r_real == r_real and r_syn == r_syn:                       # both defined (not NaN)
                pairs.append(1 - abs(r_real - r_syn) / 2)
            continue
        ra, sa = (train[a].astype(str), syn[a].astype(str)) if cat[a] else (_bins(train[a]), _bins(syn[a]))
        rb, sb = (train[b].astype(str), syn[b].astype(str)) if cat[b] else (_bins(train[b]), _bins(syn[b]))
        pairs.append(_tv_complement(ra + "\x1f" + rb, sa + "\x1f" + sb))
    shape = sum(shapes) / len(shapes) if shapes else 0.0
    pair = sum(pairs) / len(pairs) if pairs else shape
    return {"fidelity": round((shape + pair) / 2, 4), "column_shapes": round(shape, 4), "column_pairs": round(pair, 4)}


def utility(train, holdout, syn, label: str, positive) -> dict:
    """TSTR against TRTR with one classifier for every engine (histogram gradient boosting)."""
    import pandas as pd
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import roc_auc_score

    cats = {c: sorted(set(train[c].astype(str)) | set(holdout[c].astype(str)))
            for c in train.columns if str(train[c].dtype) == "category" and c != label}

    def xy(df):
        x = df.drop(columns=[label]).copy()
        for c, levels in cats.items():
            x[c] = pd.Categorical(x[c].astype(str), categories=levels)
        y = (df[label].astype(str) == str(positive)).astype(int) if positive is not None else df[label].astype(int)
        return x, y

    def auc(fit):
        x, y = xy(fit)
        if y.nunique() < 2:
            return None
        model = HistGradientBoostingClassifier(random_state=0, categorical_features="from_dtype").fit(x, y)
        xh, yh = xy(holdout)
        return round(float(roc_auc_score(yh, model.predict_proba(xh)[:, 1])), 4)

    trtr, tstr = auc(train), auc(syn)
    return {"trtr_auc": trtr, "tstr_auc": tstr,
            "tstr_drop": None if tstr is None or trtr is None else round(trtr - tstr, 4)}


def _encode(frames, fit):
    import numpy as np
    import pandas as pd

    num = [c for c in fit.columns if str(fit[c].dtype) != "category"]
    cat = [c for c in fit.columns if str(fit[c].dtype) == "category"]
    lo, hi = fit[num].min(), fit[num].max()
    scale = (hi - lo).replace(0, 1)
    out = []
    for f in frames:
        parts = [((f[num] - lo) / scale).fillna(0).to_numpy(dtype=float)]
        for c in cat:
            levels = list(fit[c].cat.categories)
            parts.append(pd.get_dummies(pd.Categorical(f[c].astype(str), categories=levels)).to_numpy(dtype=float))
        out.append(np.hstack(parts))
    return out


def distances(train, holdout, syn) -> dict:
    import numpy as np
    from sklearn.neighbors import NearestNeighbors

    def key(d):
        return d.astype(str).agg("|".join, axis=1)

    exact = float(key(syn).isin(set(key(train))).mean())
    n = min(len(syn), len(holdout))
    s = syn.sample(n=n, random_state=0) if len(syn) > n else syn
    xt, xs, xh = _encode([train, s, holdout], train)
    nn = NearestNeighbors(n_neighbors=1).fit(xt)
    d_syn, d_hold = nn.kneighbors(xs)[0][:, 0], nn.kneighbors(xh)[0][:, 0]
    return {"exact_copies": round(exact, 4),
            "dcr_ratio": round(float(np.median(d_syn) / max(np.median(d_hold), 1e-9)), 4),
            "closer_than_p5": round(float((d_syn < np.percentile(d_hold, 5)).mean()), 4)}


def attacks(train, holdout, syn, secrets: list[str], n_attacks: int) -> dict:
    """Anonymeter, upper confidence bounds. A failed attack is recorded and fails its gate (not measured)."""
    from anonymeter.evaluators import InferenceEvaluator, LinkabilityEvaluator, SinglingOutEvaluator

    def plain(d):
        d = d.copy()
        for c in d.columns:
            if str(d[c].dtype) == "category":
                d[c] = d[c].astype(str)
        return d

    ori, syn_, ctl = plain(train), plain(syn), plain(holdout)
    n = min(n_attacks, len(ctl))
    out, detail = {}, {}

    def record(name, fn):
        try:
            risk = fn().risk()
            out[name] = round(float(risk.ci[1]), 4)
            detail[name] = {"value": round(float(risk.value), 4), "ci": [round(float(x), 4) for x in risk.ci]}
        except Exception as e:  # noqa: BLE001
            detail[name] = {"error": f"{type(e).__name__}: {e}"[:300]}

    record("singling_out", lambda: SinglingOutEvaluator(ori=ori, syn=syn_, control=ctl, n_attacks=n, n_cols=3)
           .evaluate(mode="multivariate"))
    cols = list(ori.columns)
    half = len(cols) // 2
    record("linkability", lambda: LinkabilityEvaluator(ori=ori, syn=syn_, control=ctl, n_attacks=n,
                                                       aux_cols=(cols[:half], cols[half:]), n_neighbors=10)
           .evaluate(n_jobs=1))
    worst = None
    for secret in secrets:
        aux = [c for c in cols if c != secret]
        key = f"inference[{secret}]"
        record(key, lambda s=secret, a=aux: InferenceEvaluator(ori=ori, syn=syn_, control=ctl, aux_cols=a, secret=s,
                                                                n_attacks=n).evaluate(n_jobs=1))
        if key in out:
            value = out.pop(key)
            worst = value if worst is None else max(worst, value)
    if worst is not None:
        out["inference"] = worst
    return {**out, "attacks": detail}


def evaluate(train, holdout, syn, label: str | None, positive=None, dates=(), drop=(), secrets=(),
             n_attacks: int = 500) -> dict:
    """Every metric for one candidate. `drop`: columns not judged (keys, personal data); `secrets`: columns an
    inference attack tries to recover (the label is always one)."""
    warnings.filterwarnings("ignore")
    tr = prepare(train, dates=dates, drop=drop)
    ho = prepare(holdout, like=tr, dates=dates, drop=drop)
    sy = prepare(syn, like=tr, dates=dates, drop=drop)
    metrics = {"rows": len(syn)}
    metrics.update(fidelity(tr, sy, label))
    if label:
        metrics.update(utility(tr, ho, sy, label, positive))
    metrics.update(distances(tr, ho, sy))
    targets = list(dict.fromkeys([*([label] if label else []), *secrets]))
    if n_attacks:
        metrics.update(attacks(tr, ho, sy, [s for s in targets if s in tr.columns], n_attacks))
    return metrics
