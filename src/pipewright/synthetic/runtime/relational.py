"""Relational and sequential synthetic data: an entity (parent) table and child tables keyed to it (FR-49).

Standalone (no Pipewright imports): the synthetic images use it, the tests too. A child is either a feature table of
data.features_from (history per entity, joined point in time for training) or a sequence of synthetic.sequences
(events per entity, in order). Every function takes pandas DataFrames.

    split          hold out whole entities, with their children (never half an entity)
    pit_join       the point-in-time join training uses, on DataFrames (the newest child row at or before the
                   entity's cut-off, per entity and cut-off; as pipewright_runtime.features_query in BigQuery)
    integrity      keys unique, no orphan child rows, rows per entity, and history before the cut-off
    remap_keys     new keys from the key rule, the same in the parent and every child
"""

from __future__ import annotations


def save(directory: str, parent, children: dict, spec: dict) -> None:
    """A folder of tables (parent.parquet, <child>.parquet) and spec.json: how a step hands tables to the next."""
    import json
    import os

    os.makedirs(directory, exist_ok=True)
    parent.to_parquet(os.path.join(directory, "parent.parquet"), index=False)
    for name, (df, _key) in children.items():
        df.to_parquet(os.path.join(directory, f"{name}.parquet"), index=False)
    with open(os.path.join(directory, "spec.json"), "w") as f:
        json.dump(spec, f, indent=2, default=str)


def load(directory: str):
    """(parent, children name -> (DataFrame, key), spec) from a folder written by save."""
    import json
    import os

    import pandas as pd

    with open(os.path.join(directory, "spec.json")) as f:
        spec = json.load(f)
    parent = pd.read_parquet(os.path.join(directory, "parent.parquet"))
    children = {c["name"]: (pd.read_parquet(os.path.join(directory, f"{c['name']}.parquet")), c["key"])
                for c in spec["children"]}
    return parent, children, spec


def flat(parent, children: dict, spec: dict):
    """The flat table the gates are measured on: the point-in-time join of the feature children (as training
    reads it), or the entity table alone when there are only sequences."""
    sources = [{"name": c["name"], "key": c["key"], "time_column": c["order_by"], "features": c["features"]}
               for c in spec["children"] if c["kind"] == "features"]
    if not sources:
        return parent.copy()
    return pit_join(parent, {n: d for n, (d, _k) in children.items()}, spec["key"], spec["time_column"], sources)


def split(parent, children: dict, key: str, fraction: float, seed: int):
    """(train parent, train children, holdout parent, holdout children): entities held out with all their rows."""
    held_keys = set(parent[key].drop_duplicates().sample(frac=fraction, random_state=seed))
    keep = ~parent[key].isin(held_keys)
    train = parent[keep].reset_index(drop=True)
    held = parent[~keep].reset_index(drop=True)
    tr_children = {n: c[~c["__key"].isin(held_keys)].drop(columns="__key").reset_index(drop=True)
                   for n, c in ((n, c.assign(__key=c[ch_key])) for n, (c, ch_key) in children.items())}
    ho_children = {n: c[c["__key"].isin(held_keys)].drop(columns="__key").reset_index(drop=True)
                   for n, c in ((n, c.assign(__key=c[ch_key])) for n, (c, ch_key) in children.items())}
    return train, tr_children, held, ho_children


def _naive(series):
    import pandas as pd

    # ISO 8601 in any form (dates, date-times, with or without zone): a format inferred from the first value would
    # turn differently written values into NaT, and they would silently drop out of the join.
    s = series if str(series.dtype).startswith("datetime64") else pd.to_datetime(series, utc=True, errors="coerce",
                                                                                  format="ISO8601")
    s = pd.to_datetime(s, utc=True)
    return s.dt.tz_localize(None)


def pit_join(parent, children: dict, key: str, time_column: str, sources: list[dict]):
    """The flat training table from synthetic (or real) tables: for each parent row, the newest row of each feature
    child dated at or before its cut-off (time_column, a date meaning the start of that day, UTC).

    sources: [{name, key, time_column, features: {output name: column in the child}}]."""
    import pandas as pd

    out = parent.copy()
    cut = _naive(out[time_column])
    for src in sources:
        child = children[src["name"]]
        cols = list(src["features"].values())
        c = child[[src["key"], src["time_column"], *cols]].copy()
        c["__t"] = _naive(c[src["time_column"]])
        left = pd.DataFrame({"__row": range(len(out)), "__k": out[key].values, "__cut": cut.values})
        m = left.merge(c, left_on="__k", right_on=src["key"], how="left")
        m = m[m["__t"] <= m["__cut"]]
        m = m.sort_values("__t").groupby("__row", as_index=True).last()
        for name, col in src["features"].items():
            out[name] = m[col].reindex(range(len(out))).values
    return out


def integrity(parent, children: dict, key: str, time_column: str | None, real_parent=None,
              real_children: dict | None = None, specs: dict | None = None) -> tuple[dict, list[str]]:
    """Metrics and errors. Errors (a duplicate key, an orphan child row) fail the candidate; the metrics compare
    rows per entity and, for feature children, the share of history before the cut-off with the real tables."""
    import numpy as np

    metrics, errors = {}, []
    dup = int(parent[key].duplicated().sum())
    if dup:
        errors.append(f"{dup} duplicate keys in the entity table")
    keys = set(parent[key])
    for name, child in children.items():
        spec = (specs or {}).get(name, {})
        ck = spec.get("key", key)
        orphans = int((~child[ck].isin(keys)).sum())
        if orphans:
            errors.append(f"{orphans} rows of {name} refer to no entity")
        per = child.groupby(ck).size().reindex(list(keys), fill_value=0)
        metrics[f"rows_per_entity[{name}]"] = round(float(per.mean()), 3)
        if real_children is not None and name in real_children and real_parent is not None:
            real_per = real_children[name].groupby(ck).size().reindex(list(set(real_parent[key])), fill_value=0)
            metrics[f"rows_per_entity_real[{name}]"] = round(float(real_per.mean()), 3)
        tc = spec.get("time_column")
        if time_column and tc and len(child):
            cut = parent.set_index(key)[time_column]
            before = _naive(child[tc]).values <= _naive(child[ck].map(cut)).values
            metrics[f"share_before_cutoff[{name}]"] = round(float(np.mean(before)), 3)
            if real_children is not None and name in real_children and real_parent is not None:
                rc, rcut = real_children[name], real_parent.set_index(key)[time_column]
                rb = _naive(rc[tc]).values <= _naive(rc[ck].map(rcut)).values
                metrics[f"share_before_cutoff_real[{name}]"] = round(float(np.mean(rb)), 3)
    return metrics, errors


def remap_keys(parent, children: dict, key: str, rule: dict, seed: int = 42):
    """New keys from the key rule (default prefix SYN-), one per entity, the same in every child table. Returns
    (parent, children) with the key column replaced; children maps name -> (DataFrame, its key column)."""
    import random

    from .rules import column_values

    old = list(dict.fromkeys(parent[key].tolist()))
    column = {"name": key, "type": "integer" if rule.get("type") == "integer" else "string"}
    new = column_values(key, {"rule": "key", **rule}, column, len(old), random.Random(seed))
    mapping = dict(zip(old, new, strict=True))
    parent = parent.assign(**{key: parent[key].map(mapping)})
    out = {}
    for name, (child, ck) in children.items():
        out[name] = child.assign(**{ck: child[ck].map(mapping)})
    return parent, out
