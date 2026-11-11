"""Attribution drift (Pipewright 1.4.3, FR-98 stage 4): which features drive the champion's answers on the online
window, against what drove them on training data (training_stats.json's attribution_baseline, recorded when
monitoring.online.attribution is on). Pipewright's own permutation Shapley values (numpy only); label-free, so it works
before outcomes arrive. A move past the threshold sends monitoring.attribution_drift with the top movers; in 1.4.3 it
does not open a retrain case on its own."""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def check_attribution(
    run_id: str,
    scored: dsl.Input[dsl.Dataset],
    model: dsl.Input[dsl.Model],
    model_version: str,
    ml_config: dict,
) -> str:
    """Returns a one-line JSON summary (or why nothing was measured)."""
    import json
    import logging

    import pandas as pd
    from churn_net import load, predict
    from pipewright_runtime import attribution_drift, attribution_shares, emit_event

    logging.basicConfig(level=logging.INFO)
    cfg = ((ml_config.get("monitoring") or {}).get("online") or {}).get("attribution") or {}
    if not cfg.get("enabled"):
        return json.dumps({"attribution": "off"})
    stats_path = os.path.join(model.path, "training_stats.json")
    stats = json.load(open(stats_path)) if os.path.exists(stats_path) else {}
    if not stats.get("attribution_baseline"):
        return json.dumps({"attribution": "no baseline: the champion was trained before attribution was on"})
    features = stats["features"]
    df = pd.read_parquet(scored.path)
    rows = df[features].dropna().sample(n=min(int(cfg["sample_rows"]), len(df)), random_state=42) \
        .to_numpy("float32") if len(df) else None
    if rows is None or not len(rows):
        return json.dumps({"attribution": "no rows"})
    net = load(model.path)
    shares = attribution_shares(lambda z: predict(net, z), rows, stats["attribution_background"], features)
    tvd, moved = attribution_drift(stats["attribution_baseline"], shares)
    summary = {"model_version": model_version, "rows": len(rows), "distance": tvd, "threshold": cfg["threshold"],
               "drift": tvd > cfg["threshold"], "top_movers": moved, "shares": shares}
    if summary["drift"]:
        emit_event("monitoring.attribution_drift", ml_config, run_id, **summary)
    logging.info("attribution: %s", json.dumps(summary))
    return json.dumps(summary)
