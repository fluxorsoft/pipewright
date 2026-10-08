"""Prepare: split (data.split) and impute features (features.include) with training medians."""

import os
from typing import NamedTuple

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def prepare_data(
    raw_data: dsl.Input[dsl.Dataset],
    ml_config: dict,
    train_data: dsl.Output[dsl.Dataset],
    validation_data: dsl.Output[dsl.Dataset],
    test_data: dsl.Output[dsl.Dataset],
    prep: dsl.Output[dsl.Artifact],
) -> NamedTuple("PrepareOutputs", [("train_rows", int), ("validation_rows", int), ("test_rows", int)]):
    """Medians come from the training split only, so validation and test stay unseen."""
    import json
    import logging
    from collections import namedtuple

    import pandas as pd
    from mlkit import impute, numeric_features, split_frame

    logging.basicConfig(level=logging.INFO)
    data, features = ml_config["data"], ml_config["features"]["include"]
    label = data["label"]["column"]
    df = pd.read_parquet(raw_data.path).dropna(subset=[label]).reset_index(drop=True)
    train, val, test = split_frame(df, data["split"], label)
    medians = numeric_features(train, features).median().to_dict()
    train, val, test = (impute(x, features, medians) for x in (train, val, test))

    for frame, out in ((train, train_data), (val, validation_data), (test, test_data)):
        frame.to_parquet(out.path, index=False)
        out.metadata["rows"] = len(frame)
    split = data["split"]
    info = {"features": features, "label": label, "medians": medians, "strategy": split["strategy"],
            "rows": {"train": len(train), "validation": len(val), "test": len(test)}}
    if split["strategy"] == "time":
        tc = split["time_column"]
        info["periods"] = {name: [str(frame[tc].min()), str(frame[tc].max())] if len(frame) else None
                           for name, frame in (("train", train), ("validation", val), ("test", test))}
    with open(prep.path, "w") as f:
        json.dump(info, f, indent=2, default=str)
    logging.info("split %s: %s", split["strategy"], info["rows"])
    return namedtuple("PrepareOutputs", ["train_rows", "validation_rows", "test_rows"])(len(train), len(val), len(test))
