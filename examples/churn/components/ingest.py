"""Ingest: a run's source -> Dataset artifact. The source is whatever the trigger passed as source_uri:

    gs://bucket/path/file.csv | .parquet     a landing file or any object
    bq://project.dataset.table               a BigQuery table or view (bq://dataset.table: this project)

With data.features_from the source is the entity table and read_source adds the features from the feature
tables, each as it was at the row's cut-off time.

Training also appends the rows to the raw table (outputs.store), so every model has the exact data it saw.
"""

import os

from kfp import dsl

# Pinned by digest at release time (pipewright release).
COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def ingest_data(
    project: str,
    source_uri: str,
    bq_dataset: str,
    run_id: str,
    raw_table: str,
    ml_config: dict,
    raw_data: dsl.Output[dsl.Dataset],
) -> int:
    """Reads the source, keeps a copy in `raw_table` (columns the table knows) and emits a Parquet Dataset.

    With data.snapshot_table the copy is the run's snapshot, so `pipewright run --replay <run_id>` can train on
    exactly these rows again; with data.label_source the label comes from its table, within its window."""
    import logging
    from datetime import datetime, timezone

    from pipewright_runtime import read_source, write_snapshot, write_table

    logging.basicConfig(level=logging.INFO)
    df = read_source(source_uri, project, ml_config, run_id, labels=True)
    logging.info("read %d rows / %d columns from %s", len(df), df.shape[1], source_uri)
    df.to_parquet(raw_data.path, index=False)
    raw_data.metadata.update({"source_uri": source_uri, "rows": len(df), "columns": list(df.columns)})

    if (ml_config.get("data") or {}).get("snapshot_table"):
        where = write_snapshot(df, ml_config, project, bq_dataset, run_id, source_uri)
        logging.info("snapshot of %d rows in %s", len(df), where)
        return len(df)
    rows = df.copy()
    rows["run_id"] = run_id
    rows["source_uri"] = source_uri
    rows["ingested_at"] = datetime.now(timezone.utc)
    # Types come from the table's schema file; the store from outputs.store.
    where = write_table(raw_table, rows, ml_config, project, bq_dataset, run_id)
    logging.info("appended %d rows to %s", len(rows), where)
    return len(df)


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def load_data(project: str, source_uri: str, ml_config: dict, data: dsl.Output[dsl.Dataset], run_id: str = "") -> int:
    """Inference input: reads the source to a Dataset. Nothing is written to the training tables."""
    from pipewright_runtime import read_source

    df = read_source(source_uri, project, ml_config, run_id or None)
    df.to_parquet(data.path, index=False)
    data.metadata.update({"source_uri": source_uri, "rows": len(df)})
    return len(df)
