"""BigQuery ML as a training engine: a use case with a `bqml` section gets the training and scoring pipelines
Pipewright provides. They run on the environment's orchestrator like any other pipeline; their steps submit
BigQuery jobs, so the data and the model stay in BigQuery.

config.py       the `bqml` section: model types per task, options, the team's SQL files, byte limits
steps/sql/      the SQL the steps run, as templates (the team's own SQL comes from its use case folder)
steps/          the pipelines' steps (KFP components on the use case's BQML image)
pipelines.py    the training and scoring pipelines, compiled and released with the use case
"""

from pathlib import Path

HERE = Path(__file__).resolve().parent
STEPS_DIR = HERE / "steps"
REQUIREMENTS = STEPS_DIR / "requirements.txt"
PIPELINES_FILE = HERE / "pipelines.py"
TRAINING_FUNCTION = "bqml_training_pipeline"
SCORING_FUNCTION = "bqml_scoring_pipeline"

# Model types per data.label.task. Phase 1: the supervised types (k-means, PCA, imported models, ARIMA later).
MODEL_TYPES = {
    "binary": ("logistic_reg", "boosted_tree_classifier", "random_forest_classifier"),
    "multiclass": ("logistic_reg", "boosted_tree_classifier", "random_forest_classifier"),
    "regression": ("linear_reg", "boosted_tree_regressor", "random_forest_regressor"),
    "clustering": ("kmeans",),                     # tier 1c (Pipewright 1.1): no label
    "decomposition": ("pca",),
}
UNSUPERVISED = ("clustering", "decomposition")
ALL_MODEL_TYPES = sorted({t for types in MODEL_TYPES.values() for t in types})

# CREATE MODEL options Pipewright sets itself: the label, the split (test rows never reach CREATE MODEL), the
# registry (registered only after gate 1), the key (the dataset's default), explanations (bqml.explain).
RESERVED_OPTIONS = ("model_type", "input_label_cols", "data_split_method", "data_split_col",
                    "data_split_eval_fraction", "data_split_test_fraction", "model_registry", "vertex_ai_model_id",
                    "vertex_ai_model_version_aliases", "kms_key_name", "enable_global_explain")

# A byte limit on every BQML job (maximum_bytes_billed), unless the platform sets policies.bqml.max_bytes_billed.
# Tree models bill far more bytes than linear ones (their Agent Platform training is converted into bytes).
DEFAULT_MAX_BYTES_BILLED = 100 * 1024 ** 3
MIN_BYTES_BILLED = 10 * 1024 ** 2                  # BigQuery bills at least 10 MB a job

# Where the rows of a run go in a team's prepare.sql.
ROWS_PLACEHOLDER = "{rows}"
