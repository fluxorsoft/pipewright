"""Synthetic data generation (FR-49): a governed pipeline Pipewright provides when a use case enables it.

config.py           the `synthetic` section: defaults, cross-checks, the platform's policy
runtime/            gates, column rules, engines, evaluation (standalone: also copied into the synthetic images)
synthetic_steps.py  the pipeline's steps (KFP components on the synthetic images)
pipeline.py         the `synthetic` pipeline, compiled and released with the use case's own pipelines
"""

from pathlib import Path

PIPELINE = "synthetic"
TASKS = {"prepare-synthetic", "generate-synthetic", "evaluate-synthetic", "select-synthetic", "publish-synthetic"}
PIPELINE_FILE = Path(__file__).resolve().parent / "pipeline.py"
PIPELINE_FUNCTION = "synthetic_pipeline"
# The synthetic pipeline runs on two images Pipewright builds for the use case: the engine image (generation:
# MOSTLY AI and PyTorch, plus synthetic.requirements) and the evaluation image (every other step: Anonymeter,
# scikit-learn). They are separate because their packages conflict (NumPy 2 against below 2). Components read the
# images from these variables at compile time (the use case's own components read COMPONENT_IMAGE).
IMAGE_ENV = "PIPEWRIGHT_SYNTHETIC_IMAGE"
EVALUATION_IMAGE_ENV = "PIPEWRIGHT_SYNTHETIC_EVALUATION_IMAGE"
