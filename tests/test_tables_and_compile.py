import importlib.util
from pathlib import Path

import pytest
import yaml

from pipewright.build.compile import TemplateInfo, check_parameters, compile_usecase
from pipewright.config import Table, load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.tables.plan import plan_table

FIELDS = [
    {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "score", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "scored_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
PART = {"field": "scored_at", "type": "DAY"}


def live(fields=FIELDS, partition=PART, cluster=None, expiration_ms=None):
    body = {"schema": {"fields": fields}}
    if partition:
        body["timePartitioning"] = {"type": partition["type"], "field": partition["field"]}
        if expiration_ms:
            body["timePartitioning"]["expirationMs"] = expiration_ms
    if cluster:
        body["clustering"] = {"fields": cluster}
    return body


def table(fields=FIELDS, partition=PART, cluster=()):
    return Table("predictions", Path("predictions.json"), fields, partition, list(cluster))


def test_create_when_missing():
    plan = plan_table(table(), None, retention_days=30)
    assert plan.action == "create"
    assert "3 columns" in plan.changes[0] and "retention 30 days" in plan.changes[0]


def test_no_change_and_type_aliases():
    aliased = [dict(f, type={"FLOAT": "FLOAT64"}.get(f["type"], f["type"])) for f in FIELDS]
    assert plan_table(table(), live(aliased)).action == "none"


def test_additive_changes_are_allowed():
    want = FIELDS + [{"name": "model_version", "type": "STRING"}]
    have = [dict(FIELDS[0]), dict(FIELDS[1], mode="REQUIRED"), dict(FIELDS[2])]
    want[1] = dict(FIELDS[1], mode="NULLABLE")
    plan = plan_table(table(want, cluster=["customer_id"]), live(have))
    assert plan.action == "update"
    assert "add column model_version (STRING, NULLABLE)" in plan.changes
    assert "relax column score REQUIRED -> NULLABLE" in plan.changes
    assert any(c.startswith("clustering") for c in plan.changes)


def test_breaking_changes_are_blocked_with_advice():
    want = [FIELDS[0], dict(FIELDS[1], type="STRING"), FIELDS[2], {"name": "new_req", "type": "INT64",
                                                                   "mode": "REQUIRED"}]
    plan = plan_table(table(want), live(FIELDS + [{"name": "old", "type": "STRING"}]))
    assert plan.action == "blocked"
    text = " | ".join(plan.blockers)
    assert "score changes type FLOAT -> STRING" in text
    assert "new column new_req is REQUIRED" in text
    assert "column old exists in BigQuery but not in the schema file" in text
    assert plan.blockers[-1].startswith("create a new table instead (for example predictions_v2)")


def test_partition_change_is_blocked():
    plan = plan_table(table(partition={"field": "scored_at", "type": "HOUR"}), live())
    assert plan.action == "blocked" and "partitioning changes" in plan.blockers[0]


def test_retention_update():
    plan = plan_table(table(), live(), retention_days=10)
    assert plan.action == "update" and "partition expiration -> 10 days" in plan.changes


# ---------------------------------------------------------------- compile --

def info(params):
    return TemplateInfo("p", params, {"img"}, Path("x.yaml"))


def test_parameter_checks(repo):
    path = repo.write()
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    params = {name: {"type": "STRING", "default": None, "optional": False}
              for name in ("project", "region", "source_uri", "run_id")}
    params["auc_threshold"] = {"type": "NUMBER_DOUBLE", "default": 0.7, "optional": True}
    params["epochs"] = {"type": "NUMBER_INTEGER", "default": 10, "optional": True}
    params["seed"] = {"type": "NUMBER_INTEGER", "default": 42, "optional": True}
    problems, notes = check_parameters(uc, info(params))
    assert problems == [] and notes == ["seed uses the pipeline default 42"]

    del params["run_id"]
    params["secret"] = {"type": "STRING", "default": None, "optional": False}
    uc.parameters["epochz"] = 3
    problems, _ = check_parameters(uc, info(params))
    assert "template has no parameter 'run_id'; the launcher always passes it" in problems
    assert "parameters.epochz: the pipeline has no such parameter" in problems
    assert "template parameter 'secret' has no default and nothing supplies it" in problems


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_compile_real_kfp_pipeline(repo):
    path = repo.write()
    (path.parent / "pipeline.py").write_text(
        "from kfp import dsl\n\n"
        "@dsl.component(base_image='python:3.11-slim')\n"
        "def hello(project: str) -> str:\n    return project\n\n"
        "@dsl.pipeline(name='churn-training')\n"
        "def churn_training_pipeline(project: str, region: str, source_uri: str, run_id: str,\n"
        "                            auc_threshold: float = 0.7, epochs: int = 10):\n"
        "    hello(project=project)\n",
        encoding="utf-8",
    )
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    result = compile_usecase(uc)
    assert result.name == "churn-training"
    assert {"project", "region", "source_uri", "run_id", "auc_threshold", "epochs"} == set(result.parameters)
    assert result.parameters["epochs"]["default"] == 10
    assert check_parameters(uc, result) == ([], [])


def test_retention_ignored_without_partition():
    plan = plan_table(table(partition=None), live(partition=None), retention_days=30)
    assert plan.action == "none"
    assert plan.notes == ["retention_days ignored: it only applies to partitioned tables"]


def test_create_and_patch_bodies():
    from pipewright.tables.plan import patch_body, table_resource

    body = table_resource(table(cluster=["customer_id"]), "p", "d", 7, {"pipewright-usecase": "churn"})
    assert body["tableReference"] == {"projectId": "p", "datasetId": "d", "tableId": "predictions"}
    assert body["timePartitioning"] == {"type": "DAY", "field": "scored_at", "expirationMs": str(7 * 86_400_000)}
    assert body["clustering"] == {"fields": ["customer_id"]} and body["labels"]["pipewright-usecase"] == "churn"
    assert patch_body(table(), None)["clustering"] is None


TWO_PIPELINES = '''from kfp import dsl


@dsl.component(base_image="python:3.11-slim")
def train_model(project: str, epochs: int) -> str:
    return project


@dsl.component(base_image="python:3.11-slim")
def score(project: str, ml_config: dict) -> str:
    return project


@dsl.pipeline(name="churn-training")
def churn_training_pipeline(project: str, region: str, source_uri: str, run_id: str, epochs: int = 10):
    with dsl.ParallelFor([1, 2]) as trial:
        train_model(project=project, epochs=trial)


@dsl.pipeline(name="churn-inference")
def churn_inference_pipeline(project: str, region: str, source_uri: str, run_id: str, ml_config: dict = {}):
    score(project=project, ml_config=ml_config)
'''


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_compile_all_pipelines_with_steps_and_ml_config(repo):
    from pipewright.build.compile import compile_all, static_parameters

    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"),
                         "inference": {"file": "pipeline.py", "function": "churn_inference_pipeline"}}
    spec["parameters"] = {"epochs": 3}
    spec["environments"]["dev"]["parameters"] = {"epochs": 2}
    spec["resources_per_step"] = {"train-model": {"cpu": 4, "memory": "8G"}}
    spec["steps"] = {"train-model": {"retries": 2, "cache": False, "gpu": {"type": "nvidia_l4", "count": 1}},
                     "score": {"memory": "512M"}}
    path = repo.write()
    (path.parent / "pipeline.py").write_text(TWO_PIPELINES, encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    infos = compile_all(uc, out_dir=repo.root / "build")
    assert set(infos) == {"training", "inference"}
    assert check_parameters(uc, infos)[0] == []
    assert static_parameters(uc, infos["training"]) == {"epochs": 2}
    assert static_parameters(uc, infos["inference"])["ml_config"]["usecase"] == "churn"

    doc = yaml.safe_load(infos["training"].spec_path.read_text(encoding="utf-8"))
    res = doc["deploymentSpec"]["executors"]["exec-train-model"]["container"]["resources"]
    assert res["resourceCpuLimit"] == "4" and res["memoryLimit"] == 8.0
    assert res["accelerator"]["resourceType"] == "NVIDIA_L4"
    loop = next(c for n, c in doc["components"].items() if n.startswith("comp-for-loop"))
    task = loop["dag"]["tasks"]["train-model"]
    assert task["retryPolicy"]["maxRetryCount"] == 2 and task["cachingOptions"] == {}
    inference = yaml.safe_load(infos["inference"].spec_path.read_text(encoding="utf-8"))
    assert inference["deploymentSpec"]["executors"]["exec-score"]["container"]["resources"]["memoryLimit"] == 0.5

    spec["steps"]["evaluate"] = {"cpu": 2}
    path = repo.write()
    (path.parent / "pipeline.py").write_text(TWO_PIPELINES, encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    with pytest.raises(ConfigError, match="steps: no task named evaluate"):
        compile_all(uc)


def test_steps_without_a_size_get_no_resources_block():
    """Agent Platform runs a step with any resources block (even empty) on its sized-step backend, where the metadata
    server issues no ID tokens: only cpu, memory or a GPU may add one."""
    from pipewright.build.spec_patch import apply_steps

    def spec():
        return {"root": {"dag": {"tasks": {"tune": {"componentRef": {"name": "comp-tune"},
                                                    "taskInfo": {"name": "tune"}}}}},
                "components": {"comp-tune": {"executorLabel": "exec-tune"}},
                "deploymentSpec": {"executors": {"exec-tune": {"container": {"image": "i"}}}}}

    s = spec()
    apply_steps(s, {"tune": {"retries": 1, "cache": False, "ray": {"workers": 1}}})
    assert "resources" not in s["deploymentSpec"]["executors"]["exec-tune"]["container"]
    assert s["root"]["dag"]["tasks"]["tune"]["retryPolicy"]["maxRetryCount"] == 1
    s = spec()
    apply_steps(s, {"tune": {"cpu": 2}})
    assert s["deploymentSpec"]["executors"]["exec-tune"]["container"]["resources"]["cpuLimit"] == 2.0
