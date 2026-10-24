"""BigQuery ML use cases (spec.bqml, Enterprise docs/BQML.md decisions 1 to 8): the configuration."""
import importlib.util
import json

import pytest

from pipewright import bqml
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from tests.test_config_ml import ml_repo
from tests.test_config_ml import resolve as resolve_container

SCORES = [{"name": n, "type": t} for n, t in (("customer_id", "STRING"), ("predicted_label", "STRING"),
                                               ("score", "FLOAT64"), ("model_version_id", "STRING"),
                                               ("run_id", "STRING"), ("scored_at", "TIMESTAMP"))]


def bqml_repo(repo, **section):
    ml_repo(repo, inference={"output_table": "scores"})
    spec = repo.usecase["spec"]
    for key in ("pipeline", "components", "framework", "resources_per_step"):
        spec.pop(key, None)
    spec["triggers"].pop("landing", None)
    spec["outputs"]["bigquery"]["tables"]["scores"] = "schemas/scores.json"
    spec["bqml"] = {"model_type": "boosted_tree_classifier", **section}
    return repo


def write(repo, sql=None):
    path = repo.write()
    (path.parent / "schemas" / "scores.json").write_text(json.dumps(SCORES), encoding="utf-8")
    for name, text in (sql or {}).items():
        (path.parent / name).parent.mkdir(parents=True, exist_ok=True)
        (path.parent / name).write_text(text, encoding="utf-8")
    return path


def resolve(repo, env="dev", sql=None):
    return resolve_usecase(write(repo, sql), load_environment(repo.root, env))


def problems(repo, env="dev", sql=None) -> str:
    with pytest.raises(ConfigError) as e:
        resolve(repo, env, sql)
    return str(e.value)


def test_bqml_gets_pipewrights_pipelines_and_steps(repo):
    uc = resolve(bqml_repo(repo))
    assert {n: (r.file, r.function) for n, r in uc.pipelines.items()} == {
        "training": (bqml.PIPELINES_FILE, bqml.TRAINING_FUNCTION),
        "inference": (bqml.PIPELINES_FILE, bqml.SCORING_FUNCTION)}
    assert uc.components_dir == bqml.STEPS_DIR and uc.requirements == bqml.REQUIREMENTS and uc.framework == "bqml"
    section = uc.ml["bqml"]
    assert section == {"model_type": "boosted_tree_classifier", "options": {}, "explain": False,
                       "max_bytes_billed": bqml.DEFAULT_MAX_BYTES_BILLED, "transform": None, "prepare": None,
                       "registry_id": "churn-classifier",
                       "records": "gs://acme-ml-dev-artifacts/pipeline-root/churn/churn/bqml/"}
    assert uc.ml_config()["bqml"] == section          # pipelines receive it, the release contract pins it


def test_a_1_0_use_case_has_no_bqml(repo):
    uc = resolve_container(ml_repo(repo))
    assert "bqml" not in uc.ml and uc.framework == "pytorch"


def test_own_pipelines_and_components_are_refused_next_to_bqml(repo):
    bqml_repo(repo)
    repo.usecase["spec"]["framework"] = "pytorch"
    repo.usecase["spec"]["components"] = {"dir": "components/"}
    text = problems(repo)
    assert "framework: Pipewright provides the pipelines and steps of a bqml use case" in text
    assert "components: Pipewright provides" in text


def test_model_type_fits_the_task(repo):
    bqml_repo(repo, model_type="linear_reg")
    assert "bqml.model_type 'linear_reg' does not fit data.label.task binary" in problems(repo)
    repo.usecase["spec"]["data"]["label"]["task"] = "regression"
    repo.usecase["spec"]["data"]["split"]["strategy"] = "time"
    scores = [c for c in SCORES if c["name"] not in ("predicted_label", "score")] + [
        {"name": "prediction", "type": "FLOAT"}]
    path = repo.write()
    (path.parent / "schemas" / "scores.json").write_text(json.dumps(scores), encoding="utf-8")
    assert resolve_usecase(path, load_environment(repo.root, "dev")).ml["bqml"]["model_type"] == "linear_reg"


def test_options_pipewright_sets_are_refused(repo):
    bqml_repo(repo, options={"max_iterations": 30, "input_label_cols": ["x"], "enable_global_explain": True,
                             "Bad-Name": 1})
    text = problems(repo)
    assert "bqml.options.input_label_cols: Pipewright sets it" in text
    assert "bqml.options.enable_global_explain: Pipewright sets it (use bqml.explain)" in text
    assert "bqml.options.Bad-Name: not a CREATE MODEL option name" in text
    bqml_repo(repo, options={"max_iterations": 30, "L2_REG": 0.1})
    assert resolve(repo).ml["bqml"]["options"] == {"max_iterations": 30, "l2_reg": 0.1}


def test_sql_files_are_read_and_pinned(repo):
    bqml_repo(repo, transform="sql/transform.sql", prepare="sql/prepare.sql")
    sql = {"sql/transform.sql": "-- scaled\nML.STANDARD_SCALER(tenure_months) OVER () AS tenure_months, churned\n",
           "sql/prepare.sql": "SELECT *, monthly_charges / 12 AS monthly FROM {rows};\n"}
    section = resolve(repo, sql=sql).ml["bqml"]
    assert section["prepare"]["sql"] == "SELECT *, monthly_charges / 12 AS monthly FROM {rows}"
    assert section["prepare"]["file"] == "sql/prepare.sql" and len(section["prepare"]["sha256"]) == 64
    assert section["transform"]["sql"].startswith("-- scaled")
    text = problems(repo, sql={"sql/transform.sql": "TRANSFORM (churned)", "sql/prepare.sql": "SELECT 1; SELECT 2"})
    assert "bqml.transform: sql/transform.sql holds the TRANSFORM clause's expressions only" in text
    assert "bqml.prepare: sql/prepare.sql holds one statement" in text
    assert "must read the run's rows FROM {rows}" in text
    assert "bqml.transform not found" in problems(bqml_repo(repo, transform="sql/none.sql"))


def test_what_a_bqml_use_case_cannot_use(repo):
    bqml_repo(repo)
    spec = repo.usecase["spec"]
    spec["training"]["tuning"] = {"enabled": True}
    spec["data"]["split"]["validation_fraction"] = 0.1
    spec["triggers"]["landing"] = {"suffixes": [".csv"]}
    spec["steps"] = {"train": {"ray": {"workers": 2}}}
    spec["model"]["serving"] = {"enabled": True, "target": "kserve"}
    spec["environments"]["dev"].pop("model")          # dev switches serving off
    text = problems(repo)
    for expected in ("training.tuning: not available with bqml yet",
                     "data.split.validation_fraction: bqml trains on the training rows only",
                     "triggers.landing delivers gs:// files", "Ray steps are not part of a bqml use case",
                     "a bqml model is served from Model Registry"):
        assert expected in text


def test_output_table_must_hold_the_scores(repo):
    bqml_repo(repo)
    path = repo.write()
    (path.parent / "schemas" / "scores.json").write_text(json.dumps(SCORES[:2]), encoding="utf-8")
    with pytest.raises(ConfigError) as e:
        resolve_usecase(path, load_environment(repo.root, "dev"))
    assert "inference.output_table scores: add a score column (FLOAT64) for bqml scoring" in str(e.value)
    assert "add a scored_at column (TIMESTAMP)" in str(e.value)
    repo.usecase["spec"]["inference"] = {}
    assert "inference.output_table: bqml scoring writes its predictions there" in problems(repo)


def test_byte_limit_default_and_platform_policy(repo):
    bqml_repo(repo, max_bytes_billed=5 * 1024 ** 3)
    assert resolve(repo).ml["bqml"]["max_bytes_billed"] == 5 * 1024 ** 3
    repo.manifests["dev"]["policies"] = {"bqml": {"max_bytes_billed": 1024 ** 3,
                                                  "allowed_model_types": ["logistic_reg"]}}
    text = problems(repo)
    assert "bqml.max_bytes_billed 5368709120 is above dev's limit 1073741824" in text
    assert "bqml.model_type boosted_tree_classifier is not allowed in dev (platform policy): logistic_reg" in text
    bqml_repo(repo, model_type="logistic_reg")
    assert resolve(repo).ml["bqml"]["max_bytes_billed"] == 1024 ** 3      # the platform's limit is the default
    bqml_repo(repo, model_type="logistic_reg", max_bytes_billed=1000)
    assert "bqml.max_bytes_billed: at least 10485760" in problems(repo)


def test_promoted_by_retraining(repo):
    assert resolve(bqml_repo(repo)).model["promotion"] == "retrain"
    repo.usecase["spec"]["model"]["promotion"] = "model"
    assert "model.promotion: a bqml model is promoted by retraining" in problems(repo)


def test_a_bqml_versions_record_is_found_by_its_run(repo):
    from pipewright.models.registry import record_location

    uc = resolve(bqml_repo(repo))
    records = "gs://acme-ml-dev-artifacts/pipeline-root/churn/churn/bqml/"
    assert uc.ml["bqml"]["records"] == records
    assert record_location(uc, None, {"pipewright-run": "r1f4"}) == f"{records}r1f4/"
    assert record_location(uc, "gs://b/model/", {"pipewright-run": "r1"}) == "gs://b/model/"   # artifacts first
    assert record_location(uc, None, {}) is None


def test_new_usecase_from_the_bqml_example(repo, tmp_path):
    from pipewright.scaffold.new_usecase import new_usecase

    repo.write()
    dest = new_usecase(repo.root, "churn-bq", "churn", "team-bq", example="bqml")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    section = uc.ml["bqml"]
    assert section["model_type"] == "logistic_reg" and section["options"] == {"l2_reg": 0.1, "max_iterations": 20}
    assert "FROM {rows}" in section["prepare"]["sql"] and "ML.STANDARD_SCALER" in section["transform"]["sql"]
    assert uc.ml["inference"]["output_table"] == "churn_bq_scores" and uc.model["promotion"] == "retrain"
    assert (dest / "data" / "load_tables.sh").is_file() and not (dest / "components").exists()
    if importlib.util.find_spec("kfp"):
        from pipewright.build.compile import check_parameters, compile_all

        infos = compile_all(uc, "europe-west2-docker.pkg.dev/acme/img/churn-bq@sha256:" + "0" * 64, tmp_path)
        assert check_parameters(uc, infos) == ([], [])


def test_performance_key_must_be_in_the_scores(repo):
    bqml_repo(repo)
    repo.usecase["spec"]["monitoring"] = {"output_table": "model_metrics", "performance": {
        "enabled": True, "labels": {"table": "outcomes", "key": "account_id", "label": "churned"}}}
    assert "monitoring.performance.labels.key 'account_id' is not a column of inference.output_table" in \
        problems(repo)
    repo.usecase["spec"]["monitoring"]["performance"]["labels"]["key"] = "customer_id"
    assert resolve(repo).ml["monitoring"]["performance"]["enabled"]
