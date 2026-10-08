import pytest
import yaml

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError

CONTRACT = {
    "columns": [
        {"name": "customer_id", "type": "string", "required": True},
        {"name": "snapshot_date", "type": "date", "required": True},
        {"name": "tenure_months", "type": "integer", "min": 0},
        {"name": "monthly_charges", "type": "float", "min": 0},
        {"name": "contract_length_months", "type": "integer", "allowed": [1, 12, 24]},
        {"name": "churned", "type": "integer", "allowed": [0, 1]},
    ],
    "min_rows": 500,
    "unique": ["customer_id"],
}


def ml_repo(repo, **sections):
    spec = repo.usecase["spec"]
    spec["data"] = {"contract": CONTRACT, "label": {"column": "churned", "values": [0, 1]},
                    "split": {"strategy": "time", "time_column": "snapshot_date", "test_fraction": 0.2}}
    spec["data_protection"] = {"personal_data_columns": ["customer_id"], "erasure": {"key_column": "customer_id"}}
    spec["training"] = {"hyperparameters": {"learning_rate": 0.003, "hidden_units": 32}, "seed": 7}
    spec.update(sections)
    return repo


def resolve(repo, env="dev"):
    return resolve_usecase(repo.write(), load_environment(repo.root, env))


def problems(repo, env="dev") -> str:
    with pytest.raises(ConfigError) as e:
        resolve(repo, env)
    return str(e.value)


# ------------------------------------------------------------ multi-file --

def test_include_files_and_environment_folder(repo):
    spec = repo.usecase["spec"]
    evaluation = {"evaluation": {"gates": [{"metric": "auc", "min": 0.8}], "slices": ["contract_length_months"]}}
    spec["include"] = ["config/*.yaml"]
    del spec["environments"]
    ml_repo(repo)
    data = {"data": spec.pop("data")}
    path = repo.write()
    (path.parent / "config").mkdir()
    (path.parent / "config" / "data.yaml").write_text(yaml.safe_dump(data), encoding="utf-8")
    (path.parent / "config" / "evaluation.yaml").write_text(yaml.safe_dump(evaluation), encoding="utf-8")
    (path.parent / "environments").mkdir()
    (path.parent / "environments" / "dev.yaml").write_text(yaml.safe_dump({"parameters": {"epochs": 2}}),
                                                           encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    assert uc.ml["evaluation"]["gates"] == [{"metric": "auc", "min": 0.8}]
    assert uc.ml["data"]["split"]["strategy"] == "time"
    assert uc.parameters["epochs"] == 2
    assert uc.sources == ["usecase.yaml", "config/data.yaml", "config/evaluation.yaml", "environments/dev.yaml"]


def test_section_defined_twice_is_an_error(repo):
    ml_repo(repo)
    repo.usecase["spec"]["include"] = ["config/*.yaml"]
    path = repo.write()
    (path.parent / "config").mkdir()
    (path.parent / "config" / "data.yaml").write_text(yaml.safe_dump({"data": {"label": {"column": "x"}}}),
                                                      encoding="utf-8")
    with pytest.raises(ConfigError, match="`data` is defined in both usecase.yaml and config/data.yaml"):
        resolve_usecase(path, load_environment(repo.root, "dev"))


def test_include_must_match(repo):
    repo.usecase["spec"]["include"] = ["config/*.yaml"]
    assert "matched no files" in problems(repo)


def test_overrides_in_two_places(repo):
    path = repo.write()
    (path.parent / "environments").mkdir()
    (path.parent / "environments" / "dev.yaml").write_text("parameters: {epochs: 1}\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="overrides for 'dev' are in both usecase.yaml and environments/dev.yaml"):
        resolve_usecase(path, load_environment(repo.root, "dev"))


# -------------------------------------------------------------- sections --

def test_defaults_and_derived_features(repo):
    uc = resolve(ml_repo(repo))
    # features default to contract columns minus label, time column and personal data
    assert uc.ml["features"]["include"] == ["tenure_months", "monthly_charges", "contract_length_months"]
    assert uc.ml["data"]["split"]["seed"] == 7
    assert uc.ml["training"]["experiment"]["name"] == "churn"
    assert uc.ml["evaluation"]["champion"] == {"enabled": True, "metric": "auc", "min_improvement": 0.0}
    assert uc.ml["monitoring"]["drift"]["features"] == uc.ml["features"]["include"]
    cfg = uc.ml_config()
    assert cfg["usecase"] == "churn" and cfg["model_name"] == "churn-classifier" and "evaluation" in cfg


def test_personal_data_cannot_be_a_feature(repo):
    ml_repo(repo, features={"include": ["customer_id", "tenure_months"]})
    assert "'customer_id' is personal data" in problems(repo)


def test_unknown_columns_and_time_split(repo):
    ml_repo(repo, features={"include": ["tenure", "tenure_months"]},
            evaluation={"slices": ["region"], "gates": [{"metric": "auc_roc", "min": 0.7}]})
    repo.usecase["spec"]["data"]["split"] = {"strategy": "time", "time_column": "tenure_months"}
    msg = problems(repo)
    assert "features.include: column 'tenure' is not in data.contract.columns" in msg
    assert "evaluation.slices: column 'region'" in msg
    assert "'auc_roc' is not one of" in msg
    assert "time_column 'tenure_months' must be a date or timestamp column" in msg


def test_gate_metrics_are_always_computed(repo):
    ml_repo(repo, evaluation={"metrics": ["auc"], "gates": [{"metric": "brier", "max": 0.2}]})
    uc = resolve(repo)
    assert "brier" in uc.ml["evaluation"]["metrics"] and "brier" in uc.ml["evaluation"]["lower_is_better"]


def test_tuning_checks(repo):
    ml_repo(repo)
    repo.usecase["spec"]["training"]["tuning"] = {
        "enabled": True, "max_trials": 2, "parallel_trials": 4,
        "search_space": {"learning_rate": {"type": "double", "min": 0, "max": 0.1, "scale": "log"},
                         "dropout": {"type": "double", "min": 0.1, "max": 0.5},
                         "hidden_units": {"type": "discrete"}},
    }
    msg = problems(repo)
    assert "parallel_trials is greater than max_trials" in msg
    assert "learning_rate: log scale needs min > 0" in msg
    assert "dropout: give a default in training.hyperparameters.dropout" in msg
    assert "hidden_units: type discrete needs values" in msg


def test_monitoring_needs_inference_pipeline_and_table(repo):
    ml_repo(repo, monitoring={"drift": {"enabled": True}}, retraining={"enabled": True, "when": ["performance"]})
    msg = problems(repo)
    assert "inference and monitoring need pipelines.inference" in msg
    assert "set monitoring.output_table" in msg
    assert "retraining.when performance needs monitoring.performance enabled" in msg


def test_inference_pipeline_and_trigger_selection(repo):
    ml_repo(repo, monitoring={"drift": {"enabled": True}, "output_table": "model_metrics"},
            inference={"output_table": "predictions"})
    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"), "inference": {"file": "pipeline.py", "function": "infer"}}
    spec["triggers"]["schedules"].append({"name": "hourly-scoring", "cron": "0 * * * *",
                                          "timezone": "Europe/London", "pipeline": "inference"})
    repo.add_env("prod")
    uc = resolve(repo, "prod")
    assert set(uc.pipelines) == {"training", "inference"}
    assert [s["pipeline"] for s in uc.triggers["schedules"]] == ["training", "inference"]
    assert uc.ml["inference"]["decision_threshold"] == 0.5


def test_tuning_override_allowed_but_split_is_fixed(repo):
    ml_repo(repo)
    repo.usecase["spec"]["environments"]["dev"]["training"] = {"tuning": {"max_trials": 2}}
    assert resolve(repo).ml["training"]["tuning"]["max_trials"] == 2
    repo.usecase["spec"]["environments"]["dev"]["data"] = {"split": {"test_fraction": 0.5}}
    assert "may vary per environment" in problems(repo)


def test_yaml_on_key_gets_a_hint(repo):
    ml_repo(repo, retraining={"enabled": True, True: ["drift"]})      # what YAML makes of `on: [drift]`
    assert "YAML reads an unquoted key on, yes or y as true" in problems(repo)


def test_retraining_needs_a_source(repo):
    ml_repo(repo, monitoring={"drift": {"enabled": True}, "output_table": "model_metrics"},
            retraining={"enabled": True, "when": ["drift"]})
    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"), "inference": {"file": "pipeline.py", "function": "infer"}}
    assert "set retraining.source_uri" in problems(repo)
    spec["retraining"]["source_uri"] = "gs://${buckets.landing}/x.csv"
    assert resolve(repo).ml["retraining"]["source_uri"] == "gs://acme-ml-dev-landing/x.csv"


def test_registry_default_follows_promote_approval(repo):
    ml_repo(repo)
    assert resolve(repo).ml["registry"]["set_default"] == "manual"            # promote_model: manual
    repo.usecase["spec"]["approvals"]["promote_model"] = "auto"
    assert resolve(repo).ml["registry"]["set_default"] == "on_approval"
    repo.usecase["spec"]["approvals"]["promote_model"] = "manual"
    repo.usecase["spec"]["registry"] = {"set_default": "on_approval"}
    assert "lets the pipeline promote models, but approvals.promote_model is manual" in problems(repo)
