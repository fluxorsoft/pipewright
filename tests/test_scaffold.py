import importlib.util

import pytest

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.scaffold.new_usecase import list_examples, new_usecase


def test_examples_available():
    assert "churn" in list_examples()


def test_new_usecase_from_churn_example(repo):
    repo.write()
    dest = new_usecase(repo.root, "churn-nimbus", "churn", "team-nimbus")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    assert uc.owner == "team-nimbus" and uc.model["name"] == "churn-nimbus-classifier"
    assert set(uc.tables) == {f"churn_nimbus_{t}" for t in ("raw_customers", "predictions", "model_metrics",
                                                             "model_monitoring", "churn_outcomes")}
    assert uc.parameters["raw_table"] == "churn_nimbus_raw_customers"
    assert uc.parameters["bq_dataset"] == "churn"
    # the templated config files rendered, environments/dev.yaml applied
    assert uc.ml["inference"]["output_table"] == "churn_nimbus_predictions"
    assert uc.ml["monitoring"]["performance"]["labels"]["table"] == "churn_nimbus_churn_outcomes"
    assert uc.ml["training"]["hyperparameters"]["epochs"] == 5 and uc.ml["training"]["tuning"]["max_trials"] == 2
    assert uc.triggers["schedules"] == [] and uc.ml["retraining"]["enabled"] is False
    assert uc.ml["retraining"]["source_uri"] == "gs://acme-ml-dev-landing/samples/churn-nimbus/customers.csv"
    assert set(uc.pipelines) == {"training", "inference"}
    assert (dest / "data" / "sample_customers.csv").is_file() and not (dest / "example.yaml").exists()
    with pytest.raises(ConfigError, match="already exists"):
        new_usecase(repo.root, "churn-nimbus", "churn", "team-nimbus")


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_example_compiles_and_accepts_its_parameters(repo):
    from pipewright.build.compile import check_parameters, compile_all
    from pipewright.build.images import find_components

    repo.write()
    dest = new_usecase(repo.root, "churn-nimbus", "churn", "team-nimbus")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    infos = compile_all(uc)
    assert check_parameters(uc, infos) == ([], [])
    assert {"raw_table", "metrics_table", "ml_config"} <= set(infos["training"].parameters)
    assert "ml_config" in infos["inference"].parameters
    assert {"tune-trial", "train-model", "evaluate-model", "register-model"} <= set(infos["training"].tasks)
    assert {"score-batch", "check-batch", "request-retraining"} <= set(infos["inference"].tasks)
    assert len(find_components(dest / "components")) == 17


def test_bad_names_rejected(repo):
    with pytest.raises(ConfigError, match="lowercase"):
        new_usecase(repo.root, "Churn_Nimbus", "churn", "x")


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_two_usecases_from_one_example_keep_their_own_images(repo):
    from pipewright.build.compile import compile_all

    repo.write()
    a = new_usecase(repo.root, "churn-a", "churn", "team-a")
    b = new_usecase(repo.root, "churn-b", "churn", "team-b")
    env = load_environment(repo.root, "dev")
    image_a, image_b = (f"europe-west2-docker.pkg.dev/p/r/{n}@sha256:{n * 64}" for n in ("a", "b"))
    first = compile_all(resolve_usecase(a / "usecase.yaml", env), component_image=image_a)
    second = compile_all(resolve_usecase(b / "usecase.yaml", env), component_image=image_b)
    assert {i for info in first.values() for i in info.images} == {image_a}
    assert {i for info in second.values() for i in info.images} == {image_b}
