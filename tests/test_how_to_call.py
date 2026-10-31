"""FR-60 (Pipewright 1.2): deploy-model and `pipewright model endpoint` say how to call the deployed model."""
import json
from types import SimpleNamespace

from pipewright.config import load_environment, resolve_usecase
from pipewright.models import deploy
from tests.test_config_ml import ml_repo

RESOURCE = "projects/acme-ml-dev/locations/europe-west2/endpoints/1234567890"


def test_the_endpoint_its_url_the_role_and_a_sample_request(repo):
    ml_repo(repo)
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    env = load_environment(repo.root, "dev")
    text = deploy.how_to_call(uc, env, RESOURCE)
    assert "endpoint ID   1234567890" in text
    assert f"predict URL   https://europe-west2-aiplatform.googleapis.com/v1/{RESOURCE}:predict" in text
    assert "aiplatform.endpoints.predict (roles/aiplatform.user)" in text
    body = json.loads("\n".join(line[4:] for line in text.splitlines() if line.startswith("    ")))
    (instance,) = body["instances"]
    assert instance and "churned" not in instance and "customer_id" not in instance
    assert all(isinstance(v, (int, float, str, bool)) for v in instance.values())
    assert "pipewright model predict --env dev --usecase churn" in text


def test_with_online_features_an_instance_is_the_entity_id():
    uc = SimpleNamespace(ml={"online_features": {"key": "customer_id", "store": "s", "views": []}})
    assert deploy.sample_instance(uc) == {"customer_id": "<entity id>"}


def test_the_endpoint_is_found_by_its_display_name(repo):
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    env = load_environment(repo.root, "dev")
    seen = {}

    def get(url, **params):
        seen.update(params)
        return SimpleNamespace(ok=True, body={"endpoints": [{"name": RESOURCE}]})

    assert deploy.find_endpoint(uc, env, SimpleNamespace(get=get)) == RESOURCE
    assert seen["filter"] == f'display_name="{deploy.endpoint_name(uc)}"'
    none = SimpleNamespace(get=lambda url, **p: SimpleNamespace(ok=True, body={}))
    assert deploy.find_endpoint(uc, env, none) is None


def test_features_named_by_exclusion_still_give_a_sample():
    uc = SimpleNamespace(ml={
        "data": {"contract": {"columns": [{"name": "customer_id", "type": "string"}, {"name": "tenure", "type": "int"},
                                          {"name": "snapshot_date", "type": "date"}, {"name": "churned",
                                                                                     "type": "int"}]},
                 "label": {"column": "churned"}, "split": {"strategy": "time", "time_column": "snapshot_date"}},
        "features": {"exclude": ["customer_id"]}, "data_protection": {"personal_data_columns": []}})
    assert deploy.sample_instance(uc) == {"tenure": 0}
