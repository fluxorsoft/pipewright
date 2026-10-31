"""FR-76: a promoted model reads the destination's online store, never the source's (Pipewright 1.2)."""
import json
from types import SimpleNamespace

from pipewright.models import deploy, online
from pipewright.release import promote

TEST_STORE = "projects/acme-ml-dev/locations/europe-west2/featureOnlineStores/retail_online"
PROD_STORE = "projects/acme-ml-prod/locations/europe-west2/featureOnlineStores/retail_online"
VIEWS = [{"view": "customer_activity", "features": {"recency_days": "recency_days"}}]


def lookup(store):
    return {"store": store, "key": "customer_id", "views": VIEWS}


def test_the_container_spec_gets_the_lookup_and_keeps_other_variables():
    spec = {"imageUri": "serving:1", "env": [{"name": "OTHER", "value": "x"},
                                             {"name": online.ENV_VAR, "value": json.dumps(lookup(TEST_STORE))}]}
    out = online.container_spec(spec, lookup(PROD_STORE))
    assert out["imageUri"] == "serving:1"
    env = {e["name"]: e["value"] for e in out["env"]}
    assert env["OTHER"] == "x" and json.loads(env[online.ENV_VAR])["store"] == PROD_STORE
    assert spec["env"][1]["value"] == json.dumps(lookup(TEST_STORE))        # the source spec is not changed


def test_without_online_features_the_variable_goes_and_nothing_else_changes():
    assert online.container_spec({"imageUri": "s", "env": [{"name": online.ENV_VAR, "value": "{}"}]}, None) == \
        {"imageUri": "s"}
    assert online.container_spec(None, None) == {}


def test_a_model_reading_another_environments_store_is_refused():
    # The ebook's Chapter 13: prod v2, promoted from test, carried only serving.json naming test's store.
    problem = online.check(lookup(PROD_STORE), {}, {"online_features": lookup(TEST_STORE)}, "prod")
    assert problem and TEST_STORE in problem and "another environment" in problem and "serving.json" in problem


def test_the_container_variable_wins_over_serving_json():
    env_vars = {online.ENV_VAR: online.env_value(lookup(PROD_STORE))}
    assert online.check(lookup(PROD_STORE), env_vars, {"online_features": lookup(TEST_STORE)}, "prod") is None


def test_a_model_without_any_lookup_is_refused_when_online_features_are_on():
    assert "no online lookup" in online.check(lookup(PROD_STORE), {}, None, "prod")


def test_request_mode_use_cases_are_not_checked():
    assert online.check(None, {}, {"online_features": lookup(TEST_STORE)}, "prod") is None


def test_promotion_gives_the_model_the_destinations_store(monkeypatch):
    posted = {}
    source = {"artifactUri": "gs://dev/model", "containerSpec": {
        "imageUri": "serving:1", "env": [{"name": online.ENV_VAR, "value": online.env_value(lookup(TEST_STORE))}]}}
    monkeypatch.setattr(promote, "source_version", lambda *a, **k: ("10", source))
    monkeypatch.setattr(promote, "copy_model_files", lambda gcp, s, d, log=print: [])
    monkeypatch.setattr(promote, "_find_model", lambda gcp, env, name: None)
    monkeypatch.setattr(promote.publish, "pipeline_root", lambda env, uc: "gs://prod/root")
    monkeypatch.setattr(promote, "_wait", lambda gcp, env, body, what: {"modelVersionId": "2"})

    def post(url, body):
        posted["body"] = body
        return SimpleNamespace(ok=True, body={})

    gcp = SimpleNamespace(post=post)
    dst_env = SimpleNamespace(name="prod", project="acme-ml-prod", region="europe-west2", security={})
    uc = SimpleNamespace(name="churn", model={"name": "churn-classifier"}, ml={"online_features": lookup(PROD_STORE)})
    monkeypatch.setattr(promote, "_model_api", lambda env: "https://api")
    promote._promote_model_files(gcp, SimpleNamespace(name="dev"), dst_env, uc, "abc123", "10", log=lambda m: None)
    env = {e["name"]: e["value"] for e in posted["body"]["model"]["containerSpec"]["env"]}
    assert json.loads(env[online.ENV_VAR])["store"] == PROD_STORE


def test_deploy_reads_the_variable_and_serving_json_of_the_default_version(monkeypatch):
    model = SimpleNamespace(uri="gs://prod-artifacts/root/model", gca_resource=SimpleNamespace(
        container_spec=SimpleNamespace(env=[])))
    files = {("prod-artifacts", "root/model/serving.json"): json.dumps({"online_features": lookup(TEST_STORE)})}
    monkeypatch.setattr("pipewright.gcp.storage.gcs_read", lambda gcp, b, n: (files.get((b, n)), 1))
    uc = SimpleNamespace(ml={"online_features": lookup(PROD_STORE)})
    problem = deploy.online_store_problem(uc, SimpleNamespace(name="prod"), None, model)
    assert problem and "another environment" in problem
    model.gca_resource.container_spec.env = [SimpleNamespace(name=online.ENV_VAR,
                                                             value=online.env_value(lookup(PROD_STORE)))]
    assert deploy.online_store_problem(uc, SimpleNamespace(name="prod"), None, model) is None


def test_fill_missing_is_in_ml_config_only_when_switched_off(repo):
    from pipewright.config import load_environment, resolve_usecase

    path = repo.write()
    assert "serving" not in resolve_usecase(path, load_environment(repo.root, "dev")).ml     # 1.x resolves as before
    repo.usecase["spec"]["model"]["serving"]["fill_missing"] = False
    path = repo.write()
    assert resolve_usecase(path, load_environment(repo.root, "dev")).ml["serving"] == {"fill_missing": False}
