import pytest
import yaml

from pipewright.cli import main
from pipewright.config import deep_merge, load_environment, resolve_usecase
from pipewright.errors import ConfigError


def resolve(repo, env="dev"):
    path = repo.write()
    return resolve_usecase(path, load_environment(repo.root, env))


def problems_of(repo, env="dev") -> str:
    with pytest.raises(ConfigError) as e:
        resolve(repo, env)
    return str(e.value)


# ------------------------------------------------------------------ merge --

def test_deep_merge_maps_merge_and_lists_replace():
    base = {"a": {"x": 1, "y": 2}, "l": [1, 2], "s": "keep"}
    out = deep_merge(base, {"a": {"y": 3}, "l": []})
    assert out == {"a": {"x": 1, "y": 3}, "l": [], "s": "keep"}
    assert base["a"]["y"] == 2  # input untouched


# ------------------------------------------------------- happy path dev/prod --

def test_dev_overrides_and_manifest_names(repo):
    uc = resolve(repo, "dev")
    assert uc.parameters == {"auc_threshold": 0.75, "epochs": 5}
    assert uc.triggers["schedules"] == []
    assert uc.triggers["landing"] == {"pipeline": "training", "suffixes": [".csv"]}
    assert uc.model["serving"]["enabled"] is False
    assert uc.approvals["deploy_model"] == "auto"
    assert uc.resources["runner_service_account"].startswith("ml-runner-churn@")
    assert uc.resources["bigquery_dataset"] == "churn"
    assert uc.resources["landing_prefix"] == "incoming/churn/"
    assert set(uc.tables) == {"predictions", "model_metrics"}
    assert uc.tables["predictions"].partition == {"field": "scored_at", "type": "DAY"}


def test_prod_gets_base_values_plus_its_own_overrides(repo):
    repo.add_env("prod", project="acme-ml-prod")
    uc = resolve(repo, "prod")
    assert uc.parameters["epochs"] == 40
    assert uc.triggers["schedules"][0]["name"] == "nightly"
    assert uc.model["serving"] == {"enabled": True, "target": "vertex", "machine": "n1-standard-2",
                                   "replicas": {"min": 2, "max": 4}}
    assert uc.approvals["deploy_model"] == "manual"


# ------------------------------------------------------------- guardrails --

def test_platform_policy_blocks_auto_approval(repo):
    repo.add_env("prod", policies={"deploy_model": "manual"})
    repo.usecase["spec"]["environments"]["prod"]["approvals"] = {"deploy_model": "auto"}
    assert "approvals.deploy_model: auto is not allowed in prod (platform policy)" in problems_of(repo, "prod")


def test_fixed_keys_cannot_be_overridden(repo):
    repo.usecase["spec"]["environments"]["dev"]["pipeline"] = {"file": "other.py", "function": "f"}
    msg = problems_of(repo)
    assert "spec.environments.dev" in msg and "may vary per environment" in msg


def test_unknown_tenant(repo):
    repo.usecase["spec"]["tenant"] = "fraud"
    assert "tenant 'fraud' is not in environment 'dev': ask the platform team to add it" in problems_of(repo)


def test_explicit_names_must_match_manifest(repo):
    repo.usecase["spec"]["resources"] = {"bigquery_dataset": "churn_v2"}
    assert "spec.resources.bigquery_dataset is 'churn_v2' but the manifest says 'churn'" in problems_of(repo)


def test_no_manifest_requires_explicit_resources(repo):
    repo.environments["dev"] = {
        "project": "p", "region": "europe-west2", "artifacts_bucket": "b",
        "artifact_registry": {"docker": "d", "kfp": "k"},
    }
    msg = problems_of(repo)
    assert "spec.resources.runner_service_account is required" in msg
    assert "spec.resources.bigquery_dataset is required" in msg


def test_environment_file_must_agree_with_manifest(repo):
    repo.environments["dev"]["project"] = "someone-else"
    with pytest.raises(ConfigError, match="spec.project is 'someone-else' but the manifest says 'acme-ml-dev'"):
        resolve(repo)


def test_kserve_needs_private_gke(repo):
    repo.usecase["spec"]["environments"]["dev"]["model"] = {"serving": {"enabled": True, "target": "kserve"}}
    assert "kserve is only available on private-gke" in problems_of(repo)


def test_serving_needs_tenant_permission(repo):
    repo.manifests["dev"]["tenants"]["churn"]["online_serving"] = False
    repo.usecase["spec"]["environments"]["dev"]["model"] = {"serving": {"enabled": True}}
    assert "not enabled for online serving" in problems_of(repo)


def test_landing_needs_tenant_prefix(repo):
    repo.manifests["dev"]["tenants"]["churn"]["landing_prefix"] = None
    assert "has no landing prefix" in problems_of(repo)


def test_schedule_checks(repo):
    repo.usecase["spec"]["environments"]["dev"]["triggers"] = {
        "schedules": [
            {"name": "a", "cron": "0 2 * *", "timezone": "Europe/London"},
            {"name": "a", "cron": "0 2 * * *", "timezone": "London"},
        ]
    }
    msg = problems_of(repo)
    assert "must have 5 fields" in msg
    assert "time zone 'London' is not valid" in msg
    assert "schedule names must be unique" in msg


def test_table_schema_checks(repo):
    tables = repo.usecase["spec"]["outputs"]["bigquery"]["tables"]
    tables["predictions"]["partition"]["field"] = "created_at"
    tables["predictions"]["cluster"] = ["region"]
    tables["missing"] = "schemas/missing.json"
    msg = problems_of(repo)
    assert "partition field 'created_at' is not a column" in msg
    assert "cluster field 'region' is not a column" in msg
    assert "schema file not found: missing.json" in msg


def test_all_problems_reported_together(repo):
    repo.usecase["spec"]["tenant"] = "fraud"
    repo.usecase["spec"]["pipeline"]["file"] = "nope.py"
    e = problems_of(repo)
    assert "problems" in e and "tenant 'fraud'" in e and "pipelines.training.file not found" in e


# -------------------------------------------------------------------- cli --

def test_cli_validate_and_resolve(repo, capsys):
    repo.write()
    assert main(["validate", "--root", str(repo.root), "--env", "dev"]) == 0
    assert "OK    churn (dev)" in capsys.readouterr().out

    assert main(["resolve", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"]) == 0
    out = capsys.readouterr().out
    doc = yaml.safe_load(out.split("\n", 1)[1].rsplit("---", 1)[0])
    assert doc["parameters"]["epochs"] == 5 and doc["outputs"]["bigquery"]["dataset"] == "churn"


def test_cli_reports_failure_exit_code(repo, capsys):
    repo.usecase["spec"]["tenant"] = "fraud"
    repo.write()
    assert main(["validate", "--root", str(repo.root), "--env", "dev"]) == 1
    assert "FAIL  churn (dev)" in capsys.readouterr().out


# ------------------------------------------------------------ references --

def test_parameter_references(repo):
    repo.usecase["spec"]["parameters"].update({
        "bq_dataset": "${resources.bigquery_dataset}",
        "model_display_name": "${model.name}",
        "source_prefix": "gs://${buckets.landing}/${resources.landing_prefix}",
        "region": "${region}",
    })
    repo.usecase["spec"]["triggers"]["schedules"][0]["source_uri"] = "gs://${buckets.landing}/incoming/latest.csv"
    repo.add_env("prod")
    uc = resolve(repo, "prod")
    assert uc.parameters["bq_dataset"] == "churn"
    assert uc.parameters["model_display_name"] == "churn-classifier"
    assert uc.parameters["source_prefix"] == "gs://acme-ml-dev-landing/incoming/churn/"
    assert uc.parameters["auc_threshold"] == 0.75          # non-strings untouched
    assert uc.triggers["schedules"][0]["source_uri"] == "gs://acme-ml-dev-landing/incoming/latest.csv"


def test_unknown_reference(repo):
    repo.usecase["spec"]["parameters"]["bq_dataset"] = "${resources.dataset}"
    msg = problems_of(repo)
    assert "parameters.bq_dataset: unknown reference ${resources.dataset}" in msg
    assert "resources.bigquery_dataset" in msg               # lists what is available


def test_landing_can_be_turned_off_per_environment(repo):
    repo.usecase["spec"]["environments"]["dev"]["triggers"] = {"landing": None, "schedules": []}
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.triggers["landing"] is None and uc.triggers["api"] is True
