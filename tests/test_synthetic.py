"""Synthetic data generation (FR-49): the toggle, the section's checks, gates, rules, and the built-in pipeline."""

import importlib.util
import json

import pytest
import yaml

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.synthetic.runtime import gates, rules
from tests.test_config_ml import CONTRACT, ml_repo

SYNTHETIC_TABLE = [{"name": c["name"], "type": {"string": "STRING", "date": "DATE", "integer": "INTEGER",
                                                 "float": "FLOAT"}[c["type"]]} for c in CONTRACT["columns"]]
SYNTHETIC_TABLE.append({"name": "run_id", "type": "STRING"})


def synthetic_repo(repo, table=True, **synthetic):
    ml_repo(repo)
    spec = repo.usecase["spec"]
    spec["synthetic"] = {"enabled": True, "source_uri": "bq://acme-ml-dev.churn.customers",
                         "output_table": "synthetic_customers", **synthetic}
    if table:
        spec["outputs"]["bigquery"]["tables"]["synthetic_customers"] = "schemas/synthetic_customers.json"
    return repo


def write(repo):
    path = repo.write()
    (path.parent / "schemas" / "synthetic_customers.json").write_text(json.dumps(SYNTHETIC_TABLE), encoding="utf-8")
    return path


def resolve(repo, env="dev"):
    return resolve_usecase(write(repo), load_environment(repo.root, env))


def problems(repo, env="dev") -> str:
    with pytest.raises(ConfigError) as e:
        resolve(repo, env)
    return str(e.value)


# ------------------------------------------------------------------ toggle --

def test_off_by_default_changes_nothing(repo):
    uc = resolve(ml_repo(repo))
    assert uc.synthetic is None and "synthetic" not in uc.ml and "synthetic" not in uc.ml_config()
    assert set(uc.pipelines) == {"training"} and not uc.synthetic_image_needed


def test_enabled_false_is_the_same_as_absent(repo):
    ml_repo(repo).usecase["spec"]["synthetic"] = {"enabled": False, "purpose": "sharing"}
    uc = resolve(repo)
    assert uc.synthetic is None and set(uc.pipelines) == {"training"}


def test_enabled_adds_the_pipeline_pipewright_provides(repo):
    uc = resolve(synthetic_repo(repo))
    from pipewright.synthetic import PIPELINE_FILE

    assert uc.pipelines["synthetic"].file == PIPELINE_FILE and uc.synthetic_image_needed
    syn = uc.ml_config()["synthetic"]
    assert [c["name"] for c in syn["candidates"]] == ["default", "spread"]
    assert syn["candidates"][1]["sampling_temperature"] == 1.3 and syn["candidates"][0]["engine"] == "mostlyai"
    assert syn["seed"] == 7 and syn["evaluation"]["attacks"] == 1000
    # personal data: never learned; a unique personal column gets new keys
    assert syn["columns"] == {"customer_id": {"rule": "key"}} and syn["exclude"] == ["customer_id"]
    assert syn["gates"]["singling_out"] == {"max": 0.30} and syn["gates"]["fidelity"] == {"min": 0.90}


def test_switched_on_in_one_environment_only(repo):
    synthetic_repo(repo)
    repo.usecase["spec"]["synthetic"]["enabled"] = False
    repo.usecase["spec"]["environments"]["prod"] = {"synthetic": {"enabled": True, "rows": 1000}}
    repo.add_env("prod")
    assert resolve(repo).synthetic is None
    prod = resolve(repo, "prod")
    assert prod.synthetic["rows"] == 1000 and "synthetic" in prod.pipelines


def test_gates_and_privacy_cannot_vary_per_environment(repo):
    synthetic_repo(repo)
    repo.usecase["spec"]["environments"]["dev"]["synthetic"] = {"gates": {"fidelity": {"min": 0.5}}}
    assert "gates" in problems(repo)


def test_contract_tier_needs_no_pipeline_or_source(repo):
    ml_repo(repo).usecase["spec"]["synthetic"] = {"enabled": True, "tier": "contract", "rows": 50}
    uc = resolve(repo)
    assert "synthetic" not in uc.pipelines and not uc.synthetic_image_needed and uc.synthetic["rows"] == 50


# ------------------------------------------------------------------ checks --

def test_learned_needs_source_and_output_table(repo):
    synthetic_repo(repo, table=False)
    del repo.usecase["spec"]["synthetic"]["source_uri"]
    found = problems(repo)
    assert "synthetic.source_uri" in found
    assert "'synthetic_customers' is not in outputs.bigquery.tables" in found


def test_output_table_must_hold_the_contract_and_run_id(repo):
    synthetic_repo(repo)
    path = repo.write()
    (path.parent / "schemas" / "synthetic_customers.json").write_text(json.dumps(SYNTHETIC_TABLE[:2]),
                                                                      encoding="utf-8")
    with pytest.raises(ConfigError) as e:
        resolve_usecase(path, load_environment(repo.root, "dev"))
    assert "lacks contract columns tenure_months" in str(e.value) and "add a run_id column" in str(e.value)


def test_privacy_gates_only_stricter(repo):
    synthetic_repo(repo, purpose="sharing", gates={"singling_out": {"max": 0.5}, "fidelity": {"min": 0.7},
                                                   "dcr_ratio": {"min": 1.2}})
    found = problems(repo)
    assert "synthetic.gates.singling_out: max 0.5 is looser than the sharing default (0.2)" in found
    assert "fidelity" not in found.split("singling_out")[0]          # quality gates may be relaxed
    synthetic_repo(repo, purpose="sharing", gates={"fidelity": {"min": 0.7}, "dcr_ratio": {"min": 1.2}})
    g = resolve(repo).synthetic["gates"]
    assert g["fidelity"] == {"min": 0.7} and g["dcr_ratio"] == {"min": 1.2} and g["inference"] == {"max": 0.25}


def test_sdv_is_the_teams_choice(repo):
    synthetic_repo(repo, engine={"type": "sdv", "model": "ctgan"})
    found = problems(repo)
    assert "SDV is installed by your team" in found and "synthetic.requirements" in found
    synthetic_repo(repo, engine={"type": "sdv", "model": "ctgan"}, requirements="synthetic-requirements.txt")
    path = write(repo)
    (path.parent / "synthetic-requirements.txt").write_text("sdv==2.0.0\n", encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    assert uc.synthetic_requirements.name == "synthetic-requirements.txt"
    assert [c["name"] for c in uc.synthetic["candidates"]] == ["default"]       # no temperature variant for SDV


def test_rules_and_candidates_are_checked(repo):
    synthetic_repo(repo, columns={"snapshot_date": {"rule": "time", "from": "2025-12-31", "to": "2025-01-01"},
                                  "nope": {"rule": "drop"}, "churned": {"rule": "fake", "provider": "email"}},
                   candidates=[{"name": "a", "differential_privacy": {"epsilon": 2}}, {"name": "a"}],
                   sensitive_columns=["customer_id"])
    found = problems(repo)
    for part in ("snapshot_date: from is after to", "column 'nope' is not in data.contract.columns",
                 "churned: fake values are strings", "each needs a unique name",
                 "sensitive_columns: 'customer_id' is never learned"):
        assert part in found, part


def test_the_team_cannot_define_its_own_synthetic_pipeline(repo):
    synthetic_repo(repo)
    spec = repo.usecase["spec"]
    spec["pipelines"] = {"training": spec.pop("pipeline"), "synthetic": {"file": "pipeline.py", "function": "x"}}
    assert "'synthetic' was unexpected" in problems(repo)        # pipelines names only training and inference


# ------------------------------------------------------------------- gates --

def test_gate_defaults_per_purpose_and_check():
    assert gates.defaults("test-data")["closer_than_p5"] == {"max": 0.10}
    assert gates.defaults("sharing")["closer_than_p5"] == {"max": 0.05}
    g = gates.defaults("sharing")
    assert gates.check({"fidelity": 0.9}, {"fidelity": g["fidelity"]}) == []
    assert gates.check({"fidelity": 0.8}, {"fidelity": g["fidelity"]}) == ["fidelity 0.8 is below 0.85"]
    assert gates.check({}, {"inference": g["inference"]}) == ["inference: not measured"]


def test_select_keeps_the_best_passing_never_the_best_score():
    g = {"fidelity": {"min": 0.85}, "dcr_ratio": {"min": 1.0}}
    copying = {"name": "copying", "metrics": {"fidelity": 0.97, "dcr_ratio": 0.55}}
    good = {"name": "good", "metrics": {"fidelity": 0.92, "dcr_ratio": 1.1, "tstr_drop": 0.02}}
    better = {"name": "better", "metrics": {"fidelity": 0.93, "dcr_ratio": 1.2, "tstr_drop": 0.03}}
    failed = {"name": "failed", "errors": ["generation failed: boom"]}
    index, verdicts = gates.select([copying, good, better, failed], g)
    assert index == 2 and [v["passed"] for v in verdicts] == [False, True, True, False]
    assert verdicts[3]["reasons"] == ["generation failed: boom"]
    assert gates.select([copying], g)[0] is None


# ------------------------------------------------------------------- rules --

def test_contract_rows_respect_the_contract():
    rows = rules.contract_rows(CONTRACT, {"column": "churned", "values": [0, 1]},
                               {"snapshot_date": {"rule": "time", "from": "2025-01-01", "to": "2025-06-30",
                                                  "step": "month_end"}}, rows=200, seed=1)
    assert len(rows) == 200 and len({r["customer_id"] for r in rows}) == 200
    assert {r["contract_length_months"] for r in rows} <= {1, 12, 24} and {r["churned"] for r in rows} == {0, 1}
    assert all(r["tenure_months"] is None or r["tenure_months"] >= 0 for r in rows)
    assert {r["snapshot_date"][5:] for r in rows} <= {"01-31", "02-28", "03-31", "04-30", "05-31", "06-30"}
    assert rows == rules.contract_rows(CONTRACT, {"column": "churned", "values": [0, 1]},
                                       {"snapshot_date": {"rule": "time", "from": "2025-01-01", "to": "2025-06-30",
                                                          "step": "month_end"}}, rows=200, seed=1)   # seeded


def test_apply_rules_replaces_keys_and_personal_data():
    records = [{"customer_id": "REAL-1", "email": "real@corp.com", "x": 1}, {"customer_id": "REAL-2", "x": 2}]
    contract = {"columns": [{"name": "customer_id", "type": "string"}, {"name": "email", "type": "string"},
                            {"name": "x", "type": "integer"}]}
    out = rules.apply_rules(records, contract, {"customer_id": {"rule": "key", "prefix": "S-"},
                                                "email": {"rule": "fake", "provider": "email"}})
    assert [r["customer_id"] for r in out] == ["S-00000001", "S-00000002"]
    assert all(r["email"].endswith("@example.com") for r in out) and [r["x"] for r in out] == [1, 2]


# ---------------------------------------------------------------- pipeline --

def test_steps_module_names_the_image_variables():
    from pipewright.synthetic import EVALUATION_IMAGE_ENV, IMAGE_ENV, PIPELINE_FILE

    text = (PIPELINE_FILE.parent / "synthetic_steps.py").read_text(encoding="utf-8")
    assert f'"{IMAGE_ENV}"' in text and f'"{EVALUATION_IMAGE_ENV}"' in text
    assert "\nfrom pipewright" not in text and "\nimport pipewright" not in text   # the images have no Pipewright


def test_runtime_package_is_standalone():
    from pipewright.synthetic import PIPELINE_FILE

    for path in (PIPELINE_FILE.parent / "runtime").glob("*.py"):
        assert "from pipewright" not in path.read_text(encoding="utf-8"), path.name


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_image_contexts(repo, tmp_path):
    from pipewright.build.images import synthetic_build_context
    from pipewright.checks.kfp_version import compiler_version

    synthetic_repo(repo, requirements="synthetic-requirements.txt")
    path = write(repo)
    (path.parent / "synthetic-requirements.txt").write_text("sdv==2.0.0\n", encoding="utf-8")
    uc = resolve_usecase(path, load_environment(repo.root, "dev"))
    engine = synthetic_build_context(uc, tmp_path / "engine", "engine")
    evaluation = synthetic_build_context(uc, tmp_path / "evaluation", "evaluation")
    for ctx in (engine, evaluation):
        components = ctx / "components"
        assert (components / "synthetic_steps.py").is_file() and (components / "pipewright_runtime.py").is_file()
        assert (components / "pipewright_synthetic" / "engines.py").is_file()
        ini = (ctx / "kfp_config.ini").read_text(encoding="utf-8")
        assert "generate_synthetic = synthetic_steps.py" in ini and "publish_synthetic = synthetic_steps.py" in ini
        assert f"kfp=={compiler_version()}" in (ctx / "requirements.txt").read_text(encoding="utf-8")
    engine_req = (engine / "requirements.txt").read_text(encoding="utf-8")
    assert "mostlyai[local]==6.2.0" in engine_req and "torch==2.13.0+cpu" in engine_req and "sdv==2.0.0" in engine_req
    evaluation_req = (evaluation / "requirements.txt").read_text(encoding="utf-8")
    assert "anonymeter==1.1.0" in evaluation_req and "sdv" not in evaluation_req and "sdmetrics" not in evaluation_req
    assert "mostlyai" not in evaluation_req


def test_contract_synthetic_images():
    from pipewright.release.contract import ReleaseContract

    image = "r/x@sha256:" + "a" * 64
    launcher = ["project", "region", "source_uri", "run_id"]
    base = {"contract_version": "3", "release_id": "r1", "usecase": "churn", "tenant": "churn", "environment": "dev",
            "blueprint": None, "orchestrator": "vertex", "project": "p", "region": "r", "pipeline_name": "t",
            "template_uri": "https://kfp/x/sha256:1", "template_digest": "sha256:1", "component_image": image,
            "pipeline_root": "gs://b/r", "runner_service_account": "sa", "parameters": launcher,
            "static_parameters": {}, "triggers": {}}
    target = {"pipeline_name": "t", "template_uri": "https://kfp/x/sha256:1", "template_digest": "sha256:1",
              "parameters": launcher, "static_parameters": {}, "experiment": None}
    with pytest.raises(ConfigError, match="needs synthetic.image"):
        ReleaseContract.from_dict({**base, "pipelines": {"training": target, "synthetic": target}}).validate()
    ok = ReleaseContract.from_dict({**base, "pipelines": {"training": target, "synthetic": target},
                                    "synthetic": {"image": "r/s@sha256:" + "b" * 64,
                                                  "evaluation_image": "r/e@sha256:" + "c" * 64}})
    ok.validate()
    assert ok.images_for("synthetic") == {"r/s@sha256:" + "b" * 64, "r/e@sha256:" + "c" * 64}
    assert ok.images_for("training") == {image}
    old = ReleaseContract.from_dict({**base, "pipelines": {"training": target}})     # older contracts read the same
    old.validate()
    assert old.synthetic == {}


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_compile_the_synthetic_pipeline_on_its_own_image(repo):
    from pipewright.build.compile import check_parameters, compile_pipeline, static_parameters

    synthetic_repo(repo).usecase["spec"]["steps"] = {"generate-synthetic": {"cpu": 4, "memory": "8G"}}
    uc = resolve(repo)
    image = "europe-west2-docker.pkg.dev/acme-ml-dev/ml-images/churn-synthetic@sha256:" + "1" * 64
    evaluation = image.replace("churn-synthetic@", "churn-synthetic-evaluation@").replace("1" * 64, "2" * 64)
    info = compile_pipeline(uc, "synthetic", synthetic_images=(image, evaluation),
                            steps={"generate-synthetic": uc.steps["generate-synthetic"]})
    assert info.images == {image, evaluation} and info.name == "synthetic"
    assert {"project", "region", "source_uri", "run_id", "ml_config"} == set(info.parameters)
    # (alone, the template does not take the training pipeline's parameters: only those problems remain)
    assert all(p.startswith("parameters.") for p in check_parameters(uc, {"synthetic": info})[0])
    assert static_parameters(uc, info)["ml_config"]["synthetic"]["output_table"] == "synthetic_customers"
    doc = yaml.safe_load(info.spec_path.read_text(encoding="utf-8"))
    assert set(info.tasks) >= {"prepare-synthetic", "generate-synthetic", "evaluate-synthetic", "select-synthetic",
                               "publish-synthetic"}
    res = doc["deploymentSpec"]["executors"]["exec-generate-synthetic"]["container"]["resources"]
    assert res["resourceCpuLimit"] == "4"


# --------------------------------------------------------------------- CLI --

def test_cli_generate_contract_rows_and_show_gates(repo, capsys):
    import csv

    from pipewright.cli.main import main

    synthetic_repo(repo, purpose="sharing", gates={"dcr_ratio": {"min": 1.2}},
                   columns={"snapshot_date": {"rule": "time", "from": "2025-01-01", "to": "2025-03-31",
                                              "step": "month_end"}})
    write(repo)
    out = repo.root / "out" / "rows.csv"
    assert main(["synthetic", "generate", "--root", str(repo.root), "--env", "dev", "--usecase", "churn",
                 "--out", str(out), "--rows", "40"]) == 0
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(rows) == 40 and rows[0]["customer_id"].startswith("SYN-")
    assert {r["snapshot_date"] for r in rows} <= {"2025-01-31", "2025-02-28", "2025-03-31"}
    assert main(["synthetic", "gates", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"]) == 0
    text = capsys.readouterr().out
    assert "purpose sharing" in text and "dcr_ratio       min 1.2  (default 1.0)" in text


def test_cli_run_synthetic_uses_the_configured_source(repo, monkeypatch):
    from pipewright.cli.main import main
    from pipewright.runs import launch

    synthetic_repo(repo)
    write(repo)
    sent = {}
    monkeypatch.setattr(launch, "run", lambda env, body: sent.update(body) or {"status": "submitted"})
    assert main(["run", "--root", str(repo.root), "--env", "dev", "--usecase", "churn", "--pipeline", "synthetic"]) == 0
    assert sent == {"usecase": "churn", "pipeline": "synthetic", "source_uri": "bq://acme-ml-dev.churn.customers"}


def test_synthetic_schedule_learns_from_the_configured_source(repo):
    synthetic_repo(repo)
    repo.usecase["spec"]["triggers"]["schedules"] = [{"name": "monthly", "cron": "0 3 1 * *",
                                                      "timezone": "Europe/London", "pipeline": "synthetic"}]
    repo.usecase["spec"]["environments"]["dev"]["triggers"] = {"schedules": [
        {"name": "monthly", "cron": "0 3 1 * *", "timezone": "Europe/London", "pipeline": "synthetic"}]}
    uc = resolve(repo)
    assert uc.triggers["schedules"][0]["source_uri"] == "bq://acme-ml-dev.churn.customers"


def test_keys_are_never_learned(repo):
    synthetic_repo(repo)
    spec = repo.usecase["spec"]
    spec["data_protection"] = {"personal_data_columns": []}
    spec["triggers"].pop("landing")                 # a features_from source is bq://, not landing files
    spec["data"]["features_from"] = {"entity": {"columns": ["customer_id"], "time_column": "snapshot_date"},
                                     "sources": [{"table": "bq://acme-ml-dev.features.profile",
                                                  "features": ["tenure_months"]}]}
    syn = resolve(repo).synthetic
    assert syn["columns"]["customer_id"] == {"rule": "key"} and "customer_id" in syn["exclude"]


# ------------------------------------------------------------------ policy --

def test_platform_policy(repo):
    synthetic_repo(repo, purpose="sharing", gates={"fidelity": {"min": 0.8}})
    repo.manifests["dev"]["approver_groups"] = {"data-protection": {"topic": "projects/p/topics/dp",
                                                                    "subscription": "projects/p/subscriptions/dp"}}
    repo.manifests["dev"]["policies"] = {"synthetic": {
        "allowed_purposes": ["test-data", "sharing"], "allowed_engines": ["mostlyai"],
        "differential_privacy_for": ["sharing"], "gates": {"fidelity": {"min": 0.88}, "inference": {"max": 0.1}},
        "required_approvers": ["data-protection"]}}
    found = problems(repo)
    assert "purpose sharing requires differential privacy in dev" in found
    synthetic_repo(repo, purpose="sharing", gates={"fidelity": {"min": 0.8}},
                   engine={"type": "mostlyai", "differential_privacy": {"epsilon": 5}})
    syn = resolve(repo).synthetic
    assert syn["gates"]["fidelity"] == {"min": 0.88}             # raised to the platform's floor
    assert syn["gates"]["inference"] == {"max": 0.1} and syn["approvers"] == ["data-protection"]
    assert [c["name"] for c in syn["candidates"]] == ["default"]  # with DP, no temperature variant


def test_platform_can_forbid_synthetic_data(repo):
    synthetic_repo(repo)
    repo.manifests["dev"]["policies"] = {"synthetic": {"allowed": False}}
    assert "synthetic data is not allowed in dev" in problems(repo)
    ml_repo(repo).usecase["spec"].pop("synthetic", None)
    resolve(repo)                                                  # a use case without it is unaffected
