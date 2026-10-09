"""Making the YAML easier: editor schemas and modelines, `pipewright explain`, the minimal example, and a data
contract drafted from a BigQuery table."""

import importlib.util
import json

import pytest
import yaml
from jsonschema import Draft202012Validator

from pipewright.cli.main import main
from pipewright.config import load_environment, reference, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.scaffold import from_bq
from pipewright.scaffold.new_usecase import list_examples, new_usecase

# ---------------------------------------------------------------- the reference


def test_every_key_is_described_and_every_description_has_a_key():
    keys = [k for k in reference.all_keys() if not k.endswith("<name>")]
    text = reference.descriptions()
    assert [k for k in keys if k not in text] == []
    assert [k for k in text if k not in reference.all_keys()] == []


def test_editor_schemas_are_valid_and_carry_descriptions():
    schemas = reference.editor_schemas()
    for schema in schemas.values():
        Draft202012Validator.check_schema(schema)
    part = Draft202012Validator(schemas["usecase-part.schema.json"])
    assert not list(part.iter_errors({"evaluation": {"gates": [{"metric": "auc", "min": 0.7}]}}))
    assert list(part.iter_errors({"evaluaton": {}}))                           # a misspelt section is underlined
    overrides = Draft202012Validator(schemas["usecase-overrides.schema.json"])
    assert not list(overrides.iter_errors({"training": {"hyperparameters": {"epochs": 2}}}))
    assert list(overrides.iter_errors({"data": {}}))                           # data is fixed across environments
    split = schemas["usecase.schema.json"]["$defs"]["data"]["properties"]["split"]["properties"]["strategy"]
    assert "stratified" in split["description"]


def test_explain_a_key():
    e = reference.explain("data.split.strategy")
    assert e.allowed == ["random", "stratified", "time"] and e.default == "stratified"
    assert not e.per_environment
    assert reference.explain("spec.training.tuning.max_trials").per_environment
    assert reference.explain("steps.train-model.cpu").type == "number or string"   # any step name
    gates = reference.explain("evaluation.gates")
    assert gates.type == "list of mappings" and [n for n, _ in gates.children] == ["metric", "min", "max"]
    assert "(default 7)" in dict(reference.explain("retraining").children)["cooldown_days"]
    with pytest.raises(ConfigError, match="did you mean data.split.strategy"):
        reference.explain("data.splt.strategy")


def test_explain_command(capsys):
    assert main(["explain", "monitoring.drift.threshold"]) == 0
    out = capsys.readouterr().out
    assert "default:   0.2" in out and "limits:    above 0" in out
    assert main(["explain", "--list"]) == 0
    assert "data.contract.columns.type" in capsys.readouterr().out.splitlines()


# ---------------------------------------------------------------- modelines


def test_new_usecase_points_every_yaml_file_at_its_schema(repo):
    repo.write()
    dest = new_usecase(repo.root, "churn-nimbus", "churn", "team-nimbus")
    schemas = repo.root / reference.SCHEMAS_DIR
    assert sorted(p.name for p in schemas.glob("*.json")) == sorted(reference.EDITOR_SCHEMAS.values())
    first = {p.relative_to(dest).as_posix(): p.read_text(encoding="utf-8").splitlines()[0]
             for p in dest.rglob("*.yaml")}
    assert first["usecase.yaml"] == reference.MODELINE + "../../.pipewright-schemas/usecase.schema.json"
    assert first["config/data.yaml"] == reference.MODELINE + "../../../.pipewright-schemas/usecase-part.schema.json"
    assert first["environments/dev.yaml"] == (reference.MODELINE
                                              + "../../../.pipewright-schemas/usecase-overrides.schema.json")
    # Every generated file is valid against the schema it names.
    validators = {n: Draft202012Validator(s) for n, s in reference.editor_schemas().items()}
    for rel, line in first.items():
        doc = yaml.safe_load((dest / rel).read_text(encoding="utf-8"))
        assert not list(validators[line.rsplit("/", 1)[1]].iter_errors(doc)), rel


def test_schemas_command_updates_the_repo(repo, capsys):
    repo.write()
    env_file = repo.root / "environments" / "dev.yaml"
    assert main(["schemas", "--root", str(repo.root)]) == 0
    assert env_file.read_text(encoding="utf-8").startswith(
        reference.MODELINE + "../.pipewright-schemas/environment.schema.json\n")
    before = env_file.read_text(encoding="utf-8")
    assert main(["schemas", "--root", str(repo.root)]) == 0                    # idempotent
    assert env_file.read_text(encoding="utf-8") == before
    assert "added or updated in 0 of" in capsys.readouterr().out.splitlines()[-1]
    load_environment(repo.root, "dev")                                       # still loads: it is a comment


# ---------------------------------------------------------------- the minimal example


def test_minimal_example_resolves_with_defaults(repo):
    assert "minimal" in list_examples()
    repo.write()
    dest = new_usecase(repo.root, "churn-lite", "churn", "team-nimbus", example="minimal")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    assert set(uc.pipelines) == {"training"}
    assert set(uc.tables) == {"churn_lite_raw_customers", "churn_lite_model_metrics"}
    assert uc.ml["evaluation"]["gates"] == [{"metric": "auc", "min": 0.7}]
    assert uc.ml["training"]["tuning"]["enabled"] is False                   # a default
    assert "tenure_months" in uc.ml["features"]["include"] and "customer_id" not in uc.ml["features"]["include"]
    assert (dest / "components" / "train.py").is_file() and (dest / "data" / "sample_customers.csv").is_file()
    assert not (dest / "inference_pipeline.py").exists() and not (dest / "example.yaml").exists()


@pytest.mark.skipif(importlib.util.find_spec("kfp") is None, reason="kfp not installed (gcp extra)")
def test_minimal_example_compiles(repo):
    from pipewright.build.compile import check_parameters, compile_all

    repo.write()
    dest = new_usecase(repo.root, "churn-lite", "churn", "team-nimbus", example="minimal")
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    infos = compile_all(uc)
    assert check_parameters(uc, infos) == ([], [])


# ---------------------------------------------------------------- drafted from BigQuery

FIELDS = [
    {"name": "account_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "as_of", "type": "DATE"},
    {"name": "visits", "type": "INT64"},
    {"name": "spend", "type": "NUMERIC"},
    {"name": "region", "type": "STRING"},
    {"name": "tags", "type": "STRING", "mode": "REPEATED"},
    {"name": "converted", "type": "INTEGER"},
]


def test_draft_from_a_table_schema():
    text, raw, notes = from_bq.draft(FIELDS, "converted", [0, 1], "p-123456.sales.accounts")
    doc = yaml.safe_load(text)
    cols = {c["name"]: c for c in doc["data"]["contract"]["columns"]}
    assert cols["account_id"] == {"name": "account_id", "type": "string", "required": True}
    assert cols["spend"]["type"] == "float" and cols["as_of"]["type"] == "date"
    assert cols["converted"]["allowed"] == [0, 1] and "tags" not in cols
    assert doc["data"]["contract"]["allow_extra_columns"] is True
    assert doc["data"]["split"] == {"strategy": "time", "time_column": "as_of"}
    assert doc["features"]["exclude"] == ["account_id", "region"]
    assert doc["data_protection"]["personal_data_columns"] == []
    assert [f["name"] for f in raw] == ["account_id", "as_of", "visits", "spend", "region", "converted",
                                        "run_id", "source_uri", "ingested_at"]
    assert raw[3]["type"] == "NUMERIC" and raw[2]["type"] == "INTEGER"
    assert any("account_id looks like an identifier" in n for n in notes) and "features: visits, spend" in notes


def test_draft_refuses_a_label_the_model_cannot_learn():
    with pytest.raises(ConfigError, match="not a 0/1 label"):
        from_bq.draft(FIELDS, "region", ["north", "south"], "p-123456.sales.accounts")
    with pytest.raises(ConfigError, match="not a 0/1 label"):
        from_bq.draft(FIELDS, "visits", None, "p-123456.sales.accounts")
    with pytest.raises(ConfigError, match="project.dataset.table"):
        from_bq.parse_ref("accounts")


def test_new_usecase_from_bq(repo, monkeypatch, capsys):
    repo.write()
    monkeypatch.setattr(from_bq, "read_table", lambda gcp, ref, label: (FIELDS, [0, 1]))
    monkeypatch.setattr("pipewright.gcp.Gcp", lambda: None)
    assert main(["new-usecase", "--root", str(repo.root), "--name", "sales-convert", "--tenant", "churn",
                 "--owner", "team-sales", "--from-bq", "p-123456.sales.accounts", "--label", "converted"]) == 0
    out = capsys.readouterr().out
    assert "from example minimal, data contract drafted from p-123456.sales.accounts" in out
    assert "--source-uri bq://p-123456.sales.accounts" in out
    dest = repo.root / "usecases" / "sales-convert"
    uc = resolve_usecase(dest / "usecase.yaml", load_environment(repo.root, "dev"))
    assert uc.ml["data"]["label"]["column"] == "converted"
    assert uc.ml["features"]["include"] == ["visits", "spend"]
    assert not (dest / "data").exists()                                     # churn's sample does not fit
    raw = json.loads((dest / "schemas" / "raw_customers.json").read_text(encoding="utf-8"))
    assert raw[0]["name"] == "account_id"
    assert (dest / "config" / "data.yaml").read_text(encoding="utf-8").startswith(reference.MODELINE)

    assert main(["new-usecase", "--root", str(repo.root), "--name", "sales-two", "--tenant", "churn",
                 "--owner", "x", "--from", "churn", "--from-bq", "p-123456.sales.accounts", "--label", "c"]) == 2
    assert "drafts the minimal example" in capsys.readouterr().err
