"""FR-64 (Pipewright 1.2): the examples are real YAML with readable sample values, schema-checked as they stand,
and new-usecase replaces every sample value."""
import pytest
import yaml

from pipewright.config.document import load_usecase_document
from pipewright.config.schema import validate
from pipewright.scaffold import new_usecase as nu


@pytest.mark.parametrize("example", nu.list_examples())
def test_an_example_is_valid_as_it_stands(example):
    doc = load_usecase_document(nu.examples_dir() / example / "usecase.yaml")
    validate(doc.doc, "usecase", example)


@pytest.mark.parametrize("example", nu.list_examples())
def test_new_usecase_replaces_every_sample_value(example, tmp_path):
    (tmp_path / "usecases").mkdir()
    dest = nu.new_usecase(tmp_path, "acme-uc", "team-a", "owner-b", example=example)
    meta = yaml.safe_load((nu.examples_dir() / example / "example.yaml").read_text(encoding="utf-8"))
    left = {v for path in dest.rglob("*.yaml") for v in meta["sample"].values()
            if v in path.read_text(encoding="utf-8")}
    assert not left, f"{example}: sample values left after new-usecase: {left}"
    assert yaml.safe_load((dest / "usecase.yaml").read_text(encoding="utf-8"))["metadata"]["name"] == "acme-uc"


def test_one_pass_never_replaces_twice():
    sample = {"tenant": "my-tenant", "owner": "my-team"}
    assert nu.render("t: my-tenant\no: my-team\n", sample, {"tenant": "my-team", "owner": "x"}) == "t: my-team\no: x\n"
