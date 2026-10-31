"""FR-64 (Pipewright 1.2.0): the examples are real YAML with readable sample values, schema-checked as they stand,
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


# ------------------------------------------------------------------------- FR-64 (b, d): churn shows its defaults --

def _default_lines():
    """Every `# default <key>: <value>` line in the churn example, as {dotted key: value}."""
    import re

    out = {}
    for path in sorted((nu.examples_dir() / "churn").rglob("*.yaml")):
        for line in path.read_text(encoding="utf-8").splitlines():
            m = re.match(r"\s*# default ([a-z_.]+): (.+?)(\s{2,}#.*)?$", line)
            if m:
                out[m.group(1)] = yaml.safe_load(m.group(2))
    return out


def _resolver_default(key: str):
    """What the resolver fills in for a key left out: its default tables, or (keys that exist only when set) the
    value their absence means."""
    from pipewright.config import ResolvedUseCase, ml, resolve

    absent = {"model.serving.features": "request", "model.serving.fill_missing": True,
              "model.serving.scale_to_zero": False, "model.serving.accelerator": None,
              "data.label_source": None, "monitoring.performance.labels.time_column": None,
              "components.base_image": ResolvedUseCase.__dataclass_fields__["base_image"].default,
              "tests.on_release": ResolvedUseCase.__dataclass_fields__["tests_on_release"].default,
              "tests.environment": ResolvedUseCase.__dataclass_fields__["tests_environment"].default}
    if key in absent:
        return absent[key]
    head, *rest = key.split(".")
    table = {"model": {"serving": resolve._SERVING_DEFAULTS}, "approvals": resolve._APPROVAL_DEFAULTS}.get(head) \
        or ml.DEFAULTS[head]
    for part in rest:
        table = table[part]
    return table


def test_churn_shows_every_default_it_leaves_out():
    lines = _default_lines()
    assert {"model.serving.replicas", "approvals.approvers", "tests.on_release", "components.base_image"} <= set(lines)
    wrong = {k: (v, _resolver_default(k)) for k, v in lines.items() if v != _resolver_default(k)}
    assert not wrong, f"`# default` lines that are not the resolver's defaults: {wrong}"


def test_a_shown_default_is_not_also_set():
    doc = load_usecase_document(nu.examples_dir() / "churn" / "usecase.yaml").doc["spec"]
    for key in _default_lines():
        node = doc
        for part in key.split("."):
            node = node.get(part) if isinstance(node, dict) else None
        assert node is None, f"{key} is set in the churn example and also shown as a default"
