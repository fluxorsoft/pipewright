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
              "model.serving.pool": None,
              "model.serving.drain_seconds": __import__("pipewright.models.deploy", fromlist=["x"]).DRAIN_SECONDS,
              "data.label_source": None, "monitoring.performance.labels.time_column": None,
              "data.snapshot_table": None, "data.label.task": "binary", "evaluation.fairness": None,
              "feature_contract": None,
              "components.base_image": ResolvedUseCase.__dataclass_fields__["base_image"].default,
              "tests.on_release": ResolvedUseCase.__dataclass_fields__["tests_on_release"].default,
              "tests.environment": ResolvedUseCase.__dataclass_fields__["tests_environment"].default}
    if key in absent:
        return absent[key]
    if key.startswith("monitoring.online."):           # 1.4.3 (FR-98): serving_config's defaults
        from pipewright.monitoring import serving_config

        table = serving_config.ONLINE_DEFAULTS
        for part in key.split(".")[2:]:
            table = table[part]
        return table
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


# ---------------------------------------------- FR-99 (phase 6): the reference example shows the recommended feed --

def test_the_churn_readme_shows_how_to_read_its_features_from_feature_tables():
    import re

    readme = (nu.examples_dir() / "churn" / "README.md").read_text(encoding="utf-8")
    head = readme.split("## The recommended way to feed it: feature tables", 1)
    assert len(head) == 2 and "(../features/)" in head[1], "the churn README must point to feature tables up front"
    block = yaml.safe_load(re.search(r"```yaml\n(.*?)```", head[1], re.S).group(1))["data"]
    data = yaml.safe_load((nu.examples_dir() / "churn" / "config" / "data.yaml").read_text(encoding="utf-8"))["data"]
    contract = {c["name"] for c in data["contract"]["columns"]}
    entity = set(block["features_from"]["entity"]["columns"]) | {block["features_from"]["entity"]["time_column"]}
    from_tables = {f for s in block["features_from"]["sources"] for f in s["features"]}
    assert from_tables | entity | {data["label"]["column"]} == contract      # every churn column has a source
    assert block["split"] == {"strategy": "time", "time_column": "snapshot_date"}


# ------------------------------------------------- 1.4.2: the serving example shows every serving default --

def test_the_serving_example_shows_true_defaults():
    import re

    from pipewright.config import resolve
    from pipewright.online import config as online_config
    from pipewright.online import sizing

    expected = {"model.serving.target": resolve._SERVING_DEFAULTS["target"],
                "model.serving.machine": resolve._SERVING_DEFAULTS["machine"],
                "model.serving.canary_percent": 10, "model.serving.pool": None,
                "model.serving.online_store.lookup.retries": online_config.GATEWAY_RETRIES,
                "model.serving.online_store.lookup.provider": "pipewright",
                "model.serving.online_store.sync.mode": "full", "model.serving.online_store.export.enabled": False,
                **{f"model.serving.online_store.lookup.{k}": v for k, v in sizing.LOOKUP_DEFAULTS.items()}}
    shown = {}
    text = (nu.examples_dir() / "serving" / "usecase.yaml").read_text(encoding="utf-8")
    for line in text.splitlines():
        m = re.match(r"\s*# default ([a-z_.]+): (.+?)(\s{2,}#.*)?$", line)
        if m and "<" not in m.group(2):
            shown[m.group(1)] = yaml.safe_load(m.group(2))
    assert set(expected) <= set(shown), f"not shown: {set(expected) - set(shown)}"
    wrong = {k: (v, expected[k]) for k, v in shown.items() if k in expected and v != expected[k]}
    assert not wrong, wrong
    assert "TOGGLE" in text and "mode: gateway" in text


def test_a_replay_that_reproduces_the_champion_is_not_registered():
    """Found re-running the book's Chapter 5 on 1.4.2: an exact replay ties its own model and passed the champion
    rule, registering a copy. The register step leaves it out and says why."""
    text = (nu.examples_dir() / "churn" / "components" / "register.py").read_text(encoding="utf-8")
    assert 'source_uri.startswith("snapshot://") and new is not None and old is not None and new == old' in text
    assert "nothing is registered" in text
