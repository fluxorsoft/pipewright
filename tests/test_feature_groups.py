"""Phase 6 (FR-75): pipewright features register, the data team's way to register feature groups."""

from types import SimpleNamespace

import pytest

from pipewright.data import feature_groups
from pipewright.errors import ConfigError

ENV = SimpleNamespace(project="acme-ml-dev", region="europe-west2", name="dev")
SPEC = """
customer_activity:
  table: pw_retail.customer_activity
  entity: [customer_id]
  description: Monthly purchase behaviour per customer
  features:
    recency_days: Days since the last purchase
    orders_12m: Orders in the last twelve months
"""


class FakeFeatureStore:
    def __init__(self, groups=None):
        self.groups = groups or {}                 # name -> {"source": uri, "features": set()}
        self.posted = []

    def get(self, url, **params):
        if "/operations/" in url:
            return SimpleNamespace(ok=True, status=200, body={"done": True})
        name = url.split("/featureGroups/", 1)[1].split("/")[0]
        group = self.groups.get(name)
        if url.endswith("/features"):
            return SimpleNamespace(ok=True, status=200, body={"features": [
                {"name": f"x/featureGroups/{name}/features/{f}"} for f in sorted(group["features"])]})
        if group is None:
            return SimpleNamespace(ok=False, status=404, body={})
        source = {"bigQuery": {"bigQuerySource": {"inputUri": group["source"]}}}
        return SimpleNamespace(ok=True, status=200, body=source)

    def post(self, url, body, **params):
        self.posted.append((url.rsplit("/", 1)[-1], params, body))
        if url.endswith("/featureGroups"):
            self.groups[params["featureGroupId"]] = {"source": body["bigQuery"]["bigQuerySource"]["inputUri"],
                                                     "features": set()}
        else:
            self.groups[url.split("/featureGroups/", 1)[1].split("/")[0]]["features"].add(params["featureId"])
        return SimpleNamespace(ok=True, status=200, body={"name": "operations/1", "done": True})


def spec(tmp_path, text=SPEC):
    path = tmp_path / "feature_groups.yaml"
    path.write_text(text, encoding="utf-8")
    return feature_groups.load_spec(path)


def test_registers_the_group_and_its_features_then_leaves_them_be(tmp_path):
    store, said = FakeFeatureStore(), []
    done = feature_groups.register(store, ENV, spec(tmp_path), log=said.append, poll=0)
    assert done == [{"feature_group": "customer_activity", "table": "bq://acme-ml-dev.pw_retail.customer_activity",
                     "existed": False, "features_added": ["recency_days", "orders_12m"]}]
    assert [p[0] for p in store.posted] == ["featureGroups", "features", "features"]
    assert store.posted[0][2]["bigQuery"]["entityIdColumns"] == ["customer_id"]
    again = feature_groups.register(store, ENV, spec(tmp_path), log=said.append, poll=0)
    assert again[0]["existed"] and again[0]["features_added"] == [] and len(store.posted) == 3
    assert "all registered already" in said[-1]


def test_a_dry_run_creates_nothing(tmp_path):
    store, said = FakeFeatureStore(), []
    done = feature_groups.register(store, ENV, spec(tmp_path), dry_run=True, log=said.append, poll=0)
    assert store.posted == [] and done[0]["features_added"] == ["recency_days", "orders_12m"]
    assert "would be created" in said[0]


def test_an_existing_group_over_another_table_is_never_changed(tmp_path):
    store = FakeFeatureStore({"customer_activity": {"source": "bq://other.ds.t", "features": set()}})
    with pytest.raises(ConfigError, match="exists over bq://other.ds.t"):
        feature_groups.register(store, ENV, spec(tmp_path), log=lambda m: None, poll=0)
    assert store.posted == []


def test_the_spec_is_checked_before_anything_is_created(tmp_path):
    with pytest.raises(ConfigError) as e:
        spec(tmp_path, "bad-name:\n  table: t\n  features: [a]\n")
    text = str(e.value)
    assert "entity is required" in text and "dataset.table" in text and "maps each feature" in text
    with pytest.raises(ConfigError, match="not in the spec file: missing"):
        feature_groups.register(FakeFeatureStore(), ENV, spec(tmp_path), ["missing"], log=lambda m: None)


def test_the_cli_command_is_there():
    from pipewright.cli.main import build_parser

    args = build_parser().parse_args(["features", "register", "--env", "dev", "--file", "f.yaml", "--dry-run"])
    assert args.features_command == "register" and args.dry_run and args.names == []
