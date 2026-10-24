"""FR-51: online feature lookup (model.serving.features online): config, the serving handler's rows, doctor."""
import importlib.util
import sys
import types
from pathlib import Path

import pytest
import yaml

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.scaffold.new_usecase import new_usecase

STORE = "projects/acme-ml-dev/locations/europe-west2/featureOnlineStores/churn_online"


def online_repo(repo, store=STORE, change=None):
    if store:
        repo.manifests["dev"]["tenants"]["churn"]["features"] = {"online_store": store}
    repo.write()
    dest = new_usecase(repo.root, "churn-feats", "churn", "team-feats", example="features")
    data = dest / "config" / "data.yaml"
    doc = yaml.safe_load(data.read_text(encoding="utf-8"))
    for i, src in enumerate(doc["data"]["features_from"]["sources"]):
        src["online_view"] = f"view_{i}"
    if change:
        change(doc)
    data.write_text(yaml.safe_dump(doc), encoding="utf-8")
    uc = dest / "usecase.yaml"
    spec = yaml.safe_load(uc.read_text(encoding="utf-8"))
    spec["spec"].setdefault("model", {}).setdefault("serving", {})["features"] = "online"
    uc.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    return lambda: resolve_usecase(uc, load_environment(repo.root, "dev"))


def test_online_features_name_every_view_and_the_entity_key(repo):
    uc = online_repo(repo)()
    online = uc.ml["online_features"]
    assert online["store"] == STORE and online["key"] == "customer_id"
    assert [v["view"] for v in online["views"]] == ["view_0", "view_1"]
    served = {f for v in online["views"] for f in v["features"]}
    assert set(uc.ml["features"]["include"]) <= served
    assert uc.ml_config()["online_features"] == online                 # the training step packages it


def test_online_features_need_a_store_a_view_and_every_feature(repo):
    with pytest.raises(ConfigError, match="has no online store in dev"):
        online_repo(repo, store=None)()


def test_a_table_source_must_name_its_view(repo):
    def no_view(doc):
        doc["data"]["features_from"]["sources"][0].pop("online_view")

    with pytest.raises(ConfigError, match=r"sources\[0\] reads a table: name its feature view"):
        online_repo(repo, change=no_view)()


def handler():
    torch = types.ModuleType("torch")
    base = types.ModuleType("ts.torch_handler.base_handler")
    base.BaseHandler = object
    stubs = {"torch": torch, "ts": types.ModuleType("ts"), "ts.torch_handler": types.ModuleType("ts.torch_handler"),
             "ts.torch_handler.base_handler": base}
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)
    try:
        path = Path(__file__).parents[1] / "examples" / "churn" / "components" / "torchserve_handler.py"
        spec = importlib.util.spec_from_file_location("pw_test_handler", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


ONLINE = {"store": STORE, "key": "customer_id",
          "views": [{"view": "profile", "features": {"tenure_months": "tenure_months"}},
                    {"view": "support", "features": {"s_tickets": "num_support_tickets"}}]}


def test_the_handler_builds_rows_in_feature_order_and_never_defaults():
    h = handler()
    calls = []

    def fetcher(store, view, key, token):
        calls.append((view, key))
        return {"profile": {"tenure_months": 12.0}, "support": {"num_support_tickets": 3.0}}[view]

    assert h.online_row({"customer_id": "c-1"}, ONLINE, ["s_tickets", "tenure_months"], "t", fetcher) == [3.0, 12.0]
    assert h.online_row("c-2", ONLINE, ["tenure_months"], "t", fetcher) == [12.0]
    assert calls[-1] == ("support", "c-2")
    with pytest.raises(ValueError, match="no value of s_tickets"):
        h.online_row("c-3", ONLINE, ["s_tickets"], "t", lambda *a: {})
    with pytest.raises(ValueError, match="an instance is an entity ID"):
        h.online_row([1, 2], ONLINE, ["s_tickets"], "t", fetcher)
    assert h._value({"int64Value": "7"}) == 7.0 and h._value({}) is None
