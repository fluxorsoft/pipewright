"""TorchServe handler packaged into model.mar for Agent Platform prebuilt PyTorch serving.

Runs inside us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4 (Python 3.9,
TorchServe 0.12), so keep it 3.9-compatible and free of extra dependencies.

That image sets `service_envelope=json`: TorchServe unwraps the Agent Platform request

    {"instances": [[12, 70.5, 846.0, 1, 2, 1, 1, 64.3], ...]}

and calls the handler with one item per instance, each item being the bare
feature list, then wraps the returned list as {"predictions": [...]}. So the
handler must return exactly one value (the churn probability) per item.

Without the envelope (plain TorchServe), an item is {"body": <request JSON>};
that form is accepted too and yields a list of probabilities for that request.
Feature order is recorded in features.json (packaged alongside the model).

Online features (model.serving.features online, Pipewright 1.1): serving.json names the tenant's online store, its
feature views and the entity key. An instance is then an entity ID ("c-42" or {"customer_id": "c-42"}); the
handler reads each view's latest values at the regional Agent Platform endpoint, as the endpoint's service
account, and builds the row in features.json order. A missing value fails the request, never a silent default.
Each request logs which views served it, never the IDs or the values.
"""

import json
import os
import urllib.request

import torch
from ts.torch_handler.base_handler import BaseHandler

_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"


def _token() -> str:
    request = urllib.request.Request(_TOKEN_URL, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=10) as r:
        return json.loads(r.read())["access_token"]


def _value(v: dict):
    for kind in ("doubleValue", "int64Value", "boolValue", "stringValue"):
        if kind in v:
            return float(v[kind]) if kind != "stringValue" else v[kind]
    return None


def fetch(store: str, view: str, key: str, token: str) -> dict:
    """{feature name: value} of one entity in one feature view (Bigtable online serving, KEY_VALUE)."""
    region = store.split("/")[3]
    url = f"https://{region}-aiplatform.googleapis.com/v1/{store}/featureViews/{view}:fetchFeatureValues"
    body = json.dumps({"dataKey": {"key": str(key)}, "dataFormat": "KEY_VALUE"}).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as r:
        found = json.loads(r.read())
    return {f["name"]: _value(f.get("value") or {}) for f in (found.get("keyValues") or {}).get("features") or []}


def online_row(entity, online: dict, features: list, token: str, fetcher=fetch) -> list:
    """The model's input row for one entity ID, in features.json order."""
    key = entity.get(online["key"]) if isinstance(entity, dict) else entity
    if key is None or isinstance(key, (list, dict)):
        raise ValueError(f"an instance is an entity ID or {{\"{online['key']}\": ID}} with online features")
    values = {}
    for view in online["views"]:
        got = fetcher(online["store"], view["view"], key, token)
        for name, fetched in view["features"].items():
            values[name] = got.get(fetched)
    missing = [f for f in features if values.get(f) is None]
    if missing:
        raise ValueError(f"the online store has no value of {', '.join(missing)} for this entity")
    return [float(values[f]) for f in features]


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return value


class TabularChurnHandler(BaseHandler):
    def initialize(self, context):
        super().initialize(context)
        model_dir = context.system_properties.get("model_dir") or "."
        self._features, self._online = None, None
        path = os.path.join(model_dir, "serving.json")
        if os.path.isfile(path):
            with open(path) as f:
                self._online = json.load(f).get("online_features")
            with open(os.path.join(model_dir, "features.json")) as f:
                self._features = json.load(f)

    def preprocess(self, data):
        if getattr(self, "_online", None):
            return self._preprocess_online(data)
        return self._preprocess_rows(data)

    def _preprocess_online(self, data):
        self._layout, rows, token = [], [], _token()
        for item in data:
            item = _decode(item)
            if isinstance(item, dict) and ("data" in item or "body" in item):
                item = _decode(item.get("data", item.get("body")))
            if isinstance(item, dict) and "instances" in item:
                item = item["instances"]
            entities = item if isinstance(item, list) else [item]
            self._layout.append((len(entities), not isinstance(item, list)))
            rows.extend(online_row(e, self._online, self._features, token) for e in entities)
        print(json.dumps({"severity": "INFO", "message": "online features",
                          "views": [v["view"] for v in self._online["views"]], "entities": len(rows)}), flush=True)
        return torch.tensor(rows, dtype=torch.float32)

    def _preprocess_rows(self, data):
        # For each item: (number of rows, whether it was a single instance).
        self._layout = []
        rows = []
        for item in data:
            item = _decode(item)
            if isinstance(item, dict):
                item = _decode(item.get("data", item.get("body", item)))
            if isinstance(item, dict):
                item = item.get("instances", [])
            if item and isinstance(item[0], (list, tuple)):
                self._layout.append((len(item), False))
                rows.extend(item)
            else:
                self._layout.append((1, True))
                rows.append(item)
        return torch.tensor(rows, dtype=torch.float32)

    def inference(self, data, *args, **kwargs):
        with torch.no_grad():
            return self.model(data.to(self.device))

    def postprocess(self, data):
        probabilities = data.reshape(-1).cpu().tolist()
        out, start = [], 0
        for size, single in self._layout:
            chunk = probabilities[start : start + size]
            out.append(chunk[0] if single else chunk)
            start += size
        return out
