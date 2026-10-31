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
feature views and the entity key; the serving container's PIPEWRIGHT_ONLINE_FEATURES, when set, wins over it
(Pipewright 1.2: a promoted model reads the destination's store). An instance is then an entity ID ("c-42" or
{"customer_id": "c-42"}); the handler reads each view's latest values at the regional Agent Platform endpoint, as
the endpoint's service account, and builds the row in features.json order.

Empty values (Pipewright 1.2, model.serving.fill_missing, on by default): an empty or non-numeric feature, in a
request row or from the online store, is filled with the median the model was trained with (serving.json), exactly
as batch scoring fills it. A value that cannot be filled (no median, or fill_missing false) makes that instance's
prediction {"error": "... <feature> ..."}; the other instances are still scored. Models packaged before 1.2 carry no
medians and behave as before. Each request logs counts only, never the IDs or the values.
"""

import json
import os
import urllib.error
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
    try:
        with urllib.request.urlopen(request, timeout=10) as r:
            found = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise ValueError(f"the online store has no entity with this ID (view {view})") from None
        raise ValueError(f"the online store answered HTTP {e.code} (view {view})") from None
    return {f["name"]: _value(f.get("value") or {}) for f in (found.get("keyValues") or {}).get("features") or []}


def _number(value):
    """A float, or None for an empty or non-numeric value (null, "", "n/a", NaN)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if number != number else number          # NaN


def fill_row(values: dict, features: list, medians=None, where: str = "the request") -> list:
    """The row in features.json order. An empty value takes its training median when `medians` is given, as batch
    scoring's fill_missing does; without a median it is an error naming the feature."""
    row, missing = [], []
    for name in features:
        number = _number(values.get(name))
        if number is None and medians is not None:
            number = _number(medians.get(name))
        if number is None:
            missing.append(name)
        row.append(number)
    if missing:
        raise ValueError(f"{where} has no usable value of {', '.join(missing)}"
                         + ("" if medians is None else " and the model has no median for it"))
    return row


def online_row(entity, online: dict, features: list, token: str, fetcher=fetch, medians=None) -> list:
    """The model's input row for one entity ID, in features.json order."""
    key = entity.get(online["key"]) if isinstance(entity, dict) else entity
    if key is None or isinstance(key, (list, dict)):
        raise ValueError(f"an instance is an entity ID or {{\"{online['key']}\": ID}} with online features")
    values = {}
    for view in online["views"]:
        got = fetcher(online["store"], view["view"], key, token)
        for name, fetched in view["features"].items():
            values[name] = got.get(fetched)
    return fill_row(values, features, medians, where="the online store, for this entity,")


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        value = json.loads(value)
    return value


def _decode_id(value):
    """As _decode, but a bare entity ID ("C00001") is not JSON: it stays the ID."""
    try:
        return _decode(value)
    except ValueError:
        return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value


class TabularChurnHandler(BaseHandler):
    def initialize(self, context):
        super().initialize(context)
        model_dir = context.system_properties.get("model_dir") or "."
        self._features, self._online, self._medians = None, None, None
        serving = {}
        if os.path.isfile(os.path.join(model_dir, "serving.json")):
            with open(os.path.join(model_dir, "serving.json")) as f:
                serving = json.load(f)
        if os.path.isfile(os.path.join(model_dir, "features.json")):
            with open(os.path.join(model_dir, "features.json")) as f:
                self._features = json.load(f)
        self._online = serving.get("online_features")
        if serving.get("fill_missing", True) and serving.get("medians"):     # packaged from Pipewright 1.2
            self._medians = serving["medians"]
        # The environment's own lookup on the serving container wins over the one packaged at training (FR-76): a
        # promoted model's files name the store of the environment it was trained in.
        if os.environ.get("PIPEWRIGHT_ONLINE_FEATURES"):
            self._online = json.loads(os.environ["PIPEWRIGHT_ONLINE_FEATURES"])

    def preprocess(self, data):
        if getattr(self, "_online", None):
            return self._preprocess_online(data)
        return self._preprocess_rows(data)

    def _preprocess_online(self, data):
        # An entity that cannot be served gets {"error": ...} as its prediction; the others are still scored.
        self._layout, rows, self._errors, token = [], [], {}, _token()
        for item in data:
            item = _decode_id(item)
            if isinstance(item, dict) and ("data" in item or "body" in item):
                item = _decode_id(item.get("data", item.get("body")))
            if isinstance(item, dict) and "instances" in item:
                item = item["instances"]
            entities = item if isinstance(item, list) else [item]
            self._layout.append((len(entities), not isinstance(item, list)))
            for e in entities:
                try:
                    rows.append(online_row(e, self._online, self._features, token,
                                           medians=getattr(self, "_medians", None)))
                except Exception as err:  # noqa: BLE001 - reported per entity, without its ID or values
                    self._errors[len(rows)] = str(err)
                    rows.append([0.0] * len(self._features))
        print(json.dumps({"severity": "INFO", "message": "online features",
                          "views": [v["view"] for v in self._online["views"]], "entities": len(rows),
                          "not_served": len(self._errors)}), flush=True)
        return torch.tensor(rows, dtype=torch.float32)

    def _preprocess_rows(self, data):
        # For each item: (number of rows, whether it was a single instance).
        self._layout, self._errors = [], {}
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
        features = getattr(self, "_features", None)
        if features:      # empty values filled as training did; a row that cannot be filled is that row's error
            prepared = []
            for i, row in enumerate(rows):
                try:
                    if len(row) != len(features):
                        raise ValueError(f"an instance has {len(row)} values, the model takes {len(features)} "
                                         f"({', '.join(features)})")
                    values = dict(zip(features, row))       # noqa: B905 - Python 3.9 in the serving image; lengths checked above
                    prepared.append(fill_row(values, features, getattr(self, "_medians", None),
                                             where="the instance"))
                except ValueError as err:
                    self._errors[i] = str(err)
                    prepared.append([0.0] * len(features))
            rows = prepared
        return torch.tensor(rows, dtype=torch.float32)

    def inference(self, data, *args, **kwargs):
        with torch.no_grad():
            return self.model(data.to(self.device))

    def postprocess(self, data):
        probabilities = data.reshape(-1).cpu().tolist()
        for i, why in (getattr(self, "_errors", None) or {}).items():   # instances that could not be prepared
            probabilities[i] = {"error": why}
        out, start = [], 0
        for size, single in self._layout:
            chunk = probabilities[start : start + size]
            out.append(chunk[0] if single else chunk)
            start += size
        return out
