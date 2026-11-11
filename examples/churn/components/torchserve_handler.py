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
(Pipewright 1.2.0: a promoted model reads the destination's store). An instance is then an entity ID ("c-42" or
{"customer_id": "c-42"}); the handler reads each view's latest values at the regional Agent Platform endpoint, as
the endpoint's service account, and builds the row in features.json order.

Empty values (Pipewright 1.2.0, model.serving.fill_missing, on by default): an empty or non-numeric feature, in a
request row or from the online store, is filled with the median the model was trained with (serving.json), exactly
as batch scoring fills it. A value that cannot be filled (no median, or fill_missing false) makes that instance's
prediction {"error": "... <feature> ..."}; the other instances are still scored. Models packaged before 1.2.0 carry no
medians and behave as before. Each request logs counts only, never the IDs or the values.
"""

import json
import os
import urllib.error
import urllib.request

import torch
from ts.torch_handler.base_handler import BaseHandler

try:        # packaged beside the handler (train.py) for the stores of FR-104: bigquery, cloudsql, http
    import online_lookup
except ImportError:          # a model packaged before it reads the Bigtable store only
    online_lookup = None

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
        if serving.get("fill_missing", True) and serving.get("medians"):     # packaged from Pipewright 1.2.0
            self._medians = serving["medians"]
        # The environment's own lookup on the serving container wins over the one packaged at training (FR-76): a
        # promoted model's files name the store of the environment it was trained in.
        # On a shared pool (Pipewright 1.4.2, FR-105) the container's variables are the pool's, so a pool model
        # reads its settings from its own files only.
        if os.environ.get("PIPEWRIGHT_ONLINE_FEATURES") and not serving.get("pool"):
            self._online = json.loads(os.environ["PIPEWRIGHT_ONLINE_FEATURES"])
        # Behind the lookup gateway (FR-106) the gateway reads the features and sends rows in the model's order.
        if (self._online or {}).get("mode") == "gateway":
            self._online = None

    def handle(self, data, context):
        """BaseHandler's, timed: Pipewright 1.4.3 (FR-98) records each request's latency as the
        PwPredictLatencySeconds histogram (seconds: TorchServe's buckets run 5 ms to 10 s), from which KServe health
        policies take the 95th percentile. A handler of your own needs the same lines; without them the latency
        policy uses the mean."""
        import time

        started = time.perf_counter()
        out = super().handle(data, context)
        try:
            from ts.metrics.metric_type_enum import MetricTypes

            metric = context.metrics.get_metric("PwPredictLatencySeconds", MetricTypes.HISTOGRAM)
            metric.add_or_update(time.perf_counter() - started, dimension_values=[context.model_name, "Model"])
        except Exception:  # noqa: BLE001 - no metrics config with the histogram (Agent Platform, older releases)
            pass
        return out

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
            if self._online.get("type", "bigtable") != "bigtable":
                rows.extend(self._batch_rows(entities, len(rows)))
                continue
            for e in entities:
                try:
                    rows.append(online_row(e, self._online, self._features, token,
                                           medians=getattr(self, "_medians", None)))
                except Exception as err:  # noqa: BLE001 - reported per entity, without its ID or values
                    self._errors[len(rows)] = str(err)
                    rows.append([0.0] * len(self._features))
        print(json.dumps({"severity": "INFO", "message": "online features",
                          "store": self._online.get("type", "bigtable"), "entities": len(rows),
                          "not_served": len(self._errors)}), flush=True)
        return torch.tensor(rows, dtype=torch.float32)

    def _batch_rows(self, entities, offset, fetch_many=None):
        """FR-104: every entity of a request in one lookup (BigQuery, Cloud SQL through Pipewright's lookup service,
        or the team's http store), then each row filled as training did."""
        fetch_many = fetch_many or online_lookup.fetch_many
        key, rows, ids = self._online["key"], [], []
        for e in entities:
            entity = e.get(key) if isinstance(e, dict) else e
            ids.append(None if entity is None or isinstance(entity, (list, dict)) else str(entity))
        try:
            found = fetch_many(self._online, [i for i in ids if i is not None]) if any(ids) else {}
            problem = None
        except Exception as err:  # noqa: BLE001 - the store's error, without IDs or values
            found, problem = {}, str(err)
        for n, entity in enumerate(ids):
            try:
                if entity is None:
                    raise ValueError(f"an instance is an entity ID or {{\"{key}\": ID}} with online features")
                if problem:
                    raise ValueError(problem)
                values = found.get(entity)
                if values is None:
                    raise ValueError("the online store has no entity with this ID")
                rows.append(fill_row(values, self._features, getattr(self, "_medians", None),
                                     where="the online store, for this entity,"))
            except ValueError as err:
                self._errors[offset + n] = str(err)
                rows.append([0.0] * len(self._features))
        return rows

    def _preprocess_rows(self, data):
        # For each item: (number of rows, whether it was a single instance).
        self._layout, self._errors = [], {}
        rows = []
        for item in data:
            item = _decode(item)
            if isinstance(item, dict):
                item = _decode(item.get("data", item.get("body", item)))
            if isinstance(item, dict) and "row" in item:
                # Pipewright 1.4.3 (FR-98): {"_pw_prediction_id", "_pw_unit", "row"}. The IDs stay in the request
                # log (capture keeps the instance as sent) for outcome joins; the model sees the row only.
                item = item["row"]
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
