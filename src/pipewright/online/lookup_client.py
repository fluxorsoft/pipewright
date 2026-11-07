"""Read one or more entities' features from the online store a model was given (FR-51, FR-104).

This file is copied, unchanged, next to a use case's serving handler (the churn example's
components/online_lookup.py; a test keeps the copies identical), because the serving image does not install
Pipewright. So it uses the Python standard library only and runs on Python 3.9 (the Agent Platform PyTorch serving
image). It reads as the identity the serving container runs as, from the metadata server.

    fetch_many(online, ["c-1", "c-2"]) -> {"c-1": {"tenure": 12.0, ...}, "c-2": {...}}

`online` is the lookup the model carries (serving.json, or PIPEWRIGHT_ONLINE_FEATURES): without `type` it is the
Bigtable online store of Pipewright 1.1; otherwise `bigquery`, `cloudsql` (through Pipewright's lookup service, its
`url`) or `http` (the team's own store, the same contract). An entity the store does not have is left out of the
answer; the handler then fills or refuses it. Errors name the store and the HTTP status, never an ID or a value.
"""

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

_MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
_cache = {}
_lock = threading.Lock()


def _metadata(path):
    request = urllib.request.Request(_MD + path, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(request, timeout=10) as r:
        return r.read()


def access_token():
    """An access token for Google APIs, reused for 25 minutes."""
    with _lock:
        hit = _cache.get("access")
        if hit and time.time() - hit[1] < 1500:
            return hit[0]
        token = json.loads(_metadata("/token"))["access_token"]
        _cache["access"] = (token, time.time())
        return token


def id_token(audience):
    """An ID token for a lookup service or the team's store, reused for 25 minutes."""
    with _lock:
        key = "id:" + audience
        hit = _cache.get(key)
        if hit and time.time() - hit[1] < 1500:
            return hit[0]
        token = _metadata("/identity?audience=" + urllib.parse.quote(audience, safe="")).decode("utf-8")
        _cache[key] = (token, time.time())
        return token


def _post(url, body, token, timeout, what):
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST", headers={
        "Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise ValueError("the online store ({}) answered HTTP {}".format(what, e.code)) from None
    except (urllib.error.URLError, OSError) as e:
        raise ValueError("the online store ({}) did not answer: {}".format(what, type(e).__name__)) from None


def _value(v):
    for kind in ("doubleValue", "int64Value", "boolValue", "stringValue"):
        if kind in v:
            return float(v[kind]) if kind not in ("stringValue", "boolValue") else v[kind]
    return None


def _bigtable(online, ids):
    """Pipewright 1.1: one call per entity and feature view."""
    out = {}
    region = online["store"].split("/")[3]
    for entity in ids:
        values = {}
        for view in online["views"]:
            url = "https://{}-aiplatform.googleapis.com/v1/{}/featureViews/{}:fetchFeatureValues".format(
                region, online["store"], view["view"])
            try:
                found = _post(url, {"dataKey": {"key": str(entity)}, "dataFormat": "KEY_VALUE"}, access_token(), 10,
                              "view " + view["view"])
            except ValueError as e:
                if "HTTP 404" in str(e):
                    values = None
                    break
                raise
            got = {f["name"]: _value(f.get("value") or {})
                   for f in (found.get("keyValues") or {}).get("features") or []}
            for name, fetched in view["features"].items():
                values[name] = got.get(fetched)
        if values is not None:
            out[str(entity)] = values
    return out


def _bigquery(online, ids):
    """One query for all the entities, served by BI Engine (short query mode, the result cache off)."""
    names = online["features"]
    sql = "SELECT {key}, {cols} FROM `{table}` WHERE {key} IN UNNEST(@ids)".format(
        key=online["key"], cols=", ".join(names), table=online["table"])
    body = {"query": sql, "useLegacySql": False, "useQueryCache": False, "location": online["location"],
            "parameterMode": "NAMED", "timeoutMs": online.get("timeout_ms", 1000),
            "queryParameters": [{"name": "ids", "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
                                 "parameterValue": {"arrayValues": [{"value": str(i)} for i in ids]}}],
            "jobCreationMode": "JOB_CREATION_OPTIONAL"}
    url = "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries".format(online["project"])
    got = _post(url, body, access_token(), max(2, online.get("timeout_ms", 1000) / 1000 + 1), "BigQuery")
    if not got.get("jobComplete", True):
        raise ValueError("the online store (BigQuery) did not answer within {} ms".format(online.get("timeout_ms")))
    out = {}
    for row in got.get("rows") or []:
        cells = [c.get("v") for c in row["f"]]
        out[str(cells[0])] = {n: (float(v) if v is not None else None) for n, v in zip(names, cells[1:])}
    return out


def _http(online, ids):
    """Pipewright's lookup service (cloudsql) or the team's store (http): POST /lookup {entity_ids}."""
    url = online["url"].rstrip("/") + "/lookup"
    got = _post(url, {"entity_ids": [str(i) for i in ids]}, id_token(online.get("audience") or online["url"]),
                online.get("timeout_ms", 1000) / 1000.0, "lookup " + online.get("type", "http"))
    features = got.get("features")
    if not isinstance(features, dict):
        raise ValueError("the online store (lookup {}) answered without a features object".format(online["type"]))
    wanted = set(online.get("features") or [])
    out = {}
    for entity, values in features.items():
        if not isinstance(values, dict):
            raise ValueError("the online store (lookup {}) answered an entity without a features object".format(
                online["type"]))
        unknown = sorted(set(values) - wanted - {online.get("key"), "feature_timestamp"}) if wanted else []
        if unknown:
            raise ValueError("the online store (lookup {}) answered features the model does not take: {}".format(
                online["type"], ", ".join(unknown[:5])))
        for name in wanted:
            v = values.get(name)
            if v is not None and not isinstance(v, (int, float)):
                raise ValueError("the online store (lookup {}) answered {} as {}, not a number".format(
                    online["type"], name, type(v).__name__))
        out[str(entity)] = values
    return out


def fetch_many(online, ids):
    """{entity ID: {feature: value}} for the entities the store has."""
    kind = online.get("type", "bigtable")
    if kind == "bigtable":
        return _bigtable(online, ids)
    if kind == "bigquery":
        return _bigquery(online, ids)
    if kind == "cloudsql":
        if online.get("provider") == "own" or not online.get("url"):
            raise ValueError("this model's features are in Cloud SQL with the team's own lookup "
                             "(lookup.provider own): the team's serving code reads them")
        return _http(online, ids)
    if kind == "http":
        return _http(online, ids)
    raise ValueError("unknown online store type {}".format(kind))
