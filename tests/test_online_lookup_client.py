"""FR-104: the lookup client packaged beside a model's serving handler."""
from pathlib import Path

import pytest

from pipewright.online import lookup_client as lc
from tests.test_online_features import handler

ROOT = Path(__file__).parents[1]


def test_the_example_carries_an_identical_copy():
    core = (ROOT / "src/pipewright/online/lookup_client.py").read_bytes().replace(b"\r\n", b"\n")
    copy = (ROOT / "examples/churn/components/online_lookup.py").read_bytes().replace(b"\r\n", b"\n")
    assert core == copy, "copy src/pipewright/online/lookup_client.py to examples/churn/components/online_lookup.py"


def test_it_stays_python_39_and_stdlib_only():
    import ast

    tree = ast.parse((ROOT / "src/pipewright/online/lookup_client.py").read_text(encoding="utf-8"),
                     feature_version=(3, 9))
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    imported |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert imported <= {"json", "threading", "time", "urllib"}


def test_bigquery_reads_all_entities_in_one_query(monkeypatch):
    seen = []
    monkeypatch.setattr(lc, "access_token", lambda: "access")
    monkeypatch.setattr(lc, "_post", lambda url, body, token, timeout, what: seen.append(body) or {
        "jobComplete": True, "rows": [{"f": [{"v": "c-1"}, {"v": "12"}, {"v": None}]}]})
    online = {"type": "bigquery", "key": "customer_id", "features": ["tenure", "tickets"],
              "project": "acme-ml-dev", "location": "europe-west2", "table": "acme-ml-dev.churn.pw_online_x"}
    assert lc.fetch_many(online, ["c-1", "c-2"]) == {"c-1": {"tenure": 12.0, "tickets": None}}
    body = seen[0]
    assert "IN UNNEST(@ids)" in body["query"] and body["useQueryCache"] is False
    assert [v["value"] for v in body["queryParameters"][0]["parameterValue"]["arrayValues"]] == ["c-1", "c-2"]


def test_lookup_service_and_http_use_one_contract(monkeypatch):
    seen = []
    monkeypatch.setattr(lc, "id_token", lambda aud: "id:" + aud)
    monkeypatch.setattr(lc, "_post", lambda url, body, token, timeout, what: seen.append((url, body, token)) or {
        "features": {"c-1": {"tenure": 3, "tickets": 1.5}}})
    online = {"type": "cloudsql", "key": "customer_id", "features": ["tenure", "tickets"],
              "url": "https://pwl-churn-x.run.app", "provider": "pipewright"}
    assert lc.fetch_many(online, ["c-1"]) == {"c-1": {"tenure": 3, "tickets": 1.5}}
    assert seen[0] == ("https://pwl-churn-x.run.app/lookup", {"entity_ids": ["c-1"]}, "id:https://pwl-churn-x.run.app")


def test_http_answers_are_checked_against_the_model(monkeypatch):
    monkeypatch.setattr(lc, "id_token", lambda aud: "t")
    online = {"type": "http", "key": "k", "features": ["a"], "url": "https://x.example.com"}
    monkeypatch.setattr(lc, "_post", lambda *a: {"features": {"e": {"a": 1, "secret": 2}}})
    with pytest.raises(ValueError, match="features the model does not take: secret"):
        lc.fetch_many(online, ["e"])
    monkeypatch.setattr(lc, "_post", lambda *a: {"features": {"e": {"a": "high"}}})
    with pytest.raises(ValueError, match="answered a as str, not a number"):
        lc.fetch_many(online, ["e"])
    monkeypatch.setattr(lc, "_post", lambda *a: {"rows": []})
    with pytest.raises(ValueError, match="without a features object"):
        lc.fetch_many(online, ["e"])


def test_provider_own_is_the_teams_code():
    with pytest.raises(ValueError, match="the team's serving code reads them"):
        lc.fetch_many({"type": "cloudsql", "provider": "own", "key": "k", "features": []}, ["e"])


def test_the_handler_serves_a_batch_and_answers_per_entity():
    h = handler()
    obj = h.TabularChurnHandler()
    obj._online = {"type": "cloudsql", "key": "customer_id", "features": ["tenure", "tickets"]}
    obj._features, obj._medians, obj._errors = ["tickets", "tenure"], {"tickets": 2.0}, {}
    asked = []

    def fetch(online, ids):
        asked.append(ids)
        return {"c-1": {"tenure": 12, "tickets": None}, "c-2": {"tenure": 4, "tickets": 1}}

    rows = obj._batch_rows(["c-1", {"customer_id": "c-2"}, "c-9", [1]], 0, fetch)
    assert asked == [["c-1", "c-2", "c-9"]]                     # one lookup for the request
    assert rows[:2] == [[2.0, 12.0], [1.0, 4.0]]                # the empty value took its median
    assert obj._errors[2] == "the online store has no entity with this ID"
    assert "an instance is an entity ID" in obj._errors[3]


def test_a_store_error_is_every_entitys_answer_without_ids():
    h = handler()
    obj = h.TabularChurnHandler()
    obj._online = {"type": "http", "key": "k", "features": ["a"]}
    obj._features, obj._medians, obj._errors = ["a"], None, {}

    def down(online, ids):
        raise ValueError("the online store (lookup http) answered HTTP 503")
    obj._batch_rows(["e1", "e2"], 5, down)
    assert obj._errors == {5: "the online store (lookup http) answered HTTP 503",
                           6: "the online store (lookup http) answered HTTP 503"}


def test_the_lookup_service_url_is_read_once(monkeypatch):
    import io

    reads = []

    class R(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(lc, "access_token", lambda: "a")
    monkeypatch.setattr(lc.urllib.request, "urlopen", lambda req, timeout=None: reads.append(req.full_url) or R(
        b'{"uri": "https://pwl-churn-x-123.europe-west2.run.app"}'))
    online = {"type": "cloudsql", "service": "pwl-churn-x", "project": "acme-ml-dev", "region": "europe-west2"}
    lc._cache.pop("url:pwl-churn-x", None)
    assert lc.service_url(online) == lc.service_url(online) == "https://pwl-churn-x-123.europe-west2.run.app"
    assert reads == ["https://run.googleapis.com/v2/projects/acme-ml-dev/locations/europe-west2/services/pwl-churn-x"]
