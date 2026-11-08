"""FR-106 (1.4.2): the lookup gateway, in front of Cloud SQL or a team's own lookup service (the Pipewright custom
solution); off by default."""
import base64
import io
import json
import urllib.error
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.online import gateway as gw
from pipewright.online import launcher as ol
from tests.test_online_launcher import MANIFEST, ONLINE, Gcp, contract
from tests.test_online_store_types import store_repo

CALLER = "serviceAccount:app@acme-ml-dev.iam.gserviceaccount.com"


# ------------------------------------------------------------------------------------- configuration --

def test_gateway_is_off_by_default(repo):
    o = store_repo(repo, {"type": "cloudsql"})().ml[
        "online_features"]
    assert "mode" not in o and "gateway" not in o


def test_gateway_mode_with_cloudsql(repo):
    o = store_repo(repo, {"type": "cloudsql", "lookup": {"mode": "gateway", "callers": [CALLER]}})().ml[
        "online_features"]
    assert o["mode"] == "gateway" and o["gateway"]["retries"] == 3 and o["gateway"]["callers"] == [CALLER]
    assert o["gateway"]["private"] is False and o["service"] == "pwl-churn-churn-feats"


def test_gateway_needs_callers_and_its_keys_need_the_mode(repo):
    with pytest.raises(ConfigError, match="name who may call it"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"mode": "gateway"}})()
    with pytest.raises(ConfigError, match="retries: only with mode gateway"):
        store_repo(repo, {"type": "cloudsql", "lookup": {"retries": 2}})()


def test_the_custom_solution_puts_the_gateway_in_front_of_the_teams_store(repo):
    url = "https://team-store.internal.example/lookup"
    o = store_repo(repo, {"type": "http", "http": {"url": url},
                          "lookup": {"mode": "gateway", "callers": [CALLER], "max_instances": 3}})().ml[
        "online_features"]
    assert o["type"] == "http" and o["mode"] == "gateway" and o["provider"] == "pipewright"
    assert o["url"] == url and o["service"] == "pwl-churn-churn-feats" and o["lookup"]["max_instances"] == 3
    with pytest.raises(ConfigError, match="only mode gateway and the gateway's size"):
        store_repo(repo, {"type": "http", "http": {"url": url}, "lookup": {"cpu": 2}})()


def test_gateway_is_refused_for_google_api_stores(repo):
    with pytest.raises(ConfigError, match="BigQuery and Bigtable are Google APIs"):
        store_repo(repo, {"type": "bigquery", "lookup": {"mode": "gateway", "callers": [CALLER]}})()


# ------------------------------------------------------------------------------------------ the launcher --

GW_ONLINE = {**ONLINE, "mode": "gateway", "gateway": {"retries": 3, "callers": [CALLER]}}
ENDPOINT = "projects/123/locations/europe-west2/endpoints/987"


def _gcp():
    states = iter([(404, {}), (200, {"terminalCondition": {"state": "CONDITION_SUCCEEDED"},
                                      "uri": "https://pwl-churn-churn-x.run.app"})])
    return Gcp({("GET", "sqladmin"): (200, {"serverCaCert": {"cert": "CA"}}),
                ("GET", "/services/pwl-churn-churn-x"): lambda: next(states),
                ("GET", "/endpoints"): (200, {"endpoints": [{"name": ENDPOINT}]}),
                ("POST", ":getIamPolicy"): (200, {"bindings": []}),
                ("POST", "/services"): (200, {})})


def test_the_launcher_makes_the_gateway_and_grants_only_what_it_needs():
    gcp = _gcp()
    out = ol.ensure_lookup_service(contract(GW_ONLINE), MANIFEST, gcp, sleep=lambda s: None)
    assert out["gateway"] == {"endpoint": "987", "private": False, "callers": 1} and "invoker" not in out
    create = next(c for c in gcp.calls if c[0] == "POST" and c[1].endswith("/services"))
    cfg = json.loads(create[2]["template"]["containers"][0]["env"][0]["value"])
    assert cfg["gateway"]["predict_url"] == f"https://europe-west2-aiplatform.googleapis.com/v1/{ENDPOINT}:predict"
    assert cfg["usecase"] == "churn-x" and cfg["host"] == "10.41.0.3"
    sets = [c for c in gcp.calls if c[1].endswith(":setIamPolicy")]
    endpoint_policy = next(c for c in sets if "/endpoints/" in c[1])[2]["policy"]
    assert endpoint_policy["bindings"] == [{"role": "roles/aiplatform.user",
                                            "members": [
                                                "serviceAccount:pwl-churn@acme-ml-dev.iam.gserviceaccount.com"]}]
    service_policy = next(c for c in sets if "/services/" in c[1])[2]["policy"]
    assert service_policy["bindings"] == [{"role": "roles/run.invoker", "members": [CALLER]}]   # not the runner


def test_a_private_endpoint_is_called_at_its_dns_name():
    manifest = {**MANIFEST, "serving": {"endpoints": {"churn-x": {"name": "churn-x-endpoint", "resource": ENDPOINT,
                                                                  "private": True}}}}
    t = ol.gateway_target(contract(GW_ONLINE), manifest, GW_ONLINE, Gcp({}))
    assert t["private"] is True
    assert t["predict_url"] == ("https://987-europe-west2-123.prediction.p.vertexai.goog/v1/"
                                f"{ENDPOINT}:predict")


def test_the_gateway_needs_its_endpoint_first():
    with pytest.raises(ConfigError, match="does not exist yet"):
        ol.gateway_target(contract(GW_ONLINE), MANIFEST, GW_ONLINE, Gcp({("GET", "/endpoints"): (200, {})}))


def test_the_custom_solutions_service_reads_the_teams_store():
    online = {**GW_ONLINE, "type": "http", "url": "https://team.example/lookup", "audience": "https://team.example"}
    gate = {"endpoint": ENDPOINT, "predict_url": "https://x/predict", "private": False, "retries": 3}
    spec = ol.service_spec(contract(online), MANIFEST, online, None, gate)
    cfg = json.loads(spec["template"]["containers"][0]["env"][0]["value"])
    assert cfg["source"] == "http" and cfg["url"] == "https://team.example/lookup" and "host" not in cfg
    assert cfg["gateway"] == gate


# ------------------------------------------------------------------------------------------- the server --

class Source:
    def __init__(self, found):
        self.found, self.asked = found, []

    def fetch(self, ids):
        self.asked.append(ids)
        return {i: v for i, v in self.found.items() if i in ids}


class Tokens:
    def get(self):
        return "ACCESS"


def opener(answers, seen):
    answers = iter(answers)

    def open_(request, timeout=None, context=None):
        seen.append(json.loads(request.data))
        a = next(answers)
        if isinstance(a, Exception):
            raise a
        return io.BytesIO(json.dumps(a).encode())

    return open_


CFG = {"features": ["tenure", "spend"], "usecase": "churn",
       "gateway": {"predict_url": "https://europe-west2-aiplatform.googleapis.com/v1/e:predict", "retries": 3}}


def test_predict_sends_rows_in_the_models_order_and_lists_unknown_entities():
    seen = []
    g = gw.Gateway(CFG, Source({"c-1": {"spend": 2.0, "tenure": 12.0}}), Tokens(),
                   opener([{"predictions": [0.8], "deployedModelId": "55", "modelVersionId": "3"}], seen))
    body, fields = g.predict(["c-1", "c-9"])
    assert seen == [{"instances": [[12.0, 2.0]]}]
    assert body == {"predictions": {"c-1": 0.8}, "not_found": ["c-9"], "deployed_model": "55", "model_version": "3"}
    assert fields == {"asked": 2, "found": 1, "attempts": 1, "deployed_model": "55", "model_version": "3"}


def test_predict_retries_a_restarting_pool():
    seen, slept = [], []
    busy = urllib.error.HTTPError("u", 503, "busy", {}, None)
    g = gw.Gateway(CFG, Source({"c-1": {"tenure": 1, "spend": 1}}), Tokens(),
                   opener([busy, busy, {"predictions": [0.1]}], seen), sleep=slept.append)
    _, fields = g.predict(["c-1"])
    assert fields["attempts"] == 3 and slept == [0.25, 0.5]


def test_retries_stop_and_other_errors_are_not_retried():
    busy = urllib.error.HTTPError("u", 503, "busy", {}, None)
    g = gw.Gateway({**CFG, "gateway": {**CFG["gateway"], "retries": 1}}, Source({"c-1": {}}), Tokens(),
                   opener([busy, busy], []), sleep=lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        g.predict(["c-1"])
    bad = urllib.error.HTTPError("u", 400, "bad", {}, None)
    seen = []
    g = gw.Gateway(CFG, Source({"c-1": {}}), Tokens(), opener([bad], seen), sleep=lambda s: None)
    with pytest.raises(urllib.error.HTTPError):
        g.predict(["c-1"])
    assert len(seen) == 1


def test_nothing_known_means_no_call():
    seen = []
    g = gw.Gateway(CFG, Source({}), Tokens(), opener([], seen))
    body, fields = g.predict(["c-1"])
    assert seen == [] and body["not_found"] == ["c-1"] and fields["attempts"] == 0


def test_the_caller_is_read_from_the_checked_token():
    payload = base64.urlsafe_b64encode(json.dumps({"email": "app@acme.example"}).encode()).decode().rstrip("=")
    assert gw.caller_of(f"Bearer h.{payload}.s") == "app@acme.example"
    assert gw.caller_of(None) == "unknown" and gw.caller_of("Bearer junk") == "unknown"


def test_the_http_source_calls_the_teams_store_with_an_id_token():
    seen = []

    def open_(request, timeout=None):
        seen.append((request.full_url, request.headers.get("Authorization"), json.loads(request.data)))
        return io.BytesIO(json.dumps({"features": {"c-1": {"tenure": 3}}}).encode())

    tokens = gw.IdTokens(fetch=lambda aud: f"ID-{aud}")
    src = gw.HttpSource({"url": "https://team.example/lookup", "audience": "aud-1"}, tokens, open_)
    assert src.fetch(["c-1"]) == {"c-1": {"tenure": 3}}
    assert seen == [("https://team.example/lookup", "Bearer ID-aud-1", {"entity_ids": ["c-1"]})]


def test_the_server_answers_predict_only_with_a_gateway():
    from pipewright.online import server

    class Req:
        def __init__(self, path, body, gateway=None):
            self.out = {}
            handler = server.handler_for(Source({}), gateway)
            h = handler.__new__(handler)
            h.path, h.headers = path, {"Content-Length": str(len(body)), "Authorization": None}
            h.rfile = io.BytesIO(body)
            h._send = lambda code, b: self.out.update(code=code, body=b)
            h.do_POST()

    assert Req("/predict", b'{"entity_ids": ["a"]}').out["code"] == 404
    fake = SimpleNamespace(usecase="churn", predict=lambda ids: ({"predictions": {}, "not_found": ids},
                                                                 {"asked": 1, "found": 0, "attempts": 0}))
    r = Req("/predict", b'{"entity_ids": ["a"]}', fake).out
    assert r["code"] == 200 and r["body"]["not_found"] == ["a"]
    assert Req("/predict", b'{"entity_ids": []}', fake).out["code"] == 400
