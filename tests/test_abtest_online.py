"""FR-57 phase B: the gateway as an A/B router, its exposure lines, the launcher's A/B configuration, the arm."""
import copy
import hashlib
import hmac
import json
from types import SimpleNamespace

import pytest

from pipewright.abtest import assign
from pipewright.abtest import config as abc
from pipewright.abtest import online as ab_online
from pipewright.errors import ConfigError
from pipewright.online import abrouter, server
from pipewright.online import launcher as ol
from tests.test_online_launcher import MANIFEST, Gcp


class Log:
    def __init__(self):
        self.lines = []

    def put(self, name, payload):
        self.lines.append((name, payload))


class FakeGateway:
    """Scores an entity as its length / 10; `fail` raises on every call."""

    def __init__(self, deployed, fail=False, usecase="churn"):
        self.deployed, self.fail, self.usecase, self.asked = deployed, fail, usecase, []

    def predict(self, ids):
        self.asked.append(list(ids))
        if self.fail:
            raise ConnectionError("down")
        return ({"predictions": {i: len(i) / 10 for i in ids}, "not_found": [], "deployed_model": self.deployed,
                 "model_version": "v"}, {"asked": len(ids), "found": len(ids), "attempts": 1})

    def call(self, instances):
        self.asked.append(list(instances))
        if self.fail:
            raise ConnectionError("down")
        return {"predictions": [sum(x) for x in instances], "deployedModelId": self.deployed}


def router(**cfg):
    base = {"name": "lapse-v7", "mode": "ab", "state": "running", "treatment_bp": 5000, "release": "R1",
            "usecase": "churn", "tenant": "retention", "treatment_deployed_model": "T1", "breaker": {}, "feed": {}}
    base.update(cfg)
    log = Log()
    return abrouter.ABRouter(base, FakeGateway("C1"), FakeGateway("T1"), log), log


UNITS = [f"c{i}" for i in range(400)]


def test_the_router_assigns_as_batch_scoring_and_sql_do():
    for bp in (0, 1, 1000, 5000, 9999, 10000):
        assert [abrouter.arm("lapse-v7", u, bp) for u in UNITS] == [assign.arm("lapse-v7", u, bp) for u in UNITS]
    assert abrouter.arm("t", "", 10000) == "control" and abrouter.arm("t", None, 10000) == "control"


def test_each_arm_scores_its_units_and_each_unit_is_exposed_once():
    r, log = router()
    body, fields = r.predict(UNITS)
    expected = {u: assign.arm("lapse-v7", u, 5000) for u in UNITS}
    assert body["arms"] == expected and set(body["predictions"]) == set(UNITS)
    assert set(r.treatment.asked[0]) == {u for u, a in expected.items() if a == "treatment"}
    assert fields["treatment"] + fields["control"] == len(UNITS)
    exposed = [p for n, p in log.lines if n == "pipewright_ab_exposure"]
    assert len(exposed) == len(UNITS) and {p["unit"]: p["arm"] for p in exposed} == expected
    assert {p["deployed_model"] for p in exposed if p["arm"] == "treatment"} == {"T1"}
    assert not [n for n, _ in log.lines if n == "pipewright_ab_feed"]


def test_a_paused_test_sends_every_unit_to_the_control():
    r, log = router(state="paused")
    body, _ = r.predict(UNITS)
    assert set(body["arms"].values()) == {"control"} and r.treatment.asked == []


def test_the_breaker_sends_the_treatments_units_to_the_control(capsys):
    r, log = router(breaker={"window": 20, "max_error_rate": 0.05})
    r.treatment.fail = True
    tripped = False
    for k in range(25):
        try:
            r.predict([f"u{k}-{j}" for j in range(8)])
        except ConnectionError:
            continue
        tripped = True
        break
    assert tripped and r.breaker.tripped_at is not None
    assert "ab_breaker_tripped" in capsys.readouterr().out
    assert any(p.get("event") == "breaker_tripped" for _, p in log.lines)
    body, _ = r.predict(UNITS)
    assert set(body["arms"].values()) == {"control"}


def test_an_answer_from_another_model_is_counted():
    r, _ = router(treatment_deployed_model="T-expected")
    r.predict(UNITS)
    assert r.counts["mismatched_model"] == 1


def test_the_feed_carries_a_keyed_hash_never_the_unit():
    r, log = router(feed={"exposures": True, "key": "s3cret"})
    r.predict(UNITS[:50])
    feed = [p for n, p in log.lines if n == "pipewright_ab_feed"]
    assert len(feed) == 50 and all("unit" not in p for p in feed)
    want = hmac.new(b"s3cret", b"c0", hashlib.sha256).hexdigest()[:32]
    assert any(p["unit_ref"] == want for p in feed)
    r2, log2 = router(feed={"exposures": False, "key": "s3cret"})
    r2.predict(UNITS[:5])
    assert not [n for n, _ in log2.lines if n == "pipewright_ab_feed"]


def test_router_only_keeps_the_callers_order():
    r, log = router()
    instances = [[k, 1] for k in range(len(UNITS))]
    body, fields = r.predict_instances(instances, UNITS)
    assert body["predictions"] == [k + 1 for k in range(len(UNITS))]
    assert body["arms"] == [assign.arm("lapse-v7", u, 5000) for u in UNITS]
    with pytest.raises(ValueError, match="one per instance"):
        r.predict_instances(instances, UNITS[:3])


def test_shadow_answers_from_the_control_and_logs_only_a_comparison():
    r, log = router(mode="shadow")
    body, fields = r.predict(UNITS[:10])
    assert fields["shadow"] and "arms" not in body
    r._shadow_copy(UNITS[:10], body["predictions"])
    lines = [p for _, p in log.lines if p.get("event") == "shadow"]
    assert lines and lines[-1]["n"] == 10 and lines[-1]["mean_abs_diff"] == 0 and lines[-1]["agreement"] == 1
    r.treatment.fail = True
    r._shadow_copy(UNITS[:10], body["predictions"])
    assert [p for _, p in log.lines if p.get("event") == "shadow"][-1]["ok"] is False


def test_the_exposure_log_batches_per_log_and_counts_failures():
    sent = []

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"{}"

    def opener(req, timeout):
        body = json.loads(req.data)
        if body["logName"].endswith("pipewright_ab_feed"):
            raise OSError("refused")
        sent.append(body)
        return Resp()

    log = abrouter.ExposureLog("p", lambda: "tok", {"pipewright-usecase": "churn"}, opener=opener, wait=0.05)
    for k in range(5):
        log.put("pipewright_ab_exposure", {"unit": f"u{k}"})
    log.put("pipewright_ab_feed", {"unit_ref": "x"})
    log.close()
    assert log.written == 5 and log.failed == 1 and sum(len(b["entries"]) for b in sent) == 5
    assert sent[0]["logName"] == "projects/p/logs/pipewright_ab_exposure" and sent[0]["labels"]["pipewright-usecase"]


def test_the_server_builds_a_router_only_gateway():
    cfg = {"source": "none", "features": [], "usecase": "churn",
           "gateway": {"predict_url": "https://r/v1/c:predict", "endpoint": "c", "region": "r"},
           "ab_test": {"name": "lapse-v7", "project": "p", "tenant": "retention", "state": "running",
                       "treatment_bp": 1000, "treatment": {"predict_url": "https://r/v1/t:predict", "endpoint": "t",
                                                           "private": False, "region": "r"}}}
    lookup, gateway = server.build(cfg, log=Log())
    assert lookup is None and isinstance(gateway, abrouter.ABRouter)
    assert gateway.control.url.endswith("/c:predict") and gateway.treatment.url.endswith("/t:predict")


# -- the launcher's A/B configuration ------------------------------------------------------------------------------

def _section(**kw):
    s = copy.deepcopy(abc.DEFAULTS)
    s.update({"enabled": True, "name": "lapse-v7", "unit": "customer_id"}, **kw)
    return s


def _contract(section):
    target = SimpleNamespace(static_parameters={"ml_config": {"ab_test": section}})
    return SimpleNamespace(usecase="churn-x", tenant="churn", release_id="R1", pipeline=lambda name: target)


TEST = {"name": "lapse-v7", "unit": "customer_id", "mode": "ab", "state": "running",
        "allocation": [{"treatment_bp": 2000}],
        "arms": {"control": {"version": "5"}, "treatment": {"version": "7", "endpoint": "e", "deployed_model": "T1"}}}


def test_the_gateway_finds_the_arm_as_terraform_names_it():
    gcp = Gcp({("GET", "/endpoints"): (200, {"endpoints": [{"name": "projects/1/locations/r/endpoints/9"}]})})
    out = ol.gateway_target(_contract(_section()), MANIFEST, {}, gcp, arm="ab")
    assert out["endpoint"].endswith("/9") and gcp.calls[0][3]["filter"] == 'display_name="churn-x-ab-endpoint"'
    m = {**MANIFEST, "serving": {"endpoints": {"churn-x-ab": {"resource": "projects/1/locations/r/endpoints/8"}}}}
    assert ol.gateway_target(_contract(_section()), m, {}, Gcp({}), arm="ab")["endpoint"].endswith("/8")


def test_the_feed_key_is_given_only_where_the_platform_allows_exposures():
    m = {**MANIFEST, "serving": {"endpoints": {"churn-x-ab": {"resource": "projects/1/locations/r/endpoints/8"}}}}
    on = _section(publish={"events": True, "exposures": True})
    cfg, env = ol.ab_gateway(_contract(on), m, {}, Gcp({}), TEST)
    assert cfg["feed"] == {} and env == []
    m["policies"] = {"ab_test": {"feed_exposures": True}}
    m["tenants"] = {"churn": {"ab_test_feed": {"topic": "t", "key_secret": "projects/1/secrets/k"}}}
    cfg, env = ol.ab_gateway(_contract(on), m, {}, Gcp({}), TEST)
    assert cfg["feed"]["key_env"] == "PIPEWRIGHT_AB_FEED_KEY" and "key" not in cfg["feed"]
    assert env[0]["valueSource"]["secretKeyRef"]["secret"] == "projects/1/secrets/k"
    assert cfg["treatment_bp"] == 2000 and cfg["state"] == "running" and cfg["treatment_deployed_model"] == "T1"
    paused, _ = ol.ab_gateway(_contract(on), m, {}, Gcp({}), {**TEST, "state": "paused"})
    assert paused["state"] == "paused"


def test_the_cli_asks_for_a_router_only_service_only_for_a_test(monkeypatch):
    uc = SimpleNamespace(name="churn-x", tenant="churn", ml={})
    assert ol.request_from_cli(uc, SimpleNamespace(), ab=False) is None
    assert ol._cli_service(uc, True) == "pwl-churn-churn-x"


# -- the arm -----------------------------------------------------------------------------------------------------

class FakeAi:
    def __init__(self, endpoints=()):
        self.created = []
        outer = self

        class Endpoint:
            @staticmethod
            def list(filter=None, **k):
                return list(endpoints)

            @staticmethod
            def create(**kw):
                outer.created.append(kw)
                return SimpleNamespace(resource_name="projects/1/locations/r/endpoints/new")

        self.Endpoint = Endpoint


def _arms(monkeypatch, declared=(), endpoints=()):
    from pipewright.models import deploy
    from pipewright.serving import endpoints as tf

    ai = FakeAi(endpoints)
    monkeypatch.setattr(deploy, "list_endpoints", lambda a, name: list(endpoints))
    monkeypatch.setattr(tf, "declared", lambda env, key: {"x": 1} if key in declared else None)
    monkeypatch.setattr(tf, "missing_message", lambda env, key: f"{key} is declared but missing")
    uc = SimpleNamespace(name="churn-x", tenant="churn", ml={}, model={"name": "m", "serving": {}})
    return ab_online.Arms(uc, SimpleNamespace(security={}), SimpleNamespace(), log=lambda m: None), ai


def test_a_terraform_endpoint_needs_a_terraform_arm(monkeypatch):
    arms, ai = _arms(monkeypatch, declared=("churn-x",))
    with pytest.raises(ConfigError, match="add `churn-x-ab` to serving.yaml"):
        arms._endpoint(ai, create=True)
    arms, ai = _arms(monkeypatch, declared=("churn-x", "churn-x-ab"))
    with pytest.raises(ConfigError, match="declared but missing"):
        arms._endpoint(ai, create=True)


def test_the_arm_is_created_once_and_found_after(monkeypatch):
    arms, ai = _arms(monkeypatch)
    ep, created = arms._endpoint(ai, create=True)
    assert created and ai.created[0]["display_name"] == "churn-x-ab-endpoint"
    assert "encryption_spec_key_name" not in ai.created[0]
    assert arms._endpoint(ai, create=False) == (None, False)
    found = SimpleNamespace(resource_name="projects/1/locations/r/endpoints/5")
    arms, ai = _arms(monkeypatch, endpoints=[found])
    assert arms._endpoint(ai, create=True) == (found, False) and ai.created == []


def test_a_failed_gateway_stops_the_test_and_removes_the_arm(monkeypatch):
    from pipewright.loop import driver
    from pipewright.models import deploy, registry
    from pipewright.retraining.store import CaseStore
    from pipewright.runs import launch
    from tests.test_abtest_service import _svc

    s = _svc(monkeypatch, ml={"ab_test": _section(arms={"control": "champion", "treatment": {"model_version": "7"}}),
                              "inference": {"output_table": "preds"}})
    s.gcp = SimpleNamespace(session=SimpleNamespace(credentials=None))
    monkeypatch.setattr(deploy, "check_approval", lambda *a: None)
    monkeypatch.setattr(CaseStore, "current", lambda self, uc: None)
    monkeypatch.setattr(driver, "finished", lambda loop: True)
    monkeypatch.setattr(registry, "versions", lambda *a, **k: [SimpleNamespace(version_id="5", default=True)])
    monkeypatch.setattr(registry, "approve_for_test", lambda *a: {"files": []})
    monkeypatch.setattr(launch, "live_release", lambda *a: "R1")
    calls = []

    class Online:
        def start(self, test):
            calls.append("start")
            return {"treatment": {**test["arms"]["treatment"], "endpoint": "e", "deployed_model": "T1"}}

        def apply(self, test):
            calls.append(("apply", test["arms"]["treatment"].get("endpoint")))
            raise ConfigError("the gateway was not ready")

        def stop(self, test):
            calls.append(("stop", test["state"]))

    s.online = Online()
    with pytest.raises(ConfigError, match="not ready"):
        s.start("a@example.com", True)
    assert calls == ["start", ("apply", "e"), ("stop", "stopped")]
    assert s.store.get("churn", "lapse-v7")["state"] == "stopped" and s.current() is None


# -- KServe (private GKE) ------------------------------------------------------------------------------------------

KSERVE_TEST = {"name": "lapse-k1", "usecase": "churn-x", "tenant": "churn", "unit": "customer_id", "mode": "ab",
               "state": "running", "release": "R1", "allocation": [{"treatment_bp": 2000}],
               "arms": {"control": {"version": "5"}, "treatment": {
                   "version": "7", "endpoint": "kserve:serving-churn/churn-x-ab",
                   "service": "http://churn-x-ab-predictor.serving-churn.svc.cluster.local",
                   "route": "/predictions/model"}}}


class FakeKube:
    def __init__(self, objects=None):
        self.objects, self.calls = dict(objects or {}), []

    def _r(self, status, body=None):
        return SimpleNamespace(ok=200 <= status < 300, status=status, body=body or {})

    def get(self, path, **k):
        self.calls.append(("GET", path))
        return self._r(200, self.objects[path]) if path in self.objects else self._r(404)

    def post(self, path, body, **k):
        self.calls.append(("POST", path))
        self.objects[f"{path}/{body['metadata']['name']}"] = {**body, "status": {
            "availableReplicas": 2, "updatedReplicas": 2, "replicas": 2}, "metadata": {
            **body["metadata"], "resourceVersion": "1"}}
        return self._r(201, body)

    def request(self, method, path, body=None, **k):
        self.calls.append((method, path))
        return self._r(200, body)

    def delete(self, path):
        self.calls.append(("DELETE", path))
        self.objects.pop(path, None)
        return self._r(200)


def _karms(monkeypatch, kube=None, control=True, image="r-docker.pkg.dev/p/pw/launcher@sha256:ab"):
    from pipewright.models import kserve

    kube = kube or FakeKube()

    class K:
        def __init__(self, uc, env, gcp, name=None):
            self.s = {"namespace": "serving-churn", "service_account": "serving", "mirror": None}
            self.kube, self.name = kube, name or "churn-x"

        def current(self):
            if self.name == "churn-x" and control:
                return {"metadata": {"annotations": {"pipewright.dev/predict-route": "/predictions/model"}}}
            return None

        def endpoints(self):
            return {"cluster": f"http://{self.name}-predictor.serving-churn.svc.cluster.local"}

    monkeypatch.setattr(kserve, "KServe", K)
    written = {}
    from pipewright.gcp import storage

    monkeypatch.setattr(storage, "gcs_write", lambda gcp, b, n, text, **k: written.update({n: json.loads(text)}))
    uc = SimpleNamespace(name="churn-x", tenant="churn", resources={"online_serving": True},
                         model={"name": "m", "serving": {"target": "kserve"}},
                         ml={"ab_test": {"guardrails": {"technical": {"breaker": {"window": 50}}}}})
    env = SimpleNamespace(name="prod", project="p", region="r", artifacts_bucket="art", policies={}, tenants={},
                          launcher={"image": image} if image else {}, observability={})
    return ab_online.KServeArms(uc, env, SimpleNamespace(), log=lambda m: None), kube, written


def test_kserve_use_cases_get_a_kserve_arm_and_its_checks(monkeypatch):
    arms, _, _ = _karms(monkeypatch)
    assert isinstance(ab_online.arms_for(arms.uc, arms.env, None), ab_online.KServeArms)
    arms.check()
    with pytest.raises(ConfigError, match="no launcher.image"):
        _karms(monkeypatch, image=None)[0].check()
    with pytest.raises(ConfigError, match="deploy the champion first"):
        _karms(monkeypatch, control=False)[0].check()


def test_the_router_runs_in_the_namespace_and_only_it_reaches_the_arm(monkeypatch):
    arms, kube, written = _karms(monkeypatch)
    arms.apply(KSERVE_TEST)
    dep = kube.objects["/apis/apps/v1/namespaces/serving-churn/deployments/pw-ab-router-churn-x"]
    pod = dep["spec"]["template"]["spec"]
    assert pod["serviceAccountName"] == "serving" and pod["containers"][0]["image"].endswith("@sha256:ab")
    cfg = json.loads(pod["containers"][0]["env"][0]["value"])
    assert cfg["source"] == "none" and cfg["gateway"]["auth"] == "none"
    assert cfg["gateway"]["predict_url"] == "http://churn-x-predictor.serving-churn.svc.cluster.local/predictions/model"
    ab = cfg["ab_test"]
    assert ab["treatment"]["predict_url"].startswith("http://churn-x-ab-predictor") and ab["treatment_bp"] == 2000
    assert ab["follow"] == {"bucket": "art", "object": "online-sync/churn/churn-x/ab/lapse-k1.router.json",
                            "every": 30}
    pol = kube.objects["/apis/networking.k8s.io/v1/namespaces/serving-churn/networkpolicies/churn-x-ab-only-router"]
    assert pol["spec"]["podSelector"]["matchLabels"] == {"serving.kserve.io/inferenceservice": "churn-x-ab"}
    assert pol["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]["app"] == "pw-ab-router"
    assert written["online-sync/churn/churn-x/ab/lapse-k1.router.json"] == {
        "ab_test": "lapse-k1", "mode": "ab", "state": "running", "treatment_bp": 2000}
    kube.calls.clear()
    arms.stop({**KSERVE_TEST, "state": "stopped"})
    assert written["online-sync/churn/churn-x/ab/lapse-k1.router.json"]["treatment_bp"] == 0
    assert sum(1 for m, _ in kube.calls if m == "DELETE") == 3


def test_a_paused_kserve_test_sends_everyone_to_the_control():
    assert ab_online.state_of({**KSERVE_TEST, "state": "paused"})["treatment_bp"] == 0
    assert ab_online.state_of({**KSERVE_TEST, "state": "decision_required"})["state"] == "running"
    assert ab_online.is_kserve(KSERVE_TEST) and not ab_online.is_kserve(TEST)


def test_the_router_follows_the_tests_state_file():
    import io
    import urllib.error

    answers = [json.dumps({"state": "paused", "treatment_bp": 0}).encode(), 404]

    def opener(req, timeout):
        a = answers.pop(0)
        if a == 404:
            raise urllib.error.HTTPError(req.full_url, 404, "gone", {}, None)
        return io.BytesIO(a)

    class Ctx(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def wrapped(req, timeout):
        out = opener(req, timeout)
        return Ctx(out.getvalue())

    r = abrouter.ABRouter({"name": "t", "mode": "ab", "state": "running", "treatment_bp": 5000, "counts_every": 0,
                           "follow": {"bucket": "art", "object": "online-sync/x/y/ab/t.router.json", "every": 3600}},
                          FakeGateway("C"), FakeGateway("T"), None, token=lambda: "tok", opener=wrapped)
    assert r.cfg["state"] == "paused" and r._bp() == 0
    r.follow()
    assert r.cfg["state"] == "stopped"


def test_the_router_counts_errors_and_time_per_arm():
    r, _ = router()
    r.treatment.fail = True
    with pytest.raises(ConnectionError):
        r.predict(UNITS)
    assert r.counts["treatment_errors"] == 1 and r.counts["control_errors"] == 0 and r.counts["control_calls"] == 1
