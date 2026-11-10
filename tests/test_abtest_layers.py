"""FR-123: overlapping tests in layers. Tests that name a layer claim disjoint ranges of the layer's units; a unit
outside a test's range is not in the test (the default serves it, never an exposure), in Python, the component
runtime, SQL and the router alike."""
import importlib.util
from pathlib import Path

import pytest

from pipewright.abtest import assign, layers
from pipewright.abtest import config as abc
from pipewright.abtest import store as st
from pipewright.errors import ConfigError
from pipewright.online import abrouter
from tests.test_abtest_core import FakeGcs
from tests.test_abtest_online import UNITS, router
from tests.test_abtest_service import _section, _svc, _test

RUNTIME = Path(__file__).resolve().parents[1] / "src/pipewright/build/templates/pipewright_runtime.py"
LAYER = {"name": "customers", "start_bp": 3000, "end_bp": 6000, "share_bp": 3000}


def _runtime():
    spec = importlib.util.spec_from_file_location("pw_runtime_layers", RUNTIME)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------------------------------------------------- assignment --
def test_a_unit_is_in_a_test_only_inside_its_range():
    arms = [assign.arm("lapse-v7", u, 5000, LAYER) for u in UNITS]
    inside = [a for a in arms if a is not None]
    assert 0.2 < len(inside) / len(UNITS) < 0.4                          # about 30% of the units
    # inside the range, the test's own hash decides, exactly as without a layer
    assert all(a == assign.arm("lapse-v7", u, 5000) for u, a in zip(UNITS, arms, strict=True) if a)
    assert assign.arm("lapse-v7", "", 5000, LAYER) == "control"          # no key: as before
    assert [assign.arm("t", u, 5000, None) for u in UNITS] == [assign.arm("t", u, 5000) for u in UNITS]


def test_two_tests_in_a_layer_never_share_a_unit():
    a = {"name": "customers", "start_bp": 0, "end_bp": 4000}
    b = {"name": "customers", "start_bp": 4000, "end_bp": 10000}
    for u in UNITS:
        assert (assign.arm("test-a", u, 5000, a) is None) != (assign.arm("test-b", u, 5000, b) is None)


def test_every_copy_assigns_the_same_with_a_layer():
    rt = _runtime()
    for u in UNITS + ["", None, "ä-ü"]:
        expected = assign.arm("lapse-v7", u, 2000, LAYER)
        assert rt.ab_arm("lapse-v7", u, 2000, LAYER) == expected
        assert abrouter.arm("lapse-v7", u, 2000, LAYER) == expected
    assert rt.ab_sql_arm("lapse-v7", "`customer_id`", 2000, LAYER) == \
        assign.sql_arm("lapse-v7", "`customer_id`", 2000, LAYER)
    sql = assign.sql_arm("lapse-v7", "`customer_id`", 2000, LAYER)
    assert "'layer/customers'" in sql and sql.count("NULL") == 2           # the NULL unit check, the outside


# --------------------------------------------------------------------------------------------- the router --
def test_the_router_serves_outsiders_from_the_control_and_never_exposes_them():
    r, log = router(layer=LAYER)
    body, fields = r.predict(UNITS)
    outside = {u for u in UNITS if assign.arm("lapse-v7", u, 5000, LAYER) is None}
    assert outside and {u for u, a in body["arms"].items() if a is None} == outside
    assert outside <= set(r.control.asked[0]) and not outside & set(r.treatment.asked[0])
    exposed = {p["unit"] for n, p in log.lines if n == "pipewright_ab_exposure"}
    assert exposed == set(UNITS) - outside and r.counts["outside"] == len(outside)
    r.beat()
    assert log.lines[-1][1]["outside"] == len(outside)


def test_router_only_callers_get_outsiders_answered_too():
    r, _ = router(layer=LAYER)
    instances = [[i] for i in range(len(UNITS))]
    body, _ = r.predict_instances(instances, UNITS)
    assert None not in body["predictions"] and body["arms"].count(None) == r.counts["outside"] > 0


# ------------------------------------------------------------------------------------------ the registry --
def _store(monkeypatch):
    FakeGcs(monkeypatch)
    monkeypatch.setattr(layers, "gcs_read", st.gcs_read)
    monkeypatch.setattr(layers, "gcs_write", st.gcs_write)


def test_claims_take_the_first_free_range_and_refuse_when_full(monkeypatch):
    _store(monkeypatch)
    running = {("a", "t1"), ("b", "t2")}
    active = lambda uc, t: (uc, t) in running  # noqa: E731
    one = layers.claim(None, "b", {"name": "customers", "share_bp": 4000}, "customer_id", "a", "t1", active)
    two = layers.claim(None, "b", {"name": "customers", "share_bp": 5000}, "customer_id", "b", "t2", active)
    assert (one["start_bp"], one["end_bp"], two["start_bp"], two["end_bp"]) == (0, 4000, 4000, 9000)
    with pytest.raises(ConfigError, match="no 20% left"):
        layers.claim(None, "b", {"name": "customers", "share_bp": 2000}, "customer_id", "c", "t3", active)
    running.discard(("a", "t1"))                       # t1 ended: its range is free for the next claim
    three = layers.claim(None, "b", {"name": "customers", "share_bp": 2000}, "customer_id", "c", "t3",
                         lambda uc, t: (uc, t) in running | {("c", "t3")})
    assert (three["start_bp"], three["end_bp"]) == (0, 2000)
    with pytest.raises(ConfigError, match="splits units by customer_id"):
        layers.claim(None, "b", {"name": "customers", "share_bp": 500}, "account_id", "d", "t4",
                     lambda uc, t: True)


def test_a_claim_that_loses_a_race_looks_again(monkeypatch):
    _store(monkeypatch)
    real_write, calls = layers.gcs_write, []

    def racing(gcp, bucket, name, text, if_generation_match=None, **k):
        if not calls:                              # another start writes first
            calls.append(1)
            real_write(gcp, bucket, name, '{"layer": "customers", "unit": "customer_id", "tests": [{"usecase": '
                       '"x", "ab_test": "other", "start_bp": 0, "end_bp": 5000, "share_bp": 5000}]}')
        return real_write(gcp, bucket, name, text, if_generation_match=if_generation_match)

    monkeypatch.setattr(layers, "gcs_write", racing)
    got = layers.claim(None, "b", {"name": "customers", "share_bp": 5000}, "customer_id", "a", "t1",
                       lambda uc, t: True)
    assert (got["start_bp"], got["end_bp"]) == (5000, 10000)


def test_start_claims_and_stop_frees_the_range(monkeypatch):
    s = _svc(monkeypatch)
    monkeypatch.setattr(layers, "gcs_read", st.gcs_read)
    monkeypatch.setattr(layers, "gcs_write", st.gcs_write)
    section = {**_section(), "layer": {"name": "customers", "share_bp": 3000}}
    s.store.create({**_test(name="other-test", layer={"name": "customers", "start_bp": 0, "end_bp": 3000,
                                                        "share_bp": 3000}), "usecase": "upsell"})
    layers.claim(None, "b", {"name": "customers", "share_bp": 3000}, "customer_id", "upsell", "other-test",
                 lambda uc, t: True)
    got = s._claim(section)
    assert (got["start_bp"], got["end_bp"]) == (3000, 6000)
    s.env.policies = {"ab_test": {"overlap": "refuse"}}
    assert s._overlaps(section) == []                    # apart by construction: not an overlap
    with pytest.raises(ConfigError, match="already test"):
        s._overlaps(_section())                          # a test without the layer still overlaps it
    s.store.create({**_test(), "layer": got})
    s.set_state("stopped", "enough", "a@example.com")
    record, _ = layers.read(None, "b", "customers")
    assert [t["ab_test"] for t in record["tests"]] == ["other-test"]
    assert "_layers" not in s.store.usecases()


def test_plan_counts_only_the_layers_share(monkeypatch):
    s = _svc(monkeypatch)
    whole = s.plan(baseline=0.3, per_day=5000)
    s.uc.ml["ab_test"] = {**s.uc.ml["ab_test"], "layer": {"name": "customers", "share_bp": 2500}}
    part = s.plan(baseline=0.3, per_day=5000)
    assert part["layer"]["units_per_day_in_test"] == 1250 and part["days"] > whole["days"]


def test_the_layer_is_checked():
    for layer, msg in (({"name": "X"}, "layer.name"), ({"name": "customers", "share_bp": 0}, "share_bp"),
                       ({"name": "customers"}, "share_bp")):
        problems = []
        t = _section()
        t["layer"] = layer
        abc.check({"ab_test": t, "task": "binary"}, set(), "churned", lambda *a: None, problems)
        assert any(msg in p for p in problems), (layer, problems)


def test_a_full_layer_refuses_before_the_treatment_is_approved(monkeypatch):
    """Found in the QA battle test: the start recorded the treatment's approval for the test, then the layer refused."""
    from types import SimpleNamespace

    from pipewright.loop import driver
    from pipewright.models import deploy, registry
    from pipewright.retraining.store import CaseStore
    from pipewright.runs import launch

    section = {**_section(arms={"control": "champion", "treatment": {"model_version": "7"}}),
               "layer": {"name": "customers", "share_bp": 3000}}
    s = _svc(monkeypatch, ml={"ab_test": section, "inference": {"output_table": "preds"}})
    monkeypatch.setattr(layers, "gcs_read", st.gcs_read)
    monkeypatch.setattr(layers, "gcs_write", st.gcs_write)
    s.gcp = SimpleNamespace(session=SimpleNamespace(credentials=None))
    monkeypatch.setattr(deploy, "check_approval", lambda *a: None)
    monkeypatch.setattr(CaseStore, "current", lambda self, uc: None)
    monkeypatch.setattr(driver, "finished", lambda loop: True)
    monkeypatch.setattr(registry, "versions", lambda *a, **k: [SimpleNamespace(version_id="5", default=True)])
    monkeypatch.setattr(launch, "live_release", lambda *a: "R1")
    approved = []
    monkeypatch.setattr(registry, "approve_for_test", lambda *a: approved.append(a) or {"files": []})
    s.store.create({**_test(name="big", unit="customer_id", layer={"name": "customers", "start_bp": 0,
                                                                     "end_bp": 9000, "share_bp": 9000}),
                    "usecase": "upsell"})
    layers.claim(None, "b", {"name": "customers", "share_bp": 9000}, "customer_id", "upsell", "big",
                 lambda uc, t: True)
    with pytest.raises(ConfigError, match="no 30% left"):
        s.start("a@example.com", True)
    assert approved == [] and s.current() is None

    # and a refused approval frees the range it claimed
    monkeypatch.setattr(registry, "approve_for_test", lambda *a: (_ for _ in ()).throw(ConfigError("gates fail")))
    s.uc.ml["ab_test"]["layer"] = {"name": "customers", "share_bp": 1000}
    with pytest.raises(ConfigError, match="gates fail"):
        s.start("a@example.com", True)
    record, _ = layers.read(None, "b", "customers")
    assert [t["ab_test"] for t in record["tests"]] == ["big"]
