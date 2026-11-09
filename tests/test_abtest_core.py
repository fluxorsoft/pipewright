"""FR-57: assignment, statistics and the ab_test section's checks."""
import math
import random

import pytest

from pipewright.abtest import assign, stats
from pipewright.abtest import config as abc
from pipewright.abtest.store import TestStore

# -- assignment --------------------------------------------------------------------------------------------------

def test_the_bucket_is_seven_bytes_and_stable():
    b = assign.bucket("lapse-v7-vs-v5", "12346")
    assert 0 <= b < assign.SPACE and b == assign.bucket("lapse-v7-vs-v5", 12346)   # str() of the key
    assert assign.bucket("other-name", "12346") != b                                # the name salts it


def test_the_share_is_on_target_and_lowering_only_moves_units_to_control():
    units = [f"C{i:06d}" for i in range(50_000)]
    at20 = {u for u in units if assign.arm("t", u, 2000) == assign.TREATMENT}
    at10 = {u for u in units if assign.arm("t", u, 1000) == assign.TREATMENT}
    assert abs(len(at20) / len(units) - 0.2) < 0.01 and abs(len(at10) / len(units) - 0.1) < 0.01
    assert at10 <= at20                                       # nobody moves into the treatment


def test_a_unit_without_a_key_is_control():
    assert assign.arm("t", None, 9999) == assign.arm("t", "", 9999) == assign.CONTROL


def test_the_sql_names_the_same_threshold_and_quotes_the_name():
    sql = assign.sql_arm("it's", "customer_id", 2000)
    assert str(assign.threshold(2000)) in sql and "it\\'s" in sql and "SUBSTR(SHA256(" in sql
    with pytest.raises(ValueError):
        assign.threshold(10001)


# -- statistics ----------------------------------------------------------------------------------------------------

def test_norm_ppf_inverts_the_cdf():
    for p in (1e-6, 0.025, 0.3, 0.5, 0.8, 0.975, 1 - 1e-6):
        assert abs(stats.norm_cdf(stats.norm_ppf(p)) - p) < 1e-12


def test_two_proportions_match_the_textbook_values():
    c = stats.Agg.of([1] * 600 + [0] * 400)
    t = stats.Agg.of([1] * 650 + [0] * 350)
    r = stats.compare(c, t, "proportion")
    pooled = 1250 / 2000
    z = 0.05 / math.sqrt(pooled * (1 - pooled) * (2 / 1000))
    assert abs(r.diff - 0.05) < 1e-12 and abs(r.p - 2 * (1 - stats.norm_cdf(z))) < 1e-12
    assert r.ci[0] < 0.05 < r.ci[1] and abs(r.relative - 0.05 / 0.6) < 1e-12


def test_a_aa_split_rejects_about_alpha():
    rng = random.Random(3)
    hits = 0
    for _ in range(400):
        c = stats.Agg.of([rng.random() < 0.3 for _ in range(400)])
        t = stats.Agg.of([rng.random() < 0.3 for _ in range(400)])
        hits += stats.compare(c, t, "proportion").p < 0.05
    assert 0.02 < hits / 400 < 0.09


def test_cuped_reduces_variance_and_keeps_the_estimate():
    rng = random.Random(5)
    pre = [rng.gauss(10, 3) for _ in range(4000)]
    y = [p * 0.8 + rng.gauss(0, 1) for p in pre]
    arm = [i % 2 for i in range(4000)]
    rows = list(zip(y, pre, arm, strict=True))
    c = stats.Agg.of([v for v, _, a in rows if not a], covariates=[p for _, p, a in rows if not a])
    t = stats.Agg.of([v + 0.1 for v, _, a in rows if a], covariates=[p for _, p, a in rows if a])
    raw, cup = stats.compare(c, t, "mean"), stats.compare(c, t, "mean", cuped=True)
    assert cup.cuped["variance_reduction"] > 0.8 and cup.se < raw.se / 2 and abs(cup.diff - 0.1) < 0.05


def test_a_ratio_uses_the_delta_method():
    a = stats.Agg.of([10, 20, 30, 0], denominators=[2, 4, 5, 1])
    r, v = stats.estimate(a, "ratio")
    assert r == 60 / 12 and v > 0


def test_srm_and_the_share_bound():
    assert stats.srm_p(5000, 5000, 5000) == 1.0 and stats.srm_p(5200, 4800, 5000) < 0.001
    lo, hi = stats.share_bound(10_000, 2000)
    assert lo < 0.2 < hi and hi - lo < 0.03


def test_sample_size_matches_the_classic_formula_and_power_inverts_it():
    nc, nt = stats.sample_size("proportion", 0.6321, 0.03, treatment_bp=5000)
    assert 3970 < nc < 3995 and nc == nt
    assert abs(stats.power_of("proportion", 0.6321, 0.03, nc, nt) - 0.8) < 0.01
    nc2, nt2 = stats.sample_size("proportion", 0.6321, 0.03, treatment_bp=2000)
    assert nt2 < nc2 and nc2 + nt2 > nc + nt          # a smaller treatment arm needs more units in total
    with pytest.raises(ValueError):
        stats.sample_size("mean", 10, 1)              # a mean needs the sd


def test_the_sequential_p_value_only_goes_down_and_needs_more_evidence():
    p1 = stats.msprt_p(0.05, 0.0004, 0.03**2)
    assert stats.msprt_p(0.0, 0.0004, 0.03**2, previous=p1) == p1
    fixed = 2 * (1 - stats.norm_cdf(0.05 / 0.02))
    assert p1 > fixed                                 # the price of looking at any time


def test_a_business_guardrail_needs_a_significant_drop_beyond_the_tolerance():
    c = stats.Agg.of([10.0] * 500 + [12.0] * 500)
    small = stats.Agg.of([9.9] * 500 + [11.9] * 500)
    big = stats.Agg.of([9.0] * 500 + [11.0] * 500)
    assert not stats.harmed(stats.compare(c, small, "mean"), "higher", 0.02)
    assert stats.harmed(stats.compare(c, big, "mean"), "higher", 0.02)


# -- configuration ---------------------------------------------------------------------------------------------

def _ml(**ab):
    t = {**abc.DEFAULTS, "enabled": True, "name": "lapse-v7-vs-v5", "unit": "customer_id",
         "arms": {"control": "champion", "treatment": {"model_version": 7}},
         "metric": {**abc.DEFAULTS["metric"], "name": "renewed", "source": "bq://p.d.renewals",
                    "time_column": "at", "minimum_effect": 0.02}}
    t.update(ab)
    return {"ab_test": t, "data_protection": {"erasure": None}, "data": {}}


def _check(ml, task="binary"):
    problems = []
    abc.check(ml, {"customer_id", "country"}, "lapsed", lambda c, w: True, problems, task)
    return problems


def test_a_complete_section_passes():
    assert _check(_ml()) == []


def test_refusals():
    assert any("no unit-level outcome" in p for p in _check(_ml(), "forecast"))
    assert any("name" in p for p in _check(_ml(name="Bad Name")))
    assert any("model_version" in p for p in _check(_ml(arms={"control": "champion", "treatment": {}})))
    assert any("treatment_bp" in p for p in _check(_ml(allocation={"treatment_bp": 0})))
    assert any("minimum_effect" in p for p in _check(_ml(metric={**_ml()["ab_test"]["metric"],
                                                                "minimum_effect": None})))
    assert any("cuped" in p for p in _check(_ml(analysis={**abc.DEFAULTS["analysis"],
                                                         "cuped": {"enabled": True, "covariate": "x"}})))


def test_the_digest_covers_the_promises_not_the_allocation():
    t = _ml()["ab_test"]
    d = abc.digest(t)
    assert abc.digest({**t, "allocation": {"treatment_bp": 500}}) == d
    assert abc.digest({**t, "metric": {**t["metric"], "window_days": 7}}) != d


# -- the store -----------------------------------------------------------------------------------------------------

class FakeGcs:
    """gcs_read/gcs_write/gcs_list over a dict with generations."""

    def __init__(self, monkeypatch):
        from pipewright.abtest import store as st
        from pipewright.gcp import PreconditionFailed
        self.objects = {}

        def read(gcp, bucket, name):
            o = self.objects.get(name)
            return (o[0], o[1]) if o else (None, 0)

        def write(gcp, bucket, name, text, if_generation_match=None, **k):
            gen = self.objects.get(name, (None, 0))[1]
            if if_generation_match is not None and if_generation_match != gen:
                raise PreconditionFailed(name)
            self.objects[name] = (text, gen + 1)
            return gen + 1

        monkeypatch.setattr(st, "gcs_read", read)
        monkeypatch.setattr(st, "gcs_write", write)
        monkeypatch.setattr(st, "gcs_list",
                            lambda gcp, bucket, prefix: [n for n in self.objects if n.startswith(prefix)])


def test_one_active_test_per_use_case_and_a_name_used_once(monkeypatch):
    FakeGcs(monkeypatch)
    s = TestStore(None, "b")
    s.create({"usecase": "churn", "name": "t1", "state": "running"})
    with pytest.raises(ValueError, match="already has a test t1"):
        s.create({"usecase": "churn", "name": "t2", "state": "running"})
    s.update("churn", "t1", lambda r: {**r, "state": "stopped"})
    assert s.current("churn") is None
    with pytest.raises(ValueError, match="already ran"):
        s.create({"usecase": "churn", "name": "t1", "state": "running"})
    s.create({"usecase": "churn", "name": "t2", "state": "running"})
    assert s.current("churn")["name"] == "t2" and {t["name"] for t in s.list("churn")} == {"t1", "t2"}
