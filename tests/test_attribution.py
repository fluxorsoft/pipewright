"""Attribution (FR-98 stage 4, 1.4.3): Pipewright's own permutation Shapley values, shares and drift."""

import numpy as np

from pipewright.build.templates import pipewright_runtime as rt


def test_exact_for_a_linear_model():
    w = np.array([3.0, -1.0, 0.5])
    rng = np.random.default_rng(0)
    background, rows = rng.normal(size=(20, 3)), rng.normal(size=(4, 3))
    phi = rt.permutation_shapley(lambda z: z @ w, rows, background)
    assert np.allclose(phi, w * (rows - background.mean(axis=0)), atol=1e-6)


def test_shares_sum_to_one_repeat_and_find_a_moved_feature():
    rng = np.random.default_rng(1)
    background, normal = rng.normal(size=(30, 3)), rng.normal(size=(200, 3))
    shifted = normal * np.array([1.0, 1.0, 4.0])                  # the third feature spreads out
    f = lambda z: 1 / (1 + np.exp(-(z @ np.array([2.0, 1.0, 0.5]))))    # noqa: E731
    base = rt.attribution_shares(f, normal[:100], background, ["a", "b", "c"])
    again = rt.attribution_shares(f, normal[:100], background, ["a", "b", "c"])
    assert abs(sum(base.values()) - 1) < 1e-3 and base == again
    same, _ = rt.attribution_drift(base, rt.attribution_shares(f, normal[100:], background, ["a", "b", "c"]))
    moved, top = rt.attribution_drift(base, rt.attribution_shares(f, shifted[100:], background, ["a", "b", "c"]))
    assert same * 2 < moved and top[0]["feature"] == "c"
