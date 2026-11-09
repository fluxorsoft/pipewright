"""Statistics from per-arm aggregates, so the analysis never pulls rows out of BigQuery: one query returns, per arm,
n and the sums below, and everything here is arithmetic on them. Standard library only.

    Agg(n, y, yy)                       a proportion (y is 0/1) or a mean: sum y, sum y^2
    + d, dd, yd                         a ratio metric (sum y / sum d): the denominator's sums and the cross product
    + c, cc, yc                         CUPED: a pre-period covariate's sums and its cross product with y

Checked against statsmodels and 2,000 A/A splits on the book's data (spike S6): the same p-values, 4.8% false
positives at alpha 0.05, power 0.50 simulated against 0.52 from the formula, CUPED 15.4% less variance.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

# ----------------------------------------------------------------------------------------------- normal --

def norm_cdf(z: float) -> float:
    return 0.5 * math.erfc(-z / math.sqrt(2))


def norm_ppf(p: float) -> float:
    """Inverse of the standard normal CDF (Acklam's approximation and one Newton step: about 1e-15)."""
    if not 0 < p < 1:
        raise ValueError("p must be in (0, 1)")
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00)
    lo = 0.02425
    if p < lo:
        q = math.sqrt(-2 * math.log(p))
        x = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    elif p > 1 - lo:
        q = math.sqrt(-2 * math.log(1 - p))
        x = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    else:
        q = p - 0.5
        r = q * q
        x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
            (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    e = norm_cdf(x) - p                                    # one Newton step
    return x - e * math.sqrt(2 * math.pi) * math.exp(x * x / 2)


# ------------------------------------------------------------------------------------------- aggregates --

@dataclass
class Agg:
    n: int = 0
    y: float = 0.0
    yy: float = 0.0
    d: float = 0.0
    dd: float = 0.0
    yd: float = 0.0
    c: float = 0.0
    cc: float = 0.0
    yc: float = 0.0

    @classmethod
    def of(cls, values, denominators=None, covariates=None) -> Agg:
        """From unit-level values (tests and small checks; the analysis gets the sums from BigQuery)."""
        a = cls()
        values = list(values)
        denominators = None if denominators is None else list(denominators)
        covariates = None if covariates is None else list(covariates)
        for i, v in enumerate(values):
            v = float(v)
            a.n += 1
            a.y += v
            a.yy += v * v
            if denominators is not None:
                w = float(denominators[i])
                a.d += w
                a.dd += w * w
                a.yd += v * w
            if covariates is not None:
                x = float(covariates[i])
                a.c += x
                a.cc += x * x
                a.yc += v * x
        return a

    def __add__(self, o: Agg) -> Agg:
        return Agg(*(getattr(self, f) + getattr(o, f) for f in ("n", "y", "yy", "d", "dd", "yd", "c", "cc", "yc")))


def _var(n: int, s: float, ss: float) -> float:
    return max((ss - s * s / n) / (n - 1), 0.0) if n > 1 else 0.0


def _cov(n: int, s1: float, s2: float, s12: float) -> float:
    return (s12 - s1 * s2 / n) / (n - 1) if n > 1 else 0.0


def cuped_theta(both: Agg) -> float:
    """theta = cov(y, c) / var(c), from both arms together (the covariate is measured before the test, so it is the
    same distribution in both arms; pooling keeps the estimate unbiased)."""
    vc = _var(both.n, both.c, both.cc)
    return _cov(both.n, both.y, both.c, both.yc) / vc if vc > 0 else 0.0


def estimate(a: Agg, kind: str, theta: float | None = None, grand_c: float | None = None) -> tuple[float, float]:
    """(the arm's metric, the variance of that estimate). kind: proportion | mean | ratio. With theta, the CUPED
    adjusted mean: y - theta * (c - grand mean of c)."""
    if a.n < 2:
        return (a.y / a.n if a.n else 0.0), float("inf")
    if kind == "ratio":
        if a.d == 0:
            return 0.0, float("inf")
        my, md = a.y / a.n, a.d / a.n
        vy, vd, cyd = _var(a.n, a.y, a.yy), _var(a.n, a.d, a.dd), _cov(a.n, a.y, a.d, a.yd)
        r = a.y / a.d                                     # the delta method for sum y / sum d
        v = (vy / md**2 - 2 * my * cyd / md**3 + my**2 * vd / md**4) / a.n
        return r, max(v, 0.0)
    m, v = a.y / a.n, _var(a.n, a.y, a.yy)
    if theta is not None and grand_c is not None:
        mc, vc, cyc = a.c / a.n, _var(a.n, a.c, a.cc), _cov(a.n, a.y, a.c, a.yc)
        m = m - theta * (mc - grand_c)
        v = max(v + theta**2 * vc - 2 * theta * cyc, 0.0)
    return m, v / a.n


@dataclass
class Comparison:
    control: float
    treatment: float
    diff: float                        # treatment - control
    se: float
    ci: tuple                          # 95% (or 1 - alpha) interval of diff
    relative: float | None             # diff / control
    relative_ci: tuple | None          # approximate: the interval of diff over the control's estimate
    p: float                           # two-sided
    n: tuple                           # (control, treatment)
    cuped: dict = field(default_factory=dict)


def compare(control: Agg, treatment: Agg, kind: str, alpha: float = 0.05, cuped: bool = False) -> Comparison:
    """The treatment against the control: difference, interval and p-value. A proportion without CUPED uses the
    pooled standard error for the p-value (the two-proportion z-test, as statsmodels does) and the unpooled one for
    the interval; everything else uses the unpooled (Welch) standard error with the normal distribution."""
    theta = grand = None
    info = {}
    if cuped and kind != "ratio":
        both = control + treatment
        theta, grand = cuped_theta(both), (both.c / both.n if both.n else 0.0)
        _, v0 = estimate(control, kind)
        _, v1 = estimate(treatment, kind)
    m0, v0a = estimate(control, kind, theta, grand)
    m1, v1a = estimate(treatment, kind, theta, grand)
    if theta is not None:
        raw = v0 + v1
        info = {"theta": theta, "variance_reduction": (1 - (v0a + v1a) / raw) if raw > 0 else 0.0}
    diff, se = m1 - m0, math.sqrt(v0a + v1a)
    z_ci = norm_ppf(1 - alpha / 2)
    ci = (diff - z_ci * se, diff + z_ci * se)
    if kind == "proportion" and theta is None and control.n and treatment.n:
        pooled = (control.y + treatment.y) / (control.n + treatment.n)
        se_p = math.sqrt(pooled * (1 - pooled) * (1 / control.n + 1 / treatment.n))
    else:
        se_p = se
    p = 2 * (1 - norm_cdf(abs(diff) / se_p)) if se_p > 0 and math.isfinite(se_p) else 1.0
    rel = rel_ci = None
    if m0:
        rel, rel_ci = diff / m0, (ci[0] / m0, ci[1] / m0) if m0 > 0 else (ci[1] / m0, ci[0] / m0)
    return Comparison(m0, m1, diff, se, ci, rel, rel_ci, min(max(p, 0.0), 1.0), (control.n, treatment.n), info)


# ------------------------------------------------------------------------------------- shares and size --

def srm_p(n_control: int, n_treatment: int, treatment_bp: int) -> float:
    """Sample ratio mismatch: the chi-square test (1 degree of freedom) of the observed split against the plan."""
    n = n_control + n_treatment
    if n == 0 or treatment_bp in (0, 10000):
        return 1.0
    e1 = n * treatment_bp / 10000
    e0 = n - e1
    chi2 = (n_treatment - e1) ** 2 / e1 + (n_control - e0) ** 2 / e0
    return math.erfc(math.sqrt(chi2 / 2))


def share_bound(n: int, treatment_bp: int, z: float = 3.29) -> tuple[float, float]:
    """The treatment share's expected range for n units (z 3.29: two-sided p 0.001). The direct check beside the
    chi-square: spike S6 found the chi-square alone flags a 4% loss in one arm only 2.6% of the time at n = 4,276,
    so the analysis also reports how far the share is from the plan and the router's unassigned and dropped counts."""
    p = treatment_bp / 10000
    if n == 0:
        return 0.0, 1.0
    half = z * math.sqrt(p * (1 - p) / n)
    return max(p - half, 0.0), min(p + half, 1.0)


def sample_size(kind: str, baseline: float, minimum_effect: float, sd: float | None = None, alpha: float = 0.05,
                power: float = 0.8, treatment_bp: int = 5000) -> tuple[int, int]:
    """Units needed (control, treatment) to detect an absolute change of minimum_effect with the given power, for an
    allocation of treatment_bp. A proportion uses p(1-p) at the baseline; a mean or ratio needs the unit-level sd."""
    if minimum_effect <= 0:
        raise ValueError("minimum_effect must be above 0")
    if not 0 < treatment_bp < 10000:
        raise ValueError("treatment_bp must be between 0 and 10000 exclusive")
    if kind == "proportion":
        p1 = min(max(baseline + minimum_effect, 0.0), 1.0)
        var = (baseline * (1 - baseline) + p1 * (1 - p1)) / 2
    else:
        if sd is None:
            raise ValueError("a mean or ratio metric needs the unit-level standard deviation")
        var = sd * sd
    k = treatment_bp / (10000 - treatment_bp)            # treatment units per control unit
    za, zb = norm_ppf(1 - alpha / 2), norm_ppf(power)
    n_control = (za + zb) ** 2 * var * (1 + 1 / k) / minimum_effect**2
    return math.ceil(n_control), math.ceil(n_control * k)


def power_of(kind: str, baseline: float, minimum_effect: float, n_control: int, n_treatment: int,
             sd: float | None = None, alpha: float = 0.05) -> float:
    if kind == "proportion":
        p1 = min(max(baseline + minimum_effect, 0.0), 1.0)
        var0, var1 = baseline * (1 - baseline), p1 * (1 - p1)
    else:
        var0 = var1 = (sd or 0.0) ** 2
    if n_control <= 0 or n_treatment <= 0:
        return 0.0
    se = math.sqrt(var0 / n_control + var1 / n_treatment)
    if se == 0:
        return 1.0
    return norm_cdf(abs(minimum_effect) / se - norm_ppf(1 - alpha / 2))


# ------------------------------------------------------------------------------------------- sequential --

def msprt_p(diff: float, var: float, tau2: float, previous: float = 1.0) -> float:
    """The mixture sequential probability ratio test (Johari, Pekelis and Walsh): an always-valid p-value, so the
    test may be looked at after every analysis without inflating false positives. tau2 is the mixing variance (the
    planned minimum effect squared by default); previous is the last look's p-value: the p-value only goes down."""
    if var <= 0 or not math.isfinite(var):
        return previous
    lam = math.sqrt(var / (var + tau2)) * math.exp(tau2 * diff * diff / (2 * var * (var + tau2)))
    return min(previous, 1.0 / lam if lam > 0 else 1.0, 1.0)


def harmed(cmp: Comparison, direction: str, max_drop: float, alpha: float = 0.05) -> bool:
    """A business guardrail: breached when the relative change is worse than max_drop and the harm is significant
    (one-sided at alpha). direction: higher is better, or lower is better."""
    if cmp.relative is None or not math.isfinite(cmp.se) or cmp.se == 0:
        return False
    worse = -cmp.diff if direction == "higher" else cmp.diff
    worse_rel = -cmp.relative if direction == "higher" else cmp.relative
    p_one = 1 - norm_cdf(worse / cmp.se)
    return worse_rel > max_drop and p_one < alpha
