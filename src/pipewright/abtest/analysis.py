"""The analysis of an A/B test: one query per metric returns per-arm aggregates; everything else is arithmetic.

Exposures: a unit's first exposure is its first score with this test (batch: inference.output_table rows with ab_test
= the test) or its first logged call (online: the gateway's exposure lines, copied into the tenant's dataset by the
platform's log sink). A unit seen in both arms is counted as switched (it should never happen; it means the
assignment was broken, for example a renamed test) and left out.

Outcomes: events in the metric's source within window_days after the unit's first exposure. Only units whose window
has closed by the newest event in the source count, so slow outcomes are not read as failures (the label-window rule
of training, D-111). The share checks (SRM and the binomial bound) use every exposed unit; the metric uses the units
whose window closed.

Run by `pipewright ab analyse` as a person, or on the test's schedule as the tenant's runner (the launcher hands it a
request). Reads only the tenant's dataset and the sources the use case names; writes aggregates, never a row.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pipewright.abtest import stats

EXPOSURES_ONLINE = "pipewright_ab_exposure"     # the log sink's table in the tenant's dataset (online tests)


def q(name: str) -> str:
    return "`" + str(name).replace("`", "") + "`"


def table(ref: str) -> str:
    return "`" + ref.removeprefix("bq://").replace("`", "") + "`"


def exposures_sql(test: dict, project: str, dataset: str, output_table: str | None, online: bool,
                  segments: list[str]) -> str:
    """unit, arm, first_at (and the segment columns, batch only) per exposed unit; switched units flagged."""
    name = test["name"].replace("'", "")
    parts = []
    seg_cols = "".join(f", ANY_VALUE({q(s)}) AS {q(s)}" for s in segments)
    seg_null = "".join(f", CAST(NULL AS STRING) AS {q(s)}" for s in segments)
    if output_table:
        parts.append(f"SELECT CAST({q(test['unit'])} AS STRING) AS unit, arm, MIN(scored_at) AS first_at{seg_cols} "
                     f"FROM `{project}.{dataset}.{output_table}` WHERE ab_test = '{name}' AND arm IS NOT NULL "
                     f"GROUP BY unit, arm")
    if online:
        parts.append(f"SELECT CAST(jsonPayload.unit AS STRING) AS unit, jsonPayload.arm AS arm, MIN(timestamp) AS "
                     f"first_at{seg_null} FROM `{project}.{dataset}.{EXPOSURES_ONLINE}` WHERE jsonPayload.ab_test = "
                     f"'{name}' AND jsonPayload.arm IN ('control', 'treatment') GROUP BY unit, arm")
    union = "\nUNION ALL\n".join(parts)
    segs = "".join(f", ANY_VALUE({q(s)}) AS {q(s)}" for s in segments)
    return (f"WITH e AS ({union}),\n"
            f"u AS (SELECT unit, ANY_VALUE(arm) AS arm, MIN(first_at) AS first_at, COUNT(DISTINCT arm) > 1 AS "
            f"switched{segs} FROM e GROUP BY unit)\n")


def metric_sql(test: dict, metric: dict, exposures: str, cuped: dict | None, segment: str | None = None) -> str:
    """Per arm (and per segment): exposed units, matured units, and the aggregates stats.Agg needs."""
    key = q(metric.get("key") or test["unit"])
    src, when = table(metric["source"]), q(metric["time_column"])
    where = f" AND ({metric['where']})" if metric.get("where") else ""
    days = int(metric["window_days"])
    kind = metric["type"]
    y = {"proportion": "IF(COUNT(o.t) > 0, 1.0, 0.0)",
         "mean": "COALESCE(SUM(o.v), 0.0)", "ratio": "COALESCE(SUM(o.v), 0.0)"}[kind]
    v = f", CAST({q(metric['value_column'])} AS FLOAT64) AS v" if kind in ("mean", "ratio") else ", 1.0 AS v"
    dcol = f", CAST({q(metric['denominator_column'])} AS FLOAT64) AS d" if kind == "ratio" else ", 0.0 AS d"
    d = "COALESCE(SUM(o.d), 0.0)" if kind == "ratio" else "0.0"
    cov_join = cov_col = ""
    if cuped:
        cov_join = (f" LEFT JOIN (SELECT CAST({q(cuped.get('key') or test['unit'])} AS STRING) AS unit, "
                    f"CAST({q(cuped['covariate'])} AS FLOAT64) AS c FROM {table(cuped['source'])} WHERE "
                    f"feature_timestamp <= TIMESTAMP('{cuped['as_of']}') QUALIFY ROW_NUMBER() OVER (PARTITION BY "
                    f"{q(cuped.get('key') or test['unit'])} ORDER BY feature_timestamp DESC) = 1) cv "
                    "ON cv.unit = u.unit")
        cov_col = ", COALESCE(ANY_VALUE(cv.c), 0.0) AS c"
    seg = f", u.{q(segment)} AS seg" if segment else ", 'all' AS seg"
    return exposures + f""",
src AS (SELECT CAST({key} AS STRING) AS unit, TIMESTAMP({when}) AS t{v}{dcol} FROM {src} WHERE TRUE{where}),
anchor AS (SELECT MAX(t) AS newest FROM src),
per AS (SELECT u.unit, u.arm{seg}, u.first_at,
  TIMESTAMP_ADD(u.first_at, INTERVAL {days} DAY) <= (SELECT newest FROM anchor) AS matured,
  {y} AS y, {d} AS d{cov_col}
  FROM u LEFT JOIN src o
    ON o.unit = u.unit AND o.t >= u.first_at AND o.t < TIMESTAMP_ADD(u.first_at, INTERVAL {days} DAY)
  {cov_join}
  WHERE NOT u.switched
  GROUP BY u.unit, u.arm, seg, u.first_at)
SELECT arm, seg, COUNT(*) AS exposed, COUNTIF(matured) AS n,
  SUM(IF(matured, y, 0)) AS y, SUM(IF(matured, y * y, 0)) AS yy,
  SUM(IF(matured, d, 0)) AS d, SUM(IF(matured, d * d, 0)) AS dd, SUM(IF(matured, y * d, 0)) AS yd,
  {"SUM(IF(matured, c, 0))" if cuped else "0.0"} AS c, {"SUM(IF(matured, c * c, 0))" if cuped else "0.0"} AS cc,
  {"SUM(IF(matured, y * c, 0))" if cuped else "0.0"} AS yc,
  (SELECT COUNTIF(switched) FROM u) AS switched, MIN(first_at) AS first_exposure, (SELECT newest FROM anchor) AS newest
FROM per GROUP BY arm, seg"""


def aggs(rows: list[dict]) -> dict:
    """{segment: {arm: (Agg, exposed)}} and the shared facts (switched, first exposure, newest outcome)."""
    out, facts = {}, {"switched": 0, "first_exposure": None, "newest_outcome": None}
    for r in rows:
        a = stats.Agg(int(r["n"] or 0), *(float(r[k] or 0) for k in ("y", "yy", "d", "dd", "yd", "c", "cc", "yc")))
        out.setdefault(str(r["seg"]), {})[r["arm"]] = (a, int(r["exposed"] or 0))
        facts["switched"] = int(r["switched"] or 0)
        fe = r.get("first_exposure")
        facts["first_exposure"] = min(filter(None, [facts["first_exposure"], fe])) if fe else facts["first_exposure"]
        facts["newest_outcome"] = r.get("newest") or facts["newest_outcome"]
    return {"segments": out, **facts}


def _cmp_dict(c: stats.Comparison) -> dict:
    return {"control": c.control, "treatment": c.treatment, "diff": c.diff, "ci": list(c.ci), "p": c.p,
            "relative": c.relative, "relative_ci": list(c.relative_ci) if c.relative_ci else None,
            "n": list(c.n), **({"cuped": c.cuped} if c.cuped else {})}


LOSSES = {  # what each of the gateway's counts means for the comparison
    "unassigned": "the gateway served {n} unit(s) without a unit key (not assigned, not exposed)",
    "dropped": "{n} exposure line(s) were dropped or not written by the gateway",
    "mismatched_model": "{n} treatment call(s) were answered by a model other than the treatment's deployment",
    "bypassed": "the treatment's endpoint answered {n} call(s) the gateway did not send (callers around the router)",
}


def evaluate(test: dict, section: dict, primary: dict, guardrails: dict[str, dict], now: datetime,
             previous: dict | None = None) -> dict:
    """The analysis result from the aggregates: share checks, the primary metric (with CUPED and the sequential
    p-value when configured), guardrails, segments, progress against the planned sample, and what it means:
    continue, decision_required (and why), or blocked (a share check failed)."""
    a = section["analysis"]
    metric = section["metric"]
    alpha, kind = a["alpha"], metric["type"]
    bp = int((test.get("allocation") or [{}])[-1].get("treatment_bp", 0)) if test.get("allocation") else 0
    seg_all = primary["segments"].get("all", {})
    (c, c_exp), (t, t_exp) = seg_all.get("control", (stats.Agg(), 0)), seg_all.get("treatment", (stats.Agg(), 0))
    out = {"at": now.isoformat(timespec="seconds"), "exposed": {"control": c_exp, "treatment": t_exp},
           "matured": {"control": c.n, "treatment": t.n}, "switched": primary["switched"],
           "first_exposure": str(primary["first_exposure"]) if primary["first_exposure"] else None,
           "newest_outcome": str(primary["newest_outcome"]) if primary["newest_outcome"] else None}
    # 1. integrity of the split: the chi-square and the direct bound (spike S6: the chi-square alone is weak)
    blocked = []
    # the plan's share is the allocation the units were exposed under; a lowered allocation keeps its history, so the
    # check uses the first allocation of the test (units exposed before a lowering stay in their arm)
    plan_bp = int(test["allocation"][0]["treatment_bp"]) if test.get("allocation") else bp
    srm = stats.srm_p(c_exp, t_exp, plan_bp) if len(test.get("allocation") or []) <= 1 else None
    total = c_exp + t_exp
    lo, hi = stats.share_bound(total, plan_bp)
    share = t_exp / total if total else None
    out["shares"] = {"planned": plan_bp / 10000, "observed": share, "bound": [lo, hi], "srm_p": srm}
    if srm is not None and srm < 0.001:
        blocked.append(f"sample ratio mismatch: treatment share {share:.4f} against {plan_bp / 10000:.4f} planned "
                       f"(p {srm:.2g})")
    elif share is not None and total >= 100 and len(test.get("allocation") or []) <= 1 and not lo <= share <= hi:
        blocked.append(f"the treatment share {share:.4f} is outside its expected range {lo:.4f} to {hi:.4f}")
    if primary["switched"]:
        blocked.append(f"{primary['switched']} unit(s) were exposed to both arms: the assignment was broken")
    for k in ("unassigned", "dropped", "mismatched_model", "bypassed"):
        if (test.get("router") or {}).get(k):
            blocked.append(LOSSES[k].format(n=test["router"][k]))
    out["blocked"] = blocked
    # 2. the primary metric
    cuped = bool((a.get("cuped") or {}).get("enabled"))
    if c.n >= 2 and t.n >= 2:
        cmp = stats.compare(c, t, kind, alpha, cuped=cuped)
        out["primary"] = {"name": metric["name"], "type": kind, "direction": metric["direction"], **_cmp_dict(cmp)}
        better = (cmp.diff > 0) == (metric["direction"] == "higher")
        out["primary"]["treatment_better"] = better and cmp.p < alpha
        out["primary"]["treatment_worse"] = (not better) and cmp.p < alpha
        if a["method"] == "sequential":
            tau = a.get("tau") or metric["minimum_effect"]
            prev = ((previous or {}).get("primary") or {}).get("p_sequential", 1.0)
            out["primary"]["p_sequential"] = stats.msprt_p(cmp.diff, cmp.se ** 2, tau ** 2, prev)
    else:
        out["primary"] = {"name": metric["name"], "note": "not enough matured units in both arms yet"}
    # 3. the planned sample (from the control's observed baseline) and progress
    try:
        base, sd = (c.y / c.n if c.n else None), None
        if kind != "proportion" and c.n > 1:
            sd = ((c.yy - c.y * c.y / c.n) / (c.n - 1)) ** 0.5
        if base is not None and 0 < plan_bp < 10000 and (kind == "proportion" or sd):
            need_c, need_t = stats.sample_size(kind, base, metric["minimum_effect"], sd, alpha, a["power"], plan_bp)
            out["plan"] = {"needed": {"control": need_c, "treatment": need_t},
                           "progress": min(c.n / need_c, t.n / need_t) if need_c and need_t else None}
    except ValueError as e:
        out["plan"] = {"note": str(e)}
    # 4. guardrails (business)
    gres = []
    for g in section["guardrails"].get("business") or []:
        gseg = (guardrails.get(g["name"]) or {}).get("segments", {}).get("all", {})
        (gc, _), (gt, _) = gseg.get("control", (stats.Agg(), 0)), gseg.get("treatment", (stats.Agg(), 0))
        if gc.n >= 2 and gt.n >= 2:
            gcmp = stats.compare(gc, gt, g["type"], alpha)
            gres.append({"name": g["name"], "max_drop": g["max_drop"], **_cmp_dict(gcmp),
                         "breached": stats.harmed(gcmp, g["direction"], g["max_drop"], alpha)})
        else:
            gres.append({"name": g["name"], "note": "not enough matured units yet", "breached": False})
    out["guardrails"] = gres
    # 5. segments: for reading only
    segs = {}
    for name, arms in primary["segments"].items():
        # online exposures carry no segment (segments are a batch test's scored columns): no "None" segment
        if name in ("all", None, "None") or "control" not in arms or "treatment" not in arms:
            continue
        sc, st_ = arms["control"][0], arms["treatment"][0]
        if sc.n >= 30 and st_.n >= 30:
            segs[name] = _cmp_dict(stats.compare(sc, st_, kind, alpha))
    if segs:
        out["segments"] = segs
    # 6. what it means
    started = datetime.fromisoformat(test["started_at"])
    age = (now - started).total_seconds() / 86400
    d = section["duration"]
    out["age_days"] = round(age, 2)
    out["duration"] = {"min_days": d["min_days"], "max_days": d["max_days"]}     # FR-121: what comes next
    reasons = []
    if any(g.get("breached") for g in gres):
        reasons.append("a business guardrail is breached: " + ", ".join(g["name"] for g in gres if g.get("breached")))
    if age >= d["max_days"]:
        reasons.append(f"the test reached max_days ({d['max_days']})")
    elif age >= d["min_days"]:
        prog = (out.get("plan") or {}).get("progress")
        if a["method"] == "sequential" and (out["primary"].get("p_sequential", 1.0) < alpha):
            reasons.append(f"the sequential test is conclusive (p {out['primary']['p_sequential']:.3g})")
        elif prog is not None and prog >= 1:
            reasons.append("the planned sample is reached")
    out["decision_reasons"] = reasons
    out["status"] = "blocked" if blocked else ("decision_required" if reasons else "continue")
    return out


def render(result: dict, test: dict) -> str:
    """A short text for `pipewright ab status` and the decision pack."""
    lines = [f"test {test['name']} ({test.get('state')}), {result.get('age_days', result.get('days', 0))} days, "
             f"status {result['status']}"]
    if result.get("mode") == "shadow":
        s = result.get("shadow") or {}

        def num(v, f):
            return format(v, f) if isinstance(v, (int, float)) else "n/a"

        lines.append(f"  shadow: {s.get('calls', 0)} calls copied to the treatment, {s.get('failures', 0)} failed "
                     f"({num(s.get('failure_rate'), '.2%')}); {s.get('compared', 0)} scores compared: mean absolute "
                     f"difference {num(s.get('mean_abs_diff'), '.4f')}, agreement {num(s.get('agreement'), '.1%')}; "
                     f"treatment latency p50 {num(s.get('ms_p50'), '.0f')} ms, p95 {num(s.get('ms_p95'), '.0f')} ms")
        for b in result.get("blocked") or []:
            lines.append(f"  BLOCKED: {b}")
        for r in result.get("decision_reasons") or []:
            lines.append(f"  decision: {r}")
        return "\n".join(lines)
    sh = result.get("shares") or {}
    if sh.get("observed") is not None:
        lines.append(f"  shares: treatment {sh['observed']:.4f} (planned {sh['planned']:.4f}, expected "
                     f"{sh['bound'][0]:.4f} to {sh['bound'][1]:.4f}" +
                     (f", SRM p {sh['srm_p']:.3g})" if sh.get("srm_p") is not None else ")"))
    lines.append(f"  exposed: control {result['exposed']['control']}, treatment {result['exposed']['treatment']}; "
                 f"outcome window closed: control {result['matured']['control']}, treatment "
                 f"{result['matured']['treatment']}")
    p = result.get("primary") or {}
    if "diff" in p:
        rel = f" ({p['relative']:+.1%})" if p.get("relative") is not None else ""
        lines.append(f"  {p['name']}: control {p['control']:.4f}, treatment {p['treatment']:.4f}, difference "
                     f"{p['diff']:+.4f}{rel}, 95% interval {p['ci'][0]:+.4f} to {p['ci'][1]:+.4f}, p {p['p']:.3g}"
                     + (f", sequential p {p['p_sequential']:.3g}" if "p_sequential" in p else "")
                     + (f", CUPED variance -{p['cuped']['variance_reduction']:.0%}" if p.get("cuped") else ""))
    elif p:
        lines.append(f"  {p.get('name')}: {p.get('note')}")
    plan = result.get("plan") or {}
    if plan.get("needed"):
        lines.append(f"  planned sample per arm: control {plan['needed']['control']}, treatment "
                     f"{plan['needed']['treatment']}; progress {plan['progress']:.0%}")
    for g in result.get("guardrails") or []:
        lines.append(f"  guardrail {g['name']}: " + ("BREACHED" if g.get("breached") else "ok")
                     + (f", relative {g['relative']:+.1%}" if g.get("relative") is not None else
                        f" ({g.get('note')})" if g.get("note") else ""))
    for b in result.get("blocked") or []:
        lines.append(f"  BLOCKED: {b}")
    for r in result.get("decision_reasons") or []:
        lines.append(f"  decision required: {r}")
    for name, s in sorted((result.get("segments") or {}).items()):
        lines.append(f"  segment {name}: difference {s['diff']:+.4f} (p {s['p']:.3g}; for reading, not deciding)")
    return "\n".join(lines)


def shadow_sql(test: dict, project: str, dataset: str) -> str:
    """The gateway's comparison lines of a shadow test (one per shadowed call), summed."""
    name = test["name"].replace("'", "")
    j = "jsonPayload"
    n = f"CAST({j}.n AS FLOAT64)"
    return (f"SELECT COUNT(*) AS calls, COUNTIF({j}.ok = FALSE) AS failures, COALESCE(SUM({n}), 0) AS compared, "
            f"SAFE_DIVIDE(SUM(CAST({j}.mean_abs_diff AS FLOAT64) * {n}), SUM(IF({j}.mean_abs_diff IS NULL, 0, {n}))) "
            f"AS mean_abs_diff, SAFE_DIVIDE(SUM(CAST({j}.agreement AS FLOAT64) * {n}), "
            f"SUM(IF({j}.agreement IS NULL, 0, {n}))) AS agreement, "
            f"APPROX_QUANTILES(CAST({j}.ms AS FLOAT64), 100)[SAFE_OFFSET(50)] AS ms_p50, "
            f"APPROX_QUANTILES(CAST({j}.ms AS FLOAT64), 100)[SAFE_OFFSET(95)] AS ms_p95, MIN(timestamp) AS first_at "
            f"FROM `{project}.{dataset}.{EXPOSURES_ONLINE}` WHERE {j}.ab_test = '{name}' AND {j}.event = 'shadow'")


def evaluate_shadow(test: dict, section: dict, row: dict | None, now: datetime) -> dict:
    """A shadow test's verdict: how often the treatment failed, how far its scores were from the control's and how
    often they agreed, its latency; a decision is due after min_days (at most max_days). The treatment cannot win a
    shadow: what follows is an A/B test or a normal deploy."""
    row = row or {}
    started = datetime.fromisoformat(test["started_at"].replace("Z", "+00:00"))
    days = (now - started).total_seconds() / 86400
    dur = section.get("duration") or {}
    calls, failures = int(row.get("calls") or 0), int(row.get("failures") or 0)
    rate = failures / calls if calls else 0.0
    limit = float((((section.get("guardrails") or {}).get("technical") or {}).get("breaker") or {})
                  .get("max_error_rate", 0.05))
    shadow = {"calls": calls, "failures": failures, "failure_rate": round(rate, 6),
              "compared": int(row.get("compared") or 0), "mean_abs_diff": row.get("mean_abs_diff"),
              "agreement": row.get("agreement"), "ms_p50": row.get("ms_p50"), "ms_p95": row.get("ms_p95")}
    guard = [{"name": "treatment_errors", "kind": "technical", "value": round(rate, 6), "limit": limit,
              "breached": calls >= 20 and rate > limit}]
    reasons = []
    if days >= int(dur.get("max_days", 60)):
        reasons.append(f"max_days ({dur.get('max_days', 60)}) is reached")
    elif days >= int(dur.get("min_days", 14)) and shadow["compared"] > 0:
        reasons.append(f"min_days ({dur.get('min_days', 14)}) is reached with {shadow['compared']} scores compared")
    if guard[0]["breached"]:
        reasons.append(f"the treatment failed {rate:.2%} of shadowed calls (more than {limit:.0%})")
    blocked = [] if calls else ["no shadow lines yet: the gateway logs one per shadowed call"]
    status = "blocked" if blocked else ("decision_required" if reasons else "continue")
    return {"at": now.isoformat(), "mode": "shadow", "status": status, "blocked": blocked,
            "decision_reasons": reasons, "shadow": shadow, "guardrails": guard, "days": round(days, 2),
            "primary": {"name": "shadow", "treatment_better": False},
            "exposed": {"control": calls, "treatment": calls}, "matured": {"control": 0, "treatment": 0}}


def run(test: dict, section: dict, project: str, dataset: str, output_table: str | None, online: bool, query,
        now: datetime | None = None, previous: dict | None = None) -> dict:
    """Runs the queries with `query(sql) -> rows` (the caller's BigQuery, with the environment's key) and evaluates.
    The CUPED covariate is read as of the test's start."""
    now = now or datetime.now(timezone.utc)
    if test.get("mode") == "shadow":
        try:
            rows = query(shadow_sql(test, project, dataset)) if online else []
        except Exception:  # noqa: BLE001 - the sink makes the table (and its columns) with the first lines
            rows = []
        return evaluate_shadow(test, section, rows[0] if rows else None, now)
    segments = list(section["analysis"].get("segments") or [])
    exposures = exposures_sql(test, project, dataset, output_table, online, segments)
    cup = section["analysis"].get("cuped") or {}
    cuped = ({**cup, "as_of": test["started_at"]} if cup.get("enabled") else None)
    rows = query(metric_sql(test, section["metric"], exposures, cuped))
    for s in segments:
        rows += query(metric_sql(test, section["metric"], exposures, None, segment=s))
    primary = aggs(rows)
    guard = {}
    for g in section["guardrails"].get("business") or []:
        guard[g["name"]] = aggs(query(metric_sql(test, g, exposures, None)))
    return evaluate(test, section, primary, guard, now, previous)


def results_row(test: dict, result: dict, release: str | None) -> dict:
    """The row the analysis appends to the results table: aggregates and decisions, never a unit."""
    p = result.get("primary") or {}
    return {"analysed_at": result["at"], "ab_test": test["name"], "usecase": test["usecase"], "release": release,
            "status": result["status"], "metric": p.get("name"), "control": p.get("control"),
            "treatment": p.get("treatment"), "diff": p.get("diff"), "ci_low": (p.get("ci") or [None, None])[0],
            "ci_high": (p.get("ci") or [None, None])[1], "p": p.get("p"), "p_sequential": p.get("p_sequential"),
            "exposed_control": result["exposed"]["control"], "exposed_treatment": result["exposed"]["treatment"],
            "matured_control": result["matured"]["control"], "matured_treatment": result["matured"]["treatment"],
            "result_json": json.dumps(result, default=str)}


RESULTS_SCHEMA = [
    {"name": "analysed_at", "type": "TIMESTAMP"}, {"name": "ab_test", "type": "STRING"},
    {"name": "usecase", "type": "STRING"}, {"name": "release", "type": "STRING"}, {"name": "status", "type": "STRING"},
    {"name": "metric", "type": "STRING"}, {"name": "control", "type": "FLOAT"}, {"name": "treatment", "type": "FLOAT"},
    {"name": "diff", "type": "FLOAT"}, {"name": "ci_low", "type": "FLOAT"}, {"name": "ci_high", "type": "FLOAT"},
    {"name": "p", "type": "FLOAT"}, {"name": "p_sequential", "type": "FLOAT"},
    {"name": "exposed_control", "type": "INTEGER"}, {"name": "exposed_treatment", "type": "INTEGER"},
    {"name": "matured_control", "type": "INTEGER"}, {"name": "matured_treatment", "type": "INTEGER"},
    {"name": "result_json", "type": "STRING"},
]
