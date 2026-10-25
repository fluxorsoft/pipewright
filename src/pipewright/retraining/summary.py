"""The decision pack: an email-sized summary (on every decision event) and the full view (`pipewright retrain
show`). Built only from what the case holds: observations from inference runs, data checks, the champion.

The summary has no data values beyond metrics and column names, and stays under SUMMARY_LIMIT characters so it fits
a Cloud Monitoring label (alert emails).
"""

from __future__ import annotations

from pipewright.retraining.cases import approved_groups, worst_psi

SUMMARY_LIMIT = 1000


def _num(v, digits: int = 3) -> str:
    return "n/a" if v is None else f"{v:.{digits}f}" if isinstance(v, float) else str(v)


def _is_it_real(case: dict) -> list[str]:
    obs = case.get("observations") or []
    latest = obs[-1] if obs else {}
    out = []
    psi = latest.get("feature_psi") or {}
    drifted = latest.get("drifted_features") or []
    if drifted:
        top = sorted(((c, psi.get(c)) for c in drifted), key=lambda x: -(x[1] or 0))[:4]
        out.append("Drifted: " + ", ".join(f"{c} {_num(v)}" for c, v in top)
                   + (f" (+{len(drifted) - 4} more)" if len(drifted) > 4 else ""))
    if latest.get("prediction_psi") is not None:
        out.append(f"Prediction PSI {_num(latest['prediction_psi'])}")
    trend = [worst_psi(o) for o in (latest.get("recent") or []) if worst_psi(o) is not None]
    trend += [worst_psi(o) for o in obs[-5:] if worst_psi(o) is not None]
    if len(trend) > 1:
        out.append("Worst PSI, recent runs: " + ", ".join(_num(v, 2) for v in trend[-6:]))
    perf = latest.get("performance") or {}
    labelled = latest.get("labelled_rows")
    if perf:
        metric, value = next(iter(perf.items()))
        out.append(f"Live {metric} {_num(value)} on {labelled} labelled rows")
    elif labelled is not None:
        out.append(f"Performance not judged yet: {labelled} labelled rows")
    return out


def _data_checks(case: dict) -> str | None:
    latest = (case.get("observations") or [{}])[-1]
    checks = latest.get("data_checks")
    if not checks:
        return None
    if checks.get("errors"):
        return f"Data checks FAILED: {'; '.join(checks['errors'][:2])}"
    nulls = checks.get("null_fractions") or {}
    worst = max(nulls.items(), key=lambda kv: kv[1], default=None)
    rows = checks.get("rows")
    text = f"Data checks passed ({rows} rows" if rows is not None else "Data checks passed ("
    text += f"; most nulls {worst[0]} {worst[1]:.1%})" if worst and worst[1] else ")"
    return text


def summary(case: dict, event_type: str = "") -> str:
    """Plain text, under SUMMARY_LIMIT characters."""
    cid, uc, env = case["case_id"], case["usecase"], case["environment"]
    state = case["state"]
    head = {"retrain.proposed": "needs a decision", "retrain.reopened": "re-opened, needs a decision",
            "retrain.escalated": "still undecided", "retrain.approval": "approval recorded",
            "retrain.approved": "approved", "retrain.declined": "declined", "retrain.snoozed": "snoozed",
            "retrain.training_failed": "approved, but its training failed",
            "retrain.training_retried": "approved; training resubmitted",
            "retrain.training_finished": "approved; training finished"}
    lines = [f"Case {cid}: {uc} ({env}), {', '.join(case.get('triggers') or [])}: {head.get(event_type, state)}."]
    champion = case.get("champion") or {}
    if champion.get("version"):
        age = f", trained {champion['created'][:10]}" if champion.get("created") else ""
        lines.append(f"Champion: version {champion['version']}{age}.")
    lines += [f"{line}." for line in _is_it_real(case)]
    checks = _data_checks(case)
    if checks:
        lines.append(checks + ".")
    if case.get("mode") == "candidate":
        lines.append("A candidate model trains now; it is never promoted without a person.")
    else:
        lines.append(f"Retraining would use: {case.get('source_uri') or 'a source chosen by the approver'}.")
    required = case.get("required") or []
    if required:
        done = approved_groups(case)
        lines.append("Approvers: " + ", ".join(f"{g}{' (done)' if g in done else ''}" for g in required) + ".")
    if case.get("until") and state in ("snoozed", "declined"):
        lines.append(f"Until {case['until'][:16]}.")
    last = (case.get("history") or [{}])[-1]
    if last.get("by") and last.get("action") in ("approve", "decline", "snooze"):
        lines.append(f"By {last['by']}: {last.get('reason', '')[:120]}")
    lines.append(f"Decide: pipewright retrain show --env {env} --usecase {uc} --case {cid}")
    text = "\n".join(lines)
    if len(text) > SUMMARY_LIMIT:
        keep = lines[-1]
        text = text[: SUMMARY_LIMIT - len(keep) - 5].rstrip() + " ...\n" + keep
    return text


def render(case: dict) -> str:
    """The full decision pack for a terminal."""
    lines = [summary(case), "", f"State: {case['state']}  mode {case.get('mode')}  opened {case.get('opened_at')}  "
             f"created {case.get('created_at')}  requested by {case.get('requested_by')}"]
    obs = case.get("observations") or []
    if obs:
        lines += ["", "Observations (newest last):",
                  "  when                       run            worst PSI  drifted                     perf"]
        for o in obs[-10:]:
            perf = o.get("performance") or {}
            p = ", ".join(f"{k} {_num(v)}" for k, v in perf.items()) or f"{o.get('labelled_rows', 'n/a')} labels"
            lines.append(f"  {o.get('at', '')[:25]:<26} {str(o.get('run_id', ''))[:14]:<14} "
                         f"{_num(worst_psi(o)):>9}  {','.join(o.get('drifted_features') or [])[:27]:<27} {p}")
        latest = obs[-1]
        if latest.get("feature_psi"):
            lines += ["", "Feature PSI (latest): " + ", ".join(
                f"{c} {_num(v)}" for c, v in sorted(latest["feature_psi"].items(), key=lambda kv: -kv[1]))]
        checks = latest.get("data_checks") or {}
        if checks:
            lines += ["", "Data checks (latest): " + ("; ".join(checks.get("errors") or []) or "passed")]
            nulls = {k: v for k, v in (checks.get("null_fractions") or {}).items() if v}
            if nulls:
                lines.append("  nulls: " + ", ".join(f"{k} {v:.1%}" for k, v in sorted(nulls.items())))
    lines += ["", "Options:",
              f"  approve   pipewright retrain approve --case {case['case_id']} --reason ... [--source-uri <window>]",
              f"  decline   pipewright retrain decline --case {case['case_id']} --because <reason> --reason ... "
              "[--days N]",
              f"  snooze    pipewright retrain snooze  --case {case['case_id']} --because <reason> --reason ... "
              "--days N",
              "  reasons   data-problem (fix the data first), seasonal, no-harm, other",
              "  also      recalibrate inference.decision_threshold, or roll back: pipewright model approve <version>"]
    if case.get("approvals"):
        lines += ["", "Approvals:"] + [f"  {a['at']}  {a['by']} ({a.get('group') or 'direct'}): {a['reason']}"
                                       for a in case["approvals"]]
    if case.get("training"):
        t = case["training"]
        lines += ["", f"Training: {t.get('kind')} {t.get('status')} job {t.get('job_id')} on {t.get('source_uri')}"]
        if t.get("outcome"):
            ended = {"registered": f"registered v{t.get('version')}", "rejected": "rejected by the evaluation gates",
                     "succeeded": "finished", "failed": f"FAILED: {t.get('error')}"}.get(t["outcome"], t["outcome"])
            lines.append(f"  ended {str(t.get('finished_at'))[:16]}: {ended}")
            if t["outcome"] == "failed":
                lines.append(f"  retry: pipewright retrain retry --case {case['case_id']} --reason ..."
                             + ("  (transient: Pipewright retries it itself, twice)" if t.get("transient") else ""))
        attempts = case.get("training_attempts") or []
        if len(attempts) > 1 or (attempts and not t.get("outcome")):
            lines.append("  attempts: " + "; ".join(
                f"{a.get('run_key')} {a.get('outcome')}" + (f" (retried {a['retried']})" if a.get("retried") else "")
                for a in attempts))
    lines += ["", "History:"] + [
        f"  {h['at']}  {h['action']:<10} {h.get('by') or ''} {('[' + h['group'] + ']') if h.get('group') else ''} "
        f"{h.get('reason') or ''}".rstrip() for h in (case.get("history") or [])[-15:]]
    return "\n".join(lines)
