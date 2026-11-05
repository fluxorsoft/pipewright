"""pipewright report fleet (Pipewright phase 6, FR-86): every use case in every environment, on one HTML page.

The open-source counterpart of Enterprise's Fleet View, for a handful of use cases: from an MLOps repository, it reads
each environment in environments/*.yaml (its manifest, each use case's live release pointer and champion with its
lineage and integrity check, training runs from the metrics table, scoring runs from the monitoring table, pipeline
hours, the event log and the decisions waiting for a person) and writes one self-contained page. Nothing is changed.

    pipewright report fleet --out fleet.html [--mask]

It shows names, versions, digests, metrics, PSI and counts, never rows, feature values or source file names.
--mask replaces email addresses with "..." (for screenshots and sharing).
"""

from __future__ import annotations

import contextlib
import html
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DECISIONS = ("release.live", "release.promoted", "release.rolled_back", "model.approved", "model.deployed",
             "model.undeployed", "retrain.approved", "retrain.declined", "retrain.snoozed", "data.erased")
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def _rows(gcp, env, table: str | None, sql: str) -> list[dict]:
    if not table:
        return []
    from pipewright.gcp import bigquery

    try:
        return bigquery.rows(bigquery.query(gcp, env.project, sql.format(table=table),
                                            kms_key=(env.security or {}).get("bigquery_kms_key")))
    except Exception:  # noqa: BLE001 - a use case without that table has nothing to show
        return []


def _api_time(value: str) -> datetime:
    """An API timestamp (RFC 3339, nanoseconds and a Z allowed) as an aware datetime."""
    base, _, frac = value.rstrip("Z").partition(".")
    return datetime.fromisoformat(base + (f".{frac[:6]}" if frac else "")).replace(tzinfo=timezone.utc)


def _hours(gcp, env, usecase: str) -> dict:
    api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"
    # No server-side filter: Agent Platform's job filter does not match a display-name prefix (found live).
    r = gcp.get(f"{api}/pipelineJobs", pageSize=200, orderBy="create_time desc")
    hours, runs = 0.0, 0
    for j in (r.body or {}).get("pipelineJobs", []) if r.ok else []:
        if not j.get("displayName", "").startswith(f"{usecase}-r"):
            continue
        runs += 1
        if j.get("startTime") and j.get("endTime"):
            hours += (_api_time(j["endTime"]) - _api_time(j["startTime"])).total_seconds() / 3600
    return {"hours": hours, "runs": runs}


def collect(root: Path, gcp) -> dict:
    """Everything the page shows, read with the caller's credentials (an operator reads all of it)."""
    from pipewright.config import discover_usecases, load_environment, resolve_usecase, run_in
    from pipewright.decisions import pending
    from pipewright.models import lineage
    from pipewright.release.service import read_pointer

    root = Path(root)
    found = discover_usecases(root)
    usecases = sorted(found)
    envs = []
    names = sorted((p.stem for p in (root / "environments").glob("*.yaml")),
                   key=lambda n: (n not in ("dev", "test"), n != "test", n))
    for name in names:
        env = load_environment(root, name)
        out = {"name": name, "project": env.project, "usecases": {}, "events": [], "pending": [], "compute": {},
               "evidence": (env.observability or {}).get("evidence_dataset")}
        for uc_name in usecases:
            allowed = run_in(found[uc_name])
            if allowed is not None and name not in allowed:
                continue
            try:
                uc = resolve_usecase(found[uc_name], env)
            except Exception:  # noqa: BLE001 - a use case that does not resolve here is left out of this column
                continue
            pointer, _ = read_pointer(gcp, env, uc.name)
            try:
                with contextlib.redirect_stdout(sys.stderr):
                    champion = lineage.lineage(uc, env, gcp)
            except Exception:  # noqa: BLE001 - no model yet
                champion = None
            dataset = uc.resources.get("bigquery_dataset")
            metrics = (uc.parameters or {}).get("metrics_table") or ((uc.ml.get("bqml") or {}).get("metrics_table"))
            monitoring = (uc.ml.get("monitoring") or {}).get("output_table")
            out["usecases"][uc_name] = {
                "pointer": pointer.__dict__ if pointer else None,
                "champion": champion,
                "training": _rows(gcp, env, metrics and f"{env.project}.{dataset}.{metrics}",
                                  "SELECT evaluated_at, primary_metric, primary_value, decision, run_id "
                                  "FROM `{table}` ORDER BY evaluated_at"),
                "scoring": _rows(gcp, env, monitoring and f"{env.project}.{dataset}.{monitoring}",
                                 "SELECT monitored_at, model_version_id, max_feature_psi, alert "
                                 "FROM `{table}` ORDER BY monitored_at"),
                "drift_threshold": ((uc.ml.get("monitoring") or {}).get("drift") or {}).get("threshold", 0.2),
            }
            out["compute"][uc_name] = _hours(gcp, env, uc.name)
        if out["evidence"]:
            out["events"] = _rows(gcp, env, f"{env.project}.{out['evidence']}.events",
                                  "SELECT publish_time, JSON_VALUE(data, '$.event_type') AS event_type, "
                                  "JSON_VALUE(data, '$.usecase') AS usecase, JSON_VALUE(data, '$.release') AS release, "
                                  "TO_JSON_STRING(JSON_QUERY(data, '$.data')) AS detail "
                                  "FROM `{table}` ORDER BY publish_time DESC LIMIT 500")
        try:
            out["pending"] = [i.__dict__ if hasattr(i, "__dict__") else i for i in pending(root, name)]
        except Exception:  # noqa: BLE001 - shown as unknown
            out["pending"] = []
        envs.append(out)
    return {"usecases": usecases, "envs": envs, "at": datetime.now(timezone.utc)}


# ---------------------------------------------------------------------------------------------- rendering --

def esc(value) -> str:
    return html.escape("" if value is None else str(value))


def _dt(value):
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" UTC", "+00:00"))
    return value


def when(value) -> str:
    return _dt(value).strftime("%d %b %H:%M") if value else ""


def scale(lo, hi, a, b):
    span = (hi - lo) or 1
    return lambda v: a + (v - lo) / span * (b - a)


def chart_training(rows: list[dict], slot: int) -> str:
    """Primary metric of every training run, over time: filled when it became a model, hollow when refused."""
    rows = [r for r in rows if r.get("primary_value") is not None]
    if not rows:
        return '<p class="empty">No training runs recorded.</p>'
    W, H, L, R, T, B = 680, 260, 52, 16, 16, 36
    ys = [float(r["primary_value"]) for r in rows]
    lo, hi = min(ys) - 0.002, max(ys) + 0.002
    x, y = scale(0, max(len(rows) - 1, 1), L + 8, W - R - 8), scale(lo, hi, H - B, T)
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Training runs: {esc(rows[0]["primary_metric"])} '
           f'of {len(rows)} runs over time">']
    for i in range(5):
        t = lo + i * (hi - lo) / 4
        out.append(f'<line class="grid" x1="{L}" x2="{W - R}" y1="{y(t):.1f}" y2="{y(t):.1f}"/>'
                   f'<text class="tick" x="{L - 6}" y="{y(t) + 4:.1f}" text-anchor="end">{t:.3f}</text>')
    for i, (r, yv) in enumerate(zip(rows, ys, strict=True)):
        kept = r.get("decision") == "approve"
        tip = (f'{when(r["evaluated_at"])} · {r["primary_metric"]} {yv:.4f} · {"kept" if kept else "refused"} · '
               f'run {r.get("run_id")}')
        out.append(f'<circle class="pt s{slot} {"kept" if kept else "refused"}" cx="{x(i):.1f}" cy="{y(yv):.1f}" '
                   f'r="5" data-tip="{esc(tip)}"/>')
    out.append(f'<text class="tick" x="{L + 8}" y="{H - B + 18}">oldest</text>'
               f'<text class="tick" x="{W - R - 8}" y="{H - B + 18}" text-anchor="end">newest</text></svg>')
    return "".join(out)


def chart_drift(rows: list[dict], slot: int, threshold: float = 0.2) -> str:
    """The largest feature PSI of every scoring run, oldest first, against the drift threshold."""
    rows = [r for r in rows if r.get("max_feature_psi") is not None]
    if not rows:
        return '<p class="empty">No scoring runs with drift measured.</p>'
    W, H, L, R, T, B = 680, 240, 52, 16, 16, 36
    step = 0.05
    hi = math.ceil(max(max(float(r["max_feature_psi"]) for r in rows), threshold) * 1.1 / step) * step
    x, y = scale(0, max(len(rows) - 1, 1), L + 8, W - R - 8), scale(0, hi, H - B, T)
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Drift: largest feature PSI of {len(rows)} '
           f'scoring runs">']
    for t in [i * step for i in range(round(hi / step) + 1)]:
        out.append(f'<line class="grid" x1="{L}" x2="{W - R}" y1="{y(t):.1f}" y2="{y(t):.1f}"/>'
                   f'<text class="tick" x="{L - 6}" y="{y(t) + 4:.1f}" text-anchor="end">{t:.2f}</text>')
    out.append(f'<line class="threshold" x1="{L}" x2="{W - R}" y1="{y(threshold):.1f}" y2="{y(threshold):.1f}"/>'
               f'<text class="tick" x="{W - R}" y="{y(threshold) - 6:.1f}" text-anchor="end">threshold {threshold}'
               f'</text>')
    path = " ".join(f'{"M" if i == 0 else "L"}{x(i):.1f},{y(float(r["max_feature_psi"])):.1f}'
                    for i, r in enumerate(rows))
    out.append(f'<path class="line s{slot}" d="{path}"/>')
    for i, r in enumerate(rows):
        psi = float(r["max_feature_psi"])
        alert = " · alert" if r.get("alert") else ""
        tip = f'{when(r["monitored_at"])} · v{r.get("model_version_id")} · PSI {psi:.3f}{alert}'
        out.append(f'<circle class="pt s{slot} kept" cx="{x(i):.1f}" cy="{y(psi):.1f}" r="4.5" data-tip="{esc(tip)}"/>')
    out.append(f'<text class="tick" x="{L + 8}" y="{H - B + 18}">oldest</text>'
               f'<text class="tick" x="{W - R - 8}" y="{H - B + 18}" text-anchor="end">newest</text></svg>')
    return "".join(out)


def chart_compute(env: dict, usecases: list[str], top: float) -> str:
    W, ROW, L, R = 330, 30, 128, 56
    shown = [uc for uc in usecases if uc in env["compute"]]
    H = ROW * len(shown) + 10
    x = scale(0, top or 1, L, W - R)
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Pipeline hours per use case in {esc(env["name"])}">']
    for i, uc in enumerate(shown):
        c = env["compute"][uc]
        yv = 6 + i * ROW
        width = max(x(c["hours"]) - L, 0)
        tip = f'{uc} in {env["name"]}: {c["hours"]:.1f} h over {c["runs"]} runs'
        out.append(f'<text class="label" x="{L - 8}" y="{yv + 15}" text-anchor="end">{esc(uc)}</text>'
                   f'<rect class="bar s{(usecases.index(uc) % 3) + 1}" x="{L}" y="{yv + 4}" width="{width:.1f}" '
                   f'height="16" rx="4" data-tip="{esc(tip)}"/>'
                   f'<text class="value" x="{L + width + 6:.1f}" y="{yv + 16}">{c["hours"]:.1f} h</text>')
    out.append("</svg>")
    return "".join(out)


def mask(text: str, on: bool) -> str:
    return EMAIL.sub("...", text) if on else text


def _integrity(champion: dict) -> str:
    i = champion.get("integrity") or {}
    if not i:
        return ""
    result = i.get("result", "")
    css = "ok" if result == "verified" else "bad" if result == "refused" else "muted"
    return f'<br><small class="{css}">integrity {esc(result)}</small>'


def render(data: dict, masked: bool = False) -> str:
    usecases, envs = data["usecases"], data["envs"]
    slot = {uc: (i % 3) + 1 for i, uc in enumerate(usecases)}
    last = envs[-1] if envs else {"name": "-", "usecases": {}}
    in_last = sum(1 for u in last["usecases"].values() if (u["champion"] or {}).get("version"))
    decisions = [e for env in envs for e in env["events"] if e["event_type"] in DECISIONS]
    waiting = sum(len(env["pending"]) for env in envs)
    tiles = [("use cases", len(usecases)), ("environments", len(envs)), (f"models in {last['name']}", in_last),
             ("decisions kept", len(decisions)), ("waiting for a person", waiting)]
    parts = ['<section class="tiles">'] + [f'<div class="tile"><b>{v}</b><span>{esc(k)}</span></div>'
                                          for k, v in tiles] + ["</section>"]
    head = "".join(f"<th>{esc(e['name'])}</th>" for e in envs)
    parts.append(f'<h2>What is live, where</h2><div class="table-wrap"><table class="grid-table"><thead><tr>'
                 f'<th>use case</th>{head}</tr></thead><tbody>')
    for uc in usecases:
        cells = []
        for env in envs:
            u = env["usecases"].get(uc)
            if u is None:
                cells.append('<td><small>not in this environment</small></td>')
                continue
            p, c = u["pointer"] or {}, u["champion"] or {}
            ev = ((c.get("lineage") or {}).get("evaluation")) or {}
            release = (f'release <code>{esc(p.get("release_id"))}</code><br><small>{when(p.get("updated_at"))} by '
                       f'{esc(mask(p.get("updated_by") or "", masked))}</small>') if p else "<small>no release</small>"
            promoted = (c.get("labels") or {}).get("promoted-from")
            model = (f'<br>v{esc(c.get("version"))} {esc(ev.get("primary_metric") or "")} '
                     f'{esc(ev.get("value")) if ev.get("value") is not None else ""}'
                     f'{" · from " + esc(promoted) if promoted else ""}{_integrity(c)}'
                     ) if c.get("version") else "<br><small>no model</small>"
            cells.append(f"<td>{release}{model}</td>")
        parts.append(f'<tr><th><span class="key s{slot[uc]}"></span>{esc(uc)}</th>{"".join(cells)}</tr>')
    parts.append("</tbody></table></div>")
    for env in envs:
        for uc, u in env["usecases"].items():
            if len(u["training"]) >= 3:
                kept = sum(1 for r in u["training"] if r.get("decision") == "approve")
                parts.append(f'<h2>Training runs: {esc(uc)} in {esc(env["name"])}</h2><p class="caption">'
                             f'{len(u["training"])} runs; {kept} became a model (filled), {len(u["training"]) - kept}'
                             f' were refused by the gates (hollow).</p>' + chart_training(u["training"], slot[uc]))
    for env in envs:
        for uc, u in env["usecases"].items():
            if sum(1 for r in u["scoring"] if r.get("max_feature_psi") is not None) >= 3:
                alerts = sum(1 for r in u["scoring"] if r.get("alert"))
                parts.append(f'<h2>Drift: {esc(uc)} in {esc(env["name"])}</h2><p class="caption">The largest feature '
                             f'PSI of each scoring run; {alerts} of {len(u["scoring"])} runs alerted.</p>'
                             + chart_drift(u["scoring"], slot[uc], u.get("drift_threshold") or 0.2))
    top = max((c["hours"] for env in envs for c in env["compute"].values()), default=1)
    parts.append('<h2>Compute: pipeline hours</h2><p class="caption">Every pipeline run of each use case, start to '
                 'end. Cloud Billing export, filtered by the pipewright-usecase label, gives the money.</p>'
                 '<div class="multiples">')
    for env in envs:
        parts.append(f'<div><h3>{esc(env["name"])}</h3>{chart_compute(env, usecases, top)}</div>')
    parts.append("</div>")
    parts.append('<h2>Decisions</h2><div class="table-wrap"><table class="log"><thead><tr><th>when</th><th>where</th>'
                 '<th>use case</th><th>decision</th><th>by</th><th>how approved</th></tr></thead><tbody>')
    rows = sorted(((e, env["name"]) for env in envs for e in env["events"] if e["event_type"] in DECISIONS),
                  key=lambda t: str(t[0]["publish_time"]), reverse=True)
    for e, name in rows[:40]:
        d = json.loads(e.get("detail") or "{}")
        a = d.get("approval") or {}
        how = (f'GitHub, {esc(mask(a.get("actor") or "", masked))}, <a href="{esc(a.get("run_url"))}">run</a>'
               if a.get("via") == "github" else "outside CI" if a.get("via") else "")
        what = {"model.approved": f'v{d.get("version")} champion', "release.live": f'release {e.get("release")}',
                "release.rolled_back": f'rolled back to {e.get("release")}', "model.deployed":
                f'v{d.get("version")} deployed', "data.erased": "rows erased"}.get(e["event_type"], e["event_type"])
        parts.append(f'<tr><td>{when(e["publish_time"])}</td><td>{esc(name)}</td><td>{esc(e.get("usecase"))}</td>'
                     f'<td>{esc(what)}</td><td>{esc(mask(d.get("by") or "", masked))}</td><td>{how}</td></tr>')
    if not rows:
        parts.append('<tr><td colspan="6">No decisions in the event log yet.</td></tr>')
    parts.append("</tbody></table></div>")
    missing = [env["name"] for env in envs if not env["evidence"]]
    if missing:
        parts.append(f'<p class="caption">No event log in {esc(", ".join(missing))}: decisions there were not '
                     'kept.</p>')
    parts.append(f'<h2>Waiting for a person</h2><p class="caption">{waiting or "Nothing"} pending '
                 '(`pipewright decisions pending`).</p>')
    at = data["at"] if isinstance(data["at"], datetime) else _dt(data["at"])
    return PAGE.replace("{{BODY}}", "\n".join(parts)).replace("{{AT}}", at.strftime("%d %B %Y %H:%M UTC"))


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fleet report</title>
<style>
:root { color-scheme: light; --surface:#fcfcfb; --raised:#ffffff; --ink:#0b0b0b; --ink-2:#52514e; --rule:#e4e3df;
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --threshold:#d03b3b; --ok:#0f7a4e; --bad:#c22f2f; }
@media (prefers-color-scheme: dark) { :root:where(:not([data-theme="light"])) { color-scheme: dark; --surface:#1a1a19;
  --raised:#232322; --ink:#f0efec; --ink-2:#c3c2b7; --rule:#3a3a37; --s1:#3987e5; --s2:#d95926; --s3:#199e70;
  --ok:#3fbf86; --bad:#ef6b6b; } }
:root[data-theme="dark"] { color-scheme: dark; --surface:#1a1a19; --raised:#232322; --ink:#f0efec; --ink-2:#c3c2b7;
  --rule:#3a3a37; --s1:#3987e5; --s2:#d95926; --s3:#199e70; --ok:#3fbf86; --bad:#ef6b6b; }
body { margin:0; background:var(--surface); color:var(--ink); font:15px/1.5 system-ui, sans-serif; }
main { max-width:980px; margin:0 auto; padding:24px 16px 48px; }
h1 { font-size:24px; margin:0 0 4px; } h2 { font-size:17px; margin:32px 0 6px; } h3 { font-size:14px; margin:8px 0; }
.caption, small, .empty { color:var(--ink-2); } .caption { margin:0 0 8px; font-size:13px; }
small.ok { color:var(--ok); font-weight:600; } small.bad { color:var(--bad); font-weight:600; }
.tiles { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:12px; margin-top:20px; }
.tile { background:var(--raised); border:1px solid var(--rule); border-radius:10px; padding:12px 14px; }
.tile b { display:block; font-size:28px; } .tile span { color:var(--ink-2); font-size:13px; }
table { width:100%; border-collapse:collapse; font-size:13px; background:var(--raised); }
th, td { text-align:left; vertical-align:top; padding:8px; border-bottom:1px solid var(--rule); }
thead th { color:var(--ink-2); font-weight:600; } code { font-size:12px; }
.key { display:inline-block; width:10px; height:10px; border-radius:3px; margin-right:6px; }
.key.s1, .bar.s1 { background:var(--s1); fill:var(--s1); } .key.s2, .bar.s2 { background:var(--s2); fill:var(--s2); }
.key.s3, .bar.s3 { background:var(--s3); fill:var(--s3); }
svg { width:100%; height:auto; overflow:visible; }
.grid { stroke:var(--rule); stroke-width:1; } .tick, .label, .value { fill:var(--ink-2); font-size:11px; }
.label { fill:var(--ink); font-size:12px; }
.threshold { stroke:var(--threshold); stroke-width:1.5; stroke-dasharray:6 4; }
.line { fill:none; stroke-width:2; } .line.s1 { stroke:var(--s1); } .line.s2 { stroke:var(--s2); }
.line.s3 { stroke:var(--s3); }
.pt { stroke:var(--surface); stroke-width:2; } .pt.s1 { fill:var(--s1); } .pt.s2 { fill:var(--s2); }
.pt.s3 { fill:var(--s3); }
.pt.refused { fill:var(--surface); } .pt.refused.s1 { stroke:var(--s1); } .pt.refused.s2 { stroke:var(--s2); }
.pt.refused.s3 { stroke:var(--s3); } [data-tip]:hover { opacity:.8; }
.multiples { display:grid; grid-template-columns:repeat(auto-fit, minmax(280px, 1fr)); gap:16px; }
.multiples > div { background:var(--raised); border:1px solid var(--rule); border-radius:10px; padding:8px 12px; }
#tip { position:fixed; pointer-events:none; background:var(--ink); color:var(--surface); font-size:12px;
  padding:4px 8px; border-radius:6px; display:none; max-width:320px; }
.table-wrap { overflow-x:auto; }
</style></head>
<body><main>
<h1>Fleet report</h1><p class="caption">Every use case in every environment, from the records Pipewright keeps.
Generated {{AT}} by pipewright report fleet. Read only.</p>
{{BODY}}
</main><div id="tip"></div>
<script>
const tip = document.getElementById("tip");
document.addEventListener("mousemove", (e) => {
  const t = e.target.closest && e.target.closest("[data-tip]");
  if (!t) { tip.style.display = "none"; return; }
  tip.textContent = t.getAttribute("data-tip"); tip.style.display = "block";
  tip.style.left = Math.min(e.clientX + 12, innerWidth - 330) + "px"; tip.style.top = (e.clientY + 14) + "px";
});
</script></body></html>
"""
