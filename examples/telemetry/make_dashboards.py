"""Grafana dashboards for Pipewright's launcher telemetry (FR-127), one per persona, from the telemetry contract
(docs/TELEMETRY.md): only the six metrics and the launcher's spans, never data.

    python examples/telemetry/make_dashboards.py        # writes examples/telemetry/dashboards/*.json

Import each file in Grafana (Dashboards > New > Import), choosing a Prometheus and a Tempo data source. They work
with any backend that keeps the OTLP metrics under Prometheus names: Grafana Cloud, Google Cloud Managed Service for
Prometheus through Grafana, Mimir, Prometheus 3.
"""

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "dashboards"
UC = '{usecase=~"$usecase"}'
CYAN, AMBER, INDIGO, GREEN, RED, ORANGE = "#00D4FF", "#FBBF24", "#6366F1", "#10D98C", "#F87171", "#F59E0B"

PROM = {"type": "prometheus", "uid": "${prometheus}"}
TEMPO = {"type": "tempo", "uid": "${tempo}"}


def text(content, y, h=3):
    return {"type": "text", "title": "", "gridPos": {"h": h, "w": 24, "x": 0, "y": y},
            "options": {"mode": "markdown", "content": content}}


def stat(title, expr, x, y, w, color, unit="short", decimals=0, desc="", graph="area", zero=True):
    if zero:                            # a count with no series yet is 0, not "No data"
        expr = f"({expr}) or vector(0)"
    return {"type": "stat", "title": title, "description": desc, "gridPos": {"h": 5, "w": w, "x": x, "y": y},
            "datasource": PROM, "targets": [{"refId": "A", "expr": expr}],
            "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": graph,
                        "textMode": "value"},
            "fieldConfig": {"defaults": {"decimals": decimals, "unit": unit,
                                         "color": {"mode": "fixed", "fixedColor": color}}, "overrides": []}}


def series(title, targets, x, y, w, h=8, unit="short", style="line", stack=True, desc="", color=None, maximum=None):
    custom = {"drawStyle": style, "lineWidth": 2 if style == "line" else 0, "lineInterpolation": "smooth",
              "fillOpacity": 25 if style == "line" else 80}
    if stack:
        custom["stacking"] = {"mode": "normal"}
    defaults = {"unit": unit, "custom": custom, "min": 0}
    if maximum is not None:
        defaults["max"] = maximum
    if color:
        defaults["color"] = {"mode": "fixed", "fixedColor": color}
    return {"type": "timeseries", "title": title, "description": desc, "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "datasource": PROM, "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": lf}
                                            for i, (e, lf) in enumerate(targets)],
            "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
            "fieldConfig": {"defaults": defaults, "overrides": []}}


def bars(title, expr, legend, x, y, w, h=8, desc=""):
    return {"type": "bargauge", "title": title, "description": desc, "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "datasource": PROM, "targets": [{"refId": "A", "expr": expr, "legendFormat": legend, "instant": True}],
            "options": {"orientation": "horizontal", "displayMode": "gradient",
                        "reduceOptions": {"calcs": ["lastNotNull"]}, "showUnfilled": True},
            "fieldConfig": {"defaults": {"decimals": 0, "unit": "short", "color": {"mode": "continuous-BlPu"}},
                            "overrides": []}}


def traces(title, query, x, y, w, h=9, desc=""):
    """Run spans are written when the run finishes, minutes after their trigger (the trace's root), so Tempo's
    search often cannot name the root yet: list the matched spans with their use case and pipeline instead."""
    spans = "pipewright.run\"" in query
    if spans:
        query += " | select(span.pipewright.usecase, span.pipewright.pipeline, span.pipewright.state)"
    return {"type": "table", "title": title, "description": desc, "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "datasource": TEMPO, "targets": [{"refId": "A", "queryType": "traceql", "query": query, "limit": 20,
                                              "spss": 1, "tableType": "spans" if spans else "traces"}],
            "transformations": [{"id": "organize", "options": {
                "excludeByName": {"traceService": True, "traceName": True, "traceIdHidden": True, "service.name": True,
                                  "name": True},
                "indexByName": {"time": 0, "span.pipewright.usecase": 1, "pipewright.usecase": 1,
                                "span.pipewright.pipeline": 2, "pipewright.pipeline": 2, "span.pipewright.state": 3,
                                "pipewright.state": 3, "duration": 4, "spanID": 5},
                "renameByName": {"span.pipewright.usecase": "Use case", "span.pipewright.pipeline": "Pipeline",
                                 "span.pipewright.state": "State", "pipewright.usecase": "Use case",
                                 "pipewright.pipeline": "Pipeline", "pipewright.state": "State"}}}] if spans else [],
            "options": {"showHeader": True},
            "fieldConfig": {"defaults": {}, "overrides": [
                {"matcher": {"id": "byName", "options": "State"}, "properties": [
                    {"id": "mappings", "value": [
                        {"type": "value", "options": {"PIPELINE_STATE_FAILED": {"text": "FAILED", "color": RED,
                                                                                 "index": 0}}},
                        {"type": "regex", "options": {"pattern": "PIPELINE_STATE_(.*)",
                                                      "result": {"text": "$1", "color": GREEN, "index": 1}}}]},
                    {"id": "custom.cellOptions", "value": {"type": "color-text"}}]}] if spans else []}}


def heatmap(title, x, y, w, h=8):
    return {"type": "heatmap", "title": title, "gridPos": {"h": h, "w": w, "x": x, "y": y}, "datasource": PROM,
            "targets": [{"refId": "A", "expr": "sum by (le) (increase(pipewright_trigger_latency_seconds_bucket"
                                              "[$__rate_interval]))", "format": "heatmap", "legendFormat": "{{le}}"}],
            "options": {"calculate": False, "yAxis": {"unit": "s"}, "color": {"scheme": "Turbo", "mode": "scheme"},
                        "cellGap": 1, "legend": {"show": True}},
            "fieldConfig": {"defaults": {}, "overrides": []}}


def dashboard(uid, title, desc, panels, tags):
    return {"title": title, "uid": uid, "description": desc, "tags": ["pipewright", "opentelemetry", *tags],
            "timezone": "utc", "editable": True, "schemaVersion": 39, "version": 1, "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"}, "graphTooltip": 1,
            "links": [{"title": "Pipewright dashboards", "type": "dashboards", "tags": ["pipewright"], "asDropdown": True}],
            "templating": {"list": [
                {"name": "prometheus", "label": "Metrics", "type": "datasource", "query": "prometheus", "hide": 0},
                {"name": "tempo", "label": "Traces", "type": "datasource", "query": "tempo", "hide": 0},
                {"name": "usecase", "label": "Use case", "type": "query", "datasource": PROM,
                 "query": {"query": "label_values(pipewright_runs_started_total, usecase)", "refId": "usecase"},
                 "definition": "label_values(pipewright_runs_started_total, usecase)", "includeAll": True,
                 "multi": True, "allValue": ".*", "current": {"text": "All", "value": "$__all"}, "refresh": 2,
                 "sort": 1}]},
            "panels": panels}


STARTED = f"sum(increase(pipewright_runs_started_total{UC}[$__range]))"
REFUSED = f"sum(increase(pipewright_runs_refused_total{UC}[$__range]))"
DEDUP = f"sum(increase(pipewright_runs_deduplicated_total{UC}[$__range]))"
OPEN = f"sum(pipewright_cases_open{UC})"
TICKS = "sum(increase(pipewright_tick_failures_total[$__range]))"


def pq(q, by="le"):
    return f"histogram_quantile({q}, sum by ({by}) (rate(pipewright_trigger_latency_seconds_bucket[$__rate_interval])))"


RUNS = '{ resource.service.name = "pipewright-launcher" && name = "pipewright.run" }'
SLOW = '{ resource.service.name = "pipewright-launcher" && name = "pipewright.run" && duration > 10m }'
FAILED = '{ resource.service.name = "pipewright-launcher" && name = "pipewright.run" && status = error }'
REFUSALS = '{ resource.service.name = "pipewright-launcher" && name = "pipewright.run.start" && span.pipewright.outcome =~ "refused|held|submit_failed" }'

OVERVIEW = dashboard("pipewright-overview", "Pipewright: every model run, through one door",
    "The launcher starts every training, scoring and monitoring run, applies the platform's rules first, holds what "
    "needs a person and absorbs retries.", [
    text("### Every model run, through one door\nFrom a file landing, a schedule, an API call or a retrain decision, "
         "the **Pipewright launcher** starts each run as the team's own identity, after the platform's rules. "
         "What needs a person waits for one. **Names and counts only, never data.**", 0),
    stat("Runs started", STARTED, 0, 3, 5, CYAN),
    stat("Held or refused by the rules", REFUSED, 5, 3, 5, AMBER),
    stat("Retries absorbed", DEDUP, 10, 3, 5, INDIGO,
         desc="Triggers that mapped to a run already started: the deterministic run ID made the retry harmless."),
    stat("Trigger to run, p95", pq(0.95), 15, 3, 5, GREEN, unit="s", decimals=1, zero=False),
    stat("Waiting for a person", OPEN, 20, 3, 4, ORANGE, graph="none"),
    series("Runs started per use case", [(f"sum by (usecase) (increase(pipewright_runs_started_total{UC}"
                                          "[$__rate_interval]))", "{{usecase}}")], 0, 8, 12, style="bars"),
    series("How runs start", [(f"sum by (trigger) (increase(pipewright_runs_started_total{UC}[$__rate_interval]))",
                               "{{trigger}}")], 12, 8, 12),
    traces("Recent runs: open one to see the trigger, the run and every step", RUNS, 0, 16, 24),
], ["overview"])

SRE = dashboard("pipewright-sre", "Pipewright: SRE and platform",
    "Is the launcher healthy? How fast triggers become runs, which scheduled checks fail, how much the rules refuse "
    "and how many retries the run IDs absorb.", [
    text("### Launcher health\nThe launcher is Pipewright's one long-running service: every trigger passes through "
         "it. Watch how fast triggers become runs, the scheduled checks, and the share of triggers refused.", 0, 2),
    stat("Trigger to run, p50", pq(0.5), 0, 2, 6, GREEN, unit="s", decimals=1, zero=False),
    stat("Trigger to run, p95", pq(0.95), 6, 2, 6, AMBER, unit="s", decimals=1, zero=False),
    stat("Trigger to run, p99", pq(0.99), 12, 2, 6, RED, unit="s", decimals=1, zero=False),
    stat("Failed scheduled checks", TICKS, 18, 2, 6, RED),
    series("Trigger to run, by trigger (p95)", [(pq(0.95, "le, trigger"), "{{trigger}}")], 0, 7, 12, unit="s",
           stack=False),
    heatmap("Trigger to run: distribution", 12, 7, 12),
    series("Refused share of triggers", [(f"sum(increase(pipewright_runs_refused_total{UC}[$__rate_interval])) / "
                                          f"(sum(increase(pipewright_runs_started_total{UC}[$__rate_interval])) + "
                                          f"sum(increase(pipewright_runs_refused_total{UC}[$__rate_interval])))",
                                          "refused")], 0, 15, 8, unit="percentunit", stack=False, color=AMBER, maximum=1),
    series("Retries absorbed", [(f"sum by (trigger) (increase(pipewright_runs_deduplicated_total{UC}"
                                 "[$__rate_interval]))", "{{trigger}}")], 8, 15, 8, style="bars"),
    series("Failed scheduled checks, by check", [("sum by (check) (increase(pipewright_tick_failures_total"
                                                  "[$__rate_interval]))", "{{check}}")], 16, 15, 8, style="bars",
           color=RED),
    traces("Failed runs", FAILED, 0, 23, 12),
    traces("Slow runs (over 10 minutes)", SLOW, 12, 23, 12),
], ["sre", "platform"])

ML = dashboard("pipewright-ml-team", "Pipewright: ML team",
    "Your use cases: what started, what the rules refused and why, the decisions waiting for you, and every run "
    "with its steps.", [
    text("### Your use cases\nPick a use case above. Each run below opens as a trace: the trigger, the run and every "
         "pipeline step, from the orchestrator's own times.", 0, 2),
    stat("Runs started", STARTED, 0, 2, 6, CYAN),
    stat("Held or refused", REFUSED, 6, 2, 6, AMBER),
    stat("Retries absorbed", DEDUP, 12, 2, 6, INDIGO),
    stat("Decisions waiting", OPEN, 18, 2, 6, ORANGE, graph="none"),
    series("Runs started, by pipeline", [(f"sum by (pipeline) (increase(pipewright_runs_started_total{UC}"
                                          "[$__rate_interval]))", "{{pipeline}}")], 0, 7, 12, style="bars"),
    bars("Why runs were held or refused", f"sum by (reason) (increase(pipewright_runs_refused_total{UC}[$__range]))",
         "{{reason}}", 12, 7, 12),
    series("Retrain decisions waiting, by use case", [(f"sum by (usecase) (pipewright_cases_open{UC})",
                                                       "{{usecase}}")], 0, 15, 12, stack=False),
    bars("Runs started, by use case", f"sum by (usecase) (increase(pipewright_runs_started_total{UC}[$__range]))",
         "{{usecase}}", 12, 15, 12),
    traces("Latest runs and their steps", RUNS, 0, 23, 24),
], ["ml"])

GOVERNANCE = dashboard("pipewright-governance", "Pipewright: governance and risk",
    "What the platform's rules held or refused, and why; decisions waiting for a person; every run on the record. "
    "Telemetry is for operations: the evidence stays the event log.", [
    text("### The rules at work\nEvery run goes through the launcher, which applies the platform's rules before "
         "anything runs: integrity, feature contracts, scoring that waits for a person. This shows what they held "
         "and refused. **The record of decisions stays the event log and the evidence packs.**", 0),
    stat("Held for a person", f'sum(increase(pipewright_runs_refused_total{{usecase=~"$usecase",reason="held"}}'
                              "[$__range]))", 0, 3, 6, AMBER),
    stat("Refused: integrity", f'sum(increase(pipewright_runs_refused_total{{usecase=~"$usecase",reason="integrity"}}'
                               "[$__range]))", 6, 3, 6, RED),
    stat("Refused: feature contract", f'sum(increase(pipewright_runs_refused_total{{usecase=~"$usecase",'
                                      f'reason="features"}}[$__range]))', 12, 3, 6, RED),
    stat("Retrain decisions waiting", OPEN, 18, 3, 6, ORANGE, graph="none"),
    series("Held and refused, by reason", [(f"sum by (reason) (increase(pipewright_runs_refused_total{UC}"
                                            "[$__rate_interval]))", "{{reason}}")], 0, 8, 12, style="bars"),
    bars("Held and refused, by use case", f"sum by (usecase) (increase(pipewright_runs_refused_total{UC}[$__range]))",
         "{{usecase}}", 12, 8, 12),
    series("Decisions waiting for a person", [(f"sum(pipewright_cases_open{UC})", "waiting")], 0, 16, 12,
           stack=False, color=ORANGE),
    traces("Runs the rules held or refused", REFUSALS, 12, 16, 12, h=8),
], ["governance", "risk"])

if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for name, d in (("overview", OVERVIEW), ("sre", SRE), ("ml-team", ML), ("governance", GOVERNANCE)):
        (OUT / f"pipewright-{name}.json").write_text(json.dumps(d, indent=2) + "\n", encoding="utf-8")
        print(f"wrote dashboards/pipewright-{name}.json ({len(d['panels'])} panels)")
