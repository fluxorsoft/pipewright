"""Serving health (Pipewright 1.4.3, FR-98 stage 1): monitoring.health compiled into Cloud Monitoring alert
policies, one set per deployed version, whose incidents reach the environment's Pub/Sub incident channel and become
serving.degraded and serving.recovered events (the launcher's /incidents route). No polling service.

Per deployed version and severity (warning, critical):

    errors     Agent Platform: response_count 5xx over all responses (spike 4: error_count does NOT count a model's
               own 500s). KServe: TorchServe's Requests5XX over ts_inference_requests_total (PromQL)
    latency    Agent Platform: prediction_latencies, 95th percentile. KServe: the handler's PwPredictLatencySeconds
               histogram (p95); where a handler has none, the mean from TorchServe's counters, labelled mean
And once per deployed version:
    replicas   fewer ready replicas than min_ready_replicas (Agent Platform replicas; KServe kube-state)
    scaling    replicas wanted but not ready for scaling_minutes (a stuck scale-up; spike 7 and 8: on Agent Platform
               the 429s of a scale-up are not counted anywhere, so the replica gap is the only signal)

Missing data closes an incident (a silent endpoint is a coverage question, never health). Each policy carries the
Pipewright identity and the thresholds in force as user labels, which the incident message carries to the event.
On KServe, Cloud Monitoring refuses a PromQL policy on a metric that does not exist yet (TorchServe creates
Requests5XX at the first such answer): those policies are pending, created by a later sync (the launcher's tick).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipewright.gcp import error_text

MON = "https://monitoring.googleapis.com/v3"
AP_METRIC = "aiplatform.googleapis.com/prediction/online"
SEVERITIES = ("warning", "critical")


def _label(v) -> str:
    return re.sub(r"[^a-z0-9_-]", "-", str(v).lower())[:63]


def _number(v) -> str:
    """A threshold as a label value (labels take no dots): 0.05 -> 0_05."""
    return _label(f"{v:g}".replace(".", "_"))


def _ap_filter(t: dict, metric: str, extra: str = "") -> str:
    f = (f'metric.type="{AP_METRIC}/{metric}" AND resource.type="aiplatform.googleapis.com/Endpoint" AND '
         f'resource.label.endpoint_id="{t["endpoint_id"]}"')
    if not t.get("pool"):                       # on a shared pool, replicas are per pool only (spike 1)
        f += f' AND metric.label.deployed_model_id="{t["deployed_model_id"]}"'
    elif metric in ("response_count", "prediction_latencies"):
        f += f' AND metric.label.deployed_model_id="{t["deployed_model_id"]}"'
    return f + extra


def _threshold(flt: str, value: float, comparison: str = "COMPARISON_GT", aligner: str = "ALIGN_RATE",
               denominator: str | None = None, duration: str = "300s", reducer: str | None = None) -> dict:
    agg = {"alignmentPeriod": "60s", "perSeriesAligner": aligner,
           **({"crossSeriesReducer": reducer} if reducer else {})}
    c = {"filter": flt, "comparison": comparison, "thresholdValue": value, "duration": duration,
         "aggregations": [agg], "evaluationMissingData": "EVALUATION_MISSING_DATA_INACTIVE"}
    if denominator:
        c["denominatorFilter"] = denominator
        c["denominatorAggregations"] = [agg]
    return c


def _promql(query: str, minutes: int = 5) -> dict:
    return {"query": query, "duration": f"{max(60, minutes * 60)}s", "evaluationInterval": "60s"}


def _kserve_selector(t: dict) -> str:
    return f'namespace="{t["namespace"]}",pod=~"{t["isvc"]}-predictor.*"'


def _deployment(t: dict) -> str:
    return f'namespace="{t["namespace"]}",deployment="{t["isvc"]}-predictor"'


def conditions(t: dict, health: dict, latency: str = "p95") -> list[dict]:
    """[{signal, severity, threshold, condition, kind, latency?}] for one deployed version (target t)."""
    out = []
    for sev in SEVERITIES:
        rate, ms = health["error_rate"][sev], health["latency_p95_ms"][sev]
        if t["target"] == "kserve":
            sel = _kserve_selector(t)
            err = (f"sum(rate(Requests5XX{{{sel}}}[5m])) / sum(rate(ts_inference_requests_total{{{sel}}}[5m])) "
                   f"> {rate}")
            if latency == "p95":
                lat = (f"histogram_quantile(0.95, sum by (le) (rate(PwPredictLatencySeconds_bucket{{{sel}}}[5m]))) "
                       f"* 1000 > {ms}")
            else:
                lat = (f"sum(rate(ts_inference_latency_microseconds{{{sel}}}[5m])) / "
                       f"sum(rate(ts_inference_requests_total{{{sel}}}[5m])) / 1000 > {ms}")
            out.append({"signal": "errors", "severity": sev, "threshold": rate, "kind": "promql",
                        "condition": _promql(err)})
            out.append({"signal": "latency", "severity": sev, "threshold": ms, "kind": "promql", "latency": latency,
                        "condition": _promql(lat)})
        else:
            err = _threshold(_ap_filter(t, "response_count", ' AND metric.label.response_code=monitoring.regex.'
                                                              'full_match("5..")'),
                             rate, denominator=_ap_filter(t, "response_count"), reducer="REDUCE_SUM")
            # latency_type total only (GA QA, found live): the metric has a series each for model, overhead and
            # total, and each opened its own incident (three events and emails for one breach)
            lat = _threshold(_ap_filter(t, "prediction_latencies", ' AND metric.label.latency_type="total"'), ms,
                             aligner="ALIGN_PERCENTILE_95")
            out.append({"signal": "errors", "severity": sev, "threshold": rate, "kind": "threshold",
                        "condition": err})
            out.append({"signal": "latency", "severity": sev, "threshold": ms, "kind": "threshold",
                        "latency": "p95", "condition": lat})
    minimum, minutes = int(health["min_ready_replicas"]), int(health["scaling_minutes"])
    if t["target"] == "kserve":
        dep = _deployment(t)
        if minimum > 0:
            out.append({"signal": "replicas", "severity": "critical", "threshold": minimum, "kind": "promql",
                        "condition": _promql(f"kube_deployment_status_replicas_available{{{dep}}} < {minimum}")})
        out.append({"signal": "scaling", "severity": "warning", "threshold": minutes, "kind": "promql",
                    "condition": _promql(f"kube_deployment_spec_replicas{{{dep}}} - "
                                         f"kube_deployment_status_replicas_available{{{dep}}} > 0", minutes)})
    elif t.get("pool"):
        # GA QA (found live): a model on a shared pool has no replicas of its own (Agent Platform reports the pool's
        # machines at the pool, not per deployed model), so replicas and scaling policies would never see data.
        # Errors and latency are the model's and stay; the pool's capacity is the platform team's.
        pass
    else:
        if minimum > 0 and not t.get("scale_to_zero"):
            out.append({"signal": "replicas", "severity": "critical", "threshold": minimum, "kind": "threshold",
                        "condition": _threshold(_ap_filter(t, "replicas"), minimum, "COMPARISON_LT",
                                                "ALIGN_MEAN")})
        ep, dm = t["endpoint_id"], t["deployed_model_id"]
        # PromQL over Agent Platform's own metrics: replicas wanted (target) but not running (spike 7).
        sel = f'monitored_resource="aiplatform.googleapis.com/Endpoint",endpoint_id="{ep}",deployed_model_id="{dm}"'
        out.append({"signal": "scaling", "severity": "warning", "threshold": minutes, "kind": "promql",
                    "condition": _promql(f"aiplatform_googleapis_com:prediction_online_target_replicas{{{sel}}} - "
                                         f"aiplatform_googleapis_com:prediction_online_replicas{{{sel}}} > 0",
                                         minutes)})
    return out


def policy_name(uc, t: dict, c: dict) -> str:
    who = t.get("isvc") or t.get("deployed_model_id")
    arm = f", {t['arm']}" if t.get("arm") else ""             # an A/B arm, by name in the email (GA QA)
    return f"pipewright {uc.name} {uc.environment}: serving {c['signal']} {c['severity']} (v{t['version']}{arm}, {who})"


def policy_body(uc, t: dict, c: dict, channels: list[str]) -> dict:
    kind = "conditionPrometheusQueryLanguage" if c["kind"] == "promql" else "conditionThreshold"
    labels = {"pipewright-usecase": _label(uc.name), "pipewright-environment": _label(uc.environment),
              "pipewright-managed": "true", "pipewright-health": "true", "pipewright-signal": c["signal"],
              "pipewright-severity": c["severity"], "pipewright-threshold": _number(c["threshold"]),
              "pipewright-model-version": _label(t["version"]), "pipewright-target": t["target"],
              **({"pipewright-release": _label(uc.release)} if getattr(uc, "release", None) else {}),
              **({"pipewright-isvc": _label(t["isvc"])} if t.get("isvc") else
                 {"pipewright-deployed-model": _label(t["deployed_model_id"]),
                  "pipewright-endpoint": _label(t["endpoint_id"])}),
              **({"pipewright-arm": _label(t["arm"])} if t.get("arm") else {}),
              **({"pipewright-latency": c["latency"]} if c.get("latency") else {})}
    what = {"errors": f"5xx answers above {c['threshold']:g} of requests",
            "latency": f"{'95th percentile' if c.get('latency') == 'p95' else 'mean'} latency above "
                       f"{c['threshold']:g} ms",
            "replicas": f"fewer than {c['threshold']} ready replicas",
            "scaling": f"replicas wanted but not ready for {c['threshold']} minutes"}[c["signal"]]
    doc = (f"**{uc.name}** ({uc.environment}), version {t['version']}: {what}. Pipewright sends serving.degraded "
           "when this opens and serving.recovered when it closes (FR-98).")
    return {"displayName": policy_name(uc, t, c), "combiner": "OR",
            "conditions": [{"displayName": what, kind: c["condition"]}],
            "alertStrategy": {"autoClose": "1800s"}, "notificationChannels": channels,
            "documentation": {"content": doc, "mimeType": "text/markdown"},
            "severity": "CRITICAL" if c["severity"] == "critical" else "WARNING",
            "userLabels": labels, "enabled": True}


@dataclass
class HealthPlan:
    create: list[str] = field(default_factory=list)
    update: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)          # PromQL on a metric that does not exist yet
    mean_latency: list[str] = field(default_factory=list)     # KServe versions without the handler's histogram
    errors: list[str] = field(default_factory=list)


def _missing_metric(r) -> bool:
    # Two wordings seen live: "invalid PromQL metric: X" (spike 4) and "The following PromQL metric(s) are invalid:
    # X" (the 1.4.3 battle test)
    text = error_text(r).lower()
    return r.status == 400 and ("invalid promql metric" in text or "promql metric(s) are invalid" in text)


def _same(live: dict, want: dict) -> bool:
    keys = ("displayName", "severity", "enabled", "userLabels")
    return ({k: live.get(k) for k in keys} == {k: want.get(k) for k in keys}
            and sorted(live.get("notificationChannels") or []) == sorted(want["notificationChannels"])
            and [{k: v for k, v in c.items() if k != "name"} for c in live.get("conditions") or []]
            == want["conditions"])


def sync(uc, env, targets: list[dict], gcp, dry_run: bool = False) -> HealthPlan:
    """Keeps the use case's health policies for the deployed versions in targets; deletes those of versions no longer
    deployed (or all of them when monitoring.health is off)."""
    plan = HealthPlan()
    health = (uc.ml.get("monitoring") or {}).get("health") or {}
    channel = (getattr(env, "observability", None) or {}).get("incidents_channel")
    base = f"{MON}/projects/{env.project}"
    r = gcp.get(f"{base}/alertPolicies", filter=f'user_labels."pipewright-usecase"="{_label(uc.name)}" AND '
                                                f'user_labels."pipewright-environment"="{_label(uc.environment)}" AND '
                                                'user_labels."pipewright-health"="true"', pageSize=500)
    if not r.ok:
        plan.errors.append(f"alert policies not readable: {error_text(r)}")
        return plan
    live = {p["displayName"]: p for p in (r.body or {}).get("alertPolicies") or []}
    wanted = {}
    if health.get("enabled"):
        if not channel:
            plan.errors.append(f"{env.name} has no incident channel (observability.incidents_channel, set by the "
                               "platform's Terraform): health policies would notify nobody")
            return plan
        for t in targets:
            for c in conditions(t, health):
                wanted[policy_name(uc, t, c)] = (t, c)
    for name, (t, c) in wanted.items():
        body = policy_body(uc, t, c, [channel])
        have = live.get(name)
        if have is not None and _same(have, body):
            plan.unchanged.append(name)
            continue
        if dry_run:
            (plan.update if have else plan.create).append(name)
            continue
        def write(b, have=have):
            return gcp.patch(f"{MON}/{have['name']}", b) if have else gcp.post(f"{base}/alertPolicies", b)

        res = write(body)
        if not res.ok and _missing_metric(res) and c["signal"] == "latency" and c.get("latency") == "p95" \
                and t["target"] == "kserve":
            # No PwPredictLatencySeconds: the handler records no histogram. The mean, labelled so (FR-98 decision 6);
            # the same policy name, so a later sync moves it to the p95 once the histogram exists.
            mean = next(m for m in conditions(t, health, latency="mean")
                        if m["signal"] == "latency" and m["severity"] == c["severity"])
            res = write(policy_body(uc, t, mean, [channel]))
            plan.mean_latency.append(name)
        if res.ok:
            (plan.update if have else plan.create).append(name)
        elif _missing_metric(res):
            plan.pending.append(name)
        else:
            plan.errors.append(f"{name}: {error_text(res)}")
    for name, p in live.items():
        if name not in wanted:
            plan.delete.append(name)
            if not dry_run:
                d = gcp.delete(f"{MON}/{p['name']}")
                if not d.ok and d.status != 404:
                    plan.errors.append(f"delete {name}: {error_text(d)}")
    return plan


def render(plan: HealthPlan) -> str:
    lines = [f"health policies: {len(plan.create)} created, {len(plan.update)} updated, {len(plan.unchanged)} "
             f"unchanged, {len(plan.delete)} deleted"]
    if plan.pending:
        lines.append(f"  pending ({len(plan.pending)}): their metric does not exist yet (KServe creates it at the "
                     "first such answer); the launcher's tick creates them later")
    if plan.mean_latency:
        lines.append(f"  latency is the MEAN for {len(plan.mean_latency)} policies: the handler records no "
                     "PwPredictLatencySeconds histogram (add the two lines the reference handler has)")
    lines += [f"  error: {e}" for e in plan.errors]
    return "\n".join(lines)


def deployed_targets(uc, env, gcp) -> list[dict]:
    """What is deployed for the use case now: each deployed model on its endpoint (Agent Platform), or each
    InferenceService with its label (KServe), with the model version and, during an A/B test, the arm."""
    serving = uc.model.get("serving") or {}
    if not serving.get("enabled"):
        return []
    if serving.get("target") == "kserve":
        from pipewright.models import kserve

        k = kserve.KServe(uc, env, gcp)
        r = k.kube.get(k._path(), labelSelector=f"pipewright.dev/usecase={kserve._label(uc.name)}")
        out = []
        for item in (r.body or {}).get("items") or [] if r.ok else []:
            meta = item.get("metadata") or {}
            labels = meta.get("labels") or {}
            out.append({"target": "kserve", "namespace": meta.get("namespace"), "isvc": meta.get("name"),
                        "version": labels.get("pipewright.dev/version", "?"),
                        **({"arm": labels["pipewright.dev/arm"]} if labels.get("pipewright.dev/arm") else {})})
        return out
    from pipewright.models.deploy import find_endpoint

    def read(name: str | None, arm: str | None) -> list[dict]:
        if not name:
            return []
        r = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{name}")
        found = []
        for dm in (r.body or {}).get("deployedModels") or [] if r.ok else []:
            res = dm.get("dedicatedResources") or {}
            found.append({"target": "vertex", "endpoint_id": name.rsplit("/", 1)[-1], "deployed_model_id": dm["id"],
                          "version": dm.get("modelVersionId") or "?", "pool": bool(dm.get("sharedResources")),
                          "scale_to_zero": int(res.get("minReplicaCount", 1) or 0) == 0 and bool(res),
                          **({"arm": arm} if arm else {})})
        return found

    # GA QA (found live): an A/B treatment on Agent Platform serves from its own endpoint (<usecase>-ab-endpoint)
    # and had no health policies, so a failing treatment paged nobody. While it serves, both arms are watched.
    treatment = read(ab_arm_endpoint(uc, env, gcp), "treatment")
    return read(find_endpoint(uc, env, gcp), "control" if treatment else None) + treatment


def ab_arm_endpoint(uc, env, gcp) -> str | None:
    """The A/B treatment's endpoint resource name (<usecase>-ab-endpoint), or None."""
    from pipewright.abtest.online import arm_name

    api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"
    found = gcp.get(f"{api}/endpoints", filter=f'display_name="{arm_name(uc)}"')
    endpoints = (found.body or {}).get("endpoints") or [] if found.ok else []
    return endpoints[0]["name"] if endpoints else None
