"""The launcher's tick for A/B tests (FR-57, phase B3). Per running test of a live release:

1. **Results in.** The scheduled analysis runs as the tenant's runner (the tenant's job: it reads the tenant's dataset,
   which the launcher does not) and leaves its result under `online-sync/<tenant>/<usecase>/ab/results/`; the tick
   records each new one on the test (the state moves to decision_required when the result says so) and emits
   `ab_test.analysed`.
2. **Serving (online tests).** From the gateway's log lines (Cloud Logging): breaker trips and each instance's counts.
   From Cloud Monitoring: what the treatment's endpoint answered over the last hour. A trip the endpoint's errors
   confirm, or an error rate over the breaker's limit on enough calls, stops the treatment (D4): the allocation goes
   to 0, the gateway is brought up to date, the state becomes decision_required and `ab_test.guardrail_breached` is
   emitted. Unassigned, dropped and mismatched calls go on the test (`router`), which blocks a decision; calls to the
   treatment's endpoint that the router did not send are counted as `bypassed` and block it too.
3. **The schedule.** When `analysis.schedule` (cron, UTC) came due since the last start, the tenant's job is started
   with the test and the released configuration (`pipewright ab analyse --request`).

Every step is independent: a failure is noted in the result and the others still run.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pipewright.errors import ConfigError

LOG_API = "https://logging.googleapis.com/v2/entries:list"
RUN = "https://run.googleapis.com/v2"
WINDOW = timedelta(minutes=60)
LAG = timedelta(minutes=10)             # Monitoring and the log sink settle within a few minutes (spikes S4, S7)


# -- the released use case, as the launcher and the tenant's job see it ------------------------------------------

def released_usecase(contract) -> SimpleNamespace:
    """What ABTests needs, from a release contract: no checkout in the launcher or the job."""
    ml = contract.pipeline("training").static_parameters.get("ml_config") or {}
    return usecase_of(ml, contract.usecase, contract.tenant, contract.environment, list(contract.pipelines or {}))


def usecase_of(ml: dict, usecase: str, tenant: str, environment: str, pipelines: list) -> SimpleNamespace:
    platform = ml.get("platform") or {}
    return SimpleNamespace(name=usecase, tenant=tenant, environment=environment, ml=ml,
                           model={"name": ml.get("model_name"), "serving": {"enabled": True}},
                           pipelines={p: None for p in pipelines},
                           resources={"bigquery_dataset": platform.get("bigquery_dataset")}, approvals={})


def section_of(contract) -> dict:
    ml = contract.pipeline("training").static_parameters.get("ml_config") or {}
    return ml.get("ab_test") or {}


def prefix(tenant: str, usecase: str) -> str:
    return f"online-sync/{tenant}/{usecase}/ab"


# -- cron ---------------------------------------------------------------------------------------------------------

def _field(spec: str, lo: int, hi: int) -> set[int]:
    out: set[int] = set()
    for part in spec.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part in ("*", ""):
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = b = int(part)
            if step > 1:
                b = hi
        out.update(range(a, b + 1, step))
    return out


def cron_matches(cron: str, at: datetime) -> bool:
    parts = cron.split()
    if len(parts) != 5:
        raise ConfigError(f"analysis.schedule {cron!r}: five cron fields (minute hour day month weekday)")
    minute, hour, dom, month, dow = parts
    days = {d % 7 for d in _field(dow, 0, 7)}
    day_ok = at.day in _field(dom, 1, 31)
    wday_ok = (at.isoweekday() % 7) in days
    if dom != "*" and dow != "*":
        when = day_ok or wday_ok                      # cron's rule: either day field
    else:
        when = day_ok and wday_ok
    return at.minute in _field(minute, 0, 59) and at.hour in _field(hour, 0, 23) and \
        at.month in _field(month, 1, 12) and when


def due(cron: str, last: datetime, now: datetime, limit: timedelta = timedelta(days=8)) -> bool:
    """True when the schedule had a minute in (last, now] (at most `limit` back)."""
    t = max(last, now - limit).replace(second=0, microsecond=0) + timedelta(minutes=1)
    while t <= now:
        if cron_matches(cron, t):
            return True
        t += timedelta(minutes=1)
    return False


# -- the scheduled analysis ---------------------------------------------------------------------------------------

def start_analysis(contract, manifest: dict, gcp, test: dict) -> dict:
    """The tenant's job, started with the test and the released configuration (as the scheduled sync is)."""
    from pipewright.gcp.storage import gcs_write

    job = (((manifest.get("tenants") or {}).get(contract.tenant) or {}).get("features") or {}).get("sync_job")
    if not job:
        return {"status": "refused", "reason": f"tenant {contract.tenant} has no runner job (manifest "
                                               "tenants.<t>.features.sync_job): run `pipewright ab analyse`"}
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    name = f"{prefix(contract.tenant, contract.usecase)}/requests/{stamp}-{test['name']}.json"
    request = {"usecase": contract.usecase, "tenant": contract.tenant, "release": contract.release_id,
               "environment": contract.environment, "pipelines": list(contract.pipelines or {}),
               "manifest": manifest, "ml_config": contract.pipeline("training").static_parameters.get("ml_config"),
               "test": test}
    gcs_write(gcp, manifest["artifacts_bucket"], name, json.dumps(request, default=str))
    body = {"overrides": {"containerOverrides": [{"args": [
        "ab", "analyse", "--request", f"gs://{manifest['artifacts_bucket']}/{name}", "--usecase",
        contract.usecase]}]}}
    r = gcp.post(f"{RUN}/{job}:run", body)
    if not r.ok:
        raise ConfigError(f"the tenant's job {job} did not start: HTTP {r.status}")
    return {"status": "submitted", "request": name,
            "execution": ((r.body or {}).get("metadata") or {}).get("name") or (r.body or {}).get("name")}


def run_request(gcp, uri: str, usecase: str, now: datetime | None = None, query=None) -> dict:
    """The tenant's job: the analysis of the test in the request, as the runner (its BigQuery access); the results
    table gets its row, and the result is left for the launcher to record."""
    from pipewright.abtest import service
    from pipewright.config.environment import environment_from_dict
    from pipewright.gcp.storage import gcs_read, gcs_write

    bucket, _, name = uri[len("gs://"):].partition("/")
    text, _ = gcs_read(gcp, bucket, name)
    if not text:
        raise ConfigError(f"no analysis request at {uri}")
    req = json.loads(text)
    if req.get("usecase") != usecase:
        raise ConfigError(f"the analysis request is for {req.get('usecase')}, not {usecase}")
    env = environment_from_dict(req["manifest"])
    uc = usecase_of(req["ml_config"] or {}, usecase, req["tenant"], req.get("environment") or env.name,
                    req.get("pipelines") or [])
    out = service.ABTests(uc, env, gcp, log=print).analyse(test=req["test"], now=now, query=query, record=False)
    result = out["result"]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    where = f"{prefix(req['tenant'], usecase)}/results/{stamp}-{req['test']['name']}.json"
    gcs_write(gcp, bucket, where, json.dumps({"test": req["test"]["name"], "release": req.get("release"),
                                              "result": result}, default=str))
    return {"usecase": usecase, "ab_test": req["test"]["name"], "status": result["status"], "result": where}


def ingest(gcp, manifest: dict, contract, test: dict, tests) -> list[str]:
    """Each result the tenant's job left since the last one recorded, in order. Returns the files recorded."""
    from pipewright.gcp.storage import gcs_list, gcs_read

    bucket = manifest["artifacts_bucket"]
    names = sorted(n for n in gcs_list(gcp, bucket, f"{prefix(contract.tenant, contract.usecase)}/results/")
                   if n.endswith(f"-{test['name']}.json") and n > (test.get("ingested") or ""))
    done = []
    for n in names:
        text, _ = gcs_read(gcp, bucket, n)
        result = (json.loads(text) if text else {}).get("result")
        if result:
            tests.record_result(test["name"], result, ingested=n)
        done.append(n)
    return done


# -- serving: the gateway's lines and the treatment's endpoint ----------------------------------------------------

def log_lines(gcp, project: str, test: str, since: datetime, until: datetime, events: tuple[str, ...]) -> list:
    """The gateway's lines of these events for the test, oldest first (the launcher reads logs: logging.viewer)."""
    flt = (f'logName="projects/{project}/logs/pipewright_ab_exposure" AND jsonPayload.ab_test="{test}" AND '
           f'({" OR ".join(f"jsonPayload.event={json.dumps(e)}" for e in events)}) AND '
           f'timestamp>="{_z(since)}" AND timestamp<="{_z(until)}"')
    out, token = [], None
    for _ in range(20):                              # 20 pages of 1,000: a day of counts from about 14 instances
        body = {"resourceNames": [f"projects/{project}"], "filter": flt, "orderBy": "timestamp asc",
                "pageSize": 1000, **({"pageToken": token} if token else {})}
        r = gcp.post(LOG_API, body)
        if not r.ok:
            raise ConfigError(f"Cloud Logging: HTTP {r.status} (the launcher needs logging.viewer)")
        out += [{**(e.get("jsonPayload") or {}), "_ts": e.get("timestamp")}
                for e in (r.body or {}).get("entries") or []]
        token = (r.body or {}).get("nextPageToken")
        if not token:
            break
    return out


def router_calls(lines: list, start: datetime) -> tuple[dict, dict]:
    """(calls and instances sent in the window, the loss totals) from the instances' counts lines: per instance, its
    newest totals less its newest before the window (0 for an instance that started in it; a missing line before
    the window counts more calls, never fewer, so a bypass is never invented)."""
    by: dict[str, list] = {}
    for line in lines:
        by.setdefault(line.get("instance") or "?", []).append(line)
    sent = {k: 0 for k in ("treatment_calls", "treatment_instances", "control_calls", "control_instances")}
    loss = {k: 0 for k in ("unassigned", "dropped", "mismatched_model")}
    for rows in by.values():
        before = [r for r in rows if _ts(r) < start]
        inside = [r for r in rows if _ts(r) >= start]
        last = (inside or before)[-1]
        base = before[-1] if before and inside else ({} if inside else last)
        for k in sent:
            sent[k] += int(last.get(k) or 0) - int(base.get(k) or 0)
        for k in loss:
            loss[k] += int(last.get(k) or 0)
    return sent, loss


def check_serving(gcp, manifest: dict, test: dict, section: dict, now: datetime) -> dict:
    """{breach: why or None, router: loss totals, bypassed: n, note}."""
    from pipewright.loop import canary

    project = manifest["project"]
    end = now - LAG
    start = max(end - WINDOW, _dt(test.get("started_at")) or end - WINDOW)
    breaker = ((section.get("guardrails") or {}).get("technical") or {}).get("breaker") or {}
    limit = float(breaker.get("max_error_rate", 0.05))
    minimum = min(int(breaker.get("window", 200)), 20)
    since = _dt(test.get("watched_at")) or _dt(test.get("started_at")) or start
    trips = log_lines(gcp, project, test["name"], since, now, ("breaker_tripped",))
    counts = log_lines(gcp, project, test["name"], start - timedelta(minutes=3), end, ("counts",))
    sent, loss = router_calls(counts, start)
    endpoint = (test["arms"]["treatment"] or {}).get("endpoint") or ""
    metrics = canary.endpoint_metrics(gcp, project, endpoint.rsplit("/", 1)[-1], _z(start), _z(end)) if endpoint \
        else {}
    answered = sum(int(m.get("predictions") or 0) for m in metrics.values())
    errors = sum(int(m.get("errors") or 0) for m in metrics.values())
    rate = errors / answered if answered else 0.0
    out = {"router": loss, "answered": answered, "errors": errors, "sent": sent, "breach": None, "bypassed": 0,
           "window": [_z(start), _z(end)]}
    if answered >= minimum and rate > limit:
        out["breach"] = (f"the treatment's endpoint answered {answered} call(s) with {errors} error(s) "
                         f"({rate:.2%}, more than {limit:.0%})"
                         + (" and the gateway's breaker tripped" if trips else ""))
    elif trips:
        out["note"] = "the gateway's breaker tripped; its endpoint's errors are not in Cloud Monitoring yet"
        if any(_ts(t) < now - 2 * LAG for t in trips):     # waited two settling periods: trust the gateway
            out["breach"] = "the gateway's breaker tripped (the treatment's error rate over its last calls)"
    # Calls the router did not send: the endpoint answered more than either measure of what was sent explains.
    expected = max(sent["treatment_calls"], sent["treatment_instances"])
    if counts and answered > expected * 1.1 + 10:
        out["bypassed"] = answered - expected
    return out


# -- the tick -----------------------------------------------------------------------------------------------------

def tick(gcp, manifest: dict, live, emit_fn=None, refresh=None, now: datetime | None = None) -> dict:
    """refresh(contract): brings the gateway up to date (the launcher's ensure_lookup_service)."""
    from pipewright.abtest import service
    from pipewright.abtest.store import ACTIVE, TestStore
    from pipewright.config.environment import environment_from_dict

    now = now or datetime.now(timezone.utc)
    bucket = manifest.get("artifacts_bucket")
    if not bucket:
        return {"checked": 0}
    store = TestStore(gcp, bucket)
    env = environment_from_dict(manifest)
    out = {"checked": 0, "tests": []}
    for contract in live:
        section = section_of(contract)
        if not section.get("enabled"):
            continue
        test = store.current(contract.usecase)
        if not test or test.get("state") not in ACTIVE:
            continue
        out["checked"] += 1
        uc = released_usecase(contract)
        tests = service.ABTests(uc, env, gcp, log=lambda m: None)
        done = {"usecase": contract.usecase, "ab_test": test["name"], "notes": []}
        try:
            done["ingested"] = ingest(gcp, manifest, contract, test, tests)
        except Exception as e:  # noqa: BLE001 - each step on its own
            done["notes"].append(f"results not read: {str(e)[:200]}")
        test = store.get(contract.usecase, test["name"]) or test
        if (test["arms"].get("treatment") or {}).get("endpoint") and test.get("state") in ACTIVE:
            try:
                done["serving"] = _serving(gcp, manifest, contract, test, section, tests, refresh, now)
            except Exception as e:  # noqa: BLE001
                done["notes"].append(f"serving not checked: {str(e)[:200]}")
        try:
            test = store.get(contract.usecase, test["name"]) or test
            last = _dt(test.get("analysis_requested_at")) or _dt(test.get("started_at")) or now
            cron = (section.get("analysis") or {}).get("schedule")
            if cron and test.get("state") in ACTIVE and due(cron, last, now):
                started = start_analysis(contract, manifest, gcp, test)
                done["analysis"] = started
                if started["status"] == "submitted":
                    tests._change(test["name"], lambda t: {**t, "analysis_requested_at": _z(now)})
        except Exception as e:  # noqa: BLE001
            done["notes"].append(f"analysis not started: {str(e)[:200]}")
        out["tests"].append(done)
    return out


def _serving(gcp, manifest, contract, test, section, tests, refresh, now) -> dict:
    from pipewright.abtest import service

    seen = check_serving(gcp, manifest, test, section, now)
    breach = seen["breach"] if test.get("state") != "paused" and test["allocation"][-1]["treatment_bp"] else None

    def change(t):
        router = dict(t.get("router") or {})
        for k, v in {**seen["router"], "bypassed": seen["bypassed"]}.items():
            router[k] = max(int(router.get(k) or 0), int(v or 0))      # once seen, a loss stays on the record
        t["router"] = router
        t["watched_at"] = _z(now)
        if breach:
            t["allocation"] = t["allocation"] + [{"at": _z(now), "treatment_bp": 0, "by": "pipewright",
                                                  "reason": f"technical guardrail: {breach}"}]
            t["guardrails_breached"] = (t.get("guardrails_breached") or []) + [
                {"at": _z(now), "kind": "technical", "why": breach}]
            if t["state"] == "running":
                t["state"] = "decision_required"
            tests._history(t, "guardrail_breached", None, kind="technical", why=breach)
        return t

    t = tests._change(test["name"], change)
    if breach:
        service.emit(tests.env, tests.uc, gcp, "ab_test.guardrail_breached", t, kind="technical", why=breach,
                     answered=seen["answered"], errors=seen["errors"])
        if refresh is not None:
            refresh(contract)
    if seen["bypassed"] and not int((test.get("router") or {}).get("bypassed") or 0):
        service.emit(tests.env, tests.uc, gcp, "ab_test.guardrail_breached", t, kind="integrity",
                     why=f"the treatment's endpoint answered {seen['bypassed']} call(s) the gateway did not send")
    return {k: seen[k] for k in ("answered", "errors", "sent", "router", "bypassed", "breach", "window")}


def _z(d: datetime) -> str:
    return d.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _dt(s) -> datetime | None:
    if not s:
        return None
    d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _ts(line: dict) -> datetime:
    return _dt(line.get("_ts")) or datetime.fromtimestamp(float(line.get("at") or 0), timezone.utc)
