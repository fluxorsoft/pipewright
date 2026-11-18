"""Demo telemetry for the sample dashboards: a made-up platform of five use cases sends what a busy launcher would,
through Pipewright's own telemetry code (the same allowlists, names and spans as the launcher). No data, no people:
use case and team names, counts, times and generated run IDs only.

    pip install 'pipewright-mlops[otel]'
    export OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.example.com/otlp                     # your backend's OTLP/HTTP URL
    export OTEL_EXPORTER_OTLP_HEADERS="Authorization=Basic%20..."                     # if it needs one
    python examples/telemetry/demo_traffic.py --minutes 30

The endpoint and headers are read from the environment only and never written anywhere.
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from pipewright import telemetry

# use case -> (team, pipelines with their steps, triggers with weights)
USECASES = {
    "churn": ("growth", {"training": ["ingest", "validate", "train", "evaluate", "register"],
                         "inference": ["ingest", "score", "write"]},
              {"schedule": 5, "landing": 3, "api": 1}),
    "lapse-risk": ("retail", {"training": ["ingest", "validate", "train", "evaluate", "register"],
                              "inference": ["ingest", "score", "write"],
                              "monitoring": ["collect", "drift", "report"]},
                   {"landing": 6, "schedule": 2, "retrain": 1}),
    "demand-forecast": ("supply", {"training": ["ingest", "features", "train", "backtest", "register"],
                                   "inference": ["ingest", "forecast", "write"]},
                        {"schedule": 7, "api": 1}),
    "fraud-score": ("risk", {"inference": ["ingest", "score", "write"],
                             "monitoring": ["collect", "drift", "report"]},
                    {"request": 6, "landing": 2, "ab_test": 1}),
    "customer-segments": ("marketing", {"training": ["ingest", "cluster", "profile", "register"],
                                        "inference": ["ingest", "assign", "write"]},
                          {"schedule": 3, "api": 2, "loop": 1}),
}
REFUSALS = {"held": 5, "integrity": 2, "features": 2, "no_live_release": 1, "submit_failed": 1}
LATENCY = {"landing": 4.0, "schedule": 1.5, "api": 0.8, "request": 0.6, "retrain": 6.0, "ab_test": 1.2, "loop": 2.0}
CHECKS = ["retraining", "integrity", "failed_runs", "registration", "capture"]


def pick(weights: dict) -> str:
    return random.choices(list(weights), list(weights.values()))[0]


def zero(name: str, **values) -> None:
    telemetry._s.instruments[name].add(0, telemetry._attrs(telemetry._metric(name), values))


def trigger(running: list) -> None:
    usecase = random.choice(list(USECASES))
    team, pipelines, triggers = USECASES[usecase]
    how = pick(triggers)
    pipeline = "training" if how == "retrain" and "training" in pipelines else random.choice(list(pipelines))
    run_id = uuid.uuid4().hex[:12]
    roll = random.random()
    outcome = "submitted" if roll < 0.84 else "duplicate" if roll < 0.92 else "refused"
    reason = pick(REFUSALS) if outcome == "refused" else None
    if reason == "held":
        outcome = "held"
    with telemetry.span("pipewright.trigger", trigger=how, usecase=usecase, tenant=team):
        with telemetry.span("pipewright.run.start", usecase=usecase, tenant=team, pipeline=pipeline, trigger=how,
                            run_id=run_id) as s:
            time.sleep(random.uniform(0.01, 0.08))
            s.set(outcome=outcome, **({"reason": reason} if reason else {"job_id": f"{usecase}-{run_id}"}))
        tp = telemetry.traceparent()
    if outcome == "submitted":
        telemetry.count("pipewright.runs.started", usecase=usecase, pipeline=pipeline, trigger=how)
        seconds = random.lognormvariate(0, 0.6) * LATENCY[how] * (8 if random.random() < 0.03 else 1)
        telemetry.trigger_latency(seconds, how, team)
        minutes = random.uniform(1, 6) if pipeline != "training" else random.uniform(4, 14)
        running.append({"tp": tp, "start": datetime.now(timezone.utc), "minutes": minutes, "usecase": usecase,
                        "pipeline": pipeline, "run_id": run_id, "steps": pipelines[pipeline],
                        "fails": random.random() < 0.06})
    elif outcome == "duplicate":
        telemetry.count("pipewright.runs.deduplicated", usecase=usecase, trigger=how)
    else:
        telemetry.count("pipewright.runs.refused", usecase=usecase, trigger=how, reason=reason)


def finish(running: list) -> None:
    now = datetime.now(timezone.utc)
    for run in [r for r in running if now - r["start"] >= timedelta(minutes=r["minutes"])]:
        running.remove(run)
        names = run["steps"]
        cut = sorted(random.uniform(0, 1) for _ in names[:-1])
        bounds = [run["start"] + (now - run["start"]) * f for f in (0, *cut, 1)]
        failed_at = random.randrange(len(names)) if run["fails"] else None
        steps = []
        for i, name in enumerate(names):
            if failed_at is not None and i > failed_at:
                break
            steps.append({"name": name, "start": bounds[i], "end": bounds[i + 1],
                          "state": "FAILED" if i == failed_at else "SUCCEEDED"})
        end = steps[-1]["end"]
        telemetry.run_spans(run["tp"], {"start": run["start"], "end": end, "usecase": run["usecase"],
                                        "pipeline": run["pipeline"], "run_id": run["run_id"],
                                        "job_id": f"{run['usecase']}-{run['run_id']}",
                                        "state": "PIPELINE_STATE_FAILED" if run["fails"] else
                                        "PIPELINE_STATE_SUCCEEDED"}, steps)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--rate", type=float, default=0.4, help="triggers per second")
    ap.add_argument("--environment", default="demo")
    args = ap.parse_args()

    from urllib.parse import unquote

    endpoint = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"].rstrip("/")
    headers = {unquote(k.strip()): unquote(v.strip()) for k, v in
               (p.split("=", 1) for p in os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "").split(",") if "=" in p)}
    # the launcher starts telemetry from its manifest; the demo starts it the same way, from the environment
    telemetry._start(telemetry.Settings(endpoint, headers, 1.0, 15, "environment"),
                     {"environment": args.environment}, "demo")
    for usecase, (team, _, _) in USECASES.items():
        telemetry.know(usecase, team)
    # every counter combination starts at 0, so a dashboard's increase() sees the first refusal of each kind too
    # (a new Prometheus series' first sample has nothing to increase from)
    for usecase, (_, pipelines, triggers) in USECASES.items():
        for how in (*triggers, "retrain"):
            zero("pipewright.runs.deduplicated", usecase=usecase, trigger=how)
            for reason in REFUSALS:
                zero("pipewright.runs.refused", usecase=usecase, trigger=how, reason=reason)
            for pipeline in pipelines:
                zero("pipewright.runs.started", usecase=usecase, pipeline=pipeline, trigger=how)
    for check in CHECKS:
        zero("pipewright.tick.failures", check=check)
    cases = {u: 0 for u in USECASES}
    telemetry.cases_source(lambda: dict(cases))

    running: list = []
    stop = time.monotonic() + args.minutes * 60
    sent = 0
    while time.monotonic() < stop:
        trigger(running)
        sent += 1
        finish(running)
        if random.random() < 0.01:
            telemetry.count("pipewright.tick.failures", check=random.choice(CHECKS))
        if random.random() < 0.02:                      # a retrain case opens, or someone decides one
            u = random.choice(["lapse-risk", "churn", "demand-forecast"])
            cases[u] = max(0, min(4, cases[u] + random.choice((1, 1, -1))))
        if sent % 100 == 0:
            print(f"{sent} triggers, {len(running)} runs in flight, failed exports {telemetry._s.failed_exports}",
                  flush=True)
        time.sleep(random.expovariate(args.rate))
    time.sleep(16)
    telemetry.shutdown()
    print(f"done: {sent} triggers, failed exports {telemetry._s.failed_exports}")


if __name__ == "__main__":
    main()
