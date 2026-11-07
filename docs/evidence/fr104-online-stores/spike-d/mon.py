import json, os, sys, time, datetime as dt, urllib.parse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fsspike"))
from g import call  # noqa: E402

mins = int(sys.argv[1]) if len(sys.argv) > 1 else 60
end = dt.datetime.now(dt.timezone.utc)
start = end - dt.timedelta(minutes=mins)
fmt = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731


def series(flt, aligner="ALIGN_MEAN", per="60s", reducer=None, extra=""):
    q = {"filter": flt, "interval.startTime": fmt(start), "interval.endTime": fmt(end),
         "aggregation.alignmentPeriod": per, "aggregation.perSeriesAligner": aligner}
    if reducer:
        q["aggregation.crossSeriesReducer"] = reducer
    s, b = call("GET", "https://monitoring.googleapis.com/v3/projects/pw-ebook/timeSeries?" + urllib.parse.urlencode(q))
    out = {}
    for ts in b.get("timeSeries", []):
        for p in ts["points"]:
            v = p["value"]
            val = v.get("doubleValue", v.get("int64Value", (v.get("distributionValue") or {}).get("mean")))
            out.setdefault(p["interval"]["endTime"][11:16], []).append(round(float(val), 3) if val is not None else None)
    return dict(sorted(out.items()))


res = {
    "db_cpu": series('metric.type="cloudsql.googleapis.com/database/cpu/utilization" '
                     'AND resource.labels.database_id="pw-ebook:fs104d-pg"'),
    "db_conns": series('metric.type="cloudsql.googleapis.com/database/postgresql/num_backends" '
                       'AND resource.labels.database_id="pw-ebook:fs104d-pg"', reducer="REDUCE_SUM"),
    "run_instances": series('metric.type="run.googleapis.com/container/instance_count" '
                            'AND resource.labels.service_name="fs104d-lookup"', aligner="ALIGN_MAX", reducer="REDUCE_SUM"),
    "run_p99_ms": series('metric.type="run.googleapis.com/request_latencies" '
                         'AND resource.labels.service_name="fs104d-lookup"', aligner="ALIGN_PERCENTILE_99",
                         reducer="REDUCE_MAX"),
    "run_p50_ms": series('metric.type="run.googleapis.com/request_latencies" '
                         'AND resource.labels.service_name="fs104d-lookup"', aligner="ALIGN_PERCENTILE_50",
                         reducer="REDUCE_MAX"),
    "run_cpu": series('metric.type="run.googleapis.com/container/cpu/utilizations" '
                      'AND resource.labels.service_name="fs104d-lookup"', aligner="ALIGN_PERCENTILE_99",
                      reducer="REDUCE_MAX"),
}
json.dump(res, open("results/monitoring_%s.json" % end.strftime("%H%M"), "w"), indent=1)
keys = sorted(set(k for v in res.values() for k in v))
print("utc   " + " ".join(f"{k:>14}" for k in res))
for k in keys:
    print(k, " ".join(f"{str(res[m].get(k, '')):>14}" for m in res))
