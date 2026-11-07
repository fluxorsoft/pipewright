"""FR-104 spike load agent, on a VM in europe-west2 as the spike service account.

Polls gs://<bucket>/plan/*.json and runs each plan once; writes gs://<bucket>/results/<name>.json. Only the plan types
below exist (no arbitrary commands). Lookups are open-loop: requests start at a fixed rate whatever the latency, so a
slow store shows up as latency and errors, not as a lower request rate.
"""
import concurrent.futures as cf, json, os, random, ssl, statistics, sys, threading, time, urllib.error, urllib.parse, \
    urllib.request

BUCKET, P, R = sys.argv[1], "pw-ebook", "europe-west2"
MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
_tok, _lock = {}, threading.Lock()


def token():
    with _lock:
        if "t" not in _tok or time.time() - _tok["at"] > 1500:
            r = urllib.request.urlopen(urllib.request.Request(MD + "/token", headers={"Metadata-Flavor": "Google"}))
            _tok.update(t=json.loads(r.read())["access_token"], at=time.time())
        return _tok["t"]


def idtoken(aud):
    key = "id:" + aud
    with _lock:
        if key not in _tok or time.time() - _tok[key + "at"] > 1500:
            r = urllib.request.urlopen(urllib.request.Request(
                MD + "/identity?audience=" + urllib.parse.quote(aud, safe=""), headers={"Metadata-Flavor": "Google"}))
            _tok[key], _tok[key + "at"] = r.read().decode(), time.time()
        return _tok[key]


_conns = threading.local()


def call(method, url, body=None, auth=None, raw=None, ctype="application/json", timeout=30):
    """HTTPS with a kept-alive connection per thread and host, as a real serving client would hold."""
    import http.client
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    h = {"Authorization": "Bearer " + (auth or token()), "Content-Type": ctype}
    u = urllib.parse.urlsplit(url)
    pool = _conns.__dict__.setdefault("pool", {})
    for attempt in (1, 2):
        c = pool.get(u.netloc)
        if c is None:
            c = pool[u.netloc] = http.client.HTTPSConnection(u.netloc, timeout=timeout)
        try:
            c.request(method, u.path + (("?" + u.query) if u.query else ""), body=data, headers=h)
            r = c.getresponse()
            t = r.read()
            if r.status >= 400:
                return r.status, {"error": t[:300].decode(errors="replace")}
            return r.status, (json.loads(t) if t and t[:1] in b"{[" else t)
        except Exception as e:  # noqa: BLE001 - a dropped keep-alive connection: reconnect once
            pool.pop(u.netloc, None)
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
            if attempt == 2:
                return 599, {"error": str(e)[:300]}


def gcs_put(name, obj):
    call("POST", f"https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o?uploadType=media&name="
         + urllib.parse.quote(name, safe=""), raw=json.dumps(obj, indent=2).encode())


def gcs_list(prefix):
    s, b = call("GET", f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?prefix={prefix}")
    return [o["name"] for o in (b.get("items") or [])] if s == 200 else []


def gcs_get(name):
    s, b = call("GET", f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{urllib.parse.quote(name, safe='')}"
                       "?alt=media", timeout=600)
    return b


# ------------------------------------------------------------------------------------------------ lookups --

def bq_lookup(table, eid, required=False):
    body = {"query": f"SELECT * FROM `{P}.fs104_spike.{table}` WHERE entity_id = @id", "useLegacySql": False,
            "parameterMode": "NAMED", "queryParameters": [{"name": "id", "parameterType": {"type": "STRING"},
                                                           "parameterValue": {"value": eid}}],
            "useQueryCache": False, "location": R, "timeoutMs": 20000,
            "jobCreationMode": "JOB_CREATION_REQUIRED" if required else "JOB_CREATION_OPTIONAL"}
    s, b = call("POST", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/queries", body)
    ok = s == 200 and b.get("jobComplete") and int(b.get("totalRows", 0)) == 1
    return ok, s, b


def bt_lookup(view, eid):
    s, b = call("POST", f"https://{R}-aiplatform.googleapis.com/v1/projects/{P}/locations/{R}/featureOnlineStores/"
                        f"fs104spike/featureViews/{view}:fetchFeatureValues", {"dataKey": {"key": eid}})
    return s == 200 and bool(b.get("keyValues") or b.get("protoStruct")), s, b


def fs_lookup(eid):
    s, b = call("GET", f"https://firestore.googleapis.com/v1/projects/{P}/databases/fs104-spike/documents/features/{eid}")
    return s == 200 and "fields" in b, s, b


_pg = threading.local()


def pg_conn(cfg):
    import pg8000.native
    if not getattr(_pg, "c", None):
        ctx = ssl.create_default_context()
        ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE       # spike only: encrypted, not verified
        _pg.c = pg8000.native.Connection("postgres", host=cfg["host"], password=cfg["password"], database="features",
                                         ssl_context=ctx, timeout=30)
    return _pg.c


def pg_lookup(cfg, eid):
    try:
        rows = pg_conn(cfg).run("SELECT * FROM features WHERE entity_id = :id", id=eid)
        return len(rows) == 1, 200, None
    except Exception as e:  # noqa: BLE001
        _pg.c = None
        return False, 599, {"error": str(e)[:200]}


def http_lookup(url, store, eid):
    s, b = call("POST", url + "/lookup", {"store": store, "entity_ids": [eid]}, auth=idtoken(url))
    return s == 200 and bool((b or {}).get("features")), s, b


def run_rate(fn, rate, secs, n_entities, workers=128):
    lat, errs, lock = [], {}, threading.Lock()
    stats = {"bytes_processed": set(), "job_created": 0}

    def one():
        eid = f"e{random.randint(1, n_entities)}"
        t = time.perf_counter()
        ok, s, b = fn(eid)
        ms = (time.perf_counter() - t) * 1000
        with lock:
            if ok:
                lat.append(ms)
                if isinstance(b, dict) and "totalBytesProcessed" in b:
                    stats["bytes_processed"].add(b["totalBytesProcessed"])
                if isinstance(b, dict) and b.get("jobReference", {}).get("jobId"):
                    stats["job_created"] += 1
            else:
                k = f"{s}: {str((b or {}).get('error', b))[:120]}" if isinstance(b, dict) else str(s)
                errs[k] = errs.get(k, 0) + 1

    start, n = time.perf_counter(), int(rate * secs)
    with cf.ThreadPoolExecutor(workers) as ex:
        futs = []
        for i in range(n):
            delay = start + i / rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            futs.append(ex.submit(one))
        cf.wait(futs)
    lat.sort()
    q = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 1) if lat else None  # noqa: E731
    return {"rate": rate, "secs": secs, "sent": n, "ok": len(lat), "errors": errs,
            "achieved_rps": round(len(lat) / (time.perf_counter() - start), 1),
            "p50_ms": q(0.50), "p95_ms": q(0.95), "p99_ms": q(0.99), "max_ms": q(1.0),
            "mean_ms": round(statistics.mean(lat), 1) if lat else None,
            "bytes_processed_seen": sorted(stats["bytes_processed"])[:5], "jobs_created": stats["job_created"]}


def warm(fn, n_entities, k=20):
    for _ in range(k):
        fn(f"e{random.randint(1, n_entities)}")


# ------------------------------------------------------------------------------------------------ loads --

def parquet_rows(prefix):
    import pyarrow.parquet as pq, io
    for name in sorted(gcs_list(prefix)):
        if name.endswith(".parquet"):
            for batch in pq.ParquetFile(io.BytesIO(gcs_get(name))).iter_batches(10000):
                yield from batch.to_pylist()


def pg_load(cfg, prefix):
    import pg8000.native, io, csv
    ctx = ssl.create_default_context()
    ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
    admin = pg8000.native.Connection("postgres", host=cfg["host"], password=cfg["password"], database="postgres",
                                     ssl_context=ctx)
    if not admin.run("SELECT 1 FROM pg_database WHERE datname='features'"):
        admin.run("CREATE DATABASE features")
    c = pg8000.native.Connection("postgres", host=cfg["host"], password=cfg["password"], database="features",
                                 ssl_context=ctx)
    cols = [f"f{k:02d}" for k in range(30)]
    c.run("DROP TABLE IF EXISTS features")
    c.run("CREATE TABLE features (entity_id text PRIMARY KEY, feature_timestamp timestamptz, "
          + ", ".join(f"{x} double precision" for x in cols) + ")")
    t, n, buf = time.time(), 0, io.StringIO()
    w = csv.writer(buf)
    for row in parquet_rows(prefix):
        w.writerow([row["entity_id"], row["feature_timestamp"].isoformat()] + [row[x] for x in cols])
        n += 1
        if n % 100000 == 0:
            buf.seek(0); c.run("COPY features FROM STDIN WITH (FORMAT csv)", stream=buf); buf = io.StringIO()
            w = csv.writer(buf)
    buf.seek(0); c.run("COPY features FROM STDIN WITH (FORMAT csv)", stream=buf)
    c.run("ANALYZE features")
    return {"rows": n, "seconds": round(time.time() - t, 1),
            "count_in_db": c.run("SELECT count(*) FROM features")[0][0]}


def fs_load(prefix, limit):
    cols = [f"f{k:02d}" for k in range(30)]
    base = f"https://firestore.googleapis.com/v1/projects/{P}/databases/fs104-spike/documents"
    rows = []
    for row in parquet_rows(prefix):
        rows.append(row)
        if len(rows) >= limit:
            break
    t, errs = time.time(), {}

    def batch(chunk):
        writes = [{"update": {"name": f"projects/{P}/databases/fs104-spike/documents/features/{r['entity_id']}",
                              "fields": {**{x: {"doubleValue": r[x]} for x in cols},
                                         "feature_timestamp": {"timestampValue": r["feature_timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")}}}}
                  for r in chunk]
        s, b = call("POST", f"{base}:batchWrite", {"writes": writes}, timeout=120)
        if s != 200:
            errs[s] = errs.get(s, 0) + 1
            errs.setdefault("first_error", str(b)[:400])
            errs.setdefault("sample_write", json.dumps(writes[0])[:600])
    with cf.ThreadPoolExecutor(16) as ex:
        list(ex.map(batch, [rows[i:i + 400] for i in range(0, len(rows), 400)]))
    return {"rows": len(rows), "seconds": round(time.time() - t, 1), "errors": errs}


# ------------------------------------------------------------------------------------------------ main loop --

def run(plan):
    kind = plan["kind"]
    if kind == "bq_lookup":
        fn = lambda e: bq_lookup(plan["table"], e)  # noqa: E731
    elif kind == "bt_lookup":
        fn = lambda e: bt_lookup(plan["view"], e)  # noqa: E731
    elif kind == "fs_lookup":
        fn = fs_lookup
    elif kind == "pg_lookup":
        fn = lambda e: pg_lookup(plan["pg"], e)  # noqa: E731
    elif kind == "http_lookup":
        fn = lambda e: http_lookup(plan["url"], plan["store"], e)  # noqa: E731
    elif kind == "bq_billing":
        out = []
        for _ in range(plan.get("n", 10)):
            ok, s, b = bq_lookup(plan["table"], f"e{random.randint(1, plan['entities'])}", required=True)
            jid = (b or {}).get("jobReference", {}).get("jobId")
            if jid:
                s2, j = call("GET", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs/{jid}?location={R}")
                st = j.get("statistics", {}).get("query", {})
                out.append({"bytes_processed": st.get("totalBytesProcessed"), "bytes_billed": st.get("totalBytesBilled"),
                            "bi_engine": st.get("biEngineStatistics"), "cache_hit": st.get("cacheHit"),
                            "ms": int(j["statistics"]["endTime"]) - int(j["statistics"]["startTime"])})
            else:
                out.append({"error": s, "body": str(b)[:200]})
        return {"samples": out}
    elif kind == "pg_load":
        return pg_load(plan["pg"], plan["prefix"])
    elif kind == "fs_load":
        return fs_load(plan["prefix"], plan["limit"])
    else:
        return {"error": f"unknown kind {kind}"}
    warm(fn, plan["entities"])
    return {"runs": [run_rate(fn, r, plan.get("secs", 30), plan["entities"]) for r in plan["rates"]]}


done = set(n.split("/", 1)[1] for n in gcs_list("results/"))
gcs_put("results/_agent_started.json", {"at": time.time()})
while True:
    for name in sorted(gcs_list("plan/")):
        key = name.split("/", 1)[1]
        if key in done or not key.endswith(".json"):
            continue
        done.add(key)
        plan = gcs_get(name)
        t = time.time()
        try:
            res = run(plan)
        except Exception as e:  # noqa: BLE001
            res = {"error": repr(e)[:500]}
        gcs_put("results/" + key, {"plan": {k: v for k, v in plan.items() if k != "pg"}, "result": res,
                                   "started": t, "seconds": round(time.time() - t, 1)})
    time.sleep(10)
