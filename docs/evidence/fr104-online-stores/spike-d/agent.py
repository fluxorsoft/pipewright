"""FR-104 spike (d) load agent, on a VM in europe-west2 as fs104d-agent (IAM database login, no password).

Polls gs://<bucket>/plan/*.json, runs each plan once, writes gs://<bucket>/results/<name>.json. Only the plan kinds
below exist. Lookups are open-loop: requests start at a fixed rate whatever the latency.
"""
import concurrent.futures as cf, csv, io, json, queue, random, ssl, statistics, sys, threading, time, urllib.parse, \
    urllib.request

BUCKET, HOST = sys.argv[1], sys.argv[2]
MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
_tok, _lock = {}, threading.Lock()
COLS = [f"f{k:02d}" for k in range(30)]


def md(path):
    return urllib.request.urlopen(urllib.request.Request(MD + path, headers={"Metadata-Flavor": "Google"})).read()


def token():
    with _lock:
        if "t" not in _tok or time.time() - _tok["at"] > 1500:
            _tok.update(t=json.loads(md("/token"))["access_token"], at=time.time())
        return _tok["t"]


def idtoken(aud):
    with _lock:
        k = "id:" + aud
        if k not in _tok or time.time() - _tok[k + "at"] > 1500:
            _tok[k], _tok[k + "at"] = md("/identity?audience=" + urllib.parse.quote(aud, safe="")).decode(), time.time()
        return _tok[k]


EMAIL = md("/email").decode()
DBUSER = EMAIL.replace(".gserviceaccount.com", "")
_conns = threading.local()


def call(method, url, body=None, auth=None, raw=None, timeout=30):
    import http.client
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    h = {"Authorization": "Bearer " + (auth or token()), "Content-Type": "application/json"}
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
         + urllib.parse.quote(name, safe=""), raw=json.dumps(obj, indent=1).encode())


def gcs_list(prefix):
    s, b = call("GET", f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o?prefix={prefix}")
    return [o["name"] for o in (b.get("items") or [])] if s == 200 else []


def gcs_get(name):
    s, b = call("GET", f"https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{urllib.parse.quote(name, safe='')}"
                       "?alt=media", timeout=600)
    return b


# --------------------------------------------------------------------------------------------- database --

def connect(database="features", user=None, password=None):
    import pg8000.native
    ctx = ssl.create_default_context()
    ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE   # spike: encrypted (server requires SSL), not verified
    return pg8000.native.Connection(user or DBUSER, host=HOST, password=password or token(), database=database,
                                    ssl_context=ctx, timeout=30)


class Pool:
    """A bounded pool shared by all request threads, as a serving process would hold."""

    def __init__(self, n, schema):
        self.q, self.schema = queue.Queue(), schema
        for _ in range(n):
            self.q.put(None)

    def lookup(self, eid):
        c = self.q.get()
        try:
            if c is None:
                c = connect()
            rows = c.run(f"SELECT * FROM {self.schema}.features WHERE entity_id = :id", id=eid)
            return len(rows) == 1, 200, None
        except Exception as e:  # noqa: BLE001
            c = None
            return False, 599, {"error": str(e)[:160]}
        finally:
            self.q.put(c)


def http_lookup(url, eid):
    s, b = call("POST", url + "/lookup", {"entity_ids": [eid]}, auth=idtoken(url))
    return s == 200 and bool((b or {}).get("features")), s, b


def run_rate(fn, rate, secs, n_entities, workers=256, buckets=None):
    lat, errs, lock = [], {}, threading.Lock()

    def one(t0):
        eid = f"e{random.randint(1, n_entities)}"
        t = time.perf_counter()
        ok, s, b = fn(eid)
        ms = (time.perf_counter() - t) * 1000
        with lock:
            if buckets is not None:
                buckets.setdefault(int(t - t0), []).append(ms if ok else -1)
            if ok:
                lat.append(ms)
            else:
                k = f"{s}: {str((b or {}).get('error', b))[:120]}" if isinstance(b, dict) else str(s)
                errs[k] = errs.get(k, 0) + 1

    start, n = time.perf_counter(), int(rate * secs)
    late = 0
    with cf.ThreadPoolExecutor(workers) as ex:
        futs = []
        for i in range(n):
            delay = start + i / rate - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            elif delay < -0.05:
                late += 1
            futs.append(ex.submit(one, start))
        cf.wait(futs)
    lat.sort()
    q = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 1) if lat else None  # noqa: E731
    return {"rate": rate, "secs": secs, "sent": n, "ok": len(lat), "errors": errs, "sent_late": late,
            "achieved_rps": round(len(lat) / (time.perf_counter() - start), 1),
            "p50_ms": q(0.50), "p95_ms": q(0.95), "p99_ms": q(0.99), "max_ms": q(1.0),
            "mean_ms": round(statistics.mean(lat), 1) if lat else None}


def warm(fn, n_entities, k=40):
    for _ in range(k):
        fn(f"e{random.randint(1, n_entities)}")


# ----------------------------------------------------------------------------------------- setup and facts --

def setup(plan):
    """As the built-in admin user once: database, a schema per tenant, IAM users granted only their own schema."""
    out = {"as": "postgres" if plan.get("admin_password") else DBUSER, "statements": []}
    kw = {"user": "postgres", "password": plan["admin_password"]} if plan.get("admin_password") else {}
    admin = connect("postgres", **kw)
    if not admin.run("SELECT 1 FROM pg_database WHERE datname='features'"):
        admin.run("CREATE DATABASE features")
    a = connect("features", **kw)

    def ex(sql):
        try:
            a.run(sql)
            out["statements"].append([sql, "ok"])
        except Exception as e:  # noqa: BLE001
            out["statements"].append([sql, str(e)[:160]])

    ex("REVOKE ALL ON SCHEMA public FROM PUBLIC")
    ex("REVOKE CREATE ON DATABASE features FROM PUBLIC")
    for tenant, users in plan["tenants"].items():
        writer = plan["writer"][tenant]
        ex(f'GRANT "{writer}" TO CURRENT_USER')
        ex(f'CREATE SCHEMA IF NOT EXISTS {tenant} AUTHORIZATION "{writer}"')
        ex(f"REVOKE ALL ON SCHEMA {tenant} FROM PUBLIC")
        for u, mode in users.items():
            if mode == "read":
                ex(f'GRANT USAGE ON SCHEMA {tenant} TO "{u}"')
                ex(f'ALTER DEFAULT PRIVILEGES FOR ROLE "{writer}" IN SCHEMA {tenant} GRANT SELECT ON TABLES TO "{u}"')
    out["schemas"] = a.run("SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname LIKE 'tenant%'")
    return out


def facts(plan):
    c = connect()
    out = {"user": DBUSER}
    for k in ("max_connections", "superuser_reserved_connections", "ssl", "server_version", "shared_buffers",
              "effective_cache_size", "work_mem"):
        out[k] = c.run(f"SHOW {k}")[0][0]
    out["reserved_for_cloudsql"] = c.run("SELECT count(*) FROM pg_roles WHERE rolname LIKE 'cloudsql%'")[0][0]
    out["active_connections"] = c.run("SELECT count(*) FROM pg_stat_activity")[0][0]
    out["ssl_in_use"] = c.run("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")[0][0]
    # tenancy: try to read the other tenant's schema
    for t in plan.get("probe_schemas", []):
        try:
            c.run(f"SELECT count(*) FROM {t}.features")
            out[f"read_{t}"] = "allowed"
        except Exception as e:  # noqa: BLE001
            out[f"read_{t}"] = "refused: " + str(e)[:120]
    try:
        c.run("CREATE TABLE public.probe (x int)")
        out["create_in_public"] = "allowed"
    except Exception as e:  # noqa: BLE001
        out["create_in_public"] = "refused: " + str(e)[:120]
    return out


# ------------------------------------------------------------------------------------------------ loads --

def parquet_rows(prefix):
    import pyarrow.parquet as pq
    for name in sorted(gcs_list(prefix)):
        if name.endswith(".parquet"):
            for batch in pq.ParquetFile(io.BytesIO(gcs_get(name))).iter_batches(20000):
                yield from batch.to_pylist()


def copy_into(c, table, prefix):
    t, n, buf = time.time(), 0, io.StringIO()
    w = csv.writer(buf)
    for row in parquet_rows(prefix):
        w.writerow([row["entity_id"], row["feature_timestamp"].isoformat()] + [row[x] for x in COLS])
        n += 1
        if n % 100000 == 0:
            buf.seek(0)
            c.run(f"COPY {table} FROM STDIN WITH (FORMAT csv)", stream=buf)
            buf = io.StringIO()
            w = csv.writer(buf)
    buf.seek(0)
    c.run(f"COPY {table} FROM STDIN WITH (FORMAT csv)", stream=buf)
    return n, round(time.time() - t, 1)


def create(c, table):
    c.run(f"DROP TABLE IF EXISTS {table}")
    c.run(f"CREATE TABLE {table} (entity_id text PRIMARY KEY, feature_timestamp timestamptz, "
          + ", ".join(f"{x} double precision" for x in COLS) + ")")


def load(plan):
    """The first load: a table (method rename) or a generation table behind a view (method view)."""
    c, s = connect(), plan["schema"]
    if plan["method"] == "rename":
        create(c, f"{s}.features")
        n, secs = copy_into(c, f"{s}.features", plan["prefix"])
        c.run(f"ANALYZE {s}.features")
    else:
        c.run(f"DROP VIEW IF EXISTS {s}.features")
        create(c, f"{s}.features_g1")
        n, secs = copy_into(c, f"{s}.features_g1", plan["prefix"])
        c.run(f"ANALYZE {s}.features_g1")
        c.run(f"CREATE VIEW {s}.features AS SELECT * FROM {s}.features_g1")
    for g in plan.get("grant_select", []):
        c.run(f'GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO "{g}"')
    return {"rows": n, "copy_seconds": secs, "count": c.run(f"SELECT count(*) FROM {s}.features")[0][0]}


def swap(plan):
    """Reads at a steady rate the whole time while a sync loads staging and swaps; per-second results."""
    s, method, buckets, marks = plan["schema"], plan["method"], {}, {}
    reader = plan.get("url")
    pool = None if reader else Pool(plan.get("pool", 16), s)
    fn = (lambda e: http_lookup(reader, e)) if reader else pool.lookup  # noqa: E731
    warm(fn, plan["entities"])
    t0 = time.perf_counter()
    res = {}

    def sync():
        time.sleep(plan.get("lead", 10))
        c = connect()
        c.run(f"SET lock_timeout = '{plan.get('lock_timeout', '2s')}'")
        marks["load_start"] = round(time.perf_counter() - t0, 2)
        if method == "rename":
            create(c, f"{s}.features_staging")
            n, secs = copy_into(c, f"{s}.features_staging", plan["prefix"])
            c.run(f"ANALYZE {s}.features_staging")
            for g in plan.get("grant_select", []):
                c.run(f'GRANT SELECT ON {s}.features_staging TO "{g}"')
            marks["swap_start"] = round(time.perf_counter() - t0, 2)
            t = time.perf_counter()
            tries = 0
            while True:
                tries += 1
                try:
                    c.run("BEGIN")
                    c.run(f"ALTER TABLE {s}.features RENAME TO features_old")
                    c.run(f"ALTER TABLE {s}.features_staging RENAME TO features")
                    c.run("COMMIT")
                    break
                except Exception as e:  # noqa: BLE001 - lock timeout: roll back and retry
                    c.run("ROLLBACK")
                    marks.setdefault("swap_errors", []).append(str(e)[:120])
                    if tries >= 5:
                        raise
            marks["swap_ms"] = round((time.perf_counter() - t) * 1000, 1)
            marks["swap_tries"] = tries
            c.run(f"DROP TABLE {s}.features_old")
        else:
            cur = c.run(f"SELECT definition FROM pg_views WHERE schemaname = '{s}' AND viewname = 'features'")[0][0]
            old = "features_g1" if "features_g1" in cur else "features_g2"
            new = "features_g2" if old == "features_g1" else "features_g1"
            create(c, f"{s}.{new}")
            n, secs = copy_into(c, f"{s}.{new}", plan["prefix"])
            c.run(f"ANALYZE {s}.{new}")
            for g in plan.get("grant_select", []):
                c.run(f'GRANT SELECT ON {s}.{new} TO "{g}"')
            marks["swap_start"] = round(time.perf_counter() - t0, 2)
            t = time.perf_counter()
            tries = 0
            while True:
                tries += 1
                try:
                    c.run(f"CREATE OR REPLACE VIEW {s}.features AS SELECT * FROM {s}.{new}")
                    break
                except Exception as e:  # noqa: BLE001
                    marks.setdefault("swap_errors", []).append(str(e)[:120])
                    if tries >= 5:
                        raise
            marks["swap_ms"] = round((time.perf_counter() - t) * 1000, 1)
            marks["swap_tries"] = tries
            c.run(f"DROP TABLE {s}.{old}")
        marks["dropped_at"] = round(time.perf_counter() - t0, 2)
        marks["rows"], marks["copy_seconds"] = n, secs
        marks["count_after"] = c.run(f"SELECT count(*) FROM {s}.features")[0][0]

    th = threading.Thread(target=_safe, args=(sync, res))
    th.start()
    overall = run_rate(fn, plan["rate"], plan["secs"], plan["entities"], buckets=buckets)
    th.join()
    per_sec = []
    for sec in sorted(buckets):
        v = buckets[sec]
        ok = sorted(x for x in v if x >= 0)
        per_sec.append({"s": sec, "n": len(v), "errors": sum(1 for x in v if x < 0),
                        "p50": round(ok[len(ok) // 2], 1) if ok else None,
                        "p99": round(ok[min(len(ok) - 1, int(0.99 * len(ok)))], 1) if ok else None,
                        "max": round(ok[-1], 1) if ok else None})
    return {"overall": overall, "marks": marks, "sync_error": res.get("err"), "per_second": per_sec}


def _safe(fn, res):
    try:
        fn()
        return True
    except Exception as e:  # noqa: BLE001
        res["err"] = repr(e)[:400]
        return False


def run(plan):
    kind = plan["kind"]
    if kind == "setup":
        return setup(plan)
    if kind == "revoke_membership":
        c = connect()
        out = []
        for role in plan["roles"]:
            try:
                c.run(f'REVOKE "{role}" FROM "{DBUSER}"')
                out.append([role, "revoked"])
            except Exception as e:  # noqa: BLE001
                out.append([role, str(e)[:160]])
        out.append(["member_of", c.run("SELECT b.rolname FROM pg_auth_members m JOIN pg_roles b ON m.roleid = b.oid "
                                       "JOIN pg_roles u ON m.member = u.oid WHERE u.rolname = :u", u=DBUSER)])
        return out
    if kind == "facts":
        return facts(plan)
    if kind == "load":
        return load(plan)
    if kind == "swap":
        return swap(plan)
    if kind == "pg_rate":
        pool = Pool(plan["pool"], plan["schema"])
        warm(pool.lookup, plan["entities"])
        return {"runs": [run_rate(pool.lookup, r, plan.get("secs", 30), plan["entities"]) for r in plan["rates"]]}
    if kind == "http_rate":
        fn = lambda e: http_lookup(plan["url"], e)  # noqa: E731
        warm(fn, plan["entities"])
        return {"runs": [run_rate(fn, r, plan.get("secs", 30), plan["entities"]) for r in plan["rates"]]}
    return {"error": f"unknown kind {kind}"}


done = set(n.split("/", 1)[1] for n in gcs_list("results/"))
gcs_put("results/_agent_started.json", {"at": time.time(), "user": DBUSER})
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
        gcs_put("results/" + key, {"plan": {k: v for k, v in plan.items() if k != "admin_password"}, "result": res,
                                   "started": t, "seconds": round(time.time() - t, 1)})
    time.sleep(5)
