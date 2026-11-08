"""Spike FR-106 lookup gateway prototype, its loader and the in-VPC probe (one image, three modes).

  python gw.py serve   Cloud Run service: POST /lookup, POST /predict (look up Cloud SQL, then call the endpoint)
  python gw.py load    Cloud Run job: create the database and 5,000 rows of 8 features
  python gw.py probe   Cloud Run job: egress from the endpoints, latency of each path, canary split, caller logging

Counts, timings, names and outcomes in every log and result; never a feature value.
"""
import base64
import json
import os
import queue
import random
import socket
import ssl
import statistics
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import google.auth
import google.auth.transport.requests
import pg8000.native
import requests

E = os.environ.get
PROJECT, NUMBER, REGION = E("PROJECT", ""), E("PROJECT_NUMBER", ""), E("REGION", "us-central1")
FEATURES = [f"f{i}" for i in range(1, 9)]
_creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
_lock = threading.Lock()


def token() -> str:
    with _lock:
        if not _creds.valid:
            _creds.refresh(google.auth.transport.requests.Request())
        return _creds.token


# ------------------------------------------------------------------ Cloud SQL, IAM login, CA verified ----------
def connect(database):
    ctx = ssl.create_default_context(cadata=E("CA_PEM"))
    ctx.check_hostname = False          # the server certificate names the instance, not the private IP
    return pg8000.native.Connection(user=E("DB_USER"), password=token(), host=E("DB_HOST"), database=database,
                                    ssl_context=ctx, timeout=10)


_pool: "queue.Queue" = queue.Queue()


def lookup(ids):
    try:
        conn = _pool.get_nowait()
    except queue.Empty:
        conn = connect(E("DB_NAME", "fs"))
    try:
        rows = conn.run(f"SELECT customer_id, {', '.join(FEATURES)} FROM features WHERE customer_id = ANY(:ids)",
                        ids=list(ids))
        _pool.put(conn)
    except Exception:
        conn.close()
        raise
    return {r[0]: [float(v) for v in r[1:]] for r in rows}


# ------------------------------------------------------------------ the endpoints -----------------------------
def predict_url(target):
    eid = E("PSC_ENDPOINT_ID") if target == "psc" else E("STD_ENDPOINT_ID")
    path = f"/v1/projects/{PROJECT}/locations/{REGION}/endpoints/{eid}:predict"
    if target == "psc":
        return f"https://{eid}-{REGION}-{NUMBER}.prediction.p.vertexai.goog{path}"
    return f"https://{REGION}-aiplatform.googleapis.com{path}"


_cert = {}


def verify_for(target):
    """The PSC endpoint serves a self-signed certificate for *.prediction.p.vertexai.goog: fetched once, then pinned
    (trust on first use; the build must settle how it is trusted)."""
    if target != "psc":
        return True
    if "psc" not in _cert:
        host = urllib.parse.urlparse(predict_url("psc")).hostname
        pem = ssl.get_server_certificate((host, 443))
        f = tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False)
        f.write(pem)
        f.close()
        _cert["psc"] = f.name
    return _cert["psc"]


_http = requests.Session()


RETRIES = int(E("RETRIES", "0"))


def predict(target, instances):
    """A prediction is safe to repeat: retry 503s and connection errors with backoff (RETRIES, 0 = off)."""
    for attempt in range(RETRIES + 1):
        try:
            r = _http.post(predict_url(target), json={"instances": instances}, timeout=30, verify=verify_for(target),
                           headers={"Authorization": "Bearer " + token()})
            if r.status_code in (429, 503) and attempt < RETRIES:
                time.sleep(0.25 * 2 ** attempt)
                continue
            r.raise_for_status()
            out = r.json()
            out["_attempts"] = attempt + 1
            return out
        except (requests.ConnectionError, requests.Timeout):
            if attempt >= RETRIES:
                raise
            time.sleep(0.25 * 2 ** attempt)


def caller_of(header):
    """Cloud Run has checked the ID token (IAM invoker); the gateway reads who called from its payload."""
    try:
        payload = header.split(" ", 1)[1].split(".")[1]
        return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))).get("email", "?")
    except Exception:  # noqa: BLE001
        return "unknown"


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._send(200, {"ok": True})

    def do_POST(self):
        t0 = time.perf_counter()
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        ids = body.get("entity_ids") or []
        caller = caller_of(self.headers.get("Authorization", ""))
        try:
            found = lookup(ids)
            t1 = time.perf_counter()
            if self.path == "/lookup":
                out = {"found": len(found), "asked": len(ids)}
                self._send(200, {"features": found})
            else:
                target = body.get("target", "std")
                known = [i for i in ids if i in found]
                res = predict(target, [found[i] for i in known]) if known else {"predictions": []}
                preds = dict(zip(known, res.get("predictions", [])))
                out = {"target": target, "found": len(known), "asked": len(ids),
                       "deployed_model": res.get("deployedModelId"), "attempts": res.get("_attempts", 1)}
                self._send(200, {"predictions": {i: preds.get(i, {"error": "unknown entity"}) for i in ids},
                                 "deployedModelId": res.get("deployedModelId"), "caller": caller,
                                 "ms": {"lookup": round((t1 - t0) * 1000, 1),
                                        "predict": round((time.perf_counter() - t1) * 1000, 1)}})
            print(json.dumps({"route": self.path, "caller": caller, **out,
                              "ms": round((time.perf_counter() - t0) * 1000, 1)}), flush=True)
        except Exception as e:  # noqa: BLE001
            print(json.dumps({"route": self.path, "caller": caller, "error": type(e).__name__,
                              "detail": str(e)[:300]}), flush=True)
            self._send(502, {"error": type(e).__name__, "detail": str(e)[:300]})


def serve():
    ThreadingHTTPServer(("", int(E("PORT", "8080"))), Gateway).serve_forever()


# ------------------------------------------------------------------ loader ------------------------------------
def load():
    admin = connect("postgres")
    if not admin.run("SELECT 1 FROM pg_database WHERE datname = 'fs'"):
        admin.run('CREATE DATABASE "fs"')
    conn = connect("fs")
    conn.run("DROP TABLE IF EXISTS features")
    conn.run(f"CREATE TABLE features (customer_id text PRIMARY KEY, "
             f"{', '.join(f + ' double precision' for f in FEATURES)})")
    rnd = random.Random(7)
    rows = [(f"C{100000 + i}", *[round(rnd.uniform(0, 100), 3) for _ in FEATURES]) for i in range(5000)]
    t = time.perf_counter()
    for k in range(0, len(rows), 500):
        chunk = rows[k:k + 500]
        vals = ", ".join(f"(:c{j}, " + ", ".join(f":v{j}_{n}" for n in range(8)) + ")" for j in range(len(chunk)))
        params = {}
        for j, r in enumerate(chunk):
            params[f"c{j}"] = r[0]
            for n in range(8):
                params[f"v{j}_{n}"] = r[n + 1]
        conn.run(f"INSERT INTO features VALUES {vals}", **params)
    if E("LOOKUP_GRANTEE"):
        conn.run(f'GRANT SELECT ON features TO "{E("LOOKUP_GRANTEE")}"')
    n = conn.run("SELECT count(*) FROM features")[0][0]
    print(json.dumps({"loaded": n, "seconds": round(time.perf_counter() - t, 2)}))


# ------------------------------------------------------------------ probe -------------------------------------
def id_token(aud):
    import urllib.request
    url = ("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience="
           + urllib.parse.quote(aud, safe=""))
    return urllib.request.urlopen(urllib.request.Request(url, headers={"Metadata-Flavor": "Google"})).read().decode()


def pct(xs):
    xs = sorted(xs)
    return {"n": len(xs), "p50": round(statistics.median(xs), 1), "p95": round(xs[int(len(xs) * 0.95) - 1], 1),
            "max": round(xs[-1], 1)} if xs else {"n": 0}


def probe():
    gw = E("GW_URL")
    tok = id_token(gw)
    gwh = {"Authorization": "Bearer " + tok}
    result = {}
    targets = [{"name": "lookup_service_internal", "url": gw + "/healthz"},
               {"name": "cloudsql_private_ip", "tcp": E("DB_HOST") + ":5432"},
               {"name": "psc_google_apis_ip", "tcp": E("PSC_APIS_IP") + ":443"},
               {"name": "google_api_public", "url": "https://storage.googleapis.com/"},
               {"name": "internet", "url": "https://example.com/"}]
    # 1. egress from inside each endpoint's model
    for t in ("std", "psc"):
        try:
            result[f"egress_from_{t}_endpoint"] = predict(t, [{"probe": targets}])["predictions"][0]
        except Exception as e:  # noqa: BLE001
            result[f"egress_from_{t}_endpoint"] = f"{type(e).__name__}: {str(e)[:300]}"
    # the same targets from the caller's own place in the VPC, as a control
    import handler_probe
    result["egress_from_vpc_control"] = [handler_probe.probe(t) for t in targets]
    # 2. latency of each path, 4 entities a call
    ids_all = [f"C{100000 + i}" for i in range(5000)]
    n = int(E("N", "100"))
    lat = {k: [] for k in ("gateway_lookup_only", "direct_std_with_features", "direct_psc_with_features",
                           "gateway_to_std", "gateway_to_psc")}
    errors = {k: 0 for k in lat}
    canary = {}
    feats = requests.post(gw + "/lookup", json={"entity_ids": ids_all[:400]}, headers=gwh, timeout=30).json()["features"]
    for i in range(n):
        ids = random.sample(ids_all[:400], 4)
        for k in lat:
            t0 = time.perf_counter()
            try:
                if k == "gateway_lookup_only":
                    r = requests.post(gw + "/lookup", json={"entity_ids": ids}, headers=gwh, timeout=30)
                    r.raise_for_status()
                elif k.startswith("direct_"):
                    predict(k.split("_")[1], [feats[x] for x in ids])
                else:
                    r = requests.post(gw + "/predict", json={"entity_ids": ids, "target": k.split("_")[-1]},
                                      headers=gwh, timeout=30)
                    r.raise_for_status()
                    if k == "gateway_to_psc":
                        d = r.json()
                        tag = next(iter(d["predictions"].values())).get("model")
                        canary[f"{d['deployedModelId']}/{tag}"] = canary.get(f"{d['deployedModelId']}/{tag}", 0) + 1
                        result["caller_seen_by_gateway"] = d.get("caller")
                lat[k].append((time.perf_counter() - t0) * 1000)
            except Exception as e:  # noqa: BLE001
                errors[k] += 1
                result.setdefault("first_errors", {}).setdefault(k, f"{type(e).__name__}: {str(e)[:200]}")
    result["latency_ms"] = {k: pct(v) for k, v in lat.items()}
    result["errors"] = errors
    result["canary_gateway_to_psc"] = canary
    print(json.dumps(result, indent=1))


def loadtest():
    """Steady calls through the gateway to the PSC endpoint for SECONDS; per-10-second ok/error counts."""
    gw = E("GW_URL")
    gwh = {"Authorization": "Bearer " + id_token(gw)}
    end = time.time() + int(E("SECONDS", "900"))
    buckets = {}
    t0 = time.time()
    while time.time() < end:
        b = int((time.time() - t0) // 10) * 10
        ok = buckets.setdefault(b, {"ok": 0, "err": 0, "codes": {}})
        try:
            r = requests.post(gw + "/predict", json={"entity_ids": ["C100001", "C100002"], "target": "psc"},
                              headers=gwh, timeout=30)
            if r.status_code == 200:
                ok["ok"] += 1
            else:
                ok["err"] += 1
                d = r.json().get("detail", "")[:40]
                ok["codes"][d] = ok["codes"].get(d, 0) + 1
        except Exception as e:  # noqa: BLE001
            ok["err"] += 1
            ok["codes"][type(e).__name__] = ok["codes"].get(type(e).__name__, 0) + 1
        time.sleep(0.1)
        if b not in buckets or True:
            pass
    for b, v in sorted(buckets.items()):
        print(json.dumps({"t": b, "start": time.strftime("%H:%M:%S", time.gmtime(t0 + b)), **v}), flush=True)


if __name__ == "__main__":
    {"serve": serve, "load": load, "probe": probe, "loadtest": loadtest}[sys.argv[1]]()
