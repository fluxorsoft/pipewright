"""FR-92/105/106 live test: a caller inside the VPC (a Cloud Run job, as pw9b-caller), calling Pipewright's gateway.

  MODE=probe     N calls to each gateway URL (GATEWAYS, comma separated), 4 entities a call: latency, errors, the
                 deployed models answering (the canary split), not_found handling
  MODE=load      steady calls to GATEWAY for SECONDS; per 10 seconds: ok, errors, retried (from the answer)
  MODE=direct    a call straight to the private endpoint (ENDPOINT_URL) and to the public API, to show which answers

Names, counts, timings only; never a feature value or an entity ID in the output.
"""
import json
import os
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request

E = os.environ.get
MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
IDS = [f"C{100000 + i}" for i in range(int(E("IDS", "400")))]


def md(path):
    req = urllib.request.Request(MD + path, headers={"Metadata-Flavor": "Google"})
    return urllib.request.urlopen(req, timeout=10).read().decode()


_tok = {}


def id_token(aud):
    hit = _tok.get(aud)
    if not hit or time.time() - hit[1] > 1500:
        hit = (md("/identity?audience=" + urllib.parse.quote(aud, safe="")), time.time())
        _tok[aud] = hit
    return hit[0]


def post(url, body, token, timeout=30, ctx=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, **({"context": ctx} if ctx else {})) as r:
        return json.loads(r.read() or b"{}")


def pct(xs):
    xs = sorted(xs)
    return {"n": len(xs), "p50": round(statistics.median(xs), 1), "p95": round(xs[max(0, int(len(xs) * .95) - 1)], 1),
            "max": round(xs[-1], 1)} if xs else {"n": 0}


def probe():
    out = {}
    for gw in [g for g in E("GATEWAYS", "").split(",") if g]:
        tok, lat, errors, split, first_error, missing = id_token(gw), [], 0, {}, None, 0
        for _ in range(int(E("N", "100"))):
            ids = random.sample(IDS, 4) + (["NO-SUCH-ID"] if random.random() < 0.1 else [])
            t = time.perf_counter()
            try:
                r = post(gw + "/predict", {"entity_ids": ids}, tok)
                lat.append((time.perf_counter() - t) * 1000)
                key = f"{r.get('deployed_model')}/v{r.get('model_version')}"
                split[key] = split.get(key, 0) + 1
                missing += len(r.get("not_found") or [])
            except Exception as e:  # noqa: BLE001
                errors += 1
                first_error = first_error or f"{type(e).__name__}: {str(e)[:200]}"
        out[gw.split("//")[1].split(".")[0]] = {
            "latency_ms": pct(lat), "errors": errors, "first_error": first_error, "split": split,
            "not_found_reported": missing}
    print(json.dumps(out, indent=1))


def load():
    gw, buckets, t0 = E("GATEWAY"), {}, time.time()
    tok = id_token(gw)
    end = t0 + int(E("SECONDS", "900"))
    while time.time() < end:
        b = int((time.time() - t0) // 10) * 10
        s = buckets.setdefault(b, {"ok": 0, "err": 0, "kinds": {}})
        try:
            post(gw + "/predict", {"entity_ids": random.sample(IDS, 2)}, tok)
            s["ok"] += 1
        except urllib.error.HTTPError as e:
            s["err"] += 1
            s["kinds"][str(e.code)] = s["kinds"].get(str(e.code), 0) + 1
        except Exception as e:  # noqa: BLE001
            s["err"] += 1
            s["kinds"][type(e).__name__] = s["kinds"].get(type(e).__name__, 0) + 1
        time.sleep(float(E("PAUSE", "0.1")))
    for b, v in sorted(buckets.items()):
        print(json.dumps({"t": b, "at": time.strftime("%H:%M:%S", time.gmtime(t0 + b)), **v}))
    print(json.dumps({"total_ok": sum(v["ok"] for v in buckets.values()),
                      "total_err": sum(v["err"] for v in buckets.values())}))


def direct():
    import ssl

    tok = json.loads(md("/token"))["access_token"]
    out = {}
    for name, url in (("private_endpoint", E("ENDPOINT_URL")), ("public_api", E("PUBLIC_URL"))):
        if not url:
            continue
        try:
            ctx = None
            if "prediction.p.vertexai.goog" in url:
                host = urllib.parse.urlparse(url).hostname
                ctx = ssl.create_default_context(cadata=ssl.get_server_certificate((host, 443), timeout=10))
                ctx.check_hostname = False
            r = post(url, {"instances": [[0.0] * 8]}, tok, ctx=ctx)
            out[name] = {"answered": True, "predictions": len(r.get("predictions") or [])}
        except urllib.error.HTTPError as e:
            out[name] = {"answered": False, "status": e.code, "body": e.read()[:600].decode(errors="replace")}
        except Exception as e:  # noqa: BLE001
            out[name] = {"answered": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}
    print(json.dumps(out, indent=1))


{"probe": probe, "load": load, "direct": direct}[E("MODE", "probe")]()
