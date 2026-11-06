"""Phase 7 permission spike: which Cloud Storage permissions does retention renewal need, and what else do they allow?

Three identities, each with one custom role on the bucket, conditioned to objects under models/:
  get     storage.objects.get
  ret     storage.objects.get + storage.objects.setRetention
  retupd  storage.objects.get + storage.objects.setRetention + storage.objects.update
Every test re-reads the affected generation(s) as the admin afterwards (generation, metageneration, retention, hold):
the result is what the object says, not what the API answered. Writes results.json.

Run setup.sh first (it prints the variables to export), then this script, then teardown.sh. Pipewright phase 7
(FR-101), run live 2026-11-06; the results of that run are in results-2026-11-06.json.
"""
import datetime as dt, json, os, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from pathlib import Path

HERE = Path(__file__).parent
SUF, B, P = os.environ["SPIKE_SUFFIX"], os.environ["SPIKE_BUCKET"], os.environ["PROJECT"]
CONF = f" --configuration={os.environ['GCLOUD_CONFIG']}" if os.environ.get("GCLOUD_CONFIG") else ""
API, UP = f"https://storage.googleapis.com/storage/v1/b/{B}/o", f"https://storage.googleapis.com/upload/storage/v1/b/{B}/o"
OUT = HERE / os.environ.get("RESULTS", "results.json")
results = {"bucket": B, "started": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "tests": []}
_tok = {}


def token(who):
    if who not in _tok or time.time() - _tok[who][1] > 1800:
        cmd = "gcloud auth print-access-token" + CONF + (
            "" if who == "admin" else f" --impersonate-service-account=sp-lp-{who}-{SUF}@{P}.iam.gserviceaccount.com")
        t = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]
        _tok[who] = (t, time.time())
    return _tok[who][0]


def call(who, method, url, body=None, raw=None, ctype="application/json"):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(url, method=method, data=data,
                                 headers={"Authorization": "Bearer " + token(who), "Content-Type": ctype})
    try:
        r = urllib.request.urlopen(req, timeout=60)
        txt = r.read()
        return r.status, (json.loads(txt) if txt else {})
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read()).get("error", {}).get("message", "")
        except Exception:  # noqa: BLE001
            msg = ""
        return e.code, {"error": msg[:200]}


def q(name):
    return urllib.parse.quote(name, safe="")


def at(minutes):
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


def upload(name, text):
    s, b = call("admin", "POST", f"{UP}?uploadType=media&name={q(name)}", raw=text.encode(), ctype="text/plain")
    assert s == 200, (name, s, b)
    return int(b["generation"])


def meta(name, gen=None):
    s, b = call("admin", "GET", f"{API}/{q(name)}" + (f"?generation={gen}" if gen else ""))
    if s != 200:
        return {"status": s}
    return {"generation": b.get("generation"), "metageneration": b.get("metageneration"),
            "retention": b.get("retention"), "temporaryHold": b.get("temporaryHold", False)}


def patch(who, name, gen, body, **params):
    params = {"generation": str(gen), **{k: str(v) for k, v in params.items()}} if gen else params
    return call(who, "PATCH", f"{API}/{q(name)}?" + urllib.parse.urlencode(params), body)


def record(num, title, who, status, answer, after, verdict):
    row = {"test": num, "title": title, "as": who, "http": status, "answer": answer, "after": after, "result": verdict}
    results["tests"].append(row)
    OUT.write_text(json.dumps(results, indent=2))
    print(f"[{num}] {title} | as {who} | HTTP {status} | {verdict}", flush=True)


def retention_until(m):
    return ((m or {}).get("retention") or {}).get("retainUntilTime")


# ---------------------------------------------------------------- setup (as the admin) --
s, bk = call("admin", "GET", f"https://storage.googleapis.com/storage/v1/b/{B}?fields=versioning,objectRetention")
results["bucket_config"] = bk
print("bucket", bk, flush=True)
objs = {}
for n in ("u", "l", "h", "n", "w", "p", "k"):
    objs[n] = f"models/{n}/model.bin"
    upload(objs[n], f"model {n} generation 1")
objs["o"] = "outside/x.bin"
upload(objs["o"], "outside the model folder")
for n in ("u", "n", "p", "k", "o"):
    s, b = patch("admin", objs[n], None, {"retention": {"mode": "Unlocked", "retainUntilTime": at(60)}})
    assert s == 200, (n, s, b)
s, b = patch("admin", objs["l"], None, {"retention": {"mode": "Locked", "retainUntilTime": at(60)}})
assert s == 200, ("l", s, b)
s, b = patch("admin", objs["h"], None, {"temporaryHold": True})
assert s == 200, ("h", s, b)
gen1_n = int(meta(objs["n"])["generation"])
gen2_n = upload(objs["n"], "model n generation 2 (a newer live version put over the approved one)")
gens = {n: int(meta(objs[n])["generation"]) for n in objs}
gens["n"] = gen1_n
results["setup"] = {n: meta(objs[n], gens[n]) for n in objs}
results["setup"]["n_gen2"] = meta(objs["n"], gen2_n)

# wait for the conditional bindings to take effect (IAM propagation)
t0 = time.time()
while True:
    s, b = call("get", "GET", f"{API}/{q(objs['w'])}")
    if s == 200:
        break
    if time.time() - t0 > 600:
        sys.exit(f"IAM did not propagate in 10 minutes: {s} {b}")
    time.sleep(15)
results["iam_propagation_seconds"] = round(time.time() - t0)
time.sleep(30)                                                      # all three identities, not just the first

# ---------------------------------------------------------------- tests --
# 1 storage.objects.get alone reads metadata; and nothing outside models/
s, b = call("get", "GET", f"{API}/{q(objs['u'])}")
record(1, "get alone: read metadata (inside models/)", "get", s, b.get("error") or retention_until(b), None,
       "PASS: metadata readable" if s == 200 else "FAIL")
s2, b2 = call("get", "GET", f"{API}/{q(objs['o'])}")
record("1b", "get alone: read metadata OUTSIDE models/", "get", s2, b2.get("error", "readable"), None,
       "PASS: refused outside the condition" if s2 == 403 else "FINDING: readable outside")
s3, b3 = patch("get", objs["u"], gens["u"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}})
after = meta(objs["u"], gens["u"])
record("1c", "get alone: try to extend retention (control)", "get", s3, b3.get("error") or "", after,
       "PASS: refused" if s3 == 403 and retention_until(after) == retention_until(results["setup"]["u"]) else
       "FINDING")

# 2 setRetention alone (no update) extends an Unlocked retention
before = meta(objs["u"], gens["u"])
s, b = patch("ret", objs["u"], gens["u"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}})
after = meta(objs["u"], gens["u"])
ok2 = s == 200 and retention_until(after) > retention_until(before)
record(2, "get+setRetention (no update): extend Unlocked retention", "ret", s, b.get("error") or "", after,
       "PASS: extended without storage.objects.update" if ok2 else "update needed (or other): see answer")

# 3 setRetention + update extends
before = meta(objs["u"], gens["u"])
s, b = patch("retupd", objs["u"], gens["u"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(95)}})
after = meta(objs["u"], gens["u"])
record(3, "get+setRetention+update: extend Unlocked retention", "retupd", s, b.get("error") or "", after,
       "PASS: extended" if s == 200 and retention_until(after) > retention_until(before) else "FAIL")

renewer = "ret" if ok2 else "retupd"
results["renewer_role"] = renewer

# 4 Locked-mode extension
before = meta(objs["l"], gens["l"])
s, b = patch(renewer, objs["l"], gens["l"], {"retention": {"mode": "Locked", "retainUntilTime": at(90)}})
after = meta(objs["l"], gens["l"])
record(4, "extend a Locked retention to a later date", renewer, s, b.get("error") or "", after,
       "PASS: extended" if s == 200 and retention_until(after) > retention_until(before) else "FAIL")

# 5 metageneration precondition: a stale value is refused, a fresh one works
m = meta(objs["p"], gens["p"])
stale = int(m["metageneration"]) - 1 if int(m["metageneration"]) > 1 else int(m["metageneration"]) + 5
s, b = patch(renewer, objs["p"], gens["p"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}},
             ifMetagenerationMatch=stale)
after = meta(objs["p"], gens["p"])
record(5, "stale ifMetagenerationMatch is refused", renewer, s, b.get("error") or "", after,
       "PASS: 412 and unchanged" if s == 412 and retention_until(after) == retention_until(m) else "FAIL")
s, b = patch(renewer, objs["p"], gens["p"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}},
             ifMetagenerationMatch=m["metageneration"])
after = meta(objs["p"], gens["p"])
record("5b", "fresh ifMetagenerationMatch extends", renewer, s, b.get("error") or "", after,
       "PASS: extended" if s == 200 and retention_until(after) > retention_until(m) else "FAIL")

# 6 temporary hold release, by each role
for who in ("ret", "retupd"):
    s, b = patch(who, objs["h"], gens["h"], {"temporaryHold": False})
    after = meta(objs["h"], gens["h"])
    released = after.get("temporaryHold") is False
    record(f"6-{who}", "release a temporary hold", who, s, b.get("error") or "", after,
           "PASS: cannot release" if not released else "FINDING: hold released")
    if released:
        patch("admin", objs["h"], gens["h"], {"temporaryHold": True})          # put it back for the next role

# 7 shorten retention, plain and with overrideUnlockedRetention
for who in ("ret", "retupd"):
    before = meta(objs["u"], gens["u"])
    s, b = patch(who, objs["u"], gens["u"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(30)}})
    after = meta(objs["u"], gens["u"])
    record(f"7-{who}", "shorten Unlocked retention", who, s, b.get("error") or "", after,
           "PASS: cannot shorten" if retention_until(after) == retention_until(before) else "FINDING: shortened")
    s, b = patch(who, objs["u"], gens["u"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(30)}},
                 overrideUnlockedRetention="true")
    after2 = meta(objs["u"], gens["u"])
    record(f"7-{who}-override", "shorten with overrideUnlockedRetention=true", who, s, b.get("error") or "",
           after2, "PASS: cannot shorten" if retention_until(after2) == retention_until(before) else
           "FINDING: shortened")

# 7c remove retention entirely; 7d Unlocked -> Locked (a commitment nobody can undo)
for who in ("ret", "retupd"):
    before = meta(objs["u"], gens["u"])
    s, b = patch(who, objs["u"], gens["u"], {"retention": None}, overrideUnlockedRetention="true")
    after = meta(objs["u"], gens["u"])
    record(f"7c-{who}", "remove retention", who, s, b.get("error") or "", after,
           "PASS: cannot remove" if retention_until(after) == retention_until(before) else "FINDING: removed")
before = meta(objs["k"], gens["k"])
s, b = patch(renewer, objs["k"], gens["k"], {"retention": {"mode": "Locked",
                                                           "retainUntilTime": retention_until(before)}})
after = meta(objs["k"], gens["k"])
record("7d", "change Unlocked to Locked (same date)", renewer, s, b.get("error") or "", after,
       "FINDING: can turn Unlocked into Locked" if (after.get("retention") or {}).get("mode") == "Locked" else
       "PASS: cannot change the mode")

# 8 overwrite and delete (an object with NO retention, so only IAM can refuse)
for who in ("ret", "retupd"):
    before = meta(objs["w"])
    tok = token(who)
    s, b = call(who, "POST", f"{UP}?uploadType=media&name={q(objs['w'])}", raw=b"replacement", ctype="text/plain")
    after = meta(objs["w"])
    record(f"8-{who}-overwrite", "upload a replacement (no retention on the object)", who, s,
           b.get("error") or "", after,
           "PASS: cannot overwrite" if after.get("generation") == before.get("generation") else "FINDING: overwritten")
    s, b = call(who, "DELETE", f"{API}/{q(objs['w'])}?generation={before['generation']}")
    after = meta(objs["w"], before["generation"])
    record(f"8-{who}-delete", "delete the generation (no retention)", who, s, b.get("error") or "", after,
           "PASS: cannot delete" if after.get("generation") else "FINDING: deleted")
    s, b = patch(who, objs["w"], before["generation"], {"contentType": "application/x-changed",
                                                        "metadata": {"tampered": "yes"}})
    after = meta(objs["w"], before["generation"])
    s2, full = call("admin", "GET", f"{API}/{q(objs['w'])}?generation={before['generation']}")
    changed = full.get("contentType") == "application/x-changed" or (full.get("metadata") or {}).get("tampered")
    record(f"8-{who}-metadata", "change content type and custom metadata", who, s, b.get("error") or "",
           {"contentType": full.get("contentType"), "metadata": full.get("metadata")},
           "FINDING: can change metadata" if changed else "PASS: cannot change metadata")

# 9 same name, newer generation: patch the approved generation; the live one must be untouched, and vice versa
b1, b2 = meta(objs["n"], gen1_n), meta(objs["n"], gen2_n)
s, b = patch(renewer, objs["n"], gen1_n, {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}})
a1, a2 = meta(objs["n"], gen1_n), meta(objs["n"], gen2_n)
record(9, "extend the approved generation while a newer one is live", renewer, s, b.get("error") or "",
       {"approved_gen": a1, "live_gen": a2},
       "PASS: only the approved generation changed" if (retention_until(a1) > retention_until(b1)
                                                         and a2 == b2) else "FAIL")
s, b = patch(renewer, objs["n"], None, {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}})
c1, c2 = meta(objs["n"], gen1_n), meta(objs["n"], gen2_n)
record("9b", "the same call WITHOUT ?generation=", renewer, s, b.get("error") or "",
       {"approved_gen": c1, "live_gen": c2},
       "NOTE: without a generation it applies to the live version" if c2 != a2 else "unchanged")

# 10 outside the model folder
before = meta(objs["o"], gens["o"])
s, b = patch(renewer, objs["o"], gens["o"], {"retention": {"mode": "Unlocked", "retainUntilTime": at(90)}})
after = meta(objs["o"], gens["o"])
record(10, "extend retention OUTSIDE models/", renewer, s, b.get("error") or "", after,
       "PASS: refused" if retention_until(after) == retention_until(before) else "FINDING: allowed outside")

results["finished"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
OUT.write_text(json.dumps(results, indent=2))
print("DONE", flush=True)
