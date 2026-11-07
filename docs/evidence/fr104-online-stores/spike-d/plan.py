"""python plan.py <name> '<json plan>'  : upload a plan, wait for its result, print a summary, keep the raw file."""
import json, os, sys, time, urllib.parse, urllib.request
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fsspike"))
from g import call, tok  # noqa: E402

B = "pw-ebook-spike-fs104d"
name, plan = sys.argv[1], json.loads(sys.argv[2])
call("POST", f"https://storage.googleapis.com/upload/storage/v1/b/{B}/o?uploadType=media&name=plan/{name}.json", plan)
obj = urllib.parse.quote(f"results/{name}.json", safe="")
for _ in range(int(os.environ.get("WAIT", "200"))):
    s, b = call("GET", f"https://storage.googleapis.com/storage/v1/b/{B}/o/{obj}", quiet=True)
    if s == 200:
        break
    time.sleep(6)
else:
    raise SystemExit("no result yet")
d = json.loads(urllib.request.urlopen(urllib.request.Request(
    f"https://storage.googleapis.com/storage/v1/b/{B}/o/{obj}?alt=media", headers={"Authorization": "Bearer " + tok()})).read())
os.makedirs("results", exist_ok=True)
json.dump(d, open(f"results/{name}.json", "w"), indent=1)
r = d["result"]
print("==", name, d["seconds"], "s")
if "runs" in r:
    for x in r["runs"]:
        print(f"  {x['rate']:>4}/s: ok {x['ok']}/{x['sent']} achieved {x['achieved_rps']} late {x['sent_late']} "
              f"p50 {x['p50_ms']} p95 {x['p95_ms']} p99 {x['p99_ms']} max {x['max_ms']} errs {json.dumps(x['errors'])[:300]}")
elif "per_second" in r:
    o = r["overall"]
    print(f"  overall ok {o['ok']}/{o['sent']} p50 {o['p50_ms']} p99 {o['p99_ms']} max {o['max_ms']} errs "
          f"{json.dumps(o['errors'])[:300]}")
    print("  marks", json.dumps(r["marks"]), "sync_error", r.get("sync_error"))
    sw = r["marks"].get("swap_start")
    for p in r["per_second"]:
        if p["errors"] or (sw is not None and abs(p["s"] - sw) <= 3) or p["s"] % 20 == 0:
            print("   ", json.dumps(p))
else:
    print(json.dumps(r, indent=1)[:3000])
