import json, os, queue, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pg8000.native
P = "pw-ebook"; SOCK = "/cloudsql/pw-ebook:europe-west2:fs104-pg/.s.PGSQL.5432"
MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
local, tok = threading.local(), {}
def token():
    if "t" not in tok or time.time() - tok["at"] > 1500:
        r = urllib.request.urlopen(urllib.request.Request(MD, headers={"Metadata-Flavor": "Google"}))
        tok.update(t=json.loads(r.read())["access_token"], at=time.time())
    return tok["t"]
POOL = queue.Queue()
for _ in range(int(os.environ.get("POOL", "8"))):
    POOL.put(None)
def pg(ids):
    c = POOL.get()                     # a bounded pool: at most POOL connections per instance
    try:
        if c is None:
            c = pg8000.native.Connection("postgres", unix_sock=SOCK, password=os.environ["PGPASS"], database="features")
        rows = c.run("SELECT * FROM features WHERE entity_id = ANY(:ids)", ids=ids)
        cols = [x["name"] for x in c.columns]
    except Exception:
        c = None
        raise
    finally:
        POOL.put(c)
    return {r[0]: {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(cols[1:], r[1:])} for r in rows}
def fs(ids):
    out = {}
    for i in ids:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://firestore.googleapis.com/v1/projects/{P}/databases/fs104-spike/documents/features/{i}",
            headers={"Authorization": "Bearer " + token()}), timeout=10)
        f = json.loads(r.read())["fields"]
        out[i] = {k: list(v.values())[0] for k, v in f.items()}
    return out
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        try:
            feats = (pg if body["store"] == "pg" else fs)(body["entity_ids"])
            out, code = {"features": feats}, 200
        except Exception as e:
            out, code = {"error": str(e)[:200]}, 500
        data = json.dumps(out).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def log_message(self, *a): pass
ThreadingHTTPServer(("", int(os.environ.get("PORT", 8080))), H).serve_forever()
