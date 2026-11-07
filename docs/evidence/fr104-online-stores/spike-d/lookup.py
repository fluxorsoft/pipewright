import json, os, queue, ssl, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import pg8000.native

HOST, SCHEMA, POOL_N = os.environ["PGHOST"], os.environ["SCHEMA"], int(os.environ.get("POOL", "8"))
MD = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default"
_t, _l = {}, threading.Lock()


def md(p):
    return urllib.request.urlopen(urllib.request.Request(MD + p, headers={"Metadata-Flavor": "Google"})).read()


USER = md("/email").decode().replace(".gserviceaccount.com", "")


def token():
    with _l:
        if "t" not in _t or time.time() - _t["at"] > 1500:
            _t.update(t=json.loads(md("/token"))["access_token"], at=time.time())
        return _t["t"]


CTX = ssl.create_default_context()
CTX.check_hostname, CTX.verify_mode = False, ssl.CERT_NONE
POOL = queue.Queue()
for _ in range(POOL_N):
    POOL.put(None)


def lookup(ids):
    c = POOL.get()
    try:
        if c is None:
            c = pg8000.native.Connection(USER, host=HOST, password=token(), database="features", ssl_context=CTX,
                                         timeout=10)
        rows = c.run(f"SELECT * FROM {SCHEMA}.features WHERE entity_id = ANY(:ids)", ids=ids)
        cols = [x["name"] for x in c.columns]
    except Exception:
        c = None
        raise
    finally:
        POOL.put(c)
    return {r[0]: {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(cols[1:], r[1:])} for r in rows}


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        try:
            out, code = {"features": lookup(body["entity_ids"])}, 200
        except Exception as e:  # noqa: BLE001
            out, code = {"error": type(e).__name__ + ": " + str(e)[:160]}, 500
        data = json.dumps(out).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass


ThreadingHTTPServer(("", int(os.environ.get("PORT", 8080))), H).serve_forever()
