import socket
import time
import urllib.error
import urllib.request


def probe(t):
    start = time.perf_counter()
    try:
        if "tcp" in t:
            host, port = t["tcp"].rsplit(":", 1)
            socket.create_connection((host, int(port)), timeout=5).close()
            out = "connected"
        else:
            req = urllib.request.Request(t["url"], method="GET")
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    out = f"http {r.status}"
            except urllib.error.HTTPError as e:
                out = f"http {e.code}"
    except Exception as e:  # noqa: BLE001 - the spike records any failure
        out = f"{type(e).__name__}: {str(e)[:120]}"
    return {"name": t["name"], "outcome": out, "ms": round((time.perf_counter() - start) * 1000, 1)}


