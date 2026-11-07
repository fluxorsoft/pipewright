"""Tiny REST helper for the FR-104 feature store spike (run from the laptop, as the admin)."""
import json, os, subprocess, time, urllib.error, urllib.request

P, R = os.environ["PROJECT"], os.environ.get("REGION", "europe-west2")
CONF = f" --configuration={os.environ['GCLOUD_CONFIG']}" if os.environ.get("GCLOUD_CONFIG") else ""
_t = [None, 0]


def tok():
    if not _t[0] or time.time() - _t[1] > 1500:
        _t[0] = subprocess.run("gcloud auth print-access-token" + CONF, shell=True,
                               capture_output=True, text=True).stdout.strip().splitlines()[-1]
        _t[1] = time.time()
    return _t[0]


def call(method, url, body=None, ok=(200,), quiet=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, method=method, data=data, headers={
        "Authorization": "Bearer " + tok(), "Content-Type": "application/json", "x-goog-user-project": P})
    try:
        r = urllib.request.urlopen(req, timeout=300)
        t = r.read()
        return r.status, (json.loads(t) if t else {})
    except urllib.error.HTTPError as e:
        t = e.read()
        try:
            b = json.loads(t)
        except Exception:  # noqa: BLE001
            b = {"raw": t[:500].decode(errors="replace")}
        if not quiet:
            print(f"HTTP {e.code} {method} {url}: {json.dumps(b)[:600]}", flush=True)
        return e.code, b


def bq(sql, dest=None, cluster=None, params=None):
    """Run a query job and wait; returns the job resource."""
    cfg = {"query": {"query": sql, "useLegacySql": False}}
    if dest:
        cfg["query"].update(destinationTable={"projectId": P, "datasetId": dest[0], "tableId": dest[1]},
                            writeDisposition="WRITE_TRUNCATE", createDisposition="CREATE_IF_NEEDED")
        if cluster:
            cfg["query"]["clustering"] = {"fields": cluster}
    s, j = call("POST", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs",
                {"configuration": cfg, "jobReference": {"projectId": P, "location": R}})
    jid = j["jobReference"]["jobId"]
    while j.get("status", {}).get("state") != "DONE":
        time.sleep(3)
        s, j = call("GET", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs/{jid}?location={R}")
    if j["status"].get("errorResult"):
        raise SystemExit(f"query failed: {j['status']['errorResult']}")
    return j


def wait_op(url_base, op):
    while not op.get("done"):
        time.sleep(15)
        s, op = call("GET", f"{url_base}/{op['name']}")
    if op.get("error"):
        raise SystemExit(f"operation failed: {op['error']}")
    return op.get("response", {})
