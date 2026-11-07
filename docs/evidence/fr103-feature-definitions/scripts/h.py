"""Spike helpers (FR-103): BigQuery, Vertex AI and Logging through REST in pw-ebook, europe-west2."""
import json, subprocess, time, urllib.error, urllib.request
P, R, DS = "pw-ebook", "europe-west2", "fr103_spike"
BQ = f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}"
AI = f"https://{R}-aiplatform.googleapis.com/v1/projects/{P}/locations/{R}"
_t = {"v": None, "at": 0}

def tok():
    if time.time() - _t["at"] > 1500:
        _t["v"] = subprocess.run("gcloud auth print-access-token --configuration=vertexai-poc", shell=True,
                                 capture_output=True, text=True).stdout.strip().splitlines()[-1]
        _t["at"] = time.time()
    return _t["v"]

def call(m, url, body=None):
    r = urllib.request.Request(url, method=m, data=json.dumps(body).encode() if body is not None else None,
                               headers={"Authorization": "Bearer " + tok(), "Content-Type": "application/json"})
    try:
        raw = urllib.request.urlopen(r, timeout=300).read()
        return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return {"error": e.code, "message": e.read()[:600].decode(errors="replace")}

def q(sql, **extra):
    """Runs a query to completion; returns (rows, job stats)."""
    j = call("POST", f"{BQ}/jobs", {"configuration": {"query": {"query": sql, "useLegacySql": False, **extra}},
                                    "jobReference": {"location": R}})
    if "error" in j:
        return j, {}
    jid = j["jobReference"]["jobId"]
    while j.get("status", {}).get("state") != "DONE":
        time.sleep(1.5)
        j = call("GET", f"{BQ}/jobs/{jid}?location={R}")
    if j["status"].get("errorResult"):
        return {"error": j["status"]["errorResult"]}, j.get("statistics", {})
    res = call("GET", f"{BQ}/queries/{jid}?location={R}&maxResults=50")
    names = [f["name"] for f in (res.get("schema") or {}).get("fields", [])]
    rows = [dict(zip(names, [c["v"] for c in row["f"]])) for row in res.get("rows", [])]
    return rows, j.get("statistics", {})

def table(name):
    return call("GET", f"{BQ}/datasets/{DS}/tables/{name}")

def op(o):
    for _ in range(120):
        if "error" in o or not o.get("name") or o.get("done"):
            return o
        time.sleep(5)
        o = call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{o['name']}")
    return o
