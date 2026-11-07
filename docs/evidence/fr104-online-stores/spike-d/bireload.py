"""BI Engine: what is billed per lookup while BI Engine reloads a table that was just refreshed."""
import json, os, random, sys, threading, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fsspike"))
from g import P, R, bq, call  # noqa: E402

RES = f"https://bigqueryreservation.googleapis.com/v1/projects/{P}/locations/{R}/biReservation"
T = "features_g1"


def lookup():
    eid = f"e{random.randint(1, 1000000)}"
    s, b = call("POST", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/queries", {
        "query": f"SELECT * FROM `{P}.fs104d.{T}` WHERE entity_id = @id", "useLegacySql": False,
        "parameterMode": "NAMED", "queryParameters": [{"name": "id", "parameterType": {"type": "STRING"},
                                                       "parameterValue": {"value": eid}}],
        "useQueryCache": False, "location": R, "timeoutMs": 20000, "jobCreationMode": "JOB_CREATION_REQUIRED"})
    jid = b.get("jobReference", {}).get("jobId")
    s, j = call("GET", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs/{jid}?location={R}")
    q = j.get("statistics", {}).get("query", {})
    return {"t": round(time.time() - T0, 1), "billed": int(q.get("totalBytesBilled", 0)),
            "mode": (q.get("biEngineStatistics") or {}).get("biEngineMode"),
            "reasons": [r.get("code") for r in (q.get("biEngineStatistics") or {}).get("biEngineReasons", [])]}


T0 = time.time()
out = {"steps": []}
s, b = call("PATCH", RES + "?updateMask=size,preferredTables", {
    "size": str(1 << 30), "preferredTables": [{"projectId": P, "datasetId": "fs104d", "tableId": T}]})
out["reservation"] = {"status": s, "size": b.get("size")}
print("reservation", s, b.get("size"), flush=True)
time.sleep(150)
out["warm"] = [lookup() for _ in range(8)]
print("warm", [(x["mode"], x["billed"]) for x in out["warm"]], flush=True)
# refresh the table in place, as a daily feature job would
cols = ", ".join(f"ROUND(RAND()*100, 4) AS f{k:02d}" for k in range(30))
t_ref = time.time()
bq(f"SELECT CONCAT('e', CAST(i AS STRING)) AS entity_id, TIMESTAMP '2026-11-07 05:00:00+00' AS feature_timestamp, "
   f"{cols} FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS i", dest=("fs104d", T))
out["refresh_done_at"] = round(time.time() - T0, 1)
print("refreshed at", out["refresh_done_at"], "took", round(time.time() - t_ref, 1), flush=True)
end = time.time() + 240
while time.time() < end:
    x = lookup()
    out["steps"].append(x)
    print(x, flush=True)
    time.sleep(4)
os.makedirs("results", exist_ok=True)
json.dump(out, open("results/s12_bi_reload.json", "w"), indent=1)
after = out["steps"]
billed = [x for x in after if x["billed"]]
print("after refresh: lookups", len(after), "billed", len(billed), "bytes total", sum(x["billed"] for x in billed),
      "last billed at", billed[-1]["t"] if billed else None)
