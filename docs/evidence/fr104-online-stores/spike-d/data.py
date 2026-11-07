import sys, time
sys.path.insert(0, sys.argv[1])
from g import P, R, bq, call  # noqa: E402

B = "pw-ebook-spike-fs104d"
call("POST", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/datasets",
     {"datasetReference": {"projectId": P, "datasetId": "fs104d"}, "location": R,
      "defaultTableExpirationMs": "86400000", "labels": {"spike": "fr104d"}}, ok=(200, 409), quiet=True)
cols = ", ".join(f"ROUND(RAND()*100, 4) AS f{k:02d}" for k in range(30))
for g in (1, 2):
    bq(f"SELECT CONCAT('e', CAST(i AS STRING)) AS entity_id, "
       f"TIMESTAMP_ADD(TIMESTAMP '2026-11-07 00:00:00+00', INTERVAL {g} HOUR) AS feature_timestamp, {cols} "
       f"FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS i", dest=("fs104d", f"features_g{g}"))
    s, j = call("POST", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs", {
        "configuration": {"extract": {"sourceTable": {"projectId": P, "datasetId": "fs104d", "tableId": f"features_g{g}"},
                                      "destinationUris": [f"gs://{B}/export/g{g}/part-*.parquet"],
                                      "destinationFormat": "PARQUET"}},
        "jobReference": {"projectId": P, "location": R}})
    jid = j["jobReference"]["jobId"]
    while j.get("status", {}).get("state") != "DONE":
        time.sleep(3)
        s, j = call("GET", f"https://bigquery.googleapis.com/bigquery/v2/projects/{P}/jobs/{jid}?location={R}")
    print(g, j["status"].get("errorResult"), j["statistics"].get("extract", {}).get("destinationUriFileCounts"))
