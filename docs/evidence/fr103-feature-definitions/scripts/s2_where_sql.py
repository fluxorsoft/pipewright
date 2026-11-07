"""Q1b: where the SQL of a table built by a query lives: the job that wrote it (INFORMATION_SCHEMA.JOBS), and a
scheduled query (Data Transfer config)."""
from h import *
rows, st = q(f"""SELECT job_id, creation_time, statement_type, LEFT(query, 120) AS q, total_bytes_billed
FROM `region-{R}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE destination_table.dataset_id = '{DS}' AND destination_table.table_id = 'f_spend_t'
ORDER BY creation_time DESC LIMIT 3""")
print("jobs that wrote f_spend_t:", json.dumps(rows, indent=1)[:900])
print("  INFORMATION_SCHEMA.JOBS query billed bytes:", st.get("query", {}).get("totalBytesBilled"))
# A scheduled query that builds f_spend_s (paused: we only read its config)
FEAT = f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d,
  TIMESTAMP '2026-11-07 00:00:00' AS feature_timestamp FROM `{P}.{DS}.transactions` GROUP BY customer_id"""
DT = f"https://bigquerydatatransfer.googleapis.com/v1/projects/{P}/locations/{R}/transferConfigs"
c = call("POST", DT, {"displayName": "fr103-spike-sq", "dataSourceId": "scheduled_query", "destinationDatasetId": DS,
                      "schedule": "every 24 hours", "disabled": True,
                      "params": {"query": FEAT, "destination_table_name_template": "f_spend_s",
                                 "write_disposition": "WRITE_TRUNCATE"}})
print("\nscheduled query:", c.get("error"), c.get("message", "")[:300] if "error" in c else c.get("name"))
if "name" in c:
    g = call("GET", f"https://bigquerydatatransfer.googleapis.com/v1/{c['name']}")
    print("  config params keys:", list(g.get("params", {}).keys()), "| query readable:", FEAT[:40] in g["params"].get("query", ""))
    print("  destination:", g.get("destinationDatasetId"), g["params"].get("destination_table_name_template"))
    open("transfer.txt", "w").write(c["name"])
