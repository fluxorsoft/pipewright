from h import *
SA = f"fr103-spike-sq@{P}.iam.gserviceaccount.com"
print("sa", call("POST", f"https://iam.googleapis.com/v1/projects/{P}/serviceAccounts",
                 {"accountId": "fr103-spike-sq", "serviceAccount": {"displayName": "FR-103 spike (delete)"}}).get("error", "ok"))
time.sleep(20)
FEAT = f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d,
  TIMESTAMP '2026-11-07 00:00:00' AS feature_timestamp FROM `{P}.{DS}.transactions` GROUP BY customer_id"""
DT = f"https://bigquerydatatransfer.googleapis.com/v1/projects/{P}/locations/{R}/transferConfigs?serviceAccountName={SA}"
c = call("POST", DT, {"displayName": "fr103-spike-sq", "dataSourceId": "scheduled_query", "destinationDatasetId": DS,
                      "schedule": "every 24 hours", "disabled": True,
                      "params": {"query": FEAT, "destination_table_name_template": "f_spend_s",
                                 "write_disposition": "WRITE_TRUNCATE"}})
print("scheduled query:", c.get("message", "")[:300] if "error" in c else c.get("name"))
if "name" in c:
    open("transfer.txt", "w").write(c["name"])
    g = call("GET", f"https://bigquerydatatransfer.googleapis.com/v1/{c['name']}")
    print("  params:", list(g.get("params", {}).keys()), "| SQL readable from the config:", "AVG(amount)" in g["params"].get("query", ""))
    print("  links the table by name only:", g.get("destinationDatasetId"), g["params"].get("destination_table_name_template"))
    print("  updateTime:", g.get("updateTime"))
