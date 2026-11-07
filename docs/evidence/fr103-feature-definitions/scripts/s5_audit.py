"""Q4: does the BigQuery Admin Activity log record a definition change, with the new SQL and who made it?"""
from h import *
f = (f'logName="projects/{P}/logs/cloudaudit.googleapis.com%2Factivity" AND protoPayload.serviceName="bigquery.googleapis.com" '
     f'AND protoPayload.resourceName:"datasets/{DS}/tables/f_spend" AND timestamp>="2026-11-07T07:50:00Z"')
r = call("POST", "https://logging.googleapis.com/v2/entries:list", {"resourceNames": [f"projects/{P}"], "filter": f,
                                                                   "orderBy": "timestamp asc", "pageSize": 100})
for e in r.get("entries", []):
    pp = e["protoPayload"]; md = pp.get("metadata", {})
    ch = md.get("tableChange") or md.get("tableCreation") or {}
    t = ch.get("table", {})
    sql = (t.get("view") or {}).get("query") or (t.get("materializedView") or {}).get("query") or ""
    print(e["timestamp"][11:19], pp.get("methodName", "").split(".")[-1][:22].ljust(22), pp["resourceName"].split("/")[-1].ljust(11),
          "reason=" + str(ch.get("reason")), "| by", pp.get("authenticationInfo", {}).get("principalEmail", "?")[:14],
          "| SQL in entry:", ("INTERVAL 60" if "INTERVAL 60" in sql else "INTERVAL 90" if "INTERVAL 90" in sql else "yes" if sql else "no"),
          "| job:", "yes" if ch.get("jobName") else "no")
print(len(r.get("entries", [])), "entries")
