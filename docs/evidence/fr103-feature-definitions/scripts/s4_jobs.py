"""Q1d: the job history tells a rebuild with the same SQL from a rebuild with changed SQL (normalised query hash)."""
import hashlib, re
from h import *
rows, st = q(f"""SELECT creation_time, query FROM `region-{R}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE destination_table.dataset_id = '{DS}' AND destination_table.table_id = 'f_spend_t' AND state = 'DONE'
  AND error_result IS NULL ORDER BY creation_time""")
for r in rows:
    body = re.sub(r"(?is)^\s*CREATE\s+OR\s+REPLACE\s+TABLE\s+`[^`]+`\s+AS\s+", "", r["query"])
    print(r["creation_time"], hashlib.sha256(" ".join(body.split()).encode()).hexdigest()[:12],
          "INTERVAL 60" if "INTERVAL 60" in body else "INTERVAL 90")
print("bytes billed:", st.get("query", {}).get("totalBytesBilled"), "| job history kept 180 days")
