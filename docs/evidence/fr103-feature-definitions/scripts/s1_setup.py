"""Fixtures: a transactions table, and the same feature (avg_spend_90d) as a view, a materialized view and a
table built by a query (CTAS, as a pipeline or dbt would)."""
from h import *
print("dataset", call("POST", f"{BQ}/datasets", {"datasetReference": {"projectId": P, "datasetId": DS},
                                                  "location": R}).get("error", "ok"))
rows, st = q(f"""CREATE OR REPLACE TABLE `{P}.{DS}.transactions` AS
SELECT CONCAT('c', CAST(MOD(n, 200) AS STRING)) AS customer_id,
       ROUND(10 + MOD(n * 37, 900) / 10.0, 2) AS amount,
       TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL MOD(n * 13, 120) DAY) AS ts
FROM UNNEST(GENERATE_ARRAY(1, 20000)) AS n""")
print("transactions", rows if isinstance(rows, dict) else "ok")
FEAT = f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d,
  TIMESTAMP '2026-11-07 00:00:00' AS feature_timestamp
FROM `{P}.{DS}.transactions`
WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL 90 DAY)
GROUP BY customer_id"""
print("view", q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT}")[0] or "ok")
MV = f"""SELECT customer_id, AVG(amount) AS avg_spend_all, COUNT(*) AS txn_count_all
FROM `{P}.{DS}.transactions` GROUP BY customer_id"""
print("mv", q(f"CREATE MATERIALIZED VIEW `{P}.{DS}.f_spend_mv` AS {MV}")[0] or "ok")
print("ctas", q(f"CREATE OR REPLACE TABLE `{P}.{DS}.f_spend_t` AS {FEAT}")[0] or "ok")
for t in ("transactions", "f_spend_v", "f_spend_mv", "f_spend_t"):
    d = table(t)
    print("\n==", t, d.get("type"), "keys:", sorted(k for k in d if k not in ("schema",)))
    for k in ("view", "materializedView", "etag", "lastModifiedTime", "numRows", "numBytes"):
        if k in d:
            print("  ", k, json.dumps(d[k])[:300])
