"""Q3 controlled: time travel through a view (applied to its base tables?) and across CREATE OR REPLACE TABLE."""
from h import *
now = lambda: q('SELECT UNIX_MILLIS(CURRENT_TIMESTAMP()) t')[0][0]['t']
c1 = lambda extra="": q(f"SELECT avg_spend_90d a, txn_count_90d n FROM `{P}.{DS}.f_spend_v` {extra} WHERE customer_id='c1'")
FEAT = f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d, TIMESTAMP '2026-11-07' AS feature_timestamp
FROM `{P}.{DS}.transactions` WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL 90 DAY) GROUP BY customer_id"""
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT}"); q(f"CREATE OR REPLACE TABLE `{P}.{DS}.f_spend_t` AS {FEAT}")
time.sleep(5)
T1 = now(); time.sleep(3)
print("view now (before insert):", c1()[0])
q(f"INSERT `{P}.{DS}.transactions` VALUES ('c1', 5000.0, TIMESTAMP '2026-11-06 12:00:00')"); time.sleep(2)
print("view now (after insert): ", c1()[0])
print("view AS OF before insert:", c1(f"FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS({T1})")[0])
# Across CREATE OR REPLACE TABLE
cur = q(f"SELECT ROUND(AVG(avg_spend_90d),4) a FROM `{P}.{DS}.f_spend_t`")[0][0]["a"]
T2 = now(); time.sleep(3)
q(f"CREATE OR REPLACE TABLE `{P}.{DS}.f_spend_t` AS SELECT customer_id, 1.0 AS avg_spend_90d, 1 AS txn_count_90d, CURRENT_TIMESTAMP() AS feature_timestamp FROM `{P}.{DS}.transactions` GROUP BY customer_id")
time.sleep(2)
after = q(f"SELECT ROUND(AVG(avg_spend_90d),4) a FROM `{P}.{DS}.f_spend_t`")[0][0]["a"]
r = q(f"SELECT ROUND(AVG(avg_spend_90d),4) a FROM `{P}.{DS}.f_spend_t` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS({T2})")[0]
print(f"table before replace {cur}, after {after}, AS OF before replace: {r}")
# A view's definition at a time: does AS OF use the old SQL? Re-create the view (60 -> 30 days) and read AS OF before
T3 = now(); time.sleep(3)
before_sql = c1()[0]
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS SELECT customer_id, -1.0 AS avg_spend_90d, -1 AS txn_count_90d, TIMESTAMP '2026-11-07' AS feature_timestamp FROM `{P}.{DS}.transactions` GROUP BY customer_id")
time.sleep(2)
print("view with old SQL:", before_sql, "| new SQL now:", c1()[0], "| AS OF before the SQL change:", c1(f"FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS({T3})"))
