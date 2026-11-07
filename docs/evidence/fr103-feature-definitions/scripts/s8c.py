from h import *
exec(open("s8_sync.py").read().split("sync(\"first\")")[0])
print("serving now (minutes later):", fetch())
# Same SQL but feature_timestamp moved forward: does the new value replace the old?
q(f"""CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d,
TIMESTAMP '2026-11-07 08:00:00' AS feature_timestamp FROM `{P}.{DS}.transactions`
WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL 5 DAY) GROUP BY customer_id""")
sync("same SQL, later feature_timestamp"); print("serving:", fetch())
