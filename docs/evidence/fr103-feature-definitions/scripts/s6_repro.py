"""Q3: reproducing what a scoring run read, without freezing live data."""
from h import *
ds = call("GET", f"{BQ}/datasets/{DS}")
print("time travel window (hours):", ds.get("maxTimeTravelHours", "168 (default)"))
T0 = "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)"
r, st = q(f"SELECT COUNT(*) n FROM `{P}.{DS}.f_spend_v` FOR SYSTEM_TIME AS OF {T0}")
print("a) time travel on a VIEW:", r if isinstance(r, dict) else r)
r, st = q(f"SELECT COUNT(*) n, ROUND(AVG(avg_spend_90d),4) a FROM `{P}.{DS}.f_spend_t` FOR SYSTEM_TIME AS OF {T0}")
print("b) time travel on the CTAS table across CREATE OR REPLACE:", r)
r, st = q(f"SELECT COUNT(*) n FROM `{P}.{DS}.transactions` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 MINUTE)")
print("c) time travel on the source table (before the INSERT):", r)
# What a query over a view reports it read (to record for a scoring run)
r, st = q(f"SELECT * FROM `{P}.{DS}.f_spend_v`")
print("d) a query over the view lists the base tables it read:", [t["tableId"] for t in st.get("query", {}).get("referencedTables", [])],
      "| billed", st.get("query", {}).get("totalBytesBilled"))
# Snapshots
r, _ = q(f"CREATE SNAPSHOT TABLE `{P}.{DS}.f_spend_t_snap` CLONE `{P}.{DS}.f_spend_t`")
print("e) snapshot of the table:", r if isinstance(r, dict) else "ok")
r, _ = q(f"CREATE SNAPSHOT TABLE `{P}.{DS}.f_spend_v_snap` CLONE `{P}.{DS}.f_spend_v`")
print("f) snapshot of a VIEW:", (r.get("error") or {}).get("message", r) if isinstance(r, dict) else "ok")
r, _ = q(f"CREATE SNAPSHOT TABLE `{P}.{DS}.tx_snap` CLONE `{P}.{DS}.transactions` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)")
print("g) snapshot of the source as of a time:", r if isinstance(r, dict) else "ok")
d = table("f_spend_t_snap")
print("   snapshot metadata:", d.get("type"), d.get("snapshotDefinition"), "numBytes", d.get("numBytes"))
r, _ = q(f"INSERT `{P}.{DS}.f_spend_t_snap` VALUES ('x', 1, 1, CURRENT_TIMESTAMP())")
print("h) write to a snapshot:", (r.get("error") or {}).get("message", r)[:160] if isinstance(r, dict) else "allowed!")
# Row fingerprint of what was read, and its cost
for t in ("f_spend_t", "transactions"):
    r, st = q(f"SELECT COUNT(*) n, BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) fp FROM `{P}.{DS}.{t}` t")
    print(f"i) fingerprint {t}: {r[0]} processed {st['query'].get('totalBytesProcessed')} billed {st['query'].get('totalBytesBilled')} (table numBytes {table(t).get('numBytes')})")
