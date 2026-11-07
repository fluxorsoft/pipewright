from h import *
FV = f"{AI}/featureOnlineStores/fr103_store/featureViews/fr103_view"
FEAT = lambda d, col="avg_spend_90d": f"""SELECT customer_id, ROUND(AVG(amount), 4) AS {col}, COUNT(*) AS txn_count_90d, TIMESTAMP '2026-11-07' AS feature_timestamp
FROM `{P}.{DS}.transactions` WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL {d} DAY) GROUP BY customer_id"""
def sync(label):
    s = call("POST", f"{FV}:sync", {})
    name = s.get("featureViewSync")
    for _ in range(90):
        d = call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{name}")
        if d.get("finalStatus") is not None or d.get("runTime", {}).get("endTime"):
            print(f"sync ({label}):", d.get("finalStatus") or "ok", json.dumps(d.get("syncSummary") or {})); return d
        time.sleep(10)
def fetch():
    r = call("POST", f"{FV}:fetchFeatureValues", {"dataKey": {"key": "c1"}, "dataFormat": "KEY_VALUE"})
    return {f["name"]: list(f.get("value", {}).values())[0] if f.get("value") else None for f in (r.get("keyValues") or {}).get("features", [])} or r
def etags():
    return call("GET", f"{AI}/featureGroups/fr103_spend").get("etag")[-8:], call("GET", FV).get("etag")[-8:]
sync("first"); print("serving c1:", fetch(), "| bq now:", q(f"SELECT avg_spend_90d FROM `{P}.{DS}.f_spend_v` WHERE customer_id='c1'")[0])
e0 = etags()
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(60)}")
print("\nview SQL changed 90 -> 60 days. group/view etags unchanged:", etags() == e0)
print("serving c1 before the next sync:", fetch(), "| bq now:", q(f"SELECT avg_spend_90d FROM `{P}.{DS}.f_spend_v` WHERE customer_id='c1'")[0])
sync("after the SQL change"); print("serving c1 after sync:", fetch())
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(60, 'avg_spend')}")
print("\nbreaking change: column renamed. etags unchanged:", etags() == e0)
sync("after the rename"); print("serving c1:", fetch())
