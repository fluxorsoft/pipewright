"""Q2: what a feature group, a feature and a feature view record, and how a definition change reaches serving."""
from h import *
FEAT = lambda d: f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d, COUNT(*) AS txn_count_90d, TIMESTAMP '2026-11-07' AS feature_timestamp
FROM `{P}.{DS}.transactions` WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL {d} DAY) GROUP BY customer_id"""
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(90)}")
g = op(call("POST", f"{AI}/featureGroups?featureGroupId=fr103_spend", {"bigQuery": {
    "bigQuerySource": {"inputUri": f"bq://{P}.{DS}.f_spend_v"}, "entityIdColumns": ["customer_id"],
    "timeSeries": {"timestampColumn": "feature_timestamp"}}}))
print("feature group:", g.get("error") or "created")
for f in ("avg_spend_90d", "txn_count_90d"):
    print(" feature", f, op(call("POST", f"{AI}/featureGroups/fr103_spend/features?featureId={f}", {"versionColumnName": f})).get("error") or "ok")
fg = call("GET", f"{AI}/featureGroups/fr103_spend")
print("\nfeature group records:", json.dumps({k: v for k, v in fg.items() if k not in ("name",)}, indent=1)[:700])
ft = call("GET", f"{AI}/featureGroups/fr103_spend/features/avg_spend_90d")
print("feature records:", json.dumps(ft, indent=1)[:500])
s = op(call("POST", f"{AI}/featureOnlineStores?featureOnlineStoreId=fr103_store", {"bigtable": {"autoScaling": {
    "minNodeCount": 1, "maxNodeCount": 1, "cpuUtilizationTarget": 70}}}))
print("\nonline store:", s.get("error") or "created")
v = op(call("POST", f"{AI}/featureOnlineStores/fr103_store/featureViews?featureViewId=fr103_view", {
    "featureRegistrySource": {"featureGroups": [{"featureGroupId": "fr103_spend", "featureIds": ["avg_spend_90d", "txn_count_90d"]}]},
    "syncConfig": {"cron": "0 0 1 1 *"}}))
print("feature view:", v.get("error") or "created")
fv = call("GET", f"{AI}/featureOnlineStores/fr103_store/featureViews/fr103_view")
print("feature view records:", json.dumps({k: v for k, v in fv.items() if k != "name"}, indent=1)[:700])
