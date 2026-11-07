from h import *
FV = f"{AI}/featureOnlineStores/fr103_store/featureViews/fr103_view"
gone = q(f"""SELECT DISTINCT customer_id FROM `{P}.{DS}.transactions` WHERE customer_id NOT IN (SELECT customer_id FROM `{P}.{DS}.f_spend_v`) LIMIT 1""")[0][0]["customer_id"]
r = call("POST", f"{FV}:fetchFeatureValues", {"dataKey": {"key": gone}, "dataFormat": "KEY_VALUE"})
print(gone, "is not in the current definition; serving returns:", {f["name"]: f.get("value") for f in (r.get("keyValues") or {}).get("features", [])} or r)
