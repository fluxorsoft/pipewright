from h import *
exec(open("s8_sync.py").read().split("sync(\"first\")")[0])
def et():
    return {"group": call("GET", f"{AI}/featureGroups/fr103_spend").get("etag")[-10:],
            "feature": call("GET", f"{AI}/featureGroups/fr103_spend/features/avg_spend_90d").get("etag")[-10:],
            "view": call("GET", FV).get("etag")[-10:], "view_update": call("GET", FV).get("updateTime")}
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(60)}"); sync("restored name")
a = et(); print("etags:", a)
q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(5)}")
b = et(); print("after SQL change (60 -> 5 days):", {k: ('moved' if a[k] != b[k] else 'same') for k in a})
print("serving before sync:", fetch()["avg_spend_90d"], "| bq now:", q(f"SELECT avg_spend_90d FROM `{P}.{DS}.f_spend_v` WHERE customer_id='c1'")[0][0])
sync("after the change"); c = et()
print("serving after sync:", fetch()["avg_spend_90d"], "| etags after sync:", {k: ('moved' if b[k] != c[k] else 'same') for k in a})
