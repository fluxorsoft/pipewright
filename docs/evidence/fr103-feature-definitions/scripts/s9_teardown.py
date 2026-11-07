from h import *
print("feature view", op(call("DELETE", f"{AI}/featureOnlineStores/fr103_store/featureViews/fr103_view")).get("error", "deleted"))
print("online store", op(call("DELETE", f"{AI}/featureOnlineStores/fr103_store?force=true")).get("error", "deleted"))
print("feature group", op(call("DELETE", f"{AI}/featureGroups/fr103_spend?force=true")).get("error", "deleted"))
print("scheduled query", call("DELETE", f"https://bigquerydatatransfer.googleapis.com/v1/{open('transfer.txt').read().strip()}").get("error", "deleted"))
print("service account", call("DELETE", f"https://iam.googleapis.com/v1/projects/{P}/serviceAccounts/fr103-spike-sq@{P}.iam.gserviceaccount.com").get("error", "deleted"))
print("dataset", call("DELETE", f"{BQ}/datasets/{DS}?deleteContents=true").get("error", "deleted"))
print("left: stores", call("GET", f"{AI}/featureOnlineStores").get("featureOnlineStores", []), "groups", call("GET", f"{AI}/featureGroups").get("featureGroups", []),
      "datasets", [d["datasetReference"]["datasetId"] for d in call("GET", f"{BQ}/datasets").get("datasets", [])])
