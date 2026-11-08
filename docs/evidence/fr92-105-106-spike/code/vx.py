"""Spike helper: Agent Platform REST calls as the operator (gcloud config vertexai-poc). Names and ids only."""
import json, subprocess, sys, time, urllib.request, urllib.error

P, R, NUM = "pipewright-enterprise", "us-central1", "486008751903"
KMS = f"projects/{P}/locations/{R}/keyRings/pw-ent-ring-1/cryptoKeys/aiplatform"
B = f"https://{R}-aiplatform.googleapis.com/v1/projects/{P}/locations/{R}"
B1 = f"https://{R}-aiplatform.googleapis.com/v1beta1/projects/{P}/locations/{R}"
IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"
STATE = __file__.replace("vx.py", "state.json")


def tok():
    return subprocess.run("gcloud auth print-access-token --configuration=vertexai-poc", shell=True,
                          capture_output=True, text=True).stdout.strip()


def call(method, url, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + tok(), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {url} -> {e.code} {e.read().decode()[:1500]}")


def wait(op, label):
    name = op["name"]
    t = time.time()
    while True:
        o = call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{name}")
        if o.get("done"):
            if "error" in o:
                raise SystemExit(f"{label}: {json.dumps(o['error'])[:1500]}")
            print(f"{label}: done in {round(time.time() - t)} s", flush=True)
            return o.get("response", {})
        time.sleep(20)


def state():
    try:
        return json.load(open(STATE))
    except FileNotFoundError:
        return {}


def save(s):
    json.dump(s, open(STATE, "w"), indent=1)


def main(step):
    s = state()
    if step == "models":
        for tag in ("v1", "v2"):
            body = {"model": {"displayName": f"fs92-m-{tag}", "artifactUri": f"gs://{P}-fs92/m",
                              "encryptionSpec": {"kmsKeyName": KMS},
                              "containerSpec": {"imageUri": IMAGE, "env": [{"name": "MODEL_TAG", "value": tag}]}}}
            r = wait(call("POST", f"{B}/models:upload", body), f"upload {tag}")
            s[f"model_{tag}"] = r["model"]
        save(s)
    elif step == "endpoints":
        std = wait(call("POST", f"{B}/endpoints", {"displayName": "fs92-std", "encryptionSpec": {"kmsKeyName": KMS}}),
                   "endpoint std")
        psc = wait(call("POST", f"{B}/endpoints", {
            "displayName": "fs92-psc", "encryptionSpec": {"kmsKeyName": KMS},
            "privateServiceConnectConfig": {"enablePrivateServiceConnect": True, "projectAllowlist": [P],
                                            "pscAutomationConfigs": [{"projectId": P,
                                                                      "network": f"projects/{P}/global/networks/pw-ent-vpc"}]}}),
            "endpoint psc")
        s["std"], s["psc"] = std["name"], psc["name"]
        save(s)
    elif step == "pool":
        body = {"deploymentResourcePoolId": "fs92-pool", "deploymentResourcePool": {
            "dedicatedResources": {"machineSpec": {"machineType": "n1-standard-2"}, "minReplicaCount": 1,
                                   "maxReplicaCount": 2},
            "encryptionSpec": {"kmsKeyName": KMS},
            "serviceAccount": f"fs92-model@{P}.iam.gserviceaccount.com"}}
        r = wait(call("POST", f"{B}/deploymentResourcePools", body), "pool")
        s["pool"] = r.get("name") or f"projects/{NUM}/locations/{R}/deploymentResourcePools/fs92-pool"
        save(s)
    elif step.startswith("deploy:"):        # deploy:<endpoint key>:<model tag>:<traffic of the new one>
        _, ep, tag, share = step.split(":")
        epn = s[ep]
        cur = call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{epn}")
        split = {k: v for k, v in (cur.get("trafficSplit") or {}).items()}
        rest = 100 - int(share)
        if split:
            first = next(iter(split))
            split = {k: 0 for k in split}
            split[first] = rest
        split["0"] = int(share)
        body = {"deployedModel": {"model": s[f"model_{tag}"], "displayName": f"fs92-{ep}-{tag}",
                                  "sharedResources": s["pool"], "serviceAccount": f"fs92-model@{P}.iam.gserviceaccount.com"}, "trafficSplit": split}
        r = wait(call("POST", f"https://{R}-aiplatform.googleapis.com/v1/{epn}:deployModel", body),
                 f"deploy {tag} to {ep} on the pool")
        s.setdefault("deployed", {})[f"{ep}:{tag}"] = r["deployedModel"]["id"]
        save(s)
    elif step == "psc2":
        r = wait(call("POST", f"{B}/endpoints", {
            "displayName": "fs92-psc2", "encryptionSpec": {"kmsKeyName": KMS},
            "privateServiceConnectConfig": {"enablePrivateServiceConnect": True, "projectAllowlist": [P],
                                            "pscAutomationConfigs": [{"projectId": P,
                                                                      "network": f"projects/{P}/global/networks/pw-ent-vpc"}]}}),
            "endpoint psc2")
        s["psc2"] = r["name"]
        save(s)
    elif step.startswith("dedicated:"):
        _, ep, tag = step.split(":")
        body = {"deployedModel": {"model": s[f"model_{tag}"], "displayName": f"fs92-{ep}-{tag}-dedicated",
                                  "serviceAccount": f"fs92-model@{P}.iam.gserviceaccount.com",
                                  "dedicatedResources": {"machineSpec": {"machineType": "n1-standard-2"},
                                                         "minReplicaCount": 1, "maxReplicaCount": 1}},
                "trafficSplit": {"0": 100}}
        r = wait(call("POST", f"https://{R}-aiplatform.googleapis.com/v1/{s[ep]}:deployModel", body),
                 f"deploy {tag} to {ep} dedicated")
        s.setdefault("deployed", {})[f"{ep}:{tag}:dedicated"] = r["deployedModel"]["id"]
        save(s)
    elif step == "std2pool2":
        e = wait(call("POST", f"{B}/endpoints", {"displayName": "fs92-std2", "encryptionSpec": {"kmsKeyName": KMS}}), "endpoint std2")
        body = {"deploymentResourcePoolId": "fs92-pool2", "deploymentResourcePool": {
            "dedicatedResources": {"machineSpec": {"machineType": "n1-standard-2"}, "minReplicaCount": 1, "maxReplicaCount": 1},
            "encryptionSpec": {"kmsKeyName": KMS}, "serviceAccount": f"fs92-model@{P}.iam.gserviceaccount.com"}}
        wait(call("POST", f"{B}/deploymentResourcePools", body), "pool2")
        pool2 = f"projects/{NUM}/locations/{R}/deploymentResourcePools/fs92-pool2"
        body = {"deployedModel": {"model": s["model_v1"], "displayName": "fs92-std2-v1", "sharedResources": pool2,
                                  "serviceAccount": f"fs92-model@{P}.iam.gserviceaccount.com"}, "trafficSplit": {"0": 100}}
        r = wait(call("POST", f"https://{R}-aiplatform.googleapis.com/v1/{e['name']}:deployModel", body), "deploy v1 to std2 on pool2")
        print(json.dumps({"std2": e["name"], "deployed": r["deployedModel"]["id"]}))
    elif step == "v3":
        body = {"model": {"displayName": "fs92-m-v3", "artifactUri": f"gs://{P}-fs92/m3",
                          "encryptionSpec": {"kmsKeyName": KMS},
                          "containerSpec": {"imageUri": IMAGE, "env": [{"name": "MODEL_TAG", "value": "v3env"}]}}}
        s["model_v3"] = wait(call("POST", f"{B}/models:upload", body), "upload v3")["model"]
        save(s)
    elif step == "show":
        for ep in ("std", "psc"):
            e = call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{s[ep]}")
            print(ep, e["name"].split("/")[-1], json.dumps(e.get("trafficSplit")),
                  json.dumps((e.get("privateServiceConnectConfig") or {}).get("pscAutomationConfigs")))
        print(json.dumps(call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{s['pool']}"), indent=1)[:1500])
        print(json.dumps(call("GET", f"https://{R}-aiplatform.googleapis.com/v1/{s['pool']}:queryDeployedModels"))[:1500])
    elif step.startswith("resize:"):
        mx = int(step.split(":")[1])
        r = call("PATCH", f"https://{R}-aiplatform.googleapis.com/v1/{s['pool']}?updateMask=dedicated_resources.max_replica_count",
                 {"dedicatedResources": {"machineSpec": {"machineType": "n1-standard-2"}, "minReplicaCount": 1,
                                         "maxReplicaCount": mx}})
        print(json.dumps(wait(r, f"resize max to {mx}") if "name" in r and "operations" in r["name"] else r)[:800])
    elif step == "raw":
        print(json.dumps(call(sys.argv[2], sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 else None), indent=1)[:3000])


if __name__ == "__main__":
    main(sys.argv[1])
