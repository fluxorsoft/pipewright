import json, subprocess, time, urllib.error, urllib.request

RUNNER = "fs104d-runner@pw-ebook.iam.gserviceaccount.com"
tok = subprocess.run(f"gcloud --configuration=vertexai-poc auth print-access-token --impersonate-service-account={RUNNER}",
                     shell=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]
BASE = "https://run.googleapis.com/v2/projects/pw-ebook/locations/europe-west2/services"


def call(method, url, body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req)
        return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


svc = {"ingress": "INGRESS_TRAFFIC_INTERNAL_ONLY", "template": {
    "serviceAccount": "fs104d-lookup-a@pw-ebook.iam.gserviceaccount.com",
    "containers": [{"image": "us-docker.pkg.dev/cloudrun/container/hello"}]}}
for name in ("pwl-a-churn", "pwl-b-churn"):
    s, b = call("POST", f"{BASE}?serviceId={name}", svc)
    print("create", name, s, (b.get("error") or {}).get("message", "ok")[:200])
time.sleep(60)
s, b = call("GET", f"{BASE}/pwl-a-churn")
print("get pwl-a-churn", s, (b.get("error") or {}).get("message", b.get("terminalCondition", {}).get("state", "ok"))[:200])
svc["template"]["containers"][0]["resources"] = {"limits": {"cpu": "1", "memory": "1Gi"}}
s, b = call("PATCH", f"{BASE}/pwl-a-churn", svc)
print("update pwl-a-churn", s, (b.get("error") or {}).get("message", "ok")[:200])
s, b = call("GET", BASE)
print("list", s, (b.get("error") or {}).get("message", [x["name"] for x in b.get("services", [])])[:200])
