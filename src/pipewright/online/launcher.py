"""The launcher's part of the online stores (FR-104).

- A schedule with `pipeline: online_sync` runs the tenant's sync job (a Cloud Run job the platform created, running
  as the tenant's runner) with `pipewright features sync --manifest ... --usecase ...`. The launcher starts it; it
  never reads feature data itself.
- `POST /online/lookup-service {"usecase"}` (deploy-model calls it) creates or updates the use case's Cloud Run lookup
  service from the live release's configuration only: its name has the tenant's prefix, it runs as the tenant's
  lookup identity, ingress internal, egress into the environment's network, the instance's server CA pinned, and
  the tenant's runner (the endpoint's identity) may invoke it. Spike (d): Cloud Run cannot limit a runner to its
  own services by name, so the launcher creates them.

Both act from the release contract and the manifest, never from settings in the request.
"""

from __future__ import annotations

import json
import time

from pipewright.errors import ConfigError

RUN = "https://run.googleapis.com/v2"
SQLADMIN = "https://sqladmin.googleapis.com/v1"
SYNC_PIPELINE = "online_sync"


def online_of(contract) -> dict:
    ml = contract.pipeline("training").static_parameters.get("ml_config") or {}
    return ml.get("online_features") or {}


def _tenant_features(manifest: dict, tenant: str) -> dict:
    return (((manifest.get("tenants") or {}).get(tenant) or {}).get("features")) or {}


def start_sync(contract, manifest: dict, manifest_uri: str, gcp, schedule: str = "") -> dict:
    online = online_of(contract)
    if online.get("type") not in ("bigquery", "cloudsql"):
        return {"usecase": contract.usecase, "pipeline": SYNC_PIPELINE, "status": "refused",
                "reason": "the release's online store is not one Pipewright syncs (bigquery, cloudsql)"}
    job = _tenant_features(manifest, contract.tenant).get("sync_job")
    if not job:
        return {"usecase": contract.usecase, "pipeline": SYNC_PIPELINE, "status": "refused",
                "reason": f"tenant {contract.tenant} has no sync job (manifest tenants.<t>.features.sync_job)"}
    # The job runs as the tenant's runner, which reads only its own prefixes (isolate_tenant_artifacts; found live:
    # it could not read the manifest or the contracts). As for a pipeline run, the launcher hands it what it needs:
    # the manifest and the released configuration, in a request under the tenant's online-sync prefix.
    import time as _time

    from pipewright.gcp.storage import gcs_write

    name = (f"online-sync/{contract.tenant}/{contract.usecase}/requests/"
            f"{_time.strftime('%Y%m%dT%H%M%SZ', _time.gmtime())}-{contract.release_id}.json")
    request = {"usecase": contract.usecase, "tenant": contract.tenant, "release": contract.release_id,
               "manifest": manifest, "ml_config": contract.pipeline("training").static_parameters.get("ml_config")}
    gcs_write(gcp, manifest["artifacts_bucket"], name, json.dumps(request, default=str))
    body = {"overrides": {"containerOverrides": [{"args": [
        "features", "sync", "--request", f"gs://{manifest['artifacts_bucket']}/{name}", "--usecase",
        contract.usecase]}]}}
    r = gcp.post(f"{RUN}/{job}:run", body)
    if not r.ok:
        raise ConfigError(f"the sync job {job} did not start: HTTP {r.status}")
    execution = ((r.body.get("metadata") or {}).get("name")) or r.body.get("name")
    return {"usecase": contract.usecase, "pipeline": SYNC_PIPELINE, "status": "submitted", "execution": execution,
            "schedule": schedule}


def server_ca(gcp, instance: str) -> str | None:
    project, _region, name = instance.split(":")
    r = gcp.get(f"{SQLADMIN}/projects/{project}/instances/{name}")
    return (r.body.get("serverCaCert") or {}).get("cert") if r.ok else None


def service_spec(contract, manifest: dict, online: dict, ca: str | None, gateway: dict | None = None) -> dict:
    """The Cloud Run v2 service for a use case's lookup."""
    store = ((manifest.get("features") or {}).get("online_store")) or {}
    lk = store.get("lookup") or {}
    sql = store.get("cloudsql") or {}
    tf = _tenant_features(manifest, contract.tenant)
    name = online["service"]
    prefix = f"pwl-{contract.tenant}-".lower()
    if not name.startswith(prefix[:49]):
        raise ConfigError(f"lookup service {name}: not under the tenant's prefix {prefix}")
    if not tf.get("lookup_service_account"):
        raise ConfigError(f"tenant {contract.tenant} has no lookup identity "
                          "(manifest tenants.<t>.features.lookup_service_account)")
    size = online.get("lookup") or {}
    if online.get("type") == "http":           # the Pipewright custom solution: the team's store behind the gateway
        config = {"source": "http", "url": online["url"], "audience": online.get("audience") or online["url"],
                  "timeout_ms": online.get("timeout_ms", 1000), "key": online["key"], "features": online["features"]}
    else:
        config = {"host": sql.get("private_ip"), "database": online.get("database", "features"),
                  "schema": online["schema"], "table": online["table"], "key": online["key"],
                  "features": online["features"], "pool_size": size.get("pool_size", 8),
                  "timeout_ms": size.get("timeout_ms", 1000), "server_ca": ca}
    if gateway:
        config["gateway"], config["usecase"] = gateway, contract.usecase
    image = lk.get("image") or (manifest.get("launcher") or {}).get("image")
    if not image:
        raise ConfigError("the environment names no lookup image (manifest features.online_store.lookup.image)")
    template = {
        "serviceAccount": tf["lookup_service_account"],
        "maxInstanceRequestConcurrency": size.get("concurrency", 80),
        "scaling": {"maxInstanceCount": size.get("max_instances", 5)},
        "executionEnvironment": "EXECUTION_ENVIRONMENT_GEN2",
        "labels": {"pipewright-usecase": contract.usecase[:63], "pipewright-tenant": contract.tenant[:63],
                   "pipewright-release": contract.release_id.lower()[:63]},
        "containers": [{
            "image": image, "command": ["python", "-m", "pipewright.online.server"],
            "env": [{"name": "PIPEWRIGHT_LOOKUP", "value": json.dumps(config, separators=(",", ":"))}],
            "resources": {"limits": {"cpu": str(size.get("cpu", 1)), "memory": size.get("memory", "512Mi")},
                          "startupCpuBoost": bool(size.get("cpu_boost", True)),
                          "cpuIdle": size.get("billing", "request") == "request"},
            "startupProbe": {"httpGet": {"path": "/healthz"}, "periodSeconds": 3, "failureThreshold": 20},
            "ports": [{"containerPort": 8080}]}]}
    if lk.get("kms_key"):
        template["encryptionKey"] = lk["kms_key"]
    if lk.get("network"):
        template["vpcAccess"] = {"networkInterfaces": [{"network": lk["network"],
                                                        **({"subnetwork": lk["subnetwork"]}
                                                           if lk.get("subnetwork") else {})}],
                                 "egress": lk.get("egress", "PRIVATE_RANGES_ONLY")}
    return {"ingress": lk.get("ingress", "INGRESS_TRAFFIC_INTERNAL_ONLY"),
            "scaling": {"minInstanceCount": size.get("min_instances", 0)},     # service level (Google's advice)
            "labels": {"pipewright-usecase": contract.usecase[:63], "pipewright-tenant": contract.tenant[:63]},
            "template": template}


def has_service(online: dict) -> bool:
    """A Pipewright lookup service: Cloud SQL with provider pipewright, or (1.4.2) any store behind the gateway."""
    if online.get("mode") == "gateway":
        return online.get("type") in ("cloudsql", "http")
    return online.get("type") == "cloudsql" and online.get("provider", "pipewright") == "pipewright"


AIP = "https://{region}-aiplatform.googleapis.com/v1"


def gateway_target(contract, manifest: dict, online: dict, gcp) -> dict:
    """FR-106: the endpoint the gateway calls, from the manifest (a Terraform endpoint) or found by its name, and
    where to call it: the private endpoint's DNS name inside the network, or the regional API."""
    region = manifest["region"]
    entry = ((manifest.get("serving") or {}).get("endpoints") or {}).get(contract.usecase) or {}
    resource = entry.get("resource")
    if not resource:
        name = entry.get("name") or f"{contract.usecase}-endpoint"
        r = gcp.get(f"{AIP.format(region=region)}/projects/{manifest['project']}/locations/{region}/endpoints",
                    filter=f'display_name="{name}"')
        found = ((r.body or {}).get("endpoints") or []) if r.ok else []
        if not found:
            raise ConfigError(f"the gateway of {contract.usecase} calls {name}, which does not exist yet: "
                              "deploy-model creates it first")
        resource = found[0]["name"]
    private = bool(entry.get("private"))
    path = f"/v1/{resource}:predict"
    if private:
        parts = resource.split("/")
        host = entry.get("dns") or f"{parts[-1]}-{region}-{parts[1]}.prediction.p.vertexai.goog"
        url = f"https://{host}{path}"
    else:
        url = f"https://{region}-aiplatform.googleapis.com{path}"
    g = online.get("gateway") or {}
    return {"endpoint": resource, "predict_url": url, "private": private, "retries": int(g.get("retries", 3)),
            "region": region}


def allow_gateway(gcp, region: str, endpoint: str, identity: str) -> None:
    """The gateway's identity may predict on that one endpoint (resource IAM on the endpoint)."""
    api = f"{AIP.format(region=region)}/{endpoint}"
    pol = gcp.post(f"{api}:getIamPolicy", {})
    if not pol.ok:
        raise ConfigError(f"cannot read the IAM policy of {endpoint.rsplit('/', 1)[-1]} (HTTP {pol.status}): the "
                          "launcher needs aiplatform.endpoints.getIamPolicy and setIamPolicy")
    policy = pol.body or {}
    member, role = f"serviceAccount:{identity}", "roles/aiplatform.user"
    bindings = policy.get("bindings") or []
    if any(b.get("role") == role and member in (b.get("members") or []) for b in bindings):
        return
    others = [b for b in bindings if b.get("role") != role]
    mine = next((b for b in bindings if b.get("role") == role), {"role": role, "members": []})
    bindings = [*others, {"role": role, "members": sorted({*mine.get("members", []), member})}]
    r = gcp.post(f"{api}:setIamPolicy", {"policy": {**policy, "bindings": bindings}})
    if not r.ok:
        raise ConfigError(f"the gateway's identity could not be allowed to call {endpoint.rsplit('/', 1)[-1]} "
                          f"(HTTP {r.status})")


def ensure_lookup_service(contract, manifest: dict, gcp, wait: float = 600, sleep=time.sleep) -> dict:
    online = online_of(contract)
    if not has_service(online):
        raise ConfigError(f"{contract.usecase}'s release has no Pipewright lookup service (online_store cloudsql "
                          "with lookup.provider pipewright, or lookup.mode gateway)")
    project, region = manifest["project"], manifest["region"]
    ca = None
    if online.get("type") == "cloudsql":
        ca = server_ca(gcp, online["instance"])
        if not ca:      # fail closed (found live: without it the service would encrypt but not verify the database)
            raise ConfigError(f"cannot read the server CA of {online['instance']}: the launcher needs "
                              "cloudsql.instances.get (roles/cloudsql.viewer)")
    gateway = gateway_target(contract, manifest, online, gcp) if online.get("mode") == "gateway" else None
    spec = service_spec(contract, manifest, online, ca, gateway)
    if gateway:
        allow_gateway(gcp, region, gateway["endpoint"], spec["template"]["serviceAccount"])
    base = f"{RUN}/projects/{project}/locations/{region}/services"
    name = online["service"]
    current = gcp.get(f"{base}/{name}")
    if current.status == 404:
        r = gcp.post(base, spec, serviceId=name)
        action = "created"
    elif current.ok:
        r = gcp.patch(f"{base}/{name}", spec)
        action = "updated"
    else:
        raise ConfigError(f"cannot read the lookup service {name}: HTTP {current.status}")
    if not r.ok:
        msg = (r.body.get("error") or {}).get("message", "") if isinstance(r.body, dict) else ""
        raise ConfigError(f"the lookup service {name} was not {action}: HTTP {r.status} {msg[:200]}")
    deadline = time.time() + wait
    svc = {}
    while time.time() < deadline:
        got = gcp.get(f"{base}/{name}")
        svc = got.body if got.ok else {}
        if not svc.get("reconciling") and svc.get("terminalCondition", {}).get("state") == "CONDITION_SUCCEEDED":
            break
        if svc.get("terminalCondition", {}).get("state") == "CONDITION_FAILED":
            raise ConfigError(f"the lookup service {name} failed to start: "
                              f"{svc['terminalCondition'].get('message', '')[:300]}")
        sleep(5)
    else:
        raise ConfigError(f"the lookup service {name} was not ready in {int(wait)} s")
    invoker = f"serviceAccount:{contract.runner_service_account}"
    pol = gcp.post(f"{base}/{name}:getIamPolicy", {})
    policy = pol.body if pol.ok else {}
    mine = ("roles/run.invoker", "roles/run.viewer")      # call it, and read its URL (the lookup client)
    bindings = [b for b in policy.get("bindings", []) if b.get("role") not in mine]
    if gateway:     # FR-106: the callers the use case names call the gateway; the endpoint never calls it
        callers = sorted(set((online.get("gateway") or {}).get("callers") or []))
        bindings += [{"role": "roles/run.invoker", "members": callers}] if callers else []
    else:
        bindings += [{"role": role, "members": [invoker]} for role in mine]
    set_ = gcp.post(f"{base}/{name}:setIamPolicy", {"policy": {**policy, "bindings": bindings}})
    if not set_.ok:
        raise ConfigError(f"the lookup service {name}: the endpoint's identity could not be allowed to call it "
                          f"(HTTP {set_.status})")
    return {"usecase": contract.usecase, "service": name, "status": action, "url": svc.get("uri"),
            "min_instances": spec["scaling"]["minInstanceCount"],
            **({"gateway": {"endpoint": gateway["endpoint"].rsplit("/", 1)[-1], "private": gateway["private"],
                            "callers": len((online.get("gateway") or {}).get("callers") or [])}}
               if gateway else {"invoker": contract.runner_service_account})}


def request_from_cli(uc, env, log=print, gcp=None, wait: float = 900, sleep=time.sleep) -> dict | None:
    """deploy-model's part: before a model with Pipewright's Cloud SQL lookup is deployed, the launcher creates or
    updates its lookup service from the live release. Directly, or over the run requests topic in a private
    environment (then deploy-model waits until the service is ready)."""
    online = (getattr(uc, "ml", None) or {}).get("online_features") or {}
    if not has_service(online):
        return None
    from pipewright.runs import launch

    payload = {"usecase": uc.name, "kind": "online.lookup_service"}
    release = launch.live_release(env, uc.name, gcp)
    if release:
        payload["release"] = release
    topic = (env.launcher or {}).get("run_requests_topic")
    import os

    if not topic or os.environ.get("PIPEWRIGHT_RUN_VIA", "").lower() == "http":
        out = launch.call(env, {k: v for k, v in payload.items() if k != "kind"}, path="/online/lookup-service")
        log(f"lookup service {out.get('service')} {out.get('status')}: {out.get('url')}")
        return out
    launch.request(env, topic, payload, gcp)
    from pipewright.gcp import Gcp

    gcp = gcp or Gcp()
    url = f"{RUN}/projects/{env.project}/locations/{env.region}/services/{online['service']}"
    log(f"lookup service {online['service']}: requested from the launcher; waiting until it is ready")
    deadline = time.time() + wait
    while time.time() < deadline:
        r = gcp.get(url)
        if r.status == 403:
            log("cannot read the lookup service to wait for it (run.services.get); deploying anyway")
            return {"service": online["service"], "status": "requested"}
        body = r.body if r.ok else {}
        if not body.get("reconciling") and (body.get("terminalCondition") or {}).get("state") == \
                "CONDITION_SUCCEEDED" and release and (body.get("labels") or {}).get("pipewright-release") in (
                None, release.lower()[:63]) and body.get("uri"):
            log(f"lookup service {online['service']} ready: {body['uri']}")
            return {"service": online["service"], "status": "ready", "url": body["uri"]}
        sleep(10)
    raise ConfigError(f"the lookup service {online['service']} was not ready in {int(wait)} s: see the launcher's "
                      "logs")
