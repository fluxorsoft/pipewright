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
    if online.get("type") not in ("bigquery", "cloudsql") and not (online.get("type") == "http"
                                                                   and online.get("export")):
        return {"usecase": contract.usecase, "pipeline": SYNC_PIPELINE, "status": "refused",
                "reason": "the release's online store is not one Pipewright syncs (bigquery, cloudsql, or http with "
                          "export.enabled, which exports)"}
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
    if online.get("type") == "router":         # FR-57 D3: a router only, callers send features
        config = {"source": "none", "features": [], "key": None}
    elif online.get("type") == "http":         # the Pipewright custom solution: the team's store behind the gateway
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
    ingress = lk.get("ingress", "INGRESS_TRAFFIC_INTERNAL_ONLY")
    if online.get("type") == "router" and not lk:
        # FR-57: a router-only gateway where the environment has no online store (no network): the team's callers
        # reach it from outside a network, so it is IAM-only (its invokers are ab_test.callers)
        ingress = "INGRESS_TRAFFIC_ALL"
    return {"ingress": ingress,
            "scaling": {"minInstanceCount": size.get("min_instances", 0)},     # service level (Google's advice)
            "labels": {"pipewright-usecase": contract.usecase[:63], "pipewright-tenant": contract.tenant[:63]},
            "template": template}


def has_service(online: dict) -> bool:
    """A Pipewright lookup service: Cloud SQL with provider pipewright, or (1.4.2) any store behind the gateway."""
    if online.get("mode") == "gateway":
        return online.get("type") in ("cloudsql", "http")
    return online.get("type") == "cloudsql" and online.get("provider", "pipewright") == "pipewright"


AIP = "https://{region}-aiplatform.googleapis.com/v1"


def gateway_target(contract, manifest: dict, online: dict, gcp, arm: str = "") -> dict:
    """FR-106: the endpoint the gateway calls, from the manifest (a Terraform endpoint) or found by its name, and
    where to call it: the private endpoint's DNS name inside the network, or the regional API. arm "ab" (FR-57): the
    A/B treatment's endpoint, listed as `<usecase>-ab` (named `<usecase>-ab-endpoint`, as Terraform names it)."""
    region = manifest["region"]
    key = f"{contract.usecase}-{arm}" if arm else contract.usecase
    entry = ((manifest.get("serving") or {}).get("endpoints") or {}).get(key) or {}
    resource = entry.get("resource")
    if not resource:
        name = entry.get("name") or f"{key}-endpoint"
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


def ab_of(contract) -> dict:
    ml = contract.pipeline("training").static_parameters.get("ml_config") or {}
    return ml.get("ab_test") or {}


def running_ab(contract, manifest: dict, gcp) -> dict | None:
    """FR-57: the use case's active A/B test that has a serving arm (its treatment endpoint), or None."""
    if not ab_of(contract):
        return None
    from pipewright.abtest.store import ACTIVE, TestStore

    test = TestStore(gcp, manifest["artifacts_bucket"]).current(contract.usecase)
    if not test or test.get("state") not in ACTIVE or not (test.get("arms") or {}).get("treatment", {}).get("endpoint"):
        return None
    return test


def router_online(contract) -> dict:
    """D3: a use case whose callers send features gets the gateway as a router only (no lookup) during a test."""
    from pipewright.online.config import service_name

    return {"service": service_name(contract.tenant, contract.usecase), "mode": "gateway", "type": "router",
            "features": [], "gateway": {"retries": 3}}


def ab_gateway(contract, manifest: dict, online: dict, gcp, test: dict) -> tuple[dict, list]:
    """The gateway's A/B configuration (the treatment's target, the allocation, the breaker, the feed) and the
    secret environment entries it needs."""
    from pipewright.abtest.scoring import current_bp

    section = ab_of(contract)
    treatment = gateway_target(contract, manifest, online, gcp, arm="ab")
    state = "running" if test["state"] in ("running", "decision_required") else "paused"
    feed_cfg, env = {}, []
    policy = ((manifest.get("policies") or {}).get("ab_test") or {})
    feed = ((manifest.get("tenants") or {}).get(contract.tenant) or {}).get("ab_test_feed") or {}
    if (section.get("publish") or {}).get("exposures") and policy.get("feed_exposures") and feed.get("key_secret"):
        feed_cfg = {"exposures": True, "key_env": "PIPEWRIGHT_AB_FEED_KEY"}
        env = [{"name": "PIPEWRIGHT_AB_FEED_KEY",
                "valueSource": {"secretKeyRef": {"secret": feed["key_secret"], "version": "latest"}}}]
    cfg = {"name": test["name"], "mode": test.get("mode", "ab"), "state": state, "treatment_bp": current_bp(test),
           "release": contract.release_id, "usecase": contract.usecase, "tenant": contract.tenant,
           "project": manifest["project"], "unit": test["unit"],
           "treatment": {k: treatment[k] for k in ("predict_url", "endpoint", "private", "region")},
           "treatment_deployed_model": (test["arms"]["treatment"] or {}).get("deployed_model"),
           "breaker": ((section.get("guardrails") or {}).get("technical") or {}).get("breaker") or {},
           "feed": feed_cfg}
    return cfg, env


def ensure_lookup_service(contract, manifest: dict, gcp, wait: float = 600, sleep=time.sleep) -> dict:
    online = online_of(contract)
    test = running_ab(contract, manifest, gcp)
    if not has_service(online) and test:
        online = router_online(contract)            # D3: router only while the test runs
        online["gateway"]["callers"] = list(ab_of(contract).get("callers") or [])
    if not has_service(online) and online.get("type") != "router":
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
    if test and gateway:                              # FR-57: the gateway routes the test's units
        ab_cfg, secret_env = ab_gateway(contract, manifest, online, gcp, test)
        c = spec["template"]["containers"][0]
        config = json.loads(c["env"][0]["value"])
        config["ab_test"] = ab_cfg
        c["env"][0]["value"] = json.dumps(config, separators=(",", ":"))
        c["env"] += secret_env
    # The gateway's identity predicts with the platform's grant (a predict-only role on the project, in Terraform):
    # found live, a private endpoint checks predict on the project, so an endpoint-level grant is refused.
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
    store = None
    if online.get("type") == "http":       # 1.4.2: the team's own store, when it is a Cloud Run service here
        store = grant_store_invoker(gcp, base, online["url"], spec["template"]["serviceAccount"])
    return {"usecase": contract.usecase, "service": name, "status": action, "url": svc.get("uri"),
            "min_instances": spec["scaling"]["minInstanceCount"],
            **({"gateway": {"endpoint": gateway["endpoint"].rsplit("/", 1)[-1], "private": gateway["private"],
                            "callers": len((online.get("gateway") or {}).get("callers") or [])}}
               if gateway else {"invoker": contract.runner_service_account}),
            **({"store_invoker": store} if store else {})}


def grant_store_invoker(gcp, base: str, url: str, member: str) -> str | None:
    """1.4.2 (design item 6): when a team's own store is a Cloud Run service in the environment's project and region,
    let the tenant's lookup identity call it (run.invoker, added; nothing else in its policy changes). A store
    anywhere else is the team's to open (the call text names the identity). Returns the service name, or None."""
    parts = url.rstrip("/").split("/")
    origin = "/".join(parts[:3])
    listed = gcp.get(base)
    for svc in ((listed.body or {}).get("services") or []) if listed.ok else []:
        if origin not in [svc.get("uri"), *(svc.get("urls") or [])]:
            continue
        name = svc["name"]
        short = name.rsplit("/", 1)[-1]
        pol = gcp.post(f"{RUN}/{name}:getIamPolicy", {})
        policy = pol.body if pol.ok else {}
        who = f"serviceAccount:{member}"
        bindings = list(policy.get("bindings", []))
        inv = next((b for b in bindings if b.get("role") == "roles/run.invoker" and not b.get("condition")), None)
        if inv and who in inv.get("members", []):
            return short
        if inv:
            inv["members"] = sorted({*inv.get("members", []), who})
        else:
            bindings.append({"role": "roles/run.invoker", "members": [who]})
        set_ = gcp.post(f"{RUN}/{name}:setIamPolicy", {"policy": {**policy, "bindings": bindings}})
        if not set_.ok:
            raise ConfigError(f"the team's store {short}: the lookup identity {member} could not be allowed to "
                              f"call it (HTTP {set_.status}); grant it run.invoker there")
        return short
    return None


def remove_lookup_service(contract, manifest: dict, gcp) -> dict:
    """1.4.2 (design item 8): undeploy removes the use case's lookup service, so warm instances stop billing."""
    online = online_of(contract)
    if not has_service(online) and ab_of(contract) and running_ab(contract, manifest, gcp) is None:
        online = router_online(contract)            # FR-57 D3: the test's router-only service, once it has ended
    if not has_service(online) and online.get("type") != "router":
        return {"usecase": contract.usecase, "status": "none"}
    name = online["service"]
    if not name.startswith(f"pwl-{contract.tenant}-".lower()[:49]):
        raise ConfigError(f"lookup service {name}: not under the tenant's prefix pwl-{contract.tenant}-")
    r = gcp.delete(f"{RUN}/projects/{manifest['project']}/locations/{manifest['region']}/services/{name}")
    if r.status == 404:
        return {"usecase": contract.usecase, "service": name, "status": "absent"}
    if not r.ok:
        raise ConfigError(f"the lookup service {name} was not removed: HTTP {r.status}")
    return {"usecase": contract.usecase, "service": name, "status": "removed"}


def _cli_service(uc, ab: bool) -> str | None:
    """The service the CLI asks the launcher about: the lookup service, or (FR-57 D3, `ab`) the A/B router-only
    gateway of a use case whose callers send features."""
    online = (getattr(uc, "ml", None) or {}).get("online_features") or {}
    if has_service(online):
        return online["service"]
    if ab:
        from pipewright.online.config import service_name

        return service_name(uc.tenant, uc.name)
    return None


def remove_from_cli(uc, env, log=print, gcp=None, ab: bool = False) -> dict | None:
    """deploy-model --undeploy's part: ask the launcher to remove the lookup service (it holds run.admin). With `ab`,
    an A/B test's router-only service too."""
    service = _cli_service(uc, ab)
    if not service:
        return None
    online = {"service": service}
    import os

    from pipewright.runs import launch

    topic = (env.launcher or {}).get("run_requests_topic")
    if not topic or os.environ.get("PIPEWRIGHT_RUN_VIA", "").lower() == "http":
        out = launch.call(env, {"usecase": uc.name}, path="/online/lookup-service/delete")
        log(f"lookup service {online['service']}: {out.get('status')}")
        return out
    launch.request(env, topic, {"usecase": uc.name, "kind": "online.lookup_service.delete"}, gcp)
    log(f"lookup service {online['service']}: removal requested from the launcher (see its logs)")
    return {"service": online["service"], "status": "requested"}


def request_from_cli(uc, env, log=print, gcp=None, wait: float = 900, sleep=time.sleep,
                     ab: bool = False) -> dict | None:
    """deploy-model's part: before a model with Pipewright's Cloud SQL lookup is deployed, the launcher creates or
    updates its lookup service from the live release. Directly, or over the run requests topic in a private
    environment (then deploy-model waits until the service is ready). With `ab` (FR-57), the A/B gateway is brought
    up to date from the test's record, a router-only one for a use case without a lookup."""
    service = _cli_service(uc, ab)
    if not service:
        return None
    online = {"service": service}
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
