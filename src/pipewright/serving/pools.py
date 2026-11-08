"""Model co-hosting on shared deployment resource pools (FR-105): only when an ML team asks for it.

Without `model.serving.pool` a use case deploys on its own machines exactly as before. With it, its model joins a
pool the platform declared in the manifest (`serving.pools.<name>`), sharing the pool's machines with the other models
on it, on one endpoint or several. Facts from Google's documentation and the spike (docs/evidence/fr92-105-106-spike):

- a pool runs Google's prebuilt TensorFlow (generally available) or PyTorch (Preview) serving image only, the same
  one for every model on it; a team's own serving image stays on dedicated machines;
- each machine runs one model server that loads every model on the pool: model files are per model, but container
  environment variables are the pool's (the first model's), so a pool model carries its settings in its files;
- every deployed model names the pool's service account; Pipewright creates pools through the API so they get the
  environment's CMEK and the tenant's identity (the Terraform resource can set neither yet);
- a pool serves standard or private endpoints, not both (mixing failed twice with INTERNAL);
- every deploy, undeploy or resize on a pool restarts all its machines, sometimes minutes after Google reports the
  change done, and the models nobody touched get about a minute of errors: production pools keep at least two
  machines, the lookup gateway retries, changes are made one at a time and announced to every team on the pool.

The pool's settings: machine, accelerator, min_replicas (>= 1, no scale to zero), max_replicas, cpu_target, image
(the one serving image), tenant, network (standard | psc), managed_by (ci, the default: `pipewright serving pools
apply`; or terraform), service_account (default: the tenant's runner).
"""

from __future__ import annotations

import json
import re
import time
from urllib.parse import quote

from pipewright.errors import ConfigError

DEFAULTS = {"min_replicas": 1, "max_replicas": 1, "cpu_target": 60, "network": "standard", "managed_by": "ci"}
# Google's prebuilt serving images (also under vertex-ai-restricted): <repo>/prediction/<framework>-<cpu|gpu>.<v>
_PREBUILT = re.compile(r"^(?:[a-z0-9-]+-)?docker\.pkg\.dev/vertex-ai(?:-restricted)?/prediction/"
                       r"(tf2|pytorch)-(cpu|gpu)\.[0-9]+-[0-9]+(?::[A-Za-z0-9._-]+|@sha256:[0-9a-f]{64})?$")
_NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
SETTLE_SECONDS = 360           # spike: the machines kept restarting up to about 4 minutes after "done"
LOCK_WAIT_SECONDS = 3600
LOCK_STALE_SECONDS = 4 * 3600
API = "https://{region}-aiplatform.googleapis.com/v1"


def image_kind(uri: str | None) -> str | None:
    """tensorflow, pytorch, or None when the image is not one of Google's prebuilt serving images."""
    m = _PREBUILT.match(uri or "")
    return None if not m else ("tensorflow" if m.group(1) == "tf2" else "pytorch")


def is_production(env) -> bool:
    return "prod" in (env.name or "").lower()


def policy(env) -> dict:
    """policies.serving.pools: production keeps two machines and no Preview images unless the platform says so."""
    prod = is_production(env)
    out = {"allow_preview": not prod, "min_replicas": 2 if prod else 1, "settle_seconds": SETTLE_SECONDS}
    out.update((((env.policies or {}).get("serving") or {}).get("pools")) or {})
    return out


def declared(env) -> dict:
    return {n: {**DEFAULTS, **(p or {})} for n, p in
            (((getattr(env, "serving", None) or {}).get("pools")) or {}).items()}


def resource(env, name: str) -> str:
    return f"projects/{env.project}/locations/{env.region}/deploymentResourcePools/{name}"


def service_account(env, pool: dict) -> str | None:
    return pool.get("service_account") or ((env.tenants or {}).get(pool.get("tenant")) or {}).get(
        "runner_service_account")


def pool_problems(env, name: str, pool: dict) -> tuple[list[str], list[str]]:
    """The platform's side: what is wrong with a declared pool, and what to know before using it."""
    from pipewright.serving import prices

    problems, warnings, where = [], [], f"serving.pools.{name}"
    pol = policy(env)
    if not _NAME.match(name):
        problems.append(f"{where}: a pool name is lower case letters, digits and -, starting with a letter")
    if pool.get("tenant") not in (env.tenants or {}) and env.tenants:
        problems.append(f"{where}.tenant {pool.get('tenant')!r}: not a tenant of {env.name} (pools are per tenant)")
    kind = image_kind(pool.get("image"))
    if kind is None:
        problems.append(f"{where}.image {pool.get('image')!r}: a pool runs Google's prebuilt TensorFlow or PyTorch "
                        "serving image (Agent Platform does not co-host custom containers). Custom serving images "
                        "are supported on dedicated deployments, not on pools")
    elif kind == "pytorch" and not pol["allow_preview"]:
        problems.append(f"{where}.image: PyTorch co-hosting is a Google Preview, which {env.name} does not allow "
                        "(policies.serving.pools.allow_preview); use the TensorFlow image or dedicated machines")
    elif kind == "pytorch":
        warnings.append(f"{where}: PyTorch co-hosting is a Google Preview (no SLA, may change)")
    lo, hi = int(pool.get("min_replicas", 1)), int(pool.get("max_replicas", 1))
    if lo < 1:
        problems.append(f"{where}.min_replicas: at least 1 (a pool does not scale to zero)")
    if hi < lo:
        problems.append(f"{where}.max_replicas: less than min_replicas")
    if lo < int(pol["min_replicas"]):
        problems.append(f"{where}.min_replicas {lo}: {env.name} keeps at least {pol['min_replicas']} on a pool "
                        "(policies.serving.pools.min_replicas): every change to a pool restarts all its machines, "
                        "and with one machine the other models are down for about a minute each time")
    elif lo == 1:
        warnings.append(f"{where}: with one machine, every change to any model on it gives the others about a "
                        "minute of errors (fine outside production; production keeps 2)")
    if pool.get("network") not in ("standard", "psc"):
        problems.append(f"{where}.network: standard or psc (a pool serves one kind of endpoint)")
    if pool.get("managed_by") not in ("ci", "terraform"):
        problems.append(f"{where}.managed_by: ci (Pipewright, through the API) or terraform")
    if pool.get("managed_by") == "terraform" and (env.security or {}).get("kms_key"):
        problems.append(f"{where}.managed_by terraform: {env.name} requires CMEK, and the Terraform resource cannot "
                        "set a pool's encryption key or service account yet; use managed_by ci")
    if not service_account(env, pool):
        problems.append(f"{where}: no identity (service_account, or the tenant's runner in the manifest)")
    warnings.append(f"{where}: {prices.pool_cost(pool)}")
    return problems, warnings


def resolve(name: str, explicit: dict, env, tenant: str, usecase: str, problems: list[str],
            warnings: list[str]) -> dict | None:
    """The use case's side of `model.serving.pool`: the pool it joins, or None with problems."""
    from pipewright.serving import endpoints

    where = "model.serving.pool"
    pools = declared(env)
    if name not in pools:
        problems.append(f"{where} {name!r}: {env.name} has no such pool (manifest serving.pools: "
                        f"{', '.join(sorted(pools)) or 'none'}); ask the platform team, or leave it out to deploy "
                        "on the use case's own machines")
        return None
    pool = pools[name]
    if pool.get("tenant") != tenant:
        problems.append(f"{where} {name}: the pool belongs to tenant {pool.get('tenant')!r}, not {tenant!r} "
                        "(pools are per tenant)")
    sized = [k for k in ("machine", "replicas", "accelerator", "scale_to_zero") if k in (explicit or {})]
    if sized:
        problems.append(f"{where}: the pool sets the machines; remove model.serving.{', model.serving.'.join(sized)}")
    if (explicit or {}).get("target", "vertex") != "vertex":
        problems.append(f"{where}: pools are on Agent Platform (target vertex)")
    mine = endpoints.network_type(env, usecase)
    if pool.get("network") != mine:
        problems.append(f"{where} {name}: the pool serves {pool.get('network')} endpoints and {usecase}'s is {mine}; "
                        "a pool serves one kind (found live: mixing failed in Agent Platform)")
    p, w = pool_problems(env, name, pool)
    problems += [f"{x} (used by model.serving.pool)" for x in p]
    warnings += w
    return {"name": name, **{k: pool[k] for k in ("image", "network", "min_replicas", "max_replicas") if k in pool}}


def model_problems(pool_name: str, pool: dict, image: str | None, env_vars: dict) -> list[str]:
    """deploy-model's check of the registered version against its pool: the same image, no container variables."""
    out = []
    if image != pool.get("image"):
        out.append(f"its serving image {image} is not the pool's ({pool.get('image')}): every model on pool "
                   f"{pool_name} uses the same image. Custom serving images are supported on dedicated deployments, "
                   "not on pools")
    if env_vars:
        out.append(f"its serving container sets {', '.join(sorted(env_vars))}: on a pool the environment is the "
                   "pool's, so every model would run with the first model's values. Put per-model settings in "
                   "serving.json with the model's files (the handler reads them from its own folder)")
    return out


# ------------------------------------------------------------------------------------------- the API --

def _api(env) -> str:
    return API.format(region=env.region)


def create_body(env, name: str, pool: dict) -> dict:
    spec = {"machineSpec": {"machineType": pool["machine"]}, "minReplicaCount": int(pool["min_replicas"]),
            "maxReplicaCount": int(pool["max_replicas"])}
    acc = pool.get("accelerator")
    if acc:
        spec["machineSpec"].update(acceleratorType=acc["type"], acceleratorCount=int(acc.get("count", 1)))
    if pool.get("cpu_target"):
        spec["autoscalingMetricSpecs"] = [{"metricName": "aiplatform.googleapis.com/prediction/online/cpu/utilization",
                                           "target": int(pool["cpu_target"])}]
    body = {"dedicatedResources": spec, "serviceAccount": service_account(env, pool)}
    if (env.security or {}).get("kms_key"):
        body["encryptionSpec"] = {"kmsKeyName": env.security["kms_key"]}
    return {"deploymentResourcePoolId": name, "deploymentResourcePool": body}


def get(gcp, env, name: str) -> dict | None:
    r = gcp.get(f"{_api(env)}/{resource(env, name)}")
    if r.status == 404:
        return None
    if not r.ok:
        raise ConfigError(f"cannot read pool {name}: HTTP {r.status}")
    return r.body


def wait(gcp, env, op: dict, what: str, seconds: int = 3600, sleep=time.sleep) -> dict:
    name, waited = (op or {}).get("name"), 0
    if not name or "/operations/" not in name:
        return op or {}
    while waited <= seconds:
        r = gcp.get(f"{_api(env)}/{name}")
        if r.ok and (r.body or {}).get("done"):
            if r.body.get("error"):
                raise ConfigError(f"{what} failed: {r.body['error'].get('message') or r.body['error']}")
            return r.body.get("response") or {}
        sleep(15)
        waited += 15
    raise ConfigError(f"{what} did not finish in {seconds // 60} minutes ({name})")


def deployed_models(gcp, env, name: str) -> list[dict]:
    """The models on a pool: [{endpoint, id, usecase}]; use cases from the endpoints' labels."""
    r = gcp.get(f"{_api(env)}/{resource(env, name)}:queryDeployedModels")
    if not r.ok:
        return []
    out, labels = [], {}
    for ref in ((r.body or {}).get("deployedModelRefs") or []):
        ep = ref.get("endpoint", "")
        if ep not in labels:
            e = gcp.get(f"{_api(env)}/{ep}")
            labels[ep] = ((e.body or {}).get("labels") or {}) if e.ok else {}
        out.append({"endpoint": ep.rsplit("/", 1)[-1], "id": ref.get("deployedModelId"),
                    "usecase": labels[ep].get("pipewright-usecase"), "tenant": labels[ep].get("pipewright-tenant")})
    return out


def plan(gcp, env) -> list[dict]:
    """What `serving pools apply` would do: [{pool, action, detail}], nothing changed."""
    out = []
    for name, pool in sorted(declared(env).items()):
        if pool["managed_by"] != "ci":
            out.append({"pool": name, "action": "skip", "detail": "managed by Terraform"})
            continue
        current = get(gcp, env, name)
        if current is None:
            out.append({"pool": name, "action": "create", "detail": ""})
            continue
        spec = current.get("dedicatedResources") or {}
        ms = spec.get("machineSpec") or {}
        acc = pool.get("accelerator") or {}
        fixed = []
        if ms.get("machineType") != pool["machine"]:
            fixed.append(f"machine {ms.get('machineType')} -> {pool['machine']}")
        if (ms.get("acceleratorType") or None) != (acc.get("type") or None):
            fixed.append("accelerator")
        if fixed:
            out.append({"pool": name, "action": "refuse", "detail": f"{', '.join(fixed)}: a pool's machines cannot "
                        "change; declare a new pool, move the use cases to it, then remove this one"})
            continue
        lo, hi = int(spec.get("minReplicaCount", 1)), int(spec.get("maxReplicaCount", 1))
        if (lo, hi) != (int(pool["min_replicas"]), int(pool["max_replicas"])):
            out.append({"pool": name, "action": "resize",
                        "detail": f"min {lo} -> {pool['min_replicas']}, max {hi} -> {pool['max_replicas']}"})
        else:
            out.append({"pool": name, "action": "none", "detail": ""})
    return out


def apply(gcp, env, log=print, sleep=time.sleep, only: str | None = None) -> list[dict]:
    """Creates missing pools and resizes changed ones in place (PATCH, no rebuild). Never deletes a pool."""
    problems = []
    for name, pool in declared(env).items():
        p, _ = pool_problems(env, name, pool)
        problems += p
    if problems:
        raise ConfigError(problems, source=f"manifest serving.pools ({env.name})")
    steps = [s for s in plan(gcp, env) if only in (None, s["pool"])]
    for s in steps:
        name, pool = s["pool"], declared(env)[s["pool"]]
        if s["action"] == "refuse":
            raise ConfigError(f"pool {name}: {s['detail']}")
        if s["action"] == "create":
            log(f"pool {name}: creating ({pool['machine']}, {pool['min_replicas']} to {pool['max_replicas']} machines; "
                "machines start with its first model)")
            r = gcp.post(f"{_api(env)}/projects/{env.project}/locations/{env.region}/deploymentResourcePools",
                         create_body(env, name, pool))
            if not r.ok:
                raise ConfigError(f"pool {name} was not created: HTTP {r.status} "
                                  f"{((r.body or {}).get('error') or {}).get('message', '')[:300]}")
            wait(gcp, env, r.body, f"creating pool {name}", sleep=sleep)
            log(f"pool {name}: created")
        elif s["action"] == "resize":
            def resize(name=name, pool=pool):
                body = {"dedicatedResources": {"machineSpec": {"machineType": pool["machine"]},
                                               "minReplicaCount": int(pool["min_replicas"]),
                                               "maxReplicaCount": int(pool["max_replicas"])}}
                r = gcp.patch(f"{_api(env)}/{resource(env, name)}", body,
                              updateMask="dedicated_resources.min_replica_count,dedicated_resources.max_replica_count")
                if not r.ok:
                    raise ConfigError(f"pool {name} was not resized: HTTP {r.status}")
                wait(gcp, env, r.body, f"resizing pool {name}", sleep=sleep)
            change(gcp, env, name, f"resize ({s['detail']})", resize, log=log, sleep=sleep)
        elif s["action"] == "skip":
            log(f"pool {name}: managed by Terraform, nothing to do here")
        else:
            log(f"pool {name}: up to date")
    return steps


# ------------------------------------------------------------------- one change at a time, announced --

def _lock_name(name: str) -> str:
    return f"serving/pools/{name}/lock.json"


def acquire(gcp, env, name: str, what: str, log=print, sleep=time.sleep, wait_seconds: int = LOCK_WAIT_SECONDS) -> None:
    """Takes the pool's lock (an object created only if absent), waiting for a change already running."""
    from pipewright.gcp.storage import PreconditionFailed, gcs_read, gcs_write

    body = json.dumps({"by": gcp.identity(), "change": what, "since": time.time()})
    waited, said = 0, False
    while True:
        try:
            gcs_write(gcp, env.artifacts_bucket, _lock_name(name), body, if_generation_match=0)
            return
        except PreconditionFailed:
            text, generation = gcs_read(gcp, env.artifacts_bucket, _lock_name(name))
            held = json.loads(text or "{}")
            if time.time() - float(held.get("since", 0)) > LOCK_STALE_SECONDS:
                log(f"pool {name}: taking over a lock left {int((time.time() - held.get('since', 0)) // 60)} "
                    f"minutes ago by {held.get('by')} ({held.get('change')})")
                try:
                    gcs_write(gcp, env.artifacts_bucket, _lock_name(name), body, if_generation_match=generation)
                    return
                except PreconditionFailed:
                    pass
            if waited >= wait_seconds:
                raise ConfigError(f"pool {name} is busy: {held.get('by')} is making a change ({held.get('change')}); "
                                  "changes to a pool run one at a time. Try again when it has settled") from None
            if not said:
                log(f"pool {name}: waiting for another change to settle ({held.get('change')}, by {held.get('by')})")
                said = True
            sleep(30)
            waited += 30


def release(gcp, env, name: str) -> None:
    from pipewright.gcp.storage import GCS_API

    gcp.delete(f"{GCS_API}/b/{env.artifacts_bucket}/o/{quote(_lock_name(name), safe='')}")


def announce(gcp, env, name: str, what: str, phase: str, models: list[dict], log=print) -> None:
    """A notice to every team on the pool, before a change and once it has settled (an event, which the
    environment's alert channels and Fleet View receive). Names, counts and IDs only."""
    import io

    from pipewright.observability import events

    pool = declared(env).get(name) or {}
    usecases = sorted({m["usecase"] for m in models if m.get("usecase")})
    events.emit(events.event("serving.pool.change", None, env.name, pool.get("tenant"), pool=name, change=what,
                             phase=phase, usecases=usecases, models=len(models), by=gcp.identity()),
                env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)
    if phase == "starting":
        log(f"pool {name}: {what}. Every machine restarts, so {', '.join(usecases) or 'the models on it'} may get "
            "errors for about a minute (callers through the lookup gateway retry); announced")
    else:
        log(f"pool {name}: settled after {what}; announced")


def change(gcp, env, name: str, what: str, fn, log=print, sleep=time.sleep):
    """Runs one change to a pool: lock, announce, change, wait for the machines to settle, announce, unlock."""
    acquire(gcp, env, name, what, log, sleep)
    try:
        before = deployed_models(gcp, env, name)
        announce(gcp, env, name, what, "starting", before, log)
        out = fn()
        settle = int(policy(env)["settle_seconds"])
        if settle:
            log(f"pool {name}: waiting {settle // 60} minutes for its machines to finish restarting")
            sleep(settle)
        announce(gcp, env, name, what, "settled", deployed_models(gcp, env, name), log)
        return out
    finally:
        release(gcp, env, name)
