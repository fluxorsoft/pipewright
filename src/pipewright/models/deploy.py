"""pipewright deploy-model: put the Model Registry's default version of a use case's model behind an endpoint.

Follows approvals.deploy_model: `manual` requires --approve, and when approvers lists users the caller must be one
of them. Group approvers cannot be checked locally; CI environments enforce them with protected approvals.

The default version is served only when its evaluation record passes the use case's gates (models/gates.py),
which also catches a version a pipeline made the default by itself.

Rollout: by default the new version takes all traffic and older versions are removed. With --canary it takes
model.serving.canary_percent and the current version keeps the rest; `--promote` then moves all traffic to it.

A BigQuery ML version (Pipewright 1.1) is deployed from its registry entry, then checked: the endpoint's scores on
the run's test rows must match ML.PREDICT's (bqml/serving.py), or traffic goes back and the version is removed.
"""

from __future__ import annotations

from dataclasses import dataclass

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError


@dataclass
class DeployPlan:
    model: str
    version: str | None
    endpoint: str
    machine: str
    replicas: dict
    already_deployed: bool
    other_versions: list[str]
    gate_problems: list = None          # problems with the default version's evaluation record
    labels: dict = None                 # the default version's registry labels (BigQuery ML: run and model)
    store_problem: str | None = None    # online features: the model would read another environment's store (FR-76)
    integrity: object = None            # phase 6: is the default version the one approved (models/integrity.Check)
    rules_release: str | None = None    # the release whose gates were checked (FR-96)
    pool: str | None = None             # 1.4.2 (FR-105): the shared pool it deploys onto, when the use case asks
    pool_problems: list = None          # its image or container variables do not fit the pool


def endpoint_name(uc: ResolvedUseCase) -> str:
    return f"{uc.name}-endpoint"


def list_endpoints(ai, name: str) -> list:
    """The endpoints with this display name. The SDK's Endpoint.list leaves out private (Private Service Connect)
    endpoints, which PrivateEndpoint.list returns (found live, FR-92)."""
    found = ai.Endpoint.list(filter=f'display_name="{name}"')
    if found:
        return found
    private = getattr(ai, "PrivateEndpoint", None)
    return private.list(filter=f'display_name="{name}"') if private else []


def _endpoint_for(ai, uc: ResolvedUseCase, env: Environment, create: bool):
    """The use case's endpoint. One the platform's Terraform owns (FR-92) is never created here: a missing one is
    refused, so a public endpoint never stands in for a private one."""
    from pipewright.serving import endpoints as tf_endpoints

    found = list_endpoints(ai, endpoint_name(uc))
    if found:
        return found[0]
    if tf_endpoints.declared(env, uc.name):
        raise ConfigError(tf_endpoints.missing_message(env, uc.name))
    if not create:
        return None
    return ai.Endpoint.create(display_name=endpoint_name(uc),
                              labels={"pipewright-usecase": uc.name, "pipewright-tenant": uc.tenant},
                              encryption_spec_key_name=env.security.get("kms_key"))


def check_approval(uc: ResolvedUseCase, identity: str | None, approved: bool, key: str = "deploy_model") -> None:
    mode = uc.approvals.get(key, "manual")
    if mode in ("auto", "on_register"):          # on_register (Pipewright 1.2.0): the decisions workflow deploys
        return
    if not approved:
        where = " (from the pipewright-gate workflow, once its reviewers approve)" if mode == "ci" else ""
        raise ConfigError(f"approvals.{key} is {mode} in {uc.environment}: re-run with --approve{where}")
    users = [a.split(":", 1)[1] for a in uc.approvals.get("approvers", []) if a.startswith("user:")]
    if users and identity not in users:
        raise ConfigError(f"{identity or 'this identity'} is not an approver for {uc.name} "
                          f"(approvers: {', '.join(uc.approvals['approvers'])})")


def model_env_vars(model) -> dict:
    """The serving container's environment variables of a registry model version (aiplatform.Model)."""
    spec = getattr(getattr(model, "gca_resource", None), "container_spec", None)
    return {e.name: e.value for e in (getattr(spec, "env", None) or [])}


def online_store_problem(uc: ResolvedUseCase, env: Environment, gcp, model) -> str | None:
    """FR-76: with online features, the model must read this environment's online store, not the one of the
    environment it was trained or promoted from."""
    expected = uc.ml.get("online_features")
    if not expected and (uc.model["serving"].get("target") or "vertex") != "vertex":
        return None
    from pipewright.models import online

    serving_json = None
    uri = getattr(model, "uri", None) or ""
    if uri.startswith("gs://"):
        import json

        from pipewright.gcp.storage import gcs_read

        bucket, _, prefix = uri[len("gs://"):].partition("/")
        text, _ = gcs_read(gcp, bucket, f"{prefix.rstrip('/')}/serving.json")
        serving_json = json.loads(text) if text else None
    return online.check(expected, model_env_vars(model), serving_json, env.name)


def operations_in_progress(gcp, env: Environment, endpoint_resource: str) -> list[str]:
    """Unfinished operations on an endpoint (a deploy, an undeploy, a traffic change), as short descriptions."""
    resp = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{endpoint_resource}/operations",
                   filter="done=false")
    out = []
    for op in ((resp.body or {}).get("operations") or []) if resp.ok else []:
        if op.get("done"):
            continue
        meta = op.get("metadata") or {}
        kind = (meta.get("@type") or "").rsplit(".", 1)[-1].replace("OperationMetadata", "") or "an operation"
        started = ((meta.get("genericMetadata") or {}).get("createTime") or "")[:19].replace("T", " ")
        out.append(f"{kind}" + (f" started {started} UTC" if started else ""))
    return out


def refuse_if_busy(gcp, env: Environment, endpoint, name: str) -> None:
    """FR-74: one change to an endpoint at a time. A second deploy started while the first runs would deploy the
    same version twice, or be refused half-way by Agent Platform when the traffic split changes under it."""
    busy = operations_in_progress(gcp, env, endpoint.resource_name)
    if busy:
        raise ConfigError(f"{name} is busy: {', '.join(busy)}. Wait for it to finish (deploys take 10 to 20 "
                          "minutes), then run the command again: it does nothing if the version is already live")


def _aiplatform(env: Environment, creds):
    from google.cloud import aiplatform

    aiplatform.init(project=env.project, location=env.region, credentials=creds)
    return aiplatform


def plan(uc: ResolvedUseCase, env: Environment, creds) -> DeployPlan:
    if uc.model["serving"]["target"] != "vertex":
        raise ConfigError("deploy-model supports target vertex today; kserve arrives with private-gke")
    if not uc.resources.get("online_serving", False):
        raise ConfigError(f"tenant {uc.tenant!r} is not enabled for online serving in {env.name}")
    ai = _aiplatform(env, creds)
    models = ai.Model.list(filter=f'display_name="{uc.model["name"]}"')
    if not models:
        raise ConfigError(f"no model {uc.model['name']!r} in the registry yet: run the pipeline first")
    default = ai.Model(model_name=f"{models[0].resource_name}@default")
    endpoints = list_endpoints(ai, endpoint_name(uc))
    deployed = endpoints[0].list_models() if endpoints else []
    # FR-115 (Pipewright 1.4.2): with online features the endpoint must run as the tenant's runner, which reads the
    # store. A deployment of the same version made without it (under features request) is not "already live": it
    # answered every lookup with HTTP 403 (found re-running the book's Chapter 19), so it is deployed again.
    runner = uc.resources.get("runner_service_account") if uc.ml.get("online_features") else None
    current = [d for d in deployed if d.model == default.resource_name and d.model_version_id == default.version_id
               and (not runner or (getattr(d, "service_account", "") or "") == runner)]
    serving = uc.model["serving"]
    from pipewright.gcp import Gcp
    from pipewright.models import gates

    earlier = str(default.version_id).isdigit() and int(default.version_id) > 1
    from pipewright.models import integrity
    from pipewright.models.registry import pending_approval, record_location

    labels = dict(getattr(default, "labels", None) or {})
    gcp = Gcp(creds)
    spec = getattr(getattr(default, "gca_resource", None), "container_spec", None)
    check = integrity.verify(gcp, env, uc.name, str(default.version_id), default.uri,
                             getattr(spec, "image_uri", None) or None, first_version_pending=pending_approval(labels),
                             model=getattr(default, "resource_name", None))
    record = record_location(uc, default.uri, labels)
    approved = ((check.approval or {}).get("evaluation") or {}).get("record")
    if record is None and approved:          # a version that lost its labels: found by the record its approval names
        record = approved.rsplit("/", 1)[0] + "/"
    # Phase 6 (FR-96): the gates of the release that trained the version, not the working copy's.
    rules = integrity.released_rules(uc, env, gcp, integrity.release_of(gcp, record, labels))
    problems = gates.verify(gcp, record, rules.evaluation, earlier)
    if rules.note:
        check.notes.append(rules.note)
    pool_name, pool_problems = serving.get("pool"), None
    if pool_name:                         # FR-105: only when the use case asked for co-hosting
        from pipewright.serving import pools

        pool = pools.declared(env).get(pool_name) or {}
        pool_problems = pools.model_problems(pool_name, pool, getattr(spec, "image_uri", None) or None,
                                             model_env_vars(default))
        if pool and pools.get(gcp, env, pool_name) is None:
            pool_problems.append(f"pool {pool_name} does not exist yet in {env.name}: "
                                 f"pipewright serving pools apply --env {env.name}" if pool.get("managed_by") == "ci"
                                 else f"pool {pool_name} does not exist yet: the platform's Terraform creates it")
    store_problem = online_store_problem(uc, env, gcp, default)
    if store_problem and pool_name:
        store_problem += (". On a pool the model reads its store from its own files (container variables are the "
                          "pool's), so a promoted model names the store it was trained with: use lookup.mode gateway "
                          "(the model then needs no store) or dedicated machines")
    return DeployPlan(uc.model["name"], default.version_id, endpoint_name(uc), serving["machine"],
                      serving["replicas"], bool(current),
                      [f"{d.id} (v{d.model_version_id})" for d in deployed if d not in current], problems,
                      labels, store_problem, check, rules.release, pool_name, pool_problems)


def _protect_on_activation(env: Environment, creds, check, what: str, log=print) -> None:
    """1.4.1 (FR-101 item 8): with policies.integrity.lock on, the approved files are protected before the version
    becomes active (enforced under refuse; said under warn)."""
    from pipewright.gcp import Gcp
    from pipewright.models import integrity, protection

    problems = protection.protect_now(Gcp(creds), env, integrity.approved_files(check.approval), log)
    if problems:
        integrity.enforce(integrity.Check(problems=problems), env, what, log,
                          reason="its approved files cannot be protected (policies.integrity.lock)",
                          remedy="give the deployer storage.objects.setRetention on the model folders, or check the "
                                 "approved generations still exist")


def pending_approval_of(p: DeployPlan) -> bool:
    from pipewright.models.registry import pending_approval

    return pending_approval(p.labels)


def deploy(uc: ResolvedUseCase, env: Environment, creds, canary: bool = False, log=print) -> None:
    ai = _aiplatform(env, creds)
    p = plan(uc, env, creds)
    if p.integrity is not None and not pending_approval_of(p):
        from pipewright.models import integrity

        # Phase 6 (FR-93, FR-94): first, so a version that is not the one approved is reported as that; refuse (or
        # warn) before anything changes on the endpoint.
        integrity.enforce(p.integrity, env, f"v{p.version} of {p.model} is not served", log)
        _protect_on_activation(env, creds, p.integrity, f"v{p.version} of {p.model} is not served", log)
    if p.gate_problems:
        raise ConfigError(p.gate_problems + ["make a passing version the default (`pipewright model approve`), "
                                             "then deploy"],
                          source=f"v{p.version} of {p.model} is not served: the default version does not pass "
                                 f"{uc.name}'s evaluation gates")
    if p.store_problem:
        raise ConfigError(p.store_problem, source=f"v{p.version} of {p.model} is not served")
    if p.pool_problems:
        raise ConfigError(p.pool_problems, source=f"v{p.version} of {p.model} is not served on pool {p.pool}")
    from pipewright.models.registry import pending_approval

    if pending_approval(p.labels):
        raise ConfigError(f"approve it first: pipewright model approve --env {env.name} --usecase {uc.name} "
                          f"--version {p.version} --approve",
                          source=f"v{p.version} of {p.model} is not served: it is the default and labelled "
                                 "approval=pending: nobody has approved it (approvals.promote_model manual)")
    from pipewright.online import launcher as online_launcher

    if ((getattr(uc, "ml", None) or {}).get("online_features") or {}).get("mode") == "gateway":
        # FR-106: the gateway calls the endpoint, so the endpoint exists before the lookup service is made.
        _endpoint_for(ai, uc, env, create=True)
    # FR-104: the use case's Cloud SQL lookup service, created or brought up to date before the model (also when the
    # model is already served, so a changed size or setting is applied by running deploy-model again).
    online_launcher.request_from_cli(uc, env, log)
    if p.already_deployed:
        log(f"{p.endpoint} already serves {p.model} v{p.version}: nothing to do")
        return
    registered = ai.Model.list(filter=f'display_name="{p.model}"')[0]
    model = ai.Model(model_name=f"{registered.resource_name}@default")
    existing = _endpoint_for(ai, uc, env, create=False)
    if existing:
        from pipewright.gcp import Gcp

        refuse_if_busy(Gcp(creds), env, existing, p.endpoint)
    endpoint = existing or _endpoint_for(ai, uc, env, create=True)
    previous = endpoint.list_models()
    share = int(uc.model["serving"].get("canary_percent") or 10) if canary and previous else 100
    if not p.pool:                         # a pool deploy says its own line (the pool sets the machines)
        log(f"deploying {p.model} v{p.version} to {p.endpoint} on {p.machine} with {share}% of traffic "
            "(takes about 10-20 minutes)")
    # Online features (FR-51): the endpoint reads the tenant's feature views, so it runs as the tenant's runner.
    identity = uc.resources["runner_service_account"] if uc.ml.get("online_features") else None
    serving = uc.model["serving"]
    accelerator = serving.get("accelerator")             # FR-61 (Pipewright 1.2.0): within the environment's policy
    if p.pool:
        _deploy_on_pool(uc, env, creds, p, endpoint, model, previous, share, log)
        if share < 100:
            log(f"canary: v{p.version} serves {share}%, {', '.join(f'v{d.model_version_id}' for d in previous)} the "
                "rest. Watch it, then `pipewright deploy-model --promote` (or --undeploy-canary).")
            _deployed(uc, env, creds, p.version, share, p.integrity)
            return
        _undeploy_on_pool(uc, env, creds, p.pool, endpoint, previous, log)
        log(f"live: {p.endpoint} serves {p.model} v{p.version} on pool {p.pool}")
        _deployed(uc, env, creds, p.version, 100, p.integrity)
        return
    if serving.get("scale_to_zero") and int(p.replicas["min"]) == 0:
        # FR-62 (Pipewright 1.2.0): an idle endpoint runs no replica; Agent Platform offers it in v1beta1 only.
        log("scale to zero: an idle endpoint runs no replica; the first request after a quiet spell waits for one")
        from pipewright.gcp import Gcp

        deploy_scaling_to_zero(Gcp(creds), env, endpoint.resource_name, model.versioned_resource_name,
                               f"{p.model}-v{p.version}", p.machine, p.replicas["max"], share,
                               {d.id: int((endpoint.traffic_split or {}).get(d.id, 0)) for d in previous},
                               accelerator, identity)
    else:
        model.deploy(endpoint=endpoint, deployed_model_display_name=f"{p.model}-v{p.version}",
                     machine_type=p.machine, min_replica_count=max(p.replicas["min"], 1),
                     max_replica_count=p.replicas["max"], traffic_percentage=share, sync=True,
                     **({"service_account": identity} if identity else {}),
                     **({"accelerator_type": accelerator["type"], "accelerator_count": int(accelerator.get("count", 1))}
                        if accelerator else {}))
    if (p.labels or {}).get("pipewright-engine") == "bqml":
        _bqml_parity(uc, env, creds, p, endpoint, previous, log)
    if share < 100:
        log(f"canary: v{p.version} serves {share}%, {', '.join(f'v{d.model_version_id}' for d in previous)} the "
            "rest. Watch it, then `pipewright deploy-model --promote` (or --undeploy-canary).")
        _deployed(uc, env, creds, p.version, share, p.integrity)
        return
    _drain(uc, previous, log)
    for old in previous:
        log(f"undeploying previous {old.id} (v{old.model_version_id})")
        endpoint.undeploy(deployed_model_id=old.id)
        _undeployed(uc, env, creds, old.model_version_id)
    log(f"live: {p.endpoint} serves {p.model} v{p.version}")
    _deployed(uc, env, creds, p.version, 100, p.integrity)


def pool_deploy_request(model_version: str, display_name: str, pool_resource: str, identity: str, share: int,
                        previous: dict) -> dict:
    """The deployModel body onto a shared pool (FR-105): sharedResources and the pool's identity (Agent Platform
    refuses a deployed model whose service account is not the pool's, found live)."""
    body = scale_to_zero_request(model_version, display_name, "unused", 1, share, previous, None, identity)
    dm = body["deployedModel"]
    dm.pop("dedicatedResources")
    dm["sharedResources"] = pool_resource
    if identity:
        dm["serviceAccount"] = identity
    else:
        dm.pop("serviceAccount", None)
    return body


def _deploy_on_pool(uc, env, creds, p: DeployPlan, endpoint, model, previous: list, share: int, log=print) -> None:
    from pipewright.gcp import Gcp, error_text
    from pipewright.serving import pools

    gcp = Gcp(creds)
    pool = pools.declared(env)[p.pool]
    log(f"deploying {p.model} v{p.version} to {p.endpoint} on pool {p.pool} with {share}% of traffic")
    traffic = {d.id: int((endpoint.traffic_split or {}).get(d.id, 0)) for d in previous}
    body = pool_deploy_request(model.versioned_resource_name, f"{p.model}-v{p.version}",
                               pools.resource(env, p.pool), pools.service_account(env, pool), share, traffic)

    def run():
        r = gcp.post(f"https://{env.region}-aiplatform.googleapis.com/v1/{endpoint.resource_name}:deployModel", body)
        if not r.ok:
            raise ConfigError(f"deploying onto pool {p.pool} failed: {error_text(r)}")
        pools.wait(gcp, env, r.body, f"deploying {p.model} v{p.version} onto pool {p.pool}")

    pools.change(gcp, env, p.pool, f"deploy {uc.name} v{p.version}", run, log=log)


def _undeploy_on_pool(uc, env, creds, pool_name: str, endpoint, old: list, log=print) -> None:
    """Removing versions from a pool is a pool change too: one at a time, announced, settled."""
    if not old:
        return
    from pipewright.gcp import Gcp
    from pipewright.serving import pools

    def run():
        for d in old:
            log(f"undeploying previous {d.id} (v{d.model_version_id})")
            endpoint.undeploy(deployed_model_id=d.id)
            _undeployed(uc, env, creds, d.model_version_id)

    pools.change(Gcp(creds), env, pool_name,
                 f"undeploy {uc.name} " + ", ".join(f"v{d.model_version_id}" for d in old), run, log=log)


def _bqml_parity(uc: ResolvedUseCase, env: Environment, creds, p: DeployPlan, endpoint, previous: list,
                 log=print) -> None:
    """The new BigQuery ML deployment scores as ML.PREDICT does, or it is removed and traffic goes back."""
    from pipewright.bqml import serving
    from pipewright.gcp import Gcp

    new = next(d for d in endpoint.list_models() if str(d.model_version_id) == str(p.version)
               and d.id not in {o.id for o in previous})
    try:
        result = serving.parity(Gcp(creds), uc, env, p.labels, endpoint, new.id)
    except ConfigError as e:
        result = {"ok": False, "error": str(e)}
    if result.get("ok"):
        log(f"parity: {result['rows']} test rows score as ML.PREDICT does (largest difference "
            f"{result['max_difference']:.2e})")
        return
    from pipewright.serving import endpoints as tf_endpoints

    if result.get("error") and tf_endpoints.is_private(env, uc.name):
        # FR-92: never roll back because a private endpoint could not be reached from where the CLI runs.
        log(f"parity: not checked ({tf_endpoints.unreachable_note(env, uc.name)}); the version stays deployed "
            f"with {'its canary share' if previous else 'all traffic'}. Compare it from inside the network with "
            f"`pipewright model predict --env {env.name} --usecase {uc.name}` before promoting it")
        return
    if previous:
        endpoint.update(traffic_split={previous[0].id: 100, **{o.id: 0 for o in previous[1:]}, new.id: 0})
    endpoint.undeploy(deployed_model_id=new.id)
    why = result.get("error") or (f"largest difference {result['max_difference']:.2e} on {result['rows']} test rows "
                                  f"(tolerance {serving.TOLERANCE})")
    raise ConfigError(f"v{p.version} of {p.model} was removed from {p.endpoint}: its scores do not match ML.PREDICT "
                      f"({why})" + ("; the previous version serves all traffic again" if previous else ""))


def _undeployed(uc: ResolvedUseCase, env: Environment, creds, version) -> None:
    """model.undeployed (Pipewright 1.4.2, FR-112): a version left the endpoint, on an undeploy, a promote, a replace
    or an abandoned canary. Lineage, the fleet report and the evidence read it as a decision, but nothing emitted it,
    so a model's story ended at "deployed" (found re-running the book's Chapter 17)."""
    import io

    from pipewright.gcp import Gcp
    from pipewright.observability import events

    gcp = Gcp(creds)
    events.emit(events.event("model.undeployed", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                             version=str(version), endpoint=endpoint_name(uc), by=gcp.identity()),
                env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)


def _deployed(uc: ResolvedUseCase, env: Environment, creds, version, traffic: int, check=None) -> None:
    import io

    from pipewright.gcp import Gcp
    from pipewright.observability import events

    gcp = Gcp(creds)
    events.emit(events.event("model.deployed", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                             version=version, endpoint=endpoint_name(uc), traffic_percent=traffic,
                             by=gcp.identity(), **({"integrity": check.deployed()} if check is not None else {})),
                env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)
    from pipewright.serving import endpoints as tf_endpoints

    if tf_endpoints.is_private(env, uc.name):        # FR-92: the name exists once the endpoint has its address
        try:
            resource = find_endpoint(uc, env, gcp)
            if resource:
                tf_endpoints.publish_dns(gcp, env, uc.name, resource)
        except Exception as e:  # noqa: BLE001 - a missing DNS name never fails a deploy
            print(f"the private endpoint's DNS name was not written: {e}")


def _canary_state(uc: ResolvedUseCase, env: Environment, creds):
    ai = _aiplatform(env, creds)
    endpoints = list_endpoints(ai, endpoint_name(uc))
    if not endpoints:
        raise ConfigError(f"{endpoint_name(uc)} does not exist: nothing to promote")
    endpoint = endpoints[0]
    from pipewright.gcp import Gcp

    refuse_if_busy(Gcp(creds), env, endpoint, endpoint_name(uc))
    deployed = endpoint.list_models()
    if len(deployed) < 2:
        raise ConfigError(f"{endpoint_name(uc)} has no canary (it serves {len(deployed)} model)")
    traffic = endpoint.traffic_split
    newest = max(deployed, key=lambda d: int(d.model_version_id or 0))
    return endpoint, deployed, newest, traffic


# FR-109 (Pipewright 1.4.2): after the traffic moves, Agent Platform's serving front end goes on sending requests to the
# version it moved from for a while. Undeploying that version at once answered every call with 404 for about three
# minutes (found re-running the book's Chapter 13 canary on 1.4.2). So a version leaves only after a drain.
DRAIN_SECONDS = 240


def _drain(uc: ResolvedUseCase, leaving: list, log=print, sleep=None) -> None:
    """Waits model.serving.drain_seconds (default 240; 0 undeploys at once, as before 1.4.2) before versions that no
    longer take traffic are undeployed, so requests still routed to them are answered."""
    seconds = int((uc.model.get("serving") or {}).get("drain_seconds", DRAIN_SECONDS))
    if not leaving or seconds <= 0:
        return
    import time

    log(f"draining for {seconds}s: {', '.join(f'v{d.model_version_id}' for d in leaving)} no longer takes traffic, "
        "but the endpoint may route requests to it for a few minutes")
    (sleep or time.sleep)(seconds)


def promote(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    """Moves all traffic to the newest deployed version and removes the others."""
    endpoint, deployed, newest, traffic = _canary_state(uc, env, creds)
    log(f"promoting v{newest.model_version_id} (was {traffic.get(newest.id, 0)}%) to 100%")
    endpoint.update(traffic_split={newest.id: 100})
    old = [d for d in deployed if d.id != newest.id]
    if uc.model["serving"].get("pool"):        # FR-105: removing a version restarts the pool's machines
        _drain(uc, old, log)
        _undeploy_on_pool(uc, env, creds, uc.model["serving"]["pool"], endpoint, old, log)
    else:
        _drain(uc, old, log)
        for d in old:
            log(f"undeploying {d.id} (v{d.model_version_id})")
            endpoint.undeploy(deployed_model_id=d.id)
            _undeployed(uc, env, creds, d.model_version_id)
    log(f"live: {endpoint_name(uc)} serves {uc.model['name']} v{newest.model_version_id}")
    _deployed(uc, env, creds, newest.model_version_id, 100)


def undeploy_canary(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    """Abandons a canary: removes the newest deployed version; the others take its traffic back."""
    endpoint, deployed, newest, _ = _canary_state(uc, env, creds)
    rest = [d for d in deployed if d.id != newest.id]
    endpoint.update(traffic_split={**{d.id: 0 for d in rest[1:]}, rest[0].id: 100, newest.id: 0})
    if uc.model["serving"].get("pool"):        # FR-105: a pool change, one at a time and announced
        _drain(uc, [newest], log)
        _undeploy_on_pool(uc, env, creds, uc.model["serving"]["pool"], endpoint, [newest], log)
    else:
        _drain(uc, [newest], log)
        endpoint.undeploy(deployed_model_id=newest.id)
        _undeployed(uc, env, creds, newest.model_version_id)
    log(f"removed canary v{newest.model_version_id}; v{rest[0].model_version_id} serves all traffic")


def undeploy(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    ai = _aiplatform(env, creds)
    endpoints = list_endpoints(ai, endpoint_name(uc))
    if not endpoints:
        log(f"{endpoint_name(uc)} does not exist")
        return
    leaving = [d.model_version_id for d in endpoints[0].list_models()]
    endpoints[0].undeploy_all()
    for version in leaving:
        _undeployed(uc, env, creds, version)
    from pipewright.serving import endpoints as tf_endpoints
    from pipewright.serving import prices

    if tf_endpoints.is_private(env, uc.name):        # found live: a private endpoint is billed while it exists
        log(f"undeployed all models from {endpoint_name(uc)}; the private endpoint itself still costs "
            f"${prices.PSC_ENDPOINT_HOUR:.2f} an hour until the platform's Terraform removes it")
    else:
        log(f"undeployed all models from {endpoint_name(uc)} (the endpoint itself costs nothing)")


# ---------------------------------------------------------------------------------- how to call it (FR-60) --

SAMPLES = {"int": 0, "integer": 0, "float": 0.0, "double": 0.0, "numeric": 0.0, "bool": False, "boolean": False,
           "date": "2026-01-31", "timestamp": "2026-01-31T00:00:00Z"}


def sample_instance(uc: ResolvedUseCase):
    """One instance in the shape the model takes: its features (placeholder values by contract type), or the entity
    ID alone when the endpoint reads its features from the online store."""
    online = uc.ml.get("online_features")
    if online:
        return {online["key"]: "<entity id>"}
    data = uc.ml.get("data") or {}
    types = {c["name"]: str(c.get("type", "")).lower() for c in (data.get("contract") or {}).get("columns") or []}
    features = uc.ml.get("features") or {}
    names = features.get("include")
    if not names:                         # every contract column but the label, exclusions and personal data
        left_out = {(data.get("label") or {}).get("column"), (data.get("split") or {}).get("time_column"),
                    *(features.get("exclude") or []),
                    *((uc.ml.get("data_protection") or {}).get("personal_data_columns") or [])}
        names = [n for n in types if n not in left_out]
    return {f: SAMPLES.get(types.get(f, ""), "<value>") for f in names}


def sample_request(uc: ResolvedUseCase) -> list[str]:
    """The request lines of how_to_call: an instance is the values in the model's feature order (features.json),
    never named fields (found live: the serving handlers take rows; a named instance answered 503), or with online
    features the entity ID."""
    import json

    instance = sample_instance(uc)
    if uc.ml.get("online_features"):
        return [f"    {line}" for line in json.dumps({"instances": [instance]}, indent=2).splitlines()]
    return [f"    features, in this order: {', '.join(instance)}",
            f"    {json.dumps({'instances': [list(instance.values())]})}"]


def how_to_call(uc: ResolvedUseCase, env: Environment, endpoint_resource: str) -> str:
    """Pipewright 1.2.0 (FR-60): the endpoint's ID, its predict URL, the role a caller needs and a sample request."""
    import json

    from pipewright.serving import endpoints as tf_endpoints

    online = uc.ml.get("online_features") or {}
    if online.get("mode") == "gateway":        # FR-106: callers call the gateway, never the endpoint
        callers = ", ".join((online.get("gateway") or {}).get("callers") or []) or "the callers the use case names"
        return "\n".join([
            "How to call it (through the lookup gateway, from inside the network):",
            f"  service       {online.get('service')} (Cloud Run, internal), its URL in `pipewright doctor`",
            "  predict URL   <service URL>/predict",
            f"  caller needs  run.invoker on {online.get('service')} (granted to {callers}), an ID token for it",
            "  request       POST, Authorization: Bearer <ID token>, a JSON body like:",
            f"    {json.dumps({'entity_ids': ['<entity id>']})}",
            "  answer        {\"predictions\": {id: ...}, \"not_found\": [...], \"deployed_model\", \"model_version\"}",
            f"  endpoint      {endpoint_resource.rsplit('/', 1)[-1]} (called by the gateway only)",
        ])
    url = tf_endpoints.predict_url(env, uc.name, endpoint_resource)
    if tf_endpoints.is_private(env, uc.name):
        return "\n".join([
            "How to call it (a private endpoint: from inside the network only):",
            f"  endpoint ID   {endpoint_resource.rsplit('/', 1)[-1]}",
            f"  predict URL   {url}",
            "  certificate   signed by Google for *.prediction.p.vertexai.goog: fetch it once and trust it "
            "(openssl s_client), and call the DNS name, not the IP",
            "  caller needs  aiplatform.endpoints.predict (roles/aiplatform.user), from a project on the allowlist",
            "  request       POST, Authorization: Bearer <access token>, a JSON body like:",
            *sample_request(uc),
        ])
    return "\n".join([
        "How to call it:",
        f"  endpoint ID   {endpoint_resource.rsplit('/', 1)[-1]}",
        f"  predict URL   {url}",
        "  caller needs  aiplatform.endpoints.predict (roles/aiplatform.user) on the project, as its own identity",
        "  request       POST, Authorization: Bearer <access token>, a JSON body like:",
        *sample_request(uc),
        f"  try it        pipewright model predict --env {env.name} --usecase {uc.name} --instances '<json>'",
    ])


def find_endpoint(uc: ResolvedUseCase, env: Environment, gcp) -> str | None:
    """The use case's endpoint resource name, or None when it does not exist."""
    api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"
    found = gcp.get(f"{api}/endpoints", filter=f'display_name="{endpoint_name(uc)}"')
    endpoints = (found.body or {}).get("endpoints") or [] if found.ok else []
    return endpoints[0]["name"] if endpoints else None


# ---------------------------------------------------------------------------------- scale to zero (FR-62) --

IDLE_SCALEDOWN = "900s"            # 15 minutes without traffic, then no replica (Agent Platform minimum: 300s)


def scale_to_zero_request(model_version: str, display_name: str, machine: str, max_replicas: int, share: int,
                          previous: dict, accelerator: dict | None, identity: str | None) -> dict:
    """The v1beta1 deployModel body: minReplicaCount 0 with a scaleToZeroSpec. previous: {deployed model id: its
    traffic}; the new model gets share, the rest keep theirs in proportion (as the SDK splits traffic)."""
    machine_spec = {"machineType": machine, **({"acceleratorType": accelerator["type"],
                                                "acceleratorCount": int(accelerator.get("count", 1))}
                                               if accelerator else {})}
    split = {"0": share}
    total = sum(previous.values())
    if share < 100 and total:
        rest, ids = 100 - share, sorted(previous, key=lambda i: -previous[i])
        given = {i: rest * previous[i] // total for i in ids}
        given[ids[0]] += rest - sum(given.values())          # rounding goes to the busiest
        split.update({i: v for i, v in given.items() if v})
    return {"deployedModel": {"model": model_version, "displayName": display_name,
                              **({"serviceAccount": identity} if identity else {}),
                              "dedicatedResources": {"machineSpec": machine_spec, "minReplicaCount": 0,
                                                     "maxReplicaCount": int(max_replicas),
                                                     "scaleToZeroSpec": {"idleScaledownPeriod": IDLE_SCALEDOWN}}},
            "trafficSplit": split}


def deploy_scaling_to_zero(gcp, env: Environment, endpoint: str, model_version: str, display_name: str, machine: str,
                           max_replicas: int, share: int, previous: dict, accelerator: dict | None,
                           identity: str | None, wait_seconds: int = 2400, sleep=None) -> None:
    import time

    from pipewright.gcp import error_text

    sleep = sleep or time.sleep
    api = f"https://{env.region}-aiplatform.googleapis.com/v1beta1"
    body = scale_to_zero_request(model_version, display_name, machine, max_replicas, share, previous, accelerator,
                                 identity)
    r = gcp.post(f"{api}/{endpoint}:deployModel", body)
    if not r.ok:
        raise ConfigError(f"deploying with scale to zero failed: {error_text(r)}")
    name, waited = (r.body or {}).get("name"), 0
    while name and waited <= wait_seconds:
        op = gcp.get(f"{api}/{name}")
        if op.ok and (op.body or {}).get("done"):
            if (op.body or {}).get("error"):
                raise ConfigError(f"deploying with scale to zero failed: {op.body['error'].get('message')}")
            return
        sleep(30)
        waited += 30
    raise ConfigError(f"the deployment ({name}) did not finish in {wait_seconds // 60} minutes; check the endpoint "
                      "in the console")
