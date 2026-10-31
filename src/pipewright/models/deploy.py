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


def endpoint_name(uc: ResolvedUseCase) -> str:
    return f"{uc.name}-endpoint"


def check_approval(uc: ResolvedUseCase, identity: str | None, approved: bool, key: str = "deploy_model") -> None:
    mode = uc.approvals.get(key, "manual")
    if mode == "auto":
        return
    if not approved:
        raise ConfigError(f"approvals.{key} is manual in {uc.environment}: re-run with --approve")
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
    if not expected:
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
    endpoints = ai.Endpoint.list(filter=f'display_name="{endpoint_name(uc)}"')
    deployed = endpoints[0].list_models() if endpoints else []
    current = [d for d in deployed if d.model == default.resource_name and d.model_version_id == default.version_id]
    serving = uc.model["serving"]
    from pipewright.gcp import Gcp
    from pipewright.models import gates

    earlier = str(default.version_id).isdigit() and int(default.version_id) > 1
    from pipewright.models.registry import record_location

    record = record_location(uc, default.uri, getattr(default, "labels", None))
    problems = gates.verify(Gcp(creds), record, uc.ml["evaluation"], earlier)
    return DeployPlan(uc.model["name"], default.version_id, endpoint_name(uc), serving["machine"],
                      serving["replicas"], bool(current),
                      [f"{d.id} (v{d.model_version_id})" for d in deployed if d not in current], problems,
                      dict(getattr(default, "labels", None) or {}),
                      online_store_problem(uc, env, Gcp(creds), default))


def deploy(uc: ResolvedUseCase, env: Environment, creds, canary: bool = False, log=print) -> None:
    ai = _aiplatform(env, creds)
    p = plan(uc, env, creds)
    if p.gate_problems:
        raise ConfigError(p.gate_problems + ["make a passing version the default (`pipewright model approve`), "
                                             "then deploy"],
                          source=f"v{p.version} of {p.model} is not served: the default version does not pass "
                                 f"{uc.name}'s evaluation gates")
    if p.store_problem:
        raise ConfigError(p.store_problem, source=f"v{p.version} of {p.model} is not served")
    from pipewright.models.registry import pending_approval

    if pending_approval(p.labels):
        raise ConfigError(f"approve it first: pipewright model approve --env {env.name} --usecase {uc.name} "
                          f"--version {p.version} --approve",
                          source=f"v{p.version} of {p.model} is not served: it became the default as the model's "
                                 "first version, and nobody has approved it (approvals.promote_model manual)")
    if p.already_deployed:
        log(f"{p.endpoint} already serves {p.model} v{p.version}: nothing to do")
        return
    registered = ai.Model.list(filter=f'display_name="{p.model}"')[0]
    model = ai.Model(model_name=f"{registered.resource_name}@default")
    endpoints = ai.Endpoint.list(filter=f'display_name="{p.endpoint}"')
    if endpoints:
        from pipewright.gcp import Gcp

        refuse_if_busy(Gcp(creds), env, endpoints[0], p.endpoint)
    endpoint = endpoints[0] if endpoints else ai.Endpoint.create(
        display_name=p.endpoint, labels={"pipewright-usecase": uc.name, "pipewright-tenant": uc.tenant},
        encryption_spec_key_name=env.security.get("kms_key"))
    previous = endpoint.list_models()
    share = int(uc.model["serving"].get("canary_percent") or 10) if canary and previous else 100
    log(f"deploying {p.model} v{p.version} to {p.endpoint} on {p.machine} with {share}% of traffic "
        "(takes about 10-20 minutes)")
    # Online features (FR-51): the endpoint reads the tenant's feature views, so it runs as the tenant's runner.
    identity = uc.resources["runner_service_account"] if uc.ml.get("online_features") else None
    model.deploy(endpoint=endpoint, deployed_model_display_name=f"{p.model}-v{p.version}", machine_type=p.machine,
                 min_replica_count=max(p.replicas["min"], 1), max_replica_count=p.replicas["max"],
                 traffic_percentage=share, sync=True, **({"service_account": identity} if identity else {}))
    if (p.labels or {}).get("pipewright-engine") == "bqml":
        _bqml_parity(uc, env, creds, p, endpoint, previous, log)
    if share < 100:
        log(f"canary: v{p.version} serves {share}%, {', '.join(f'v{d.model_version_id}' for d in previous)} the "
            "rest. Watch it, then `pipewright deploy-model --promote` (or --undeploy-canary).")
        _deployed(uc, env, creds, p.version, share)
        return
    for old in previous:
        log(f"undeploying previous {old.id} (v{old.model_version_id})")
        endpoint.undeploy(deployed_model_id=old.id)
    log(f"live: {p.endpoint} serves {p.model} v{p.version}")
    _deployed(uc, env, creds, p.version, 100)


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
    if previous:
        endpoint.update(traffic_split={previous[0].id: 100, **{o.id: 0 for o in previous[1:]}, new.id: 0})
    endpoint.undeploy(deployed_model_id=new.id)
    why = result.get("error") or (f"largest difference {result['max_difference']:.2e} on {result['rows']} test rows "
                                  f"(tolerance {serving.TOLERANCE})")
    raise ConfigError(f"v{p.version} of {p.model} was removed from {p.endpoint}: its scores do not match ML.PREDICT "
                      f"({why})" + ("; the previous version serves all traffic again" if previous else ""))


def _deployed(uc: ResolvedUseCase, env: Environment, creds, version, traffic: int) -> None:
    import io

    from pipewright.gcp import Gcp
    from pipewright.observability import events

    gcp = Gcp(creds)
    events.emit(events.event("model.deployed", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                             version=version, endpoint=endpoint_name(uc), traffic_percent=traffic,
                             by=gcp.identity()),
                env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)


def _canary_state(uc: ResolvedUseCase, env: Environment, creds):
    ai = _aiplatform(env, creds)
    endpoints = ai.Endpoint.list(filter=f'display_name="{endpoint_name(uc)}"')
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


def promote(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    """Moves all traffic to the newest deployed version and removes the others."""
    endpoint, deployed, newest, traffic = _canary_state(uc, env, creds)
    log(f"promoting v{newest.model_version_id} (was {traffic.get(newest.id, 0)}%) to 100%")
    endpoint.update(traffic_split={newest.id: 100})
    for d in deployed:
        if d.id != newest.id:
            log(f"undeploying {d.id} (v{d.model_version_id})")
            endpoint.undeploy(deployed_model_id=d.id)
    log(f"live: {endpoint_name(uc)} serves {uc.model['name']} v{newest.model_version_id}")
    _deployed(uc, env, creds, newest.model_version_id, 100)


def undeploy_canary(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    """Abandons a canary: removes the newest deployed version; the others take its traffic back."""
    endpoint, deployed, newest, _ = _canary_state(uc, env, creds)
    rest = [d for d in deployed if d.id != newest.id]
    endpoint.update(traffic_split={**{d.id: 0 for d in rest[1:]}, rest[0].id: 100, newest.id: 0})
    endpoint.undeploy(deployed_model_id=newest.id)
    log(f"removed canary v{newest.model_version_id}; v{rest[0].model_version_id} serves all traffic")


def undeploy(uc: ResolvedUseCase, env: Environment, creds, log=print) -> None:
    ai = _aiplatform(env, creds)
    endpoints = ai.Endpoint.list(filter=f'display_name="{endpoint_name(uc)}"')
    if not endpoints:
        log(f"{endpoint_name(uc)} does not exist")
        return
    endpoints[0].undeploy_all()
    log(f"undeployed all models from {endpoint_name(uc)} (the endpoint itself costs nothing)")
