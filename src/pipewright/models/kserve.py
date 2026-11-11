"""model.serving.target kserve (the private-gke blueprint): the registry's default model version as a KServe
InferenceService in the tenant's serving namespace.

The model server is the container the version was registered with (Model Registry containerSpec: a prebuilt Agent
Platform serving image, or the team's own), with the environment Agent Platform gives it (AIP_STORAGE_URI, routes,
port), so requests and responses are the same on both platforms: {"instances": [...]} -> {"predictions": [...]}.
Images outside the environment's registry come from its mirror (a private cluster pulls nothing else).

KServe runs in Standard mode (Deployments, Services and autoscaling): replicas follow model.serving.replicas, the
size model.serving.machine (as a pod size). Standard mode has no traffic splitting, so there is no canary here; a new
version replaces the running one with a rolling update. Pods run as the namespace's serving service account, whose
identity may only read the tenant's models.

The manifest's `kserve` block says where: namespaces by tenant, the image mirror, and `exposure`: cluster (a Service
for callers in the cluster) or internal_lb (also an internal load balancer, for callers in the VPC).
"""

from __future__ import annotations

import json
import time

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.models.names import oldest
from pipewright.serving import capture, torchserve_metrics

API = "/apis/serving.kserve.io/v1beta1"
# Agent Platform's defaults for its prebuilt serving containers (TorchServe based), when the registry has none.
DEFAULT_ROUTES = {"predict": "/predictions/model", "health": "/ping", "port": 8080}


def _label(value) -> str:
    out = "".join(c if c.isalnum() or c in "-_." else "-" for c in str(value).lower())
    return out.strip("-_.")[:63]


def service_name(uc: ResolvedUseCase) -> str:
    """The InferenceService's name: the use case's (a DNS label, short enough for KServe's `-predictor` suffix)."""
    return _label(uc.name).replace("_", "-").replace(".", "-")[:45].rstrip("-")


def settings(uc: ResolvedUseCase, env: Environment) -> dict:
    cfg = getattr(env, "kserve", None) or {}
    if not cfg:
        raise ConfigError(f"model.serving.target kserve: {env.name} has no KServe (the manifest has no `kserve` "
                          "block): ask the platform team")
    namespace = (cfg.get("namespaces") or {}).get(uc.tenant)
    if not namespace:
        raise ConfigError(f"tenant {uc.tenant!r} has no serving namespace in {env.name}: ask the platform team to "
                          "turn on online serving for it")
    return {"namespace": namespace, "mirror": cfg.get("mirror"), "service_account": cfg.get("service_account")
            or "serving", "exposure": cfg.get("exposure") or "cluster", "verifier_image": cfg.get("verifier_image")}


def serving_image(image: str, env: Environment, mirror: str | None) -> str:
    """The image as the cluster pulls it: from the environment's registry as is, anything else from the mirror."""
    host = (env.artifact_registry.get("docker") or "").split("/", 2)
    own = "/".join(host[:2]) + "/" if len(host) >= 2 else ""
    if (own and image.startswith(own)) or not mirror:
        return image
    return f"{mirror.rstrip('/')}/{image}"


def _pinned_files(isvc: dict | None) -> list | None:
    """The files (generation, SHA-256) a running InferenceService's pods check, or None when it is not pinned."""
    pred = ((isvc or {}).get("spec") or {}).get("predictor") or {}
    for c in pred.get("initContainers") or []:
        for e in c.get("env") or []:
            if e.get("name") == "PIPEWRIGHT_APPROVED_FILES":
                return json.loads(e["value"]).get("files")
    return None


def mirrored_from(image: str | None, mirror: str | None) -> str | None:
    """The image a pod's mirrored image stands for (the inverse of serving_image): what the approval recorded. The
    mirror's own digest differs from the source's (a pull and push writes a new manifest; found live in 1.4.1), so the
    approved image is judged by its source, as it was approved."""
    prefix = f"{mirror.rstrip('/')}/" if mirror else ""
    return image[len(prefix):] if image and prefix and image.startswith(prefix) else image


def default_version(uc: ResolvedUseCase, env: Environment, gcp) -> dict:
    """The registry's default version of the use case's model (REST, as the caller)."""
    from pipewright.gcp import error_text

    api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"
    r = gcp.get(f"{api}/models", filter=f'display_name="{uc.model["name"]}"')
    if not r.ok:
        raise ConfigError(f"Model Registry: {error_text(r)}")
    models = oldest((r.body or {}).get("models") or [])
    if not models:
        raise ConfigError(f"no model {uc.model['name']!r} in the registry yet: run the pipeline first")
    r = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{models[0]['name'].split('@')[0]}@default")
    if not r.ok:
        raise ConfigError(f"Model Registry: {error_text(r)}")
    return r.body


def observed(uc: ResolvedUseCase) -> bool:
    """1.4.3 (FR-98): health or capture is on, so the server's metrics are scraped."""
    mon = (getattr(uc, "ml", None) or {}).get("monitoring") or {}
    cap = ((getattr(uc, "model", None) or {}).get("serving") or {}).get("capture") or {}
    return bool((mon.get("health") or {}).get("enabled") or cap.get("enabled"))


def apply_pod_monitoring(k, log=print) -> None:
    """1.4.3 (FR-98): Managed Prometheus scrapes the predictor's TorchServe metrics (port 8082)."""
    from pipewright.kube import message

    body = torchserve_metrics.pod_monitoring(k.name, k.s["namespace"], k.name)
    path = f"/apis/monitoring.googleapis.com/v1/namespaces/{k.s['namespace']}/podmonitorings"
    current = k.kube.get(f"{path}/{body['metadata']['name']}")
    if current.ok:
        body["metadata"]["resourceVersion"] = current.body["metadata"]["resourceVersion"]
        r = k.kube.request("PUT", f"{path}/{body['metadata']['name']}", body)
    else:
        r = k.kube.post(path, body)
    if not r.ok:
        log(f"warning: the PodMonitoring for {k.name} was not applied ({r.status} {message(r)}): health and coverage "
            "need Managed Prometheus to scrape port 8082; ask the platform team (RBAC on podmonitorings)")


def inference_service(uc: ResolvedUseCase, env: Environment, version: dict, s: dict,
                      pinned: dict | None = None, name: str | None = None, labels: dict | None = None) -> dict:
    """pinned (Pipewright 1.4.1, FR-101 item 2): the approved copy the pods load, and that an init container checks
    file by file (generation and SHA-256) before the server starts; a changed copy keeps the new pod from starting.
    name and labels (FR-57): an A/B test's treatment arm, a second InferenceService beside the use case's."""
    from pipewright.ray_clusters.service import pod_resources

    serving = uc.model["serving"]
    spec = version.get("containerSpec") or {}
    if not spec.get("imageUri"):
        raise ConfigError(f"v{version.get('versionId')} of {uc.model['name']} has no serving container in the registry")
    port = int(((spec.get("ports") or [{}])[0]).get("containerPort") or DEFAULT_ROUTES["port"])
    predict = spec.get("predictRoute") or DEFAULT_ROUTES["predict"]
    health = spec.get("healthRoute") or DEFAULT_ROUTES["health"]
    env_vars = [{"name": "AIP_STORAGE_URI", "value": (pinned or {}).get("folder") or version["artifactUri"]},
                {"name": "AIP_HTTP_PORT", "value": str(port)},
                {"name": "AIP_PREDICT_ROUTE", "value": predict},
                {"name": "AIP_HEALTH_ROUTE", "value": health},
                {"name": "AIP_MODE", "value": "PREDICTION"}]
    env_vars += [{"name": e["name"], "value": e.get("value", "")} for e in spec.get("env") or []]
    extra = labels or {}
    name = name or service_name(uc)
    labels = {"app.kubernetes.io/managed-by": "pipewright", "pipewright.dev/usecase": _label(uc.name),
              "pipewright.dev/model": _label(uc.model["name"]), "pipewright.dev/version": _label(version["versionId"]),
              **extra}
    container = {"name": "kserve-container", "image": serving_image(spec["imageUri"], env, s["mirror"]),
                 "env": env_vars, "ports": [{"containerPort": port, "protocol": "TCP"}],
                 "resources": pod_resources(serving["machine"]),
                 "readinessProbe": {"httpGet": {"path": health, "port": port}, "initialDelaySeconds": 10,
                                    "periodSeconds": 10, "failureThreshold": 30}}
    if spec.get("command"):
        container["command"] = spec["command"]
    if spec.get("args"):
        container["args"] = spec["args"]
    if observed(uc) and torchserve_metrics.is_torchserve(container["image"]):
        # 1.4.3 (FR-98): TorchServe's metrics in Prometheus mode, with the handler's latency histogram
        container = torchserve_metrics.container_settings(container)
    predictor = {"serviceAccountName": s["service_account"],
                 "minReplicas": max(int(serving["replicas"]["min"]), 1),   # Standard mode does not scale to zero
                 "maxReplicas": int(serving["replicas"]["max"]),
                 "containers": [container]}
    logger = capture.kserve_logger(uc, s["namespace"])
    if logger:                                    # 1.4.3 (FR-98): capture through the tenant's receiver
        predictor["logger"] = logger
    if pinned:
        import json

        from pipewright.models.integrity import POD_CHECK

        labels["pipewright.dev/pinned"] = "true"
        predictor["initContainers"] = [{
            "name": "pipewright-check", "image": s.get("verifier_image") or container["image"],
            "command": ["python3", "-c", POD_CHECK],
            "env": [{"name": "PIPEWRIGHT_APPROVED_FILES",
                     "value": json.dumps({"folder": pinned["folder"], "files": pinned["files"]}, sort_keys=True)}],
            "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}}]
    return {
        "apiVersion": "serving.kserve.io/v1beta1", "kind": "InferenceService",
        "metadata": {"name": name, "namespace": s["namespace"], "labels": labels, "annotations": {
            "serving.kserve.io/deploymentMode": "Standard",
            "serving.kserve.io/autoscalerClass": "hpa", "serving.kserve.io/metric": "cpu",
            "serving.kserve.io/targetUtilizationPercentage": "70",
            "pipewright.dev/model-resource": f"{version['name'].split('@')[0]}@{version['versionId']}",
            "pipewright.dev/predict-route": predict}},
        "spec": {"predictor": predictor},
    }


def load_balancer(uc: ResolvedUseCase, s: dict, port: int) -> dict:
    """exposure internal_lb: an internal passthrough load balancer in the VPC in front of the predictor pods."""
    name = service_name(uc)
    return {"apiVersion": "v1", "kind": "Service",
            "metadata": {"name": f"{name}-ilb", "namespace": s["namespace"],
                         "labels": {"app.kubernetes.io/managed-by": "pipewright",
                                    "pipewright.dev/usecase": _label(uc.name)},
                         "annotations": {"networking.gke.io/load-balancer-type": "Internal"}},
            "spec": {"type": "LoadBalancer", "selector": {"serving.kserve.io/inferenceservice": name},
                     "ports": [{"name": "http", "port": 80, "targetPort": port, "protocol": "TCP"}]}}


class KServe:
    def __init__(self, uc: ResolvedUseCase, env: Environment, gcp, name: str | None = None):
        from pipewright.kube import KubeClient

        self.uc, self.env, self.gcp = uc, env, gcp
        self.s = settings(uc, env)
        self.kube = KubeClient.from_manifest({"kubernetes": env.kubernetes}, gcp, dns=True)
        self.name = name or service_name(uc)

    def _path(self, name: str = "") -> str:
        return f"{API}/namespaces/{self.s['namespace']}/inferenceservices" + (f"/{name}" if name else "")

    def current(self) -> dict | None:
        from pipewright.kube import message

        r = self.kube.get(self._path(self.name))
        if r.status == 404:
            return None
        if not r.ok:
            raise ConfigError(f"cannot read InferenceService {self.name} in {self.s['namespace']}: {r.status} "
                              f"{message(r)}")
        return r.body

    def apply(self, body: dict) -> str:
        from pipewright.kube import message

        current = self.current()
        if current is None:
            r = self.kube.post(self._path(), body)
            action = "created"
        else:
            body = {**body, "metadata": {**body["metadata"],
                                         "resourceVersion": current["metadata"]["resourceVersion"]}}
            r = self.kube.request("PUT", self._path(self.name), body)
            action = "updated"
        if not r.ok:
            raise ConfigError(f"InferenceService {self.name}: {r.status} {message(r)}")
        return action

    def wait_ready(self, version: str, timeout: int = 1200, log=print) -> dict:
        """Until the InferenceService runs the version and is Ready (a new version: its rollout has finished)."""
        deadline, last = time.time() + timeout, ""
        while time.time() < deadline:
            isvc = self.current() or {}
            conditions = {c["type"]: c for c in (isvc.get("status") or {}).get("conditions") or []}
            ready = conditions.get("Ready", {})
            running = ((isvc.get("metadata") or {}).get("labels") or {}).get("pipewright.dev/version")
            state = f"{ready.get('status', 'Unknown')} {ready.get('reason') or ''}".strip()
            if state != last:
                log(f"  {self.name}: Ready {state}")
                last = state
            if ready.get("status") == "True" and running == _label(version) and self._rolled_out():
                return isvc
            time.sleep(15)
        raise ConfigError(f"InferenceService {self.name} did not become ready in {timeout // 60} minutes: see its "
                          f"pods in {self.s['namespace']}")

    def _rolled_out(self) -> bool:
        r = self.kube.get(f"/apis/apps/v1/namespaces/{self.s['namespace']}/deployments/{self.name}-predictor")
        if not r.ok:
            return False
        st, want = r.body.get("status") or {}, (r.body.get("spec") or {}).get("replicas") or 1
        return (st.get("updatedReplicas") or 0) >= want and (st.get("availableReplicas") or 0) >= want and \
            (st.get("replicas") or 0) == (st.get("updatedReplicas") or 0)

    def ensure_load_balancer(self, port: int) -> None:
        from pipewright.kube import message

        body = load_balancer(self.uc, self.s, port)
        path = f"/api/v1/namespaces/{self.s['namespace']}/services"
        r = self.kube.get(f"{path}/{body['metadata']['name']}")
        if r.status == 404:
            r = self.kube.post(path, body)
            if not r.ok:
                raise ConfigError(f"internal load balancer for {self.name}: {r.status} {message(r)}")

    def delete(self) -> bool:
        r = self.kube.delete(self._path(self.name))
        self.kube.delete(f"/api/v1/namespaces/{self.s['namespace']}/services/{self.name}-ilb")
        return r.ok

    def predict(self, instances: list) -> dict:
        """A prediction through the Kubernetes API's service proxy, as the caller (RBAC: services/proxy in the
        tenant's serving namespace)."""
        from pipewright.kube import message

        isvc = self.current()
        if isvc is None:
            raise ConfigError(f"{self.name} is not deployed in {self.s['namespace']}: run `pipewright deploy-model`")
        route = (isvc["metadata"].get("annotations") or {}).get("pipewright.dev/predict-route") or \
            DEFAULT_ROUTES["predict"]
        r = self.kube.post(f"/api/v1/namespaces/{self.s['namespace']}/services/{self.name}-predictor:80/proxy"
                           f"{route}", {"instances": instances})
        if not r.ok:
            raise ConfigError(f"prediction from {self.name} failed: {r.status} {message(r)}")
        return r.body

    def how_to_call(self) -> str | None:
        """Pipewright 1.2.0 (FR-60) on KServe (found in 1.2.0 QA: only Agent Platform endpoints said it): where callers
        reach the model, the route, what a caller needs and a sample request. None when it is not deployed."""
        from pipewright.models.deploy import sample_request

        isvc = self.current()
        if isvc is None:
            return None
        route = (isvc["metadata"].get("annotations") or {}).get("pipewright.dev/predict-route") or \
            DEFAULT_ROUTES["predict"]
        where = self.endpoints()
        return "\n".join([
            "How to call it:",
            f"  service       InferenceService {self.s['namespace']}/{self.name}",
            f"  in the cluster  POST {where['cluster']}{route}",
            *([f"  in the VPC    POST {where['vpc']}{route}  (internal load balancer, no public address)"]
              if where.get("vpc") else []),
            "  caller needs  network access to the namespace or the load balancer (network policy, firewall); from "
            "outside, the Kubernetes API's services/proxy in the namespace (RBAC), as `model predict` uses",
            "  request       a JSON body like:",
            *sample_request(self.uc),
            f"  try it        pipewright model predict --env {self.env.name} --usecase {self.uc.name} "
            "--instances '<json>'",
        ])

    def endpoints(self) -> dict:
        """Where callers reach it: in the cluster, and in the VPC with exposure internal_lb."""
        out = {"cluster": f"http://{self.name}-predictor.{self.s['namespace']}.svc.cluster.local"}
        r = self.kube.get(f"/api/v1/namespaces/{self.s['namespace']}/services/{self.name}-ilb")
        if r.ok:
            ingress = ((r.body.get("status") or {}).get("loadBalancer") or {}).get("ingress") or []
            if ingress:
                out["vpc"] = f"http://{ingress[0].get('ip')}"
        return out


def deploy(uc: ResolvedUseCase, env: Environment, gcp, canary: bool = False, dry_run: bool = False,
           log=print) -> dict:
    from pipewright.models import gates, integrity
    from pipewright.models.registry import pending_approval

    if canary:
        raise ConfigError("model.serving.target kserve has no canary: KServe in Standard mode cannot split traffic "
                          "between versions. Deploy without --canary (a rolling update replaces the running version)")
    if not uc.resources.get("online_serving", False):
        raise ConfigError(f"tenant {uc.tenant!r} is not enabled for online serving in {env.name}")
    k = KServe(uc, env, gcp)
    version = default_version(uc, env, gcp)
    vid = str(version["versionId"])
    earlier = vid.isdigit() and int(vid) > 1
    labels = version.get("labels") or {}
    # Phase 6 (FR-96): the gates of the release that trained the version, not the working copy's.
    rules = integrity.released_rules(uc, env, gcp, integrity.release_of(gcp, version.get("artifactUri"), labels))
    problems = gates.verify(gcp, version["artifactUri"], rules.evaluation, earlier)
    pending = pending_approval(labels)
    check = integrity.verify(gcp, env, uc.name, vid, version.get("artifactUri"),
                             (version.get("containerSpec") or {}).get("imageUri"), first_version_pending=pending,
                             model=version.get("name"))
    if rules.note:
        check.notes.append(rules.note)
    body = inference_service(uc, env, version, k.s)       # as it would be unpinned, for the plan below
    current = k.current()
    running = ((current or {}).get("metadata", {}).get("labels") or {}).get("pipewright.dev/version")
    serving = uc.model["serving"]
    log(f"model {uc.model['name']} default version v{vid} -> InferenceService {k.s['namespace']}/{k.name} "
        f"({serving['machine']} as a pod, replicas {body['spec']['predictor']['minReplicas']}-"
        f"{serving['replicas']['max']}, exposure {k.s['exposure']})"
        + ("  [already live]" if running == _label(vid) else (f"; replaces v{running}" if running else "")))
    log("evaluation gates: " + ("pass" if not problems else "FAIL\n  " + "\n  ".join(problems)))
    verdict = ("PENDING: nobody has approved this first version" if pending else "verified" if not check.problems
               else "NOT what was approved\n  " + "\n  ".join(check.problems))
    log(f"approval: {verdict}")
    if dry_run:
        return {"version": vid, "dry_run": True}
    if pending:
        raise ConfigError(f"approve it first: pipewright model approve --env {env.name} --usecase {uc.name} "
                          f"--version {vid} --approve",
                          source=f"v{vid} of {uc.model['name']} is not served: it is the default and labelled "
                                 "approval=pending: nobody has approved it (approvals.promote_model manual)")
    integrity.enforce(check, env, f"v{vid} of {uc.model['name']} is not served", log)
    from pipewright.models import protection

    protect = protection.protect_now(gcp, env, integrity.approved_files(check.approval), log)
    if protect:                          # 1.4.1 (FR-101 item 8): lock on activation
        integrity.enforce(integrity.Check(problems=protect), env, f"v{vid} of {uc.model['name']} is not served", log,
                          reason="its approved files cannot be protected (policies.integrity.lock)",
                          remedy="check the deployer may extend retention and the approved generations still exist")
    if problems:
        raise ConfigError(problems + ["make a passing version the default (`pipewright model approve`), then deploy"],
                          source=f"v{vid} of {uc.model['name']} is not served: the default version does not pass "
                                 f"{uc.name}'s evaluation gates")
    pinned = None
    approved = integrity.approved_files(check.approval) if integrity.policy(env)["pin"] else None
    if approved and not check.problems:
        # 1.4.1 (FR-101 item 2): the pods load a copy of exactly the approved generations, checked at every start.
        pinned = integrity.pin_copy(gcp, approved, log)
        body = inference_service(uc, env, version, k.s, pinned)
        lock_days = integrity.policy(env)["lock"]["days"]
        if lock_days > 0:
            integrity.lock(gcp, env, {"folder": pinned["folder"], "files": pinned["files"]}, log)
    elif integrity.policy(env)["pin"]:
        log("integrity: the approval records no model files (approved before Pipewright 1.4.0) or the check found "
            "problems: the pods load the live model folder")
    current_pin = ((current or {}).get("metadata", {}).get("annotations") or {}).get("pipewright.dev/approved-copy")
    if pinned:
        body["metadata"]["annotations"]["pipewright.dev/approved-copy"] = pinned["folder"]
    same_files = _pinned_files(current) == ((pinned or {}).get("files") or None)
    # 1.4.3 (FR-98): capture or health turned on or off changes the predictor, not the version
    pred_now = ((current or {}).get("spec") or {}).get("predictor") or {}
    def shape(pred):
        return (pred.get("logger"),
                [(e.get("name")) for c in pred.get("containers") or [] for e in c.get("env") or []
                 if e.get("name") == "TS_METRICS_MODE"],
                [(p.get("name"), p.get("containerPort")) for c in pred.get("containers") or []
                 for p in c.get("ports") or []])

    same_obs = shape(pred_now) == shape(body["spec"]["predictor"])
    # Found in the 1.4.3 battle test: an InferenceService that never reconciled still carried the version's label, so
    # it was "already live" and never repaired. Live means Ready too.
    ready = any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in ((current or {}).get("status") or {}).get("conditions") or [])
    if observed(uc):
        apply_pod_monitoring(k, log)
    if running == _label(vid) and current_pin == (pinned or {}).get("folder") and same_files and same_obs and ready:
        log(f"v{vid} of {uc.model['name']} is already live on {k.name}")
        log(k.how_to_call() or "")
        return {"version": vid, "status": "live"}
    if running == _label(vid) and current_pin == (pinned or {}).get("folder") and not same_files:
        # Found live in 1.4.1: a changed copy was rewritten above, but the pods' check still named the old generations.
        log("integrity: the serving copy was rewritten from the approved generations (it had changed): the pods' "
            "check is updated (a rolling update)")
    log(f"{k.apply(body)} InferenceService {k.name}; waiting for it to be ready (pulls the image, loads the model)")
    k.wait_ready(vid, log=log)
    port = body["spec"]["predictor"]["containers"][0]["ports"][0]["containerPort"]
    if k.s["exposure"] == "internal_lb":
        k.ensure_load_balancer(port)
    log(f"live: {k.name} serves {uc.model['name']} v{vid} ({', '.join(f'{a}: {u}' for a, u in k.endpoints().items())})")
    _deployed(uc, env, gcp, vid, k, check, pinned)
    log(k.how_to_call() or "")
    return {"version": vid, "status": "deployed"}


def _deployed(uc: ResolvedUseCase, env: Environment, gcp, version: str, k: KServe, check=None,
              pinned: dict | None = None) -> None:
    import io

    from pipewright.observability import events

    events.emit(events.event("model.deployed", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                             version=version, endpoint=f"kserve:{k.s['namespace']}/{k.name}", traffic_percent=100,
                             target="kserve", by=gcp.identity(),
                             **({"integrity": check.deployed()} if check is not None else {}),
                             **({"approved_copy": pinned} if pinned else {})),
                env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)


def undeploy(uc: ResolvedUseCase, env: Environment, gcp, log=print) -> None:
    k = KServe(uc, env, gcp)
    if k.current() is None:
        log(f"{k.name} is not deployed in {k.s['namespace']}")
        return
    k.delete()
    log(f"removed InferenceService {k.s['namespace']}/{k.name} (its pods stop; nothing is left running)")
