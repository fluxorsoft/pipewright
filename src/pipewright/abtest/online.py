"""The treatment's serving arm (FR-57, phase B): its own endpoint, the gateway routing to it, removal at the end.

    start(test)  the treatment deployed on `<usecase>-ab-endpoint` (the platform's Terraform endpoint `<usecase>-ab`
                 where the use case's endpoint is Terraform's, private when it is; otherwise created here, as
                 deploy-model creates the use case's): dedicated machines like the control's, or the use case's shared
                 pool (D2). Returns the arm; the service stores it, then calls apply.
    apply(test)  the launcher brings the gateway up to date from the stored test: routing on start and resume, the
                 control alone on a pause, a lower allocation.
    stop(test)   the gateway stops routing (a router-only service is removed), the arm drains, is undeployed and,
                 when Pipewright created it, deleted. The winner reaches the use case's own endpoint by deploy-model,
                 as any approved version does.
"""

from __future__ import annotations

from pipewright.errors import ConfigError

ARM = "ab"


def arm_key(uc) -> str:
    return f"{uc.name}-{ARM}"


def arm_name(uc) -> str:
    return f"{arm_key(uc)}-endpoint"


class Arms:
    def __init__(self, uc, env, gcp, log=print):
        self.uc, self.env, self.gcp, self.log = uc, env, gcp, log

    def check(self) -> None:
        """Before anything is approved or deployed (nothing to check for an Agent Platform endpoint)."""

    def _ai(self):
        from pipewright.models import deploy

        return deploy._aiplatform(self.env, self.gcp.session.credentials)

    def _endpoint(self, ai, create: bool):
        from pipewright.models import deploy
        from pipewright.serving import endpoints as tf_endpoints

        found = deploy.list_endpoints(ai, arm_name(self.uc))
        if found:
            return found[0], False
        if tf_endpoints.declared(self.env, arm_key(self.uc)):
            raise ConfigError(tf_endpoints.missing_message(self.env, arm_key(self.uc)))
        if tf_endpoints.declared(self.env, self.uc.name):
            raise ConfigError(f"{self.uc.name}'s endpoint is made by the platform's Terraform, so its A/B arm must be "
                              f"too: ask the platform team to add `{arm_key(self.uc)}` to serving.yaml (the same "
                              "privacy as the use case's endpoint), then start the test")
        if not create:
            return None, False
        kms = (self.env.security or {}).get("kms_key")
        ep = ai.Endpoint.create(display_name=arm_name(self.uc),
                                labels={"pipewright-usecase": self.uc.name, "pipewright-ab": "treatment"},
                                **({"encryption_spec_key_name": kms} if kms else {}))
        return ep, True

    def _event(self, kind: str, test: dict, version, **fields) -> None:
        import io

        from pipewright.gcp import Gcp
        from pipewright.observability import events

        gcp = Gcp(self.gcp.session.credentials)
        events.emit(events.event(kind, self.uc.name, self.env.name, self.uc.tenant, None, None,
                                 model=self.uc.model["name"], version=str(version), endpoint=arm_name(self.uc),
                                 ab_test=test["name"], by=gcp.identity(), **fields),
                    (self.env.observability or {}).get("events_topic"), gcp, stream=io.StringIO(),
                    region=self.env.region)

    def start(self, test: dict) -> dict:
        from pipewright.models import deploy
        from pipewright.serving import endpoints as tf_endpoints

        ai = self._ai()
        endpoint, created = self._endpoint(ai, create=True)
        version = str(test["arms"]["treatment"]["version"])
        if any(str(d.model_version_id) == version for d in endpoint.list_models()):
            self.log(f"{arm_name(self.uc)} already serves v{version}")
        else:
            self._deploy(ai, endpoint, test, version)
        endpoint = deploy.list_endpoints(ai, arm_name(self.uc))[0]
        deployed = next((d for d in endpoint.list_models() if str(d.model_version_id) == version), None)
        if deployed is None:
            raise ConfigError(f"v{version} is not on {arm_name(self.uc)} after the deploy")
        if tf_endpoints.is_private(self.env, arm_key(self.uc)):
            tf_endpoints.publish_dns(self.gcp, self.env, arm_key(self.uc), endpoint.resource_name, self.log)
        self._event("model.deployed", test, version, traffic_percent=100)
        return {"treatment": {**test["arms"]["treatment"], "endpoint": endpoint.resource_name,
                              "deployed_model": deployed.id, "created_endpoint": created}}

    def _deploy(self, ai, endpoint, test: dict, version: str) -> None:
        from pipewright.models import deploy

        registered = ai.Model.list(filter=f'display_name="{self.uc.model["name"]}"')
        if not registered:
            raise ConfigError(f"model {self.uc.model['name']} is not registered")
        model = ai.Model(model_name=f"{registered[0].resource_name}@{version}")
        serving = self.uc.model.get("serving") or {}
        name = f"{self.uc.model['name']}-v{version}-ab"
        pool = serving.get("pool")
        if pool:                                      # D2: the use case's pool, no extra machines
            from pipewright.gcp import error_text
            from pipewright.serving import pools

            body = deploy.pool_deploy_request(model.versioned_resource_name, name, pools.resource(self.env, pool),
                                              pools.service_account(self.env, pools.declared(self.env)[pool]), 100, {})

            def run():
                r = self.gcp.post(f"https://{self.env.region}-aiplatform.googleapis.com/v1/"
                                  f"{endpoint.resource_name}:deployModel", body)
                if not r.ok:
                    raise ConfigError(f"deploying the treatment onto pool {pool} failed: {error_text(r)}")
                pools.wait(self.gcp, self.env, r.body, f"deploying the A/B treatment v{version} onto pool {pool}")

            pools.change(self.gcp, self.env, pool, f"A/B test {test['name']}: deploy v{version}", run, log=self.log)
            return
        identity = self.uc.resources.get("runner_service_account") if self.uc.ml.get("online_features") else None
        accelerator = serving.get("accelerator")
        replicas = serving.get("replicas") or {}
        self.log(f"deploying the treatment v{version} to {arm_name(self.uc)} on {serving.get('machine')} "
                 "(takes about 10-20 minutes)")
        model.deploy(endpoint=endpoint, deployed_model_display_name=name, machine_type=serving.get("machine"),
                     min_replica_count=max(int(replicas.get("min", 1)), 1),
                     max_replica_count=max(int(replicas.get("max", 1)), 1), traffic_percentage=100, sync=True,
                     **({"service_account": identity} if identity else {}),
                     **({"accelerator_type": accelerator["type"],
                         "accelerator_count": int(accelerator.get("count", 1))} if accelerator else {}))

    def apply(self, test: dict) -> None:
        from pipewright.online import launcher as online_launcher

        online_launcher.request_from_cli(self.uc, self.env, self.log, gcp=self.gcp, ab=True)

    def stop(self, test: dict) -> None:
        from pipewright.models import deploy
        from pipewright.online import launcher as online_launcher

        if online_launcher.has_service(self.uc.ml.get("online_features") or {}):
            self.apply(test)                          # the gateway goes back to the control alone
        else:
            online_launcher.remove_from_cli(self.uc, self.env, self.log, gcp=self.gcp, ab=True)
        ai = self._ai()
        endpoint, _ = self._endpoint(ai, create=False)
        if endpoint is None:
            return
        leaving = endpoint.list_models()
        deploy._drain(self.uc, leaving, self.log)
        for d in leaving:
            self.log(f"undeploying the treatment {d.id} (v{d.model_version_id}) from {arm_name(self.uc)}")
            endpoint.undeploy(deployed_model_id=d.id)
            self._event("model.undeployed", test, d.model_version_id)
        if ((test.get("arms") or {}).get("treatment") or {}).get("created_endpoint"):
            endpoint.delete()
            self.log(f"deleted {arm_name(self.uc)}")


# ----------------------------------------------------------------------------------------- KServe (private GKE) --
#
# On KServe the treatment is a second InferenceService, `<service>-ab`, serving its approved copy (pinned, checked
# by the pods at every start, as deploy-model's). The router is the gateway's server as a Deployment and a Service
# in the tenant's serving namespace, running as the namespace's serving identity: it calls both InferenceServices at
# their cluster addresses. A NetworkPolicy admits only the router's pods to the arm, so no caller can bypass the
# assignment. The router follows the test's state file (a pause, a lower allocation, the launcher stopping the
# treatment) within 30 seconds; nobody needs to change it.

ROUTER = "pw-ab-router"


def state_object(test: dict) -> str:
    """The small file a KServe router follows, in the tenant's online-sync prefix (its identity reads only that)."""
    return f"online-sync/{test['tenant']}/{test['usecase']}/ab/{test['name']}.router.json"


def state_of(test: dict) -> dict:
    from pipewright.abtest.scoring import current_bp

    state = "running" if test.get("state") in ("running", "decision_required") else test.get("state")
    return {"ab_test": test["name"], "state": state, "treatment_bp": current_bp(test) if state == "running" else 0,
            "mode": test.get("mode", "ab")}


def write_state(gcp, bucket: str, test: dict) -> None:
    import json

    from pipewright.gcp.storage import gcs_write

    gcs_write(gcp, bucket, state_object(test), json.dumps(state_of(test), sort_keys=True))


def is_kserve(test: dict) -> bool:
    return str(((test.get("arms") or {}).get("treatment") or {}).get("endpoint") or "").startswith("kserve:")


class KServeArms:
    def __init__(self, uc, env, gcp, log=print):
        self.uc, self.env, self.gcp, self.log = uc, env, gcp, log

    def _control(self):
        from pipewright.models import kserve

        return kserve.KServe(self.uc, self.env, self.gcp)

    def arm_name(self) -> str:
        from pipewright.models import kserve

        return kserve.service_name(self.uc)[:42].rstrip("-") + "-ab"

    def router_name(self) -> str:
        from pipewright.models import kserve

        return f"{ROUTER}-{kserve.service_name(self.uc)}"[:63].rstrip("-")

    def _arm(self):
        from pipewright.models import kserve

        return kserve.KServe(self.uc, self.env, self.gcp, name=self.arm_name())

    def check(self) -> None:
        """Before anything is approved or deployed: KServe here, the router's image, the control serving."""
        if not self.uc.resources.get("online_serving", False):
            raise ConfigError(f"tenant {self.uc.tenant!r} is not enabled for online serving in {self.env.name}")
        if not (self.env.launcher or {}).get("image"):
            raise ConfigError(f"{self.env.name}'s manifest names no launcher.image: the A/B router runs it as pods "
                              "(the platform's Terraform writes it for tenants with A/B tests)")
        if self._control().current() is None:
            raise ConfigError(f"{self.uc.name} serves nothing on KServe yet: deploy the champion first "
                              f"(pipewright deploy-model --env {self.env.name} --usecase {self.uc.name})")

    def _version(self, version: str) -> dict:
        from pipewright.gcp import error_text
        from pipewright.models import kserve

        default = kserve.default_version(self.uc, self.env, self.gcp)
        r = self.gcp.get(f"https://{self.env.region}-aiplatform.googleapis.com/v1/"
                         f"{default['name'].split('@')[0]}@{version}")
        if not r.ok:
            raise ConfigError(f"v{version} of {self.uc.model['name']}: {error_text(r)}")
        return r.body

    def start(self, test: dict) -> dict:
        from pipewright.models import integrity, kserve

        version = str(test["arms"]["treatment"]["version"])
        v = self._version(version)
        check = integrity.verify(self.gcp, self.env, self.uc.name, version, v.get("artifactUri"),
                                 (v.get("containerSpec") or {}).get("imageUri"), model=v.get("name"))
        integrity.enforce(check, self.env, f"v{version} is not served as the A/B treatment", self.log)
        approved = integrity.approved_files(check.approval)
        pinned = integrity.pin_copy(self.gcp, approved, self.log) if approved and not check.problems else None
        arm = self._arm()
        body = kserve.inference_service(self.uc, self.env, v, arm.s, pinned, name=arm.name, labels={
            "pipewright.dev/ab-test": kserve._label(test["name"]), "pipewright.dev/arm": "treatment"})
        if pinned:
            body["metadata"]["annotations"]["pipewright.dev/approved-copy"] = pinned["folder"]
        self.log(f"{arm.apply(body)} InferenceService {arm.s['namespace']}/{arm.name} for the treatment v{version}; "
                 "waiting for it to be ready")
        arm.wait_ready(version, log=self.log)
        self._event("model.deployed", test, version, arm, traffic_percent=100)
        return {"treatment": {**test["arms"]["treatment"], "endpoint": f"kserve:{arm.s['namespace']}/{arm.name}",
                              "deployed_model": version, "service": arm.endpoints()["cluster"],
                              "route": body["metadata"]["annotations"]["pipewright.dev/predict-route"],
                              "created_endpoint": True, **({"approved_copy": pinned["folder"]} if pinned else {})}}

    def _event(self, kind: str, test: dict, version, arm, **fields) -> None:
        import io

        from pipewright.observability import events

        events.emit(events.event(kind, self.uc.name, self.env.name, self.uc.tenant, None, None,
                                 model=self.uc.model["name"], version=str(version),
                                 endpoint=f"kserve:{arm.s['namespace']}/{arm.name}", target="kserve",
                                 ab_test=test["name"], by=self.gcp.identity(), **fields),
                    (self.env.observability or {}).get("events_topic"), self.gcp, stream=io.StringIO(),
                    region=self.env.region)

    def router_config(self, test: dict) -> dict:
        from pipewright.models import kserve

        control = self._control()
        isvc = control.current() or {}
        route = (isvc.get("metadata", {}).get("annotations") or {}).get("pipewright.dev/predict-route") or \
            kserve.DEFAULT_ROUTES["predict"]
        t = test["arms"]["treatment"]
        section = (self.uc.ml or {}).get("ab_test") or {}
        policy = (self.env.policies or {}).get("ab_test") or {}
        feed = ((self.env.tenants or {}).get(self.uc.tenant) or {}).get("ab_test_feed") or {}
        feed_cfg = {"exposures": True, "key_secret": feed["key_secret"]} if (
            (section.get("publish") or {}).get("exposures") and policy.get("feed_exposures")
            and feed.get("key_secret")) else {}
        state = state_of(test)
        return {"source": "none", "features": [], "key": None, "usecase": self.uc.name,
                "gateway": {"predict_url": control.endpoints()["cluster"] + route, "auth": "none", "retries": 3},
                "ab_test": {"name": test["name"], "mode": state["mode"], "state": state["state"],
                            "treatment_bp": state["treatment_bp"], "release": test.get("release"),
                            "usecase": self.uc.name, "tenant": self.uc.tenant, "project": self.env.project,
                            "unit": test["unit"], "treatment": {"predict_url": t["service"] + t["route"],
                                                                "auth": "none"},
                            "treatment_deployed_model": None,
                            "breaker": ((section.get("guardrails") or {}).get("technical") or {}).get("breaker") or {},
                            "feed": feed_cfg,
                            "follow": {"bucket": self.env.artifacts_bucket, "object": state_object(test),
                                       "every": 30}}}

    def manifests(self, test: dict | None) -> list[tuple[str, str, dict | None]]:
        """(collection path, name, body) of the router's Deployment and Service and the arm's NetworkPolicy; the
        bodies only with a test."""
        import json

        from pipewright.models import kserve

        s = self._control().s
        ns, name = s["namespace"], self.router_name()
        uc_label = kserve._label(self.uc.name)
        paths = [(f"/apis/apps/v1/namespaces/{ns}/deployments", name),
                 (f"/api/v1/namespaces/{ns}/services", name),
                 (f"/apis/networking.k8s.io/v1/namespaces/{ns}/networkpolicies", f"{self.arm_name()}-only-router")]
        if test is None:
            return [(p, n, None) for p, n in paths]
        select = {"app": ROUTER, "pipewright.dev/usecase": uc_label}
        labels = {**select, "app.kubernetes.io/managed-by": "pipewright",
                  "pipewright.dev/ab-test": kserve._label(test["name"])}
        deployment = {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": ns, "labels": labels},
            "spec": {"replicas": 2, "selector": {"matchLabels": select},
                     "template": {"metadata": {"labels": labels}, "spec": {
                         "serviceAccountName": s["service_account"],
                         "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                             "seccompProfile": {"type": "RuntimeDefault"}},
                         "containers": [{
                             "name": "router", "image": self.env.launcher["image"],
                             "command": ["python", "-m", "pipewright.online.server"],
                             "env": [{"name": "PIPEWRIGHT_LOOKUP",
                                      "value": json.dumps(self.router_config(test), separators=(",", ":"))}],
                             "ports": [{"containerPort": 8080}],
                             "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 5},
                             "livenessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 20},
                             "resources": {"requests": {"cpu": "500m", "memory": "256Mi"},
                                           "limits": {"cpu": "1", "memory": "512Mi"}},
                             "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                 "capabilities": {"drop": ["ALL"]}}}]}}}}
        service = {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": ns, "labels": labels},
                   "spec": {"type": "ClusterIP", "selector": select, "ports": [{"port": 80, "targetPort": 8080}]}}
        policy = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                  "metadata": {"name": paths[2][1], "namespace": ns, "labels": labels},
                  "spec": {"podSelector": {"matchLabels": {"serving.kserve.io/inferenceservice": self.arm_name()}},
                           "policyTypes": ["Ingress"],
                           "ingress": [{"from": [{"podSelector": {"matchLabels": select}}]}]}}
        return [(p, n, b) for (p, n), b in zip(paths, (deployment, service, policy), strict=True)]

    def apply(self, test: dict) -> None:
        """The router brought up to date from the test (made at the start; afterwards it follows the state file, so
        a pause or a lower allocation only rewrites that file)."""
        from pipewright.kube import message

        write_state(self.gcp, self.env.artifacts_bucket, test)
        kube = self._control().kube
        made = False
        for path, name, body in self.manifests(test):
            r = kube.get(f"{path}/{name}")
            if r.status == 404:                  # what exists is left alone: replacing the Deployment would roll its
                r = kube.post(path, body)        # pods for a change the routers read from the state file anyway
                made = True                      # (found in the battle test: a pause rolled the router)
            if not r.ok:
                raise ConfigError(f"the A/B router's {body['kind']} {name}: {r.status} {message(r)}")
        if not made:
            s = state_of(test)
            self.log(f"the A/B router follows the state file within 30 seconds: {s['state']}, treatment "
                     f"{s['treatment_bp'] / 100:g}%")
            return
        self._wait_router(kube)
        self.log(f"the A/B router: POST {self.router_url()}/predict with {{\"instances\": [...], \"units\": [...]}} "
                 "(in the cluster)")

    def router_url(self) -> str:
        return f"http://{self.router_name()}.{self._control().s['namespace']}.svc.cluster.local"

    def _wait_router(self, kube, timeout: int = 300, sleep=None) -> None:
        import time

        path, name, _ = self.manifests(None)[0]
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = kube.get(f"{path}/{name}")
            st = ((r.body or {}).get("status") or {}) if r.ok else {}
            if (st.get("availableReplicas") or 0) >= 1 and st.get("updatedReplicas") == st.get("replicas"):
                return
            (sleep or time.sleep)(10)
        raise ConfigError(f"the A/B router {name} did not become ready in {timeout // 60} minutes: see its pods")

    def stop(self, test: dict) -> None:
        write_state(self.gcp, self.env.artifacts_bucket, test)      # every router sends everyone to the control now
        kube = self._control().kube
        for path, name, _ in self.manifests(None):
            kube.delete(f"{path}/{name}")
        arm = self._arm()
        if arm.current() is not None:
            arm.delete()
            self._event("model.undeployed", test, test["arms"]["treatment"]["version"], arm)
        self.log(f"removed the treatment's InferenceService {arm.s['namespace']}/{arm.name} and the A/B router")


def arms_for(uc, env, gcp, log=print):
    """The serving arm for the use case's target: an Agent Platform endpoint, or a KServe InferenceService."""
    target = (uc.model.get("serving") or {}).get("target", "vertex")
    return KServeArms(uc, env, gcp, log) if target == "kserve" else Arms(uc, env, gcp, log)
