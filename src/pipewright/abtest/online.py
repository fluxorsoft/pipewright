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
        """Before anything is approved or deployed: an online arm Pipewright can make here."""
        target = (self.uc.model.get("serving") or {}).get("target", "vertex")
        if target != "vertex":
            raise ConfigError(f"an online A/B test needs model.serving.target vertex: the gateway routes to Agent "
                              f"Platform endpoints, and {target} has no gateway yet. A batch test works here: set "
                              "model.serving.enabled false for the test's environment, or score in batch only")

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
