"""The launcher's side of Ray clusters: create and delete a run's cluster for a step, and clean up after killed steps.

Why the launcher: a cluster runs as the tenant's runner identity, and creating it needs permission to act as that
identity. The launcher already has it (it submits every pipeline job as the runner); the runner does not have it on
itself. So a step asks the launcher, and nothing new is granted to tenants.

What a step can get is fixed by its release, not by the request: the cluster's size comes from the live contract
(`ray.steps`, checked against the platform's limits when it was released) and the image is the release's Ray image.
The request only names the use case, the run and the step, and must come from the use case's runner identity.

Every cluster carries labels (use case, tenant, run, step, release, a deadline), so cleanup finds clusters whose run
has ended or whose deadline has passed even when the step that made them was killed.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from pipewright.ray_clusters.settings import cluster_id, platform_settings
from pipewright.release.contract import ReleaseContract

log = logging.getLogger("pipewright.launcher")

MANAGED = {"managed-by": "pipewright", "pipewright-ray": "per-run"}
GRACE_MINUTES = 30               # beyond a cluster's own max_minutes before cleanup removes it regardless
TERMINAL = {"PIPELINE_STATE_SUCCEEDED", "PIPELINE_STATE_FAILED", "PIPELINE_STATE_CANCELLED"}


class RayRequestError(Exception):
    """A request that can never succeed (bad step, wrong caller): reported, not retried."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _api(region: str) -> str:
    return f"https://{region}-aiplatform.googleapis.com/v1"


def _label(value) -> str:
    """A valid label value: lowercase letters, digits, - and _, at most 63 characters."""
    out = "".join(c if c.isalnum() or c in "-_" else "-" for c in str(value).lower())
    return out[:63]


def cluster_body(contract: ReleaseContract, step: str, run_id: str, settings: dict, security: dict | None,
                 now: float | None = None) -> dict:
    """The PersistentResource for a step's cluster (the REST form of vertex_ray.create_ray_cluster, plus CMEK,
    which the SDK does not expose)."""
    ray = contract.ray
    spec = ray["steps"][step]
    image = ray["image"]
    accelerator = spec.get("accelerator") or {}

    def pool(pool_id: str, machine: str, count: int, with_accelerator: bool) -> dict:
        machine_spec = {"machineType": machine}
        if with_accelerator and accelerator:
            machine_spec.update({"acceleratorType": accelerator["type"], "acceleratorCount": int(accelerator["count"])})
        return {"id": pool_id, "replicaCount": str(count), "machineSpec": machine_spec,
                "diskSpec": {"bootDiskType": "pd-ssd", "bootDiskSizeGb": 100}}

    workers = int(spec["workers"])
    same = spec["worker_machine"] == spec["head_machine"] and not accelerator
    if workers == 0 or same:
        # Identical machines go in one pool: the head is its first node (as the SDK does).
        pools = [pool("head-node", spec["head_machine"], 1 + workers, workers == 0)]
    else:
        pools = [pool("head-node", spec["head_machine"], 1, False),
                 pool("worker-pool1", spec["worker_machine"], workers, True)]
    deadline = int((now or time.time()) + 60 * (int(spec["max_minutes"]) + GRACE_MINUTES))
    body = {
        "displayName": f"{contract.usecase} {step} {run_id}"[:128],
        "resourcePools": pools,
        "resourceRuntimeSpec": {
            "raySpec": {"resourcePoolImages": {p["id"]: image for p in pools}},
            "serviceAccountSpec": {"enableCustomServiceAccount": True,
                                   "serviceAccount": contract.runner_service_account},
        },
        "labels": {**MANAGED, "pipewright-usecase": _label(contract.usecase),
                   "pipewright-tenant": _label(contract.tenant), "run-id": _label(run_id),
                   "pipewright-step": _label(step), "pipewright-release": _label(contract.release_id),
                   "pipewright-deadline": str(deadline)},
    }
    if settings.get("network_attachment"):
        body["pscInterfaceConfig"] = {"networkAttachment": settings["network_attachment"]}
    if (security or {}).get("kms_key"):
        body["encryptionSpec"] = {"kmsKeyName": security["kms_key"]}
    return body


class RayClusters:
    def __init__(self, gcp, manifest_getter):
        self.gcp = gcp
        self.manifest = manifest_getter

    def settings(self, contract: ReleaseContract) -> dict:
        manifest = self.manifest()
        tenant = (manifest.get("tenants") or {}).get(contract.tenant) or {}
        return platform_settings(manifest.get("ray"), tenant.get("ray"),
                                 private=bool((manifest.get("security") or {}).get("network")))

    def _check(self, contract: ReleaseContract, step: str, caller: str | None) -> None:
        if caller is not None and caller.lower() != contract.runner_service_account.lower():
            raise RayRequestError(f"{caller or 'an unidentified caller'} may not ask for {contract.usecase}'s Ray "
                                  "clusters: only its runner identity may", 403)
        if not contract.ray or step not in (contract.ray.get("steps") or {}):
            raise RayRequestError(f"step {step!r} of {contract.usecase} release {contract.release_id} has no Ray "
                                  "cluster (steps.<step>.ray)")

    def create(self, contract: ReleaseContract, step: str, run_id: str, caller: str | None) -> dict:
        self._check(contract, step, caller)
        if not run_id:
            raise RayRequestError("run_id is required")
        settings = self.settings(contract)
        if not settings.get("enabled", True):
            raise RayRequestError(f"Ray clusters are not enabled for tenant {contract.tenant} in this environment")
        if settings["mode"] != "per_run":
            raise RayRequestError(f"Ray mode {settings['mode']} is not supported")
        cid = cluster_id(contract.usecase, run_id, step)
        parent = f"projects/{contract.project}/locations/{contract.region}"
        body = cluster_body(contract, step, run_id, settings, self.manifest().get("security"))
        r = self.gcp.post(f"{_api(contract.region)}/{parent}/persistentResources", body, persistentResourceId=cid)
        name = f"{parent}/persistentResources/{cid}"
        if r.status == 409:
            return {"cluster": name, "status": "exists"}
        if not r.ok:
            message = (r.body.get("error") or {}).get("message", "") if isinstance(r.body, dict) else str(r.body)
            if r.status == 400:            # the API refused the cluster itself: retrying cannot help
                raise RayRequestError(f"creating {cid} was refused: {message}"[:500], 400)
            raise RuntimeError(f"creating {cid} failed: {r.status} {message}"[:500])
        log.info("ray cluster %s for %s %s (run %s)", cid, contract.usecase, step, run_id)
        # What the cluster was made with: the launcher records it in a ray.cluster.created event (evidence).
        record = {"cluster": name, "step": step, "image": contract.ray["image"],
                  "service_account": body["resourceRuntimeSpec"]["serviceAccountSpec"]["serviceAccount"],
                  "kms_key": (body.get("encryptionSpec") or {}).get("kmsKeyName"),
                  "network_attachment": (body.get("pscInterfaceConfig") or {}).get("networkAttachment"),
                  "pools": [{"machine": p["machineSpec"]["machineType"], "nodes": int(p["replicaCount"])}
                            for p in body["resourcePools"]]}
        return {"cluster": name, "status": "creating", "record": record}

    def delete(self, contract: ReleaseContract, step: str, run_id: str, caller: str | None) -> dict:
        self._check(contract, step, caller)
        cid = cluster_id(contract.usecase, run_id, step)
        name = f"projects/{contract.project}/locations/{contract.region}/persistentResources/{cid}"
        r = self.gcp.delete(f"{_api(contract.region)}/{name}")
        if r.status == 404:
            return {"cluster": name, "status": "gone"}
        if not r.ok:
            raise RuntimeError(f"deleting {cid} failed: {r.status}")
        log.info("ray cluster %s deleted", cid)
        return {"cluster": name, "status": "deleting"}

    # ---------------------------------------------------------------- cleanup (the launcher's tick)

    def _clusters(self, project: str, region: str) -> list[dict]:
        out, token = [], None
        while True:
            params = {"pageSize": 100, **({"pageToken": token} if token else {})}
            r = self.gcp.get(f"{_api(region)}/projects/{project}/locations/{region}/persistentResources", **params)
            if not r.ok:
                raise RuntimeError(f"listing Ray clusters failed: {r.status}")
            out += [p for p in r.body.get("persistentResources", [])
                    if all((p.get("labels") or {}).get(k) == v for k, v in MANAGED.items())]
            token = r.body.get("nextPageToken")
            if not token:
                return out

    def _run_state(self, project: str, region: str, run_id: str) -> str | None:
        r = self.gcp.get(f"{_api(region)}/projects/{project}/locations/{region}/pipelineJobs",
                         filter=f'labels.run-id="{run_id}"', pageSize=5)
        jobs = r.body.get("pipelineJobs", []) if r.ok else []
        if not r.ok:
            return "unknown"
        return jobs[0].get("state") if jobs else None

    def reap(self, project: str, region: str, now: float | None = None) -> list[dict]:
        """Deletes clusters whose run has ended, or whose deadline has passed. Never fails the tick."""
        now = now or time.time()
        results = []
        try:
            clusters = self._clusters(project, region)
        except Exception as e:  # noqa: BLE001
            log.warning("ray cleanup skipped: %s", e)
            return [{"status": "skipped", "reason": str(e)[:200]}]
        for c in clusters:
            labels = c.get("labels") or {}
            if c.get("state") in ("STOPPING",):
                continue
            deadline = int(labels.get("pipewright-deadline") or 0)
            state = self._run_state(project, region, labels.get("run-id", ""))
            reason = None
            if deadline and now > deadline:
                reason = "past its deadline"
            elif state in TERMINAL:
                reason = f"its run ended ({state.removeprefix('PIPELINE_STATE_').lower()})"
            elif state is None and _age_minutes(c, now) > GRACE_MINUTES:
                reason = "no pipeline run has its run ID"
            if not reason:
                continue
            r = self.gcp.delete(f"{_api(region)}/{c['name']}")
            results.append({"cluster": c["name"].rsplit("/", 1)[-1], "reason": reason,
                            "status": "deleted" if r.ok or r.status == 404 else f"error {r.status}"})
            log.info("ray cleanup: %s %s", c["name"], reason)
        return results


def _age_minutes(cluster: dict, now: float) -> float:
    created = cluster.get("createTime")
    if not created:
        return 0.0
    try:
        t = datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    return (now - t.replace(tzinfo=t.tzinfo or timezone.utc).timestamp()) / 60
