"""Rollback proposals (Pipewright 1.4.3, FR-98, decision kind rollback.proposal). Off unless
monitoring.health.propose_rollback_hours is above 0. Never automatic.

A critical serving.degraded within that many hours of the version's deployment opens a case at
gs://<artifacts>/cases/_rollback/<usecase>/<case_id>.json (the launcher writes cases/ only) and sends
rollback.proposed to the release's approver groups (and the governance, sre and product personas on Pub/Sub). A
person decides with `pipewright rollback approve | decline`; approving runs the existing path: the previous version
becomes the default through `model approve` (its gates and the released rules checked again) and is deployed.
One case per incident: a repeated message finds the case it opened.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pipewright.gcp import gcs_list, gcs_read, gcs_write

PREFIX = "cases/_rollback"
MODEL_SIGNALS = ("errors", "latency")     # what a version can cause; replicas and scaling are capacity
STARTUP_SECONDS = 3600                     # replicas missing this soon after a deploy: the version does not start


def case_name(usecase: str, case_id: str) -> str:
    return f"{PREFIX}/{usecase}/{case_id}.json"


def _when(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _deployed_at(contract, incident: dict, gcp) -> datetime | None:
    """When the incident's version started serving: the deployed model's createTime (Agent Platform); otherwise when
    the release went live."""
    if incident.get("endpoint") and incident.get("deployed_model"):
        r = gcp.get(f"https://{contract.region}-aiplatform.googleapis.com/v1/projects/{contract.project}/locations/"
                    f"{contract.region}/endpoints/{incident['endpoint']}")
        for dm in (r.body or {}).get("deployedModels") or [] if r.ok else []:
            if dm.get("id") == incident["deployed_model"]:
                return _when(dm.get("createTime"))
    return _when(contract.created_at)


def _previous(contract, version: str, gcp) -> str | None:
    """The version before the one serving (the highest lower version ID of the use case's model)."""
    name = (contract.metadata or {}).get("model")
    if not name or not str(version).isdigit():
        return None
    api = f"https://{contract.region}-aiplatform.googleapis.com/v1"
    found = gcp.get(f"{api}/projects/{contract.project}/locations/{contract.region}/models",
                    filter=f'display_name="{name}"')
    models = (found.body or {}).get("models") or [] if found.ok else []
    if not models:
        return None
    listed = gcp.get(f"{api}/{sorted(models, key=lambda m: m.get('createTime', ''))[0]['name']}:listVersions",
                     pageSize=100)
    ids = sorted(int(v["versionId"]) for v in (listed.body or {}).get("models") or [] if listed.ok
                 and str(v.get("versionId", "")).isdigit() and int(v["versionId"]) < int(version))
    return str(ids[-1]) if ids else None


def maybe_propose(contract, incident: dict, gcp, bucket: str, now: datetime | None = None) -> dict | None:
    ml = (contract.static_parameters or {}).get("ml_config") or {}
    hours = int(((ml.get("monitoring") or {}).get("health") or {}).get("propose_rollback_hours") or 0)
    if hours <= 0 or incident.get("state") != "open" or incident.get("severity") != "critical":
        return None
    now = now or datetime.now(timezone.utc)
    since = _deployed_at(contract, incident, gcp)
    if since is None or (now - since).total_seconds() > hours * 3600:
        return None
    if incident.get("signal") not in MODEL_SIGNALS and not (
            incident.get("signal") == "replicas" and (now - since).total_seconds() <= STARTUP_SECONDS):
        # GA QA: too few ready replicas or a stuck scale-up is capacity, not the version, and a rollback would not
        # help (serving.degraded is still sent); except replicas missing in the first hour after a deploy, which is
        # a version that does not start
        return None
    case_id = f"rb-{incident.get('incident_id') or now.strftime('%Y%m%d%H%M%S')}"[:60].replace(".", "-")
    name = case_name(contract.usecase, case_id)
    existing, _ = gcs_read(gcp, bucket, name)
    if existing:
        return None                                       # proposed already for this incident
    version = incident.get("model_version")
    previous = _previous(contract, version, gcp)
    if not previous:
        return None                     # GA QA: nothing earlier to go back to (a proposal nobody could act on)
    case = {"kind": "rollback.proposal", "case_id": case_id, "usecase": contract.usecase,
            "environment": contract.environment, "release": contract.release_id, "status": "open",
            "version": version, "previous_version": previous,
            "signal": incident.get("signal"), "threshold": incident.get("threshold"),
            "observed_value": incident.get("observed_value"), "incident_id": incident.get("incident_id"),
            "hours_since_deploy": round((now - since).total_seconds() / 3600, 2),
            "approvers": list(((ml.get("approvals") or {}).get("groups") or [])),
            "created_at": now.isoformat(), "decision": None}
    gcs_write(gcp, bucket, name, json.dumps(case, indent=2), if_generation_match=0)
    return {k: case[k] for k in ("case_id", "version", "previous_version", "signal", "threshold", "observed_value",
                                 "hours_since_deploy", "incident_id")}


def cases(gcp, bucket: str, usecase: str) -> list[dict]:
    out = []
    for n in gcs_list(gcp, bucket, f"{PREFIX}/{usecase}/"):
        text, _ = gcs_read(gcp, bucket, n)
        if text:
            out.append(json.loads(text))
    return sorted(out, key=lambda c: c.get("created_at") or "", reverse=True)


def decide(gcp, bucket: str, usecase: str, case_id: str, approve: bool, identity: str | None,
           reason: str = "") -> dict:
    """Records the decision on the case (generation-checked: two people cannot both decide)."""
    from pipewright.errors import ConfigError

    text, generation = gcs_read(gcp, bucket, case_name(usecase, case_id))
    if not text:
        raise ConfigError(f"no rollback case {case_id} for {usecase}")
    case = json.loads(text)
    if case["status"] != "open":
        by = (case.get("decision") or {}).get("by")
        raise ConfigError(f"rollback case {case_id} is {case['status']}, decided by {by}")
    case["status"] = "approved" if approve else "declined"
    case["decision"] = {"by": identity, "at": datetime.now(timezone.utc).isoformat(), "approved": approve,
                        "reason": reason}
    gcs_write(gcp, bucket, case_name(usecase, case_id), json.dumps(case, indent=2), if_generation_match=generation)
    return case
