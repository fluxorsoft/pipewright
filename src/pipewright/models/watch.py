"""What is in use, checked against its approval (Pipewright 1.4.1, FR-101 items 4 and 6).

`in_use` finds every version an environment depends on: each model's default (what scoring uses), every version
deployed on an Agent Platform endpoint, and every KServe InferenceService (with the approved copy its pods load).
`check` verifies each against its newest approval (`integrity.verify`). The launcher's watch runs it on its tick and
records `model.integrity` when a result changes, plus a `model.integrity.check` heartbeat each run; `pipewright
integrity check` runs it once with an exit code for CI.

Severity (spike findings, section 8.18): KServe loads its files at every pod start and scoring loads them every run,
so a change there is an `incident`; an Agent Platform endpoint serves the copy it took at registration, so a change
in the bucket is `drift` until the next promote or GKE deploy. Reads metadata only; messages name use cases,
versions, files, digests and endpoints, never a value.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from pipewright.errors import ConfigError

UNCHANGED, CHANGED, UNAPPROVED, CANNOT = "unchanged", "changed", "unapproved", "cannot_check"


@dataclass
class Use:
    usecase: str
    model: str
    version: str
    where: str                        # default | endpoint:<display name> | kserve:<namespace>/<name>
    kind: str                         # default | vertex | kserve
    resource: str | None = None
    artifact_uri: str | None = None
    image: str | None = None
    traffic: int | None = None
    copy: dict | None = None          # KServe: the approved copy its pods check (folder, files)
    status: str = UNCHANGED
    severity: str | None = None
    problems: list = field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.usecase}|{self.where}|v{self.version}"

    def record(self) -> dict:
        out = {k: v for k, v in asdict(self).items() if k not in ("copy", "image") and v not in (None, [], "")}
        out["problems"] = self.problems[:5]
        return out


def _api(env) -> str:
    return f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"


def _usecase_of(labels: dict, models: dict | None, display: str) -> str | None:
    if models and display in models:
        return models[display]
    return (labels or {}).get("usecase") or (labels or {}).get("pipewright-usecase")


def in_use(gcp, env, models: dict | None = None, usecases: set | None = None) -> tuple[list[Use], list[str]]:
    """(uses, notes). models: {model display name: use case} (the launcher passes its live releases'); without it the
    registry labels name the use case. usecases: only these."""
    uses, notes, by_resource = [], [], {}
    token, listed = None, []
    while True:
        r = gcp.get(f"{_api(env)}/models", pageSize=100, **({"pageToken": token} if token else {}))
        if not r.ok:
            raise ConfigError(f"Model Registry: HTTP {r.status}")
        listed += (r.body or {}).get("models") or []
        token = (r.body or {}).get("nextPageToken")
        if not token:
            break
    for m in listed:
        display = m.get("displayName")
        uc = _usecase_of(m.get("labels"), models, display)
        base = m["name"].split("@")[0]
        by_resource[base] = (uc, display)
        if not uc or (usecases and uc not in usecases):
            continue
        d = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{base}@default")
        if not d.ok:
            notes.append(f"{display}: its default version cannot be read (HTTP {d.status})")
            continue
        b = d.body or {}
        uses.append(Use(uc, display, str(b.get("versionId")), "default", "default", b.get("name"),
                        b.get("artifactUri"), (b.get("containerSpec") or {}).get("imageUri")))
    r = gcp.get(f"{_api(env)}/endpoints", pageSize=100)
    for ep in ((r.body or {}).get("endpoints") or []) if r.ok else []:
        split = ep.get("trafficSplit") or {}
        for dm in ep.get("deployedModels") or []:
            base = (dm.get("model") or "").split("@")[0]
            uc, display = by_resource.get(base, (None, None))
            if not uc or (usecases and uc not in usecases):
                continue
            vid = str(dm.get("modelVersionId") or "")
            v = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{base}@{vid}")
            b = v.body or {} if v.ok else {}
            uses.append(Use(uc, display, vid, f"endpoint:{ep.get('displayName')}", "vertex", base,
                            b.get("artifactUri"), (b.get("containerSpec") or {}).get("imageUri"),
                            int(split.get(dm.get("id"), 0))))
    if not r.ok:
        notes.append(f"endpoints cannot be listed (HTTP {r.status})")
    namespaces = ((getattr(env, "kserve", None) or {}).get("namespaces") or {})
    if namespaces:
        try:
            from pipewright.kube import KubeClient

            kube = KubeClient.from_manifest({"kubernetes": env.kubernetes}, gcp, dns=True)
            for ns in namespaces.values():
                kr = kube.get(f"/apis/serving.kserve.io/v1beta1/namespaces/{ns}/inferenceservices")
                if not kr.ok:
                    notes.append(f"KServe services in {ns} cannot be listed ({kr.status})")
                    continue
                for isvc in (kr.body or {}).get("items") or []:
                    uses.append(_kserve_use(isvc, ns, by_resource, usecases, env.kserve.get("mirror")))
            uses = [u for u in uses if u is not None]
        except Exception as e:  # noqa: BLE001 - the cluster may be unreachable from here: said, not hidden
            notes.append(f"KServe cannot be checked from here: {str(e).splitlines()[0][:160]}")
    return uses, notes


def _kserve_use(isvc: dict, ns: str, by_resource: dict, usecases: set | None,
                mirror: str | None = None) -> Use | None:
    meta = isvc.get("metadata") or {}
    ann, labels = meta.get("annotations") or {}, meta.get("labels") or {}
    resource = ann.get("pipewright.dev/model-resource") or ""
    base, _, vid = resource.partition("@")
    uc, display = by_resource.get(base, (labels.get("pipewright.dev/usecase"), labels.get("pipewright.dev/model")))
    if not uc or (usecases and uc not in usecases):
        return None
    pred = (isvc.get("spec") or {}).get("predictor") or {}
    env_vars = {e["name"]: e.get("value") for c in pred.get("containers") or [] for e in c.get("env") or []}
    copy = None
    for c in pred.get("initContainers") or []:
        for e in c.get("env") or []:
            if e.get("name") == "PIPEWRIGHT_APPROVED_FILES":
                copy = json.loads(e["value"])
    from pipewright.models.kserve import mirrored_from

    image = mirrored_from(((pred.get("containers") or [{}])[0]).get("image"), mirror)
    return Use(uc, display or labels.get("pipewright.dev/model", ""), vid, f"kserve:{ns}/{meta.get('name')}",
               "kserve", base, env_vars.get("AIP_STORAGE_URI"), image, 100, copy)


def _copy_problems(gcp, copy: dict) -> list[str]:
    """The KServe copy's live files are still the generations the deploy recorded (metadata only: a generation never
    changes its bytes, and the pods check SHA-256 themselves at start)."""
    from urllib.parse import quote

    bucket, _, prefix = copy["folder"].removeprefix("gs://").partition("/")
    out = []
    for f in copy["files"]:
        name = quote(prefix + "/" + f["file"], safe="")
        r = gcp.get(f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{name}")
        if not r.ok:
            out.append(f"serving copy {f['file']}: missing (HTTP {r.status})")
        elif str(r.body.get("generation")) != str(f["generation"]):
            out.append(f"serving copy {f['file']}: generation {r.body.get('generation')} is not the approved copy "
                       f"{f['generation']}: the next pod will not start")
    return out


def check(gcp, env, models: dict | None = None, usecases: set | None = None) -> tuple[list[Use], list[str]]:
    """Every use, verified against its newest approval."""
    from pipewright.models import integrity

    uses, notes = in_use(gcp, env, models, usecases)
    for u in uses:
        try:
            if u.kind == "kserve" and u.copy:
                c = integrity.verify(gcp, env, u.usecase, u.version, None, u.image)
                c.problems += _copy_problems(gcp, u.copy)
            else:
                c = integrity.verify(gcp, env, u.usecase, u.version, u.artifact_uri, u.image)
        except Exception as e:  # noqa: BLE001 - a check that cannot run is said, never counted as unchanged
            u.status, u.problems = CANNOT, [str(e).splitlines()[0][:200]]
            continue
        missing = [p for p in c.problems if "no model.approved event" in p]
        if missing:
            u.status, u.problems = UNAPPROVED, missing if u.kind == "default" else [
                f"v{u.version} serves on {u.where.split(':', 1)[-1]} with no model.approved event: deployed outside "
                "Pipewright (the console, gcloud or the API), so nobody approved it"]
        elif c.problems:
            u.status, u.problems = CHANGED, c.problems
        if u.status != UNCHANGED:
            u.severity = "drift" if u.kind == "vertex" and u.status == CHANGED else "incident"
    return uses, notes


def exit_code(uses: list[Use]) -> int:
    """0 every approval unchanged; 1 any changed or unapproved; 2 something could not be checked."""
    if any(u.status in (CHANGED, UNAPPROVED) for u in uses):
        return 1
    return 2 if any(u.status == CANNOT for u in uses) else 0


# -- the launcher's watch: events only when a result changes, and a heartbeat ---------------------------------------

def transitions(previous: dict, uses: list[Use]) -> list[dict]:
    """model.integrity payloads for what changed since the last run: changed, unapproved, restored, gone."""
    out, now = [], {u.key: u for u in uses}
    for key, u in now.items():
        was = (previous.get(key) or {}).get("status")
        if u.status == was or (was is None and u.status == UNCHANGED):
            continue
        status = "restored" if u.status == UNCHANGED else u.status
        if u.status == CANNOT and was in (None, UNCHANGED):
            status = CANNOT
        out.append({**u.record(), "status": status, "previous": was or "new"})
    return out


def run(gcp, env, state: dict, models: dict | None = None) -> dict:
    """One watch run: check, compare with `state` (the last results, updated in place) and return what to record."""
    uses, notes = check(gcp, env, models)
    changes = transitions(state.get("results") or {}, uses)
    state["results"] = {u.key: {"status": u.status} for u in uses}
    counts = {s: sum(1 for u in uses if u.status == s) for s in (UNCHANGED, CHANGED, UNAPPROVED, CANNOT)}
    return {"changes": changes, "heartbeat": {"checked": len(uses), **counts, "notes": notes[:10]},
            "uses": [u.record() for u in uses]}
