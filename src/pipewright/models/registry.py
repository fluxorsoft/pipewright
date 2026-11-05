"""pipewright model list / approve: the Model Registry versions of a use case's model.

The default version is the champion: inference pipelines score with it and training compares challengers to it.
With registry.set_default: manual, training registers candidates and `model approve` makes one the default,
following approvals.promote_model (manual needs --approve, and a listed user approver). A version becomes the
default only when its evaluation record passes the use case's gates, recomputed here (models/gates.py).
"""

from __future__ import annotations

from dataclasses import dataclass

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.models import gates
from pipewright.models.deploy import check_approval


@dataclass
class Version:
    version_id: str
    default: bool
    aliases: list[str]
    description: str
    created: str
    labels: dict
    gates: str = ""          # "pass", "FAIL: <first problem>", "no record" or "" (not checked)


def _resource(uc: ResolvedUseCase, env: Environment, creds) -> str:
    from google.cloud import aiplatform

    aiplatform.init(project=env.project, location=env.region, credentials=creds)
    models = aiplatform.Model.list(filter=f'display_name="{uc.model["name"]}"')
    if not models:
        raise ConfigError(f"no model {uc.model['name']!r} in the registry yet: run the training pipeline first")
    return models[0].resource_name


def _registry(uc: ResolvedUseCase, env: Environment, creds):
    from google.cloud.aiplatform.models import ModelRegistry

    return ModelRegistry(model=_resource(uc, env, creds))


def artifact_uri(resource: str, version: str, uc: ResolvedUseCase | None = None) -> str | None:
    from google.cloud import aiplatform

    model = aiplatform.Model(model_name=f"{resource}@{version}")
    return record_location(uc, model.uri, getattr(model, "labels", None))


def record_location(uc: ResolvedUseCase | None, uri: str | None, labels: dict | None) -> str | None:
    """Where a version's evaluation record is: next to its artifacts, or, for a BigQuery ML version (no artifacts:
    the model stays in BigQuery), in the run's folder of the use case's bqml records, named by its run label."""
    if uri:
        return uri
    run = (labels or {}).get("pipewright-run")
    records = ((uc.ml if uc else {}) or {}).get("bqml", {}).get("records")
    return f"{records}{run}/" if run and records else None


def gate_problems(uc: ResolvedUseCase, creds, resource: str, version, earlier_versions: bool) -> list[str]:
    """earlier_versions: a version older than this one exists, so a champion comparison must be recorded."""
    from pipewright.gcp import Gcp

    return gates.verify(Gcp(creds), artifact_uri(resource, str(version), uc), uc.ml["evaluation"], earlier_versions)


def version_labels(env: Environment, creds, resource: str) -> dict[str, dict]:
    """{version: labels}: the SDK's version listing leaves labels out; the REST listing has them."""
    from pipewright.gcp import Gcp

    try:
        resp = Gcp(creds).get(f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}:listVersions",
                              pageSize=100)
    except Exception:  # noqa: BLE001 - labels are a nicety for the listing
        return {}
    if not resp.ok:
        return {}
    return {str(m.get("versionId")): m.get("labels") or {} for m in (resp.body or {}).get("models", [])}


def _number(version_id) -> int:
    return int(version_id) if str(version_id).isdigit() else 0


def versions(uc: ResolvedUseCase, env: Environment, creds, check_gates: bool = True, limit: int = 10) -> list[Version]:
    from google.cloud.aiplatform.models import ModelRegistry

    resource = _resource(uc, env, creds)
    out = []
    labelled = version_labels(env, creds, resource)
    for v in ModelRegistry(model=resource).list_versions():
        aliases = list(v.version_aliases or [])
        out.append(Version(str(v.version_id), "default" in aliases, [a for a in aliases if a != "default"],
                           v.version_description or "", str(v.version_create_time)[:19],
                           labelled.get(str(v.version_id)) or dict(getattr(v, "labels", None) or {})))
    out.sort(key=lambda v: _number(v.version_id), reverse=True)
    oldest = min((_number(v.version_id) for v in out), default=0)
    for v in out[:limit] if check_gates and gates.needs_record(uc.ml["evaluation"]) else []:
        try:
            problems = gate_problems(uc, creds, resource, v.version_id, _number(v.version_id) > oldest)
        except Exception as e:  # noqa: BLE001 - listing must not fail because one record cannot be read
            v.gates = f"not checked: {type(e).__name__}"
            continue
        v.gates = "pass" if not problems else ("no record" if problems[0].startswith("no evaluation record")
                                               else f"FAIL: {problems[0]}")
    return out


def render(uc: ResolvedUseCase, rows: list[Version]) -> str:
    lines = [f"{uc.model['name']} ({uc.environment})"]
    for v in rows:
        status = "DEFAULT" if v.default else v.labels.get("status", "")
        gate = f"  [gates {v.gates}]" if v.gates else ""
        unapproved = "  (not approved: `pipewright model approve`)" if v.default and pending_approval(v.labels) else ""
        lines.append(f"  v{v.version_id:<4} {status:<10} {v.created}  {v.description}{gate}{unapproved}")
    return "\n".join(lines)


# FR-83 (Pipewright 1.2.0): Model Registry makes a model's first version its default by itself. Under a manual policy
# (registry.set_default manual, from approvals.promote_model manual) the register step labels that version
# approval=pending: it is the default, but nobody approved it. `pipewright model approve` records the approval;
# until then `model list` says so, `doctor` warns and `deploy-model` refuses it. Scoring is unchanged.
APPROVAL_LABEL = "approval"


def pending_approval(labels: dict | None) -> bool:
    return (labels or {}).get(APPROVAL_LABEL) == "pending"


def _evaluated(creds, resource: str, version: str, uc: ResolvedUseCase) -> dict:
    """The approval's `evaluation` field: the record the gates were checked against, by digest (Pipewright 1.3.1).
    Empty when the version has no record or it cannot be read: the approval itself never fails for it."""
    from pipewright.gcp import Gcp

    try:
        found = gates.record_digest(Gcp(creds), artifact_uri(resource, str(version), uc))
    except Exception:  # noqa: BLE001 - the digest adds to the trail; reading it must not block an approval
        return {}
    return {"evaluation": found} if found else {}


def _judged(uc: ResolvedUseCase, env: Environment, creds, resource: str, version: str, log=print):
    """What an approval is judged on and by (phase 6): the released rules, the evaluation record, every model file
    and the serving image. Returns (rules, record for the model.approved event, the version's record folder)."""
    import json

    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    gcp = Gcp(creds)
    body = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}@{version}").body or {}
    labels = body.get("labels") or {}
    folder = record_location(uc, body.get("artifactUri"), labels)
    lineage = {}
    if folder and folder.startswith("gs://"):
        from pipewright.gcp import gcs_read

        bucket, _, prefix = folder.removeprefix("gs://").partition("/")
        try:
            text, _ = gcs_read(gcp, bucket, f"{prefix.rstrip('/')}/lineage.json".lstrip("/"))
            lineage = json.loads(text) if text else {}
        except Exception:  # noqa: BLE001 - no lineage record: the release comes from the version's label
            lineage = {}
    rules = integrity.released_rules(uc, env, gcp, lineage.get("release") or labels.get("release"))
    record = {**_evaluated(creds, resource, version, uc), "rules": rules.record()}
    try:
        files = integrity.model_files(gcp, body.get("artifactUri"))
        image = integrity.serving_image(gcp, (body.get("containerSpec") or {}).get("imageUri"))
    except ConfigError as e:
        if integrity.policy(env)["mode"] != "warn":
            raise ConfigError(str(e), source=f"v{version} cannot be approved: its model files cannot be "
                                             "fingerprinted, so the approval could not show what it approved") from None
        log(f"integrity WARNING (policies.integrity.mode warn): the model files were not fingerprinted: {e}")
        files, image = None, None
    if files:
        record["files"] = files
    if image:
        record["serving_image"] = image
    return rules, record, folder


def _check_rules(uc: ResolvedUseCase, env: Environment, rules, identity: str | None, approved: bool,
                 version: str, log=print) -> ResolvedUseCase:
    """FR-96: the approvers and gates are the released ones. Returns the use case as released, for the gate check."""
    import dataclasses

    from pipewright.models import integrity

    if rules.note:
        log(f"integrity: {rules.note}")
    if rules.differs:
        check = integrity.Check(problems=[
            f"the working copy's {key} differs from release {rules.release}'s, which trained v{version}: approve with "
            "the rules it was built with (check out the release's commit), or release the change and retrain"
            for key in rules.differs])
        integrity.enforce(check, env, f"v{version} of {uc.model['name']}", log)
    changes = {"approvals": rules.approvals, "ml": {**uc.ml, "evaluation": rules.evaluation}}
    if dataclasses.is_dataclass(uc):
        released = dataclasses.replace(uc, **changes)
    else:                                   # a stand-in in tests
        from types import SimpleNamespace

        released = SimpleNamespace(**{**vars(uc), **changes})
    check_approval(released, identity, approved, key="promote_model")
    return released


def _emit_approval(uc, env, creds, version: str, identity, record: dict, **extra) -> None:
    import io

    from pipewright.gcp import Gcp
    from pipewright.observability import events

    events.emit(events.event("model.approved", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                             version=version, by=identity, **extra, **record),
                env.observability.get("events_topic"), Gcp(creds), stream=io.StringIO(), region=env.region)


def _approve_first_version(uc: ResolvedUseCase, env: Environment, creds, resource: str, version: str,
                           identity: str | None, log=print, judged=None) -> None:
    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    gcp = Gcp(creds)
    rules, record, folder = judged or _judged(uc, env, creds, resource, version, log)
    url = f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}@{version}"
    current = gcp.get(url)
    labels = {**((current.body or {}).get("labels") or {}), APPROVAL_LABEL: "approved"}
    resp = gcp.patch(url, {"labels": labels}, updateMask="labels")
    if not resp.ok:
        raise ConfigError(f"recording the approval of v{version}: HTTP {resp.status}")
    locked = integrity.lock(gcp, env, record.get("files"), log)
    _emit_approval(uc, env, creds, version, identity, {**record, **({"lock": locked} if locked else {})},
                   previous=None, first_version=True)
    log(f"{uc.model['name']}: v{version}, the first version and already the default, is approved"
        + (f" by {identity}" if identity else ""))


def approve(uc: ResolvedUseCase, env: Environment, creds, version: str, identity: str | None, approved: bool,
            log=print) -> None:
    from google.cloud.aiplatform.models import ModelRegistry

    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    resource = _resource(uc, env, creds)
    registry = ModelRegistry(model=resource)
    known = {str(v.version_id): v for v in registry.list_versions()}
    version = version.removeprefix("v")
    if version not in known:
        raise ConfigError(f"{uc.model['name']} has no version {version} (versions: {', '.join(sorted(known))})")
    first = "default" in (known[version].version_aliases or [])
    if first and not pending_approval(version_labels(env, creds, resource).get(version)):
        log(f"v{version} of {uc.model['name']} is already the default")
        return
    # Phase 6 (FR-96): judged by the rules of the release that trained it, not the working copy's.
    rules, record, folder = _judged(uc, env, creds, resource, version, log)
    released = _check_rules(uc, env, rules, identity, approved, version, log)
    # Pipewright recomputes the gates from the version's evaluation record; it does not trust the pipeline.
    earlier = any(_number(k) < _number(version) for k in known)
    problems = gates.verify(Gcp(creds), folder, released.ml["evaluation"], earlier)
    if problems:
        raise ConfigError(problems, source=f"v{version} of {uc.model['name']} cannot become the default: it does "
                                           f"not pass {uc.name}'s evaluation gates (release {rules.release or '?'})")
    if first:
        _approve_first_version(uc, env, creds, resource, version, identity, log, (rules, record, folder))
        return
    previous = next((k for k, v in known.items() if "default" in (v.version_aliases or [])), None)
    # Moving the `default` alias is what makes a version the default.
    registry.add_version_aliases(new_aliases=["default"], version=version)
    locked = integrity.lock(Gcp(creds), env, record.get("files"), log)
    _emit_approval(uc, env, creds, version, identity, {**record, **({"lock": locked} if locked else {})},
                   previous=previous)
    online = not ((uc.ml or {}).get("bqml") or {}).get("forecast")       # a forecast is never served online
    log(f"{uc.model['name']}: default v{previous} -> v{version}"
        + (f" (approved by {identity})" if identity else "")
        + ". Inference uses it from the next run"
        + ("; `pipewright deploy-model` serves it online." if online else "."))
