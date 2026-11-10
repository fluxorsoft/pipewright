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
from pipewright.models.names import oldest


@dataclass
class Version:
    version_id: str
    default: bool
    aliases: list[str]
    description: str
    created: str
    labels: dict
    gates: str = ""          # "pass", "FAIL: <first problem>", "no record" or "" (not checked)
    approved: bool | None = None   # the default only: False when the event log has no model.approved for it


def _resource(uc: ResolvedUseCase, env: Environment, creds) -> str:
    from google.cloud import aiplatform

    aiplatform.init(project=env.project, location=env.region, credentials=creds)
    models = oldest(aiplatform.Model.list(filter=f'display_name="{uc.model["name"]}"'))
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


def _kfp_run_started(env, gcp, run: str) -> str:
    """When the Kubeflow run with this run ID was created ("" when not found), through the cluster's API as the
    caller (the DNS endpoint from outside the VPC, as Fleet View reads it)."""
    from pipewright.adapters.kfp import api_path, run_labels
    from pipewright.kube import KubeClient

    kube = KubeClient.from_manifest({"kubernetes": env.kubernetes}, gcp, dns=True)
    namespace = env.kubernetes.get("namespace") or "kubeflow"
    r = kube.get(f"{api_path(namespace)}/runs", page_size=100, sort_by="created_at desc")
    for item in ((r.body or {}).get("runs") or []) if r.ok else []:
        if run_labels(item.get("description")).get("run-id") == run:
            return str(item.get("created_at") or "")
    return ""


def champion_expected(env: Environment, creds, resource: str, version: str, known) -> bool:
    """Whether a champion existed when this version was trained, so its record must hold a comparison: a version
    registered before this version's training run started (FR-124, found in the QA battle test: two runs trained in
    parallel, neither had a champion, and the second to register was refused for not comparing with the first).
    Where the run's start cannot be found (a Kubeflow run, a version without a run label), any lower-numbered version
    counts, as before."""
    from pipewright.gcp import Gcp

    earlier = [k for k in known if _number(k) < _number(version)]
    if not earlier:
        return False
    try:
        gcp = Gcp(creds)
        api = f"https://{env.region}-aiplatform.googleapis.com/v1"
        listed = gcp.get(f"{api}/{resource}:listVersions", pageSize=100)
        rows = {str(v.get("versionId")): v for v in ((listed.body or {}).get("models") or [])} if listed.ok else {}
        labels = (rows.get(str(version)) or {}).get("labels") or {}
        run = labels.get("pipewright-run") or str(labels.get("run_id") or "").removeprefix("run-")
        if not run:
            return True
        jobs = gcp.get(f"{api}/projects/{env.project}/locations/{env.region}/pipelineJobs",
                       filter=f'labels."run-id"="{run}"', pageSize=5)
        found = ((jobs.body or {}).get("pipelineJobs") or []) if jobs.ok else []
        started = min((j.get("createTime") or "" for j in found), default="")
        if not started and getattr(env, "kubernetes", None):
            started = _kfp_run_started(env, gcp, run)        # orchestrator kfp (found in the QA battle test in ent)
        if not started:
            return True
        return any(str((rows.get(k) or {}).get("versionCreateTime") or "") < started for k in earlier)
    except Exception:  # noqa: BLE001 - the stricter rule when the run cannot be read
        return True


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
    default = next((v for v in out if v.default), None)
    if check_gates and default and not pending_approval(default.labels):
        default.approved = approval_on_record(uc, env, creds, default.version_id)
    for v in out[:limit] if check_gates and gates.needs_record(uc.ml["evaluation"]) else []:
        try:
            problems = gate_problems(uc, creds, resource, v.version_id,
                                     _number(v.version_id) > oldest and champion_expected(
                                         env, creds, resource, v.version_id, {x.version_id: x for x in out}))
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
        if status == "champion":          # a former default keeps the label its register step gave it (found
            status = ""                   # re-running the book on 1.4.2): the champion is the default, shown above
        gate = f"  [gates {v.gates}]" if v.gates else ""
        unapproved = "  (not approved: `pipewright model approve`)" if v.default and unapproved_default(v) else ""
        lines.append(f"  v{v.version_id:<4} {status:<10} {v.created}  {v.description}{gate}{unapproved}")
    return "\n".join(lines)


# FR-83 (Pipewright 1.2.0): Model Registry makes a model's first version its default by itself. Under a manual policy
# (registry.set_default manual, from approvals.promote_model manual) the register step labels that version
# approval=pending: it is the default, but nobody approved it. `pipewright model approve` records the approval;
# until then `model list` says so, `doctor` warns and `deploy-model` refuses it. Scoring is unchanged.
APPROVAL_LABEL = "approval"


def pending_approval(labels: dict | None) -> bool:
    return (labels or {}).get(APPROVAL_LABEL) == "pending"


def approval_on_record(uc, env: Environment, creds, version: str) -> bool | None:
    """Phase 6: does the event log hold a model.approved event for this version? None when it cannot tell (no
    event log, or it cannot be read). Found live on private GKE: a register step that predates the approval=pending
    label leaves a first version the default with nothing to say nobody approved it."""
    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    try:
        events = integrity.approval_events(Gcp(creds), env, uc.name, str(version))
    except Exception:  # noqa: BLE001 - the listing and approve must not fail because the log cannot be read
        return None
    return None if events is None else bool(events)


def unapproved_default(v: Version) -> bool:
    return pending_approval(v.labels) or v.approved is False


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
    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    gcp = Gcp(creds)
    body = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}@{version}").body or {}
    labels = body.get("labels") or {}
    folder = record_location(uc, body.get("artifactUri"), labels)
    rules = integrity.released_rules(uc, env, gcp, integrity.release_of(gcp, folder, labels))
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
        registered = registered_problem(gcp, env, uc.name, version, files)
        if registered.get("record"):
            record["registered_files"] = registered["record"]
        if registered.get("problems"):
            integrity.enforce(integrity.Check(problems=registered["problems"]), env,
                              f"v{version} of {uc.model['name']} cannot be approved", log,
                              reason="what Agent Platform serves is the copy it took at registration, and the model "
                                     "folder has changed since",
                              remedy="retrain (or register the folder again) so the copy and the folder match")
    if image:
        record["serving_image"] = image
    features = _features_judged(uc, env, gcp, rules, version, log)
    if features:
        record["features"] = features
    if labels.get("pipewright-engine") == "bqml" and labels.get("pipewright-bq-model"):
        dataset = (uc.resources or {}).get("bigquery_dataset")
        model = integrity.bq_model(gcp, f"{env.project}.{dataset}.{labels['pipewright-bq-model']}")
        if model:
            record["bq_model"] = model
    return rules, record, folder


def _features_judged(uc: ResolvedUseCase, env: Environment, gcp, rules, version: str, log=print) -> dict | None:
    """FR-103 (story 8): the approval covers the feature definitions. With a feature contract (the one the version
    was trained with), its sources are read live and recorded on the approval; with policies.integrity.features
    check on, a definition that differs from the contract refuses the approval (under mode warn, says so)."""
    from pipewright.models import definitions, integrity

    section = rules.features
    pol = integrity.policy(env)["features"]
    if not section:
        if pol["check"] and (uc.ml.get("data") or {}).get("features_from"):
            log("integrity: the use case has no feature_contract, so the approval cannot pin its feature definitions "
                "(`pipewright features contract`)")
        return None
    problems, notes, now = definitions.check(gcp, section.get("sources") or [], uc.ml["data"].get("features_from"),
                                             env.project, env.region, pol["built_tables"],
                                             (env.security or {}).get("bigquery_kms_key"))
    for n in notes:
        log(f"integrity: {n}")
    if problems:
        if pol["check"]:
            integrity.enforce(integrity.Check(problems=problems), env,
                              f"v{version} of {uc.model['name']} cannot be approved",
                              log, reason=f"its feature definitions are not the ones in feature contract "
                                          f"{section.get('version')}, which it was trained with",
                              remedy="update the contract (`pipewright features contract`), release and retrain")
        else:
            for p in problems:
                log(f"integrity WARNING (policies.integrity.features.check off): {p}")
    return definitions.approval_record(section, now)


def registered_problem(gcp, env: Environment, usecase: str, version: str, files: dict) -> dict:
    """Pipewright 1.4.1 (FR-101 item 3): the register step records the folder's files when Agent Platform copies it
    (model.registered `files`). Every file it recorded must still be in the folder with the same SHA-256; files added
    after registration (lineage.json) are fine. Returns {"record", "problems"}; empty when nothing was recorded
    (registered before 1.4.1)."""
    from pipewright.models import approvals

    try:
        found = approvals.events(gcp, env, "model.registered", usecase, str(version)) or []
    except Exception:  # noqa: BLE001 - an unreadable log is said by the approval check itself
        return {}
    reg = next((e.get("files") for e in found if (e.get("files") or {}).get("files")), None)
    if not reg:
        return {}
    now = {f["file"]: f["sha256"] for f in files.get("files") or []}
    problems = []
    for f in reg["files"]:
        if f["file"] not in now:
            problems.append(f"{f['file']}: registered (sha256 {f['sha256'][:16]}), now missing from the folder")
        elif now[f["file"]] != f["sha256"]:
            problems.append(f"{f['file']}: sha256 {f['sha256'][:16]} when registered, {now[f['file']][:16]} now")
    return {"record": {"sha256": reg.get("sha256"), "count": reg.get("count")}, "problems": problems}


def _check_rules(uc: ResolvedUseCase, env: Environment, rules, identity: str | None, approved: bool,
                 version: str, log=print) -> ResolvedUseCase:
    """FR-96: the approvers and gates are the released ones. Returns the use case as released, for the gate check."""
    import dataclasses

    from pipewright.models import integrity

    if rules.note:
        log(f"integrity: {rules.note}")
    if rules.differs:
        check = integrity.Check(problems=[f"the working copy's {key} differs from release {rules.release}'s, which "
                                          f"trained v{version}" for key in rules.differs])
        integrity.enforce(check, env, f"v{version} of {uc.model['name']} cannot be approved", log,
                          reason="it is judged by the rules of the release that trained it",
                          remedy="approve from that release's commit, or release the change and retrain")
    changes = {"approvals": rules.approvals, "ml": {**uc.ml, "evaluation": rules.evaluation}}
    if dataclasses.is_dataclass(uc):
        released = dataclasses.replace(uc, **changes)
    else:                                   # a stand-in in tests
        from types import SimpleNamespace

        released = SimpleNamespace(**{**vars(uc), **changes})
    check_approval(released, identity, approved, key="promote_model")
    return released


def _clear_pending(env: Environment, creds, resource: str, version: str) -> None:
    """The register step labels a version approval=pending under a manual policy; once approved it is `approved`
    (found live in phase 6: a later version kept `pending`, and deploy-model refused it as unapproved)."""
    from pipewright.gcp import Gcp

    gcp = Gcp(creds)
    url = f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}@{version}"
    labels = (gcp.get(url).body or {}).get("labels") or {}
    if labels.get(APPROVAL_LABEL) == "pending":
        resp = gcp.patch(url, {"labels": {**labels, APPROVAL_LABEL: "approved"}}, updateMask="labels")
        if not resp.ok:
            raise ConfigError(f"recording the approval of v{version}: HTTP {resp.status}")


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
    log(f"{uc.model['name']}: v{version}, already the default, is approved"
        + (f" by {identity}" if identity else ""))


AB_LABEL = "pipewright-ab"


def approve_for_test(uc: ResolvedUseCase, env: Environment, creds, version: str, test: str, identity: str | None,
                     approved: bool, log=print) -> dict:
    """FR-57: approve a candidate for an A/B test without making it the default. Judged as `approve` judges a version
    (the released rules and approvers, the gates recomputed from its evaluation record), its files locked like an
    approval's, a label set on the version (the Admin Activity entry the approval cross-check reads) and
    `model.approved` recorded with `ab_test`, so deploy, scoring and the watch check the treatment's bytes as they
    check the champion's. The default does not move. Returns the approval record (its files)."""
    from google.cloud.aiplatform.models import ModelRegistry

    from pipewright.gcp import Gcp
    from pipewright.models import integrity

    resource = _resource(uc, env, creds)
    registry = ModelRegistry(model=resource)
    known = {str(v.version_id): v for v in registry.list_versions()}
    version = str(version).removeprefix("v")
    if version not in known:
        raise ConfigError(f"{uc.model['name']} has no version {version} (versions: {', '.join(sorted(known))})")
    if "default" in (known[version].version_aliases or []):
        raise ConfigError(f"v{version} is already the default (the control): an A/B test needs another version")
    rules, record, folder = _judged(uc, env, creds, resource, version, log)
    released = _check_rules(uc, env, rules, identity, approved, version, log)
    earlier = champion_expected(env, creds, resource, version, known)
    problems = gates.verify(Gcp(creds), folder, released.ml["evaluation"], earlier)
    if problems:
        raise ConfigError(problems, source=f"v{version} of {uc.model['name']} cannot be tested: it does not pass "
                                           f"{uc.name}'s evaluation gates (release {rules.release or '?'})")
    gcp = Gcp(creds)
    url = f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}@{version}"
    current = gcp.get(url)
    labels = {**((current.body or {}).get("labels") or {}), AB_LABEL: test[:63]}
    resp = gcp.patch(url, {"labels": labels}, updateMask="labels")
    if not resp.ok:
        raise ConfigError(f"recording the approval of v{version} for the test: HTTP {resp.status}")
    locked = integrity.lock(gcp, env, record.get("files"), log)
    _emit_approval(uc, env, creds, version, identity, {**record, **({"lock": locked} if locked else {})},
                   previous=None, ab_test=test)
    log(f"{uc.model['name']}: v{version} approved for A/B test {test}" + (f" by {identity}" if identity else "")
        + "; the default is unchanged")
    return record


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
        # Phase 6: the event log decides, not the label (a register step may predate it).
        if approval_on_record(uc, env, creds, version) is not False:
            log(f"v{version} of {uc.model['name']} is already the default")
            return
        log(f"v{version} of {uc.model['name']} is the default, but no approval of it is on record: approving it")
    # Phase 6 (FR-96): judged by the rules of the release that trained it, not the working copy's.
    rules, record, folder = _judged(uc, env, creds, resource, version, log)
    released = _check_rules(uc, env, rules, identity, approved, version, log)
    # Pipewright recomputes the gates from the version's evaluation record; it does not trust the pipeline.
    earlier = champion_expected(env, creds, resource, version, known)
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
    _clear_pending(env, creds, resource, version)
    locked = integrity.lock(Gcp(creds), env, record.get("files"), log)
    _emit_approval(uc, env, creds, version, identity, {**record, **({"lock": locked} if locked else {})},
                   previous=previous)
    online = not ((uc.ml or {}).get("bqml") or {}).get("forecast")       # a forecast is never served online
    log(f"{uc.model['name']}: default v{previous} -> v{version}"
        + (f" (approved by {identity})" if identity else "")
        + ". Inference uses it from the next run"
        + ("; `pipewright deploy-model` serves it online." if online else "."))
