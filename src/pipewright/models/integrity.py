"""What runs is what was approved (Pipewright phase 6: FR-93, FR-94, FR-96, FR-97).

An approval records what it was judged on: the evaluation record (FR-89), every model file and the serving image
(FR-94), and the rules it was judged by, from the release that trained the version (FR-96). It can lock the approved
files with Cloud Storage object retention (FR-97). Before a version is deployed or promoted, `verify` compares what
is on file now with that record.

Messages name records, files, digests, metrics and gates: never a row, a feature value or a slice value.
`policies.integrity` in the manifest: `mode` refuse (the default) or warn; `lock` {days, mode} for the retention lock.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from urllib.parse import quote

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError

GCS = "https://storage.googleapis.com/storage/v1"
MAX_FILES = 500                      # a model folder with more files is recorded by count and digest only
MODES = ("refuse", "warn")
_MANIFEST_TYPES = ", ".join([
    "application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json"])


def policy(env: Environment) -> dict:
    """The environment's integrity policy, with its defaults: refuse, and no lock."""
    p = dict((getattr(env, "policies", None) or {}).get("integrity") or {})
    mode = p.get("mode", "refuse")
    if mode not in MODES:
        raise ConfigError(f"policies.integrity.mode must be one of {', '.join(MODES)}, not {mode!r}")
    lock = dict(p.get("lock") or {})
    return {"mode": mode, "lock": {"days": int(lock.get("days") or 0), "mode": lock.get("mode", "unlocked")}}


# -- model files (FR-94) ------------------------------------------------------------------------------------------

def _objects(gcp, folder: str) -> list[dict]:
    bucket, _, prefix = folder.removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    out, token = [], None
    while True:
        params = {"prefix": prefix, "fields": "items(name,size,generation),nextPageToken"}
        if token:
            params["pageToken"] = token
        r = gcp.get(f"{GCS}/b/{bucket}/o", **params)
        if not r.ok:
            raise ConfigError(f"cannot list {folder}: HTTP {r.status}")
        out += (r.body or {}).get("items", [])
        token = (r.body or {}).get("nextPageToken")
        if not token:
            return [dict(o, bucket=bucket, relative=o["name"][len(prefix):]) for o in out if o["name"] != prefix]


def _sha256(gcp, bucket: str, name: str, generation=None) -> str:
    h = hashlib.sha256()
    params = {"alt": "media", **({"generation": str(generation)} if generation else {})}
    r = gcp.session.get(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}", params=params, stream=True, timeout=600)
    if r.status_code != 200:
        raise ConfigError(f"cannot read gs://{bucket}/{name}: HTTP {r.status_code}")
    for chunk in r.iter_content(1 << 20):
        h.update(chunk)
    return h.hexdigest()


def _digest(files: list[dict]) -> str:
    return hashlib.sha256(json.dumps([[f["file"], f["sha256"]] for f in files]).encode()).hexdigest()


def model_files(gcp, folder: str | None) -> dict | None:
    """Every file under a version's model folder, with its generation and SHA-256, and one digest over them all.
    None when there is no folder (a BigQuery ML version keeps its model in BigQuery)."""
    if not folder or not folder.startswith("gs://"):
        return None
    objects = sorted(_objects(gcp, folder), key=lambda o: o["relative"])
    if not objects:
        return {"folder": folder, "count": 0, "sha256": _digest([]), "files": []}
    files = [{"file": o["relative"], "generation": int(o["generation"]), "size": int(o.get("size") or 0),
              "sha256": _sha256(gcp, o["bucket"], o["name"], o["generation"])} for o in objects]
    out = {"folder": folder, "count": len(files), "sha256": _digest(files)}
    if len(files) <= MAX_FILES:
        out["files"] = files
    return out


def compare_files(gcp, approved: dict, folder: str) -> list[str]:
    """What differs between the approved model files and those on file now: names only, never contents. A file
    whose generation is unchanged has not been rewritten (Cloud Storage gives every write a new generation); one
    with a new generation is read again, so a byte-identical rewrite still matches."""
    now = {o["relative"]: o for o in _objects(gcp, folder)}
    if "files" not in approved:
        current = model_files(gcp, folder) or {}
        return [] if current.get("sha256") == approved.get("sha256") else [
            f"the model files under {folder} are not the approved ones ({approved.get('count')} files approved, "
            f"digest {str(approved.get('sha256'))[:16]}; now {current.get('count')} files, digest "
            f"{str(current.get('sha256'))[:16]})"]
    problems = []
    was = {f["file"]: f for f in approved["files"]}
    for name in sorted(set(was) - set(now)):
        problems.append(f"{name}: approved, now missing")
    for name in sorted(set(now) - set(was)):
        problems.append(f"{name}: added after the approval")
    for name in sorted(set(was) & set(now)):
        if int(now[name]["generation"]) == int(was[name]["generation"]):
            continue
        if _sha256(gcp, now[name]["bucket"], now[name]["name"], now[name]["generation"]) != was[name]["sha256"]:
            problems.append(f"{name}: changed after the approval (sha256 {was[name]['sha256'][:16]} approved)")
    return problems


# -- a BigQuery ML model (FR-94): the model stays in BigQuery, so its etag and last change stand for files -----

def bq_model(gcp, ref: str | None) -> dict | None:
    """project.dataset.model -> {model, etag, modified}. None when there is no reference or it cannot be read."""
    if not ref or ref.count(".") != 2:
        return None
    project, dataset, model = ref.split(".")
    r = gcp.get(f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/models/{model}")
    if not r.ok:
        return None
    body = r.body or {}
    return {"model": ref, "etag": body.get("etag"), "modified": body.get("lastModifiedTime")}


def compare_bq_model(gcp, approved: dict | None) -> list[str]:
    if not approved or not approved.get("etag"):
        return []
    now = bq_model(gcp, approved.get("model"))
    if now is None:
        return [f"the BigQuery ML model {approved.get('model')} approved is missing, or cannot be read"]
    if now["etag"] != approved["etag"]:
        return [f"the BigQuery ML model {approved.get('model')} changed after the approval (replaced or retrained "
                "in place)"]
    return []


def record_sha256(gcp, record: str | None) -> str | None:
    """The SHA-256 of one object (the evaluation record an approval names), or None when it is not there."""
    if not record or not record.startswith("gs://"):
        return None
    from pipewright.gcp import gcs_read

    bucket, _, name = record.removeprefix("gs://").partition("/")
    text, _ = gcs_read(gcp, bucket, name)
    return None if text is None else hashlib.sha256(text.encode("utf-8")).hexdigest()


# -- the serving image (FR-94) ------------------------------------------------------------------------------------

def resolve_image(gcp, image: str | None) -> str | None:
    """The image's digest (sha256:...), from its reference or its registry. None when it cannot be resolved."""
    if not image:
        return None
    if "@sha256:" in image:
        return image.split("@", 1)[1]
    host, _, rest = image.partition("/")
    path, _, tag = rest.rpartition(":")
    if not path or "/" in tag:
        path, tag = rest, "latest"
    try:
        r = gcp.session.head(f"https://{host}/v2/{path}/manifests/{tag}", headers={"Accept": _MANIFEST_TYPES},
                             timeout=60)
    except Exception:  # noqa: BLE001 - a registry we cannot reach: recorded as unresolved
        return None
    return r.headers.get("Docker-Content-Digest") if r.status_code == 200 else None


def serving_image(gcp, image: str | None) -> dict | None:
    if not image:
        return None
    return {"image": image, "digest": resolve_image(gcp, image), "pinned": "@sha256:" in image}


def compare_image(gcp, approved: dict | None, image: str | None) -> list[str]:
    if not approved or not approved.get("digest"):
        return []
    if image != approved.get("image"):
        return [f"the serving image is {image}, not the approved {approved.get('image')}"]
    now = resolve_image(gcp, image)
    if now is None:
        return [f"the serving image {image} cannot be resolved now, so it cannot be shown to be the approved one"]
    if now != approved["digest"]:
        return [f"the serving image {image} now resolves to {now[:19]}, not the approved {approved['digest'][:19]} "
                "(its tag moved: register images by digest)"]
    return []


# -- the rules an approval is judged by (FR-96) -------------------------------------------------------------------

def _canonical(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


@dataclass
class Rules:
    """The gates and approvals a version is judged by, and where they came from."""
    evaluation: dict
    approvals: dict
    release: str | None
    source: str                                   # release, or working copy (with the reason in `note`)
    note: str | None = None
    differs: list[str] = field(default_factory=list)

    def record(self) -> dict:
        return {"release": self.release, "source": self.source, "evaluation_sha256": _canonical(self.evaluation),
                "approvals_sha256": _canonical(self.approvals)}


def release_of(gcp, folder: str | None, labels: dict | None) -> str | None:
    """The release that trained a version: its lineage.json, else its `release` label."""
    if folder and folder.startswith("gs://"):
        from pipewright.gcp import gcs_read

        bucket, _, prefix = folder.removeprefix("gs://").partition("/")
        try:
            text, _ = gcs_read(gcp, bucket, f"{prefix.rstrip('/')}/lineage.json".lstrip("/"))
            if text and json.loads(text).get("release"):
                return json.loads(text)["release"]
        except Exception:  # noqa: BLE001 - no lineage record: the label says it, or nothing does
            pass
    return (labels or {}).get("release")


def released_rules(uc: ResolvedUseCase, env: Environment, gcp, release_id: str | None) -> Rules:
    """The gates (ml.evaluation) and approvals of the release that trained a version. A release from before phase 6
    has the gates (its ml_config) but not the approvers: those then come from the working copy, with a note. The
    working copy may be stricter, never looser: `differs` names the keys where it is not the released value."""
    local_eval, local_appr = dict(uc.ml.get("evaluation") or {}), dict(uc.approvals or {})
    if not release_id:
        return Rules(local_eval, local_appr, None, "working copy",
                     "the version names no release (trained before Pipewright 1.1): the working copy's rules apply")
    from pipewright.release.contract import contract_name
    from pipewright.release.service import read_contract
    from pipewright.retraining.service import ml_config

    try:
        contract = read_contract(gcp, env, contract_name(uc.name, release_id))
    except ConfigError:
        return Rules(local_eval, local_appr, release_id, "working copy",
                     f"release {release_id} has no contract in {env.name} (trained elsewhere): the working copy's "
                     "rules apply")
    released_eval = dict(ml_config(contract).get("evaluation") or {})
    released_appr = dict((contract.metadata or {}).get("approvals") or {})
    note = None if released_appr else (f"release {release_id} predates Pipewright 1.4.0 and does not record its "
                                       "approvers: "
                                       "the working copy's approvers apply")
    rules = Rules(released_eval or local_eval, released_appr or local_appr, release_id,
                  "release" if released_eval else "working copy", note)
    if released_eval and _canonical(released_eval) != _canonical(local_eval):
        rules.differs.append("ml.evaluation")
    if released_appr and _canonical(released_appr) != _canonical(local_appr):
        rules.differs.append("approvals")
    return rules


# -- the approval record and the check before deploy and promote (FR-93) ------------------------------------------

def approval_events(gcp, env: Environment, usecase: str, version: str) -> list[dict] | None:
    """The version's model.approved events, newest first. None when the environment keeps no event log."""
    dataset = (env.observability or {}).get("evidence_dataset")
    if not dataset:
        return None
    from pipewright.gcp.bigquery import query, rows

    def p(name, value):
        return {"name": name, "parameterType": {"type": "STRING"}, "parameterValue": {"value": value}}

    sql = (f"SELECT CAST(publish_time AS STRING) AS published, TO_JSON_STRING(data) AS event "
           f"FROM `{env.project}.{dataset}.events` WHERE JSON_VALUE(data, '$.event_type') = 'model.approved' "
           "AND JSON_VALUE(data, '$.usecase') = @usecase AND JSON_VALUE(data, '$.environment') = @env "
           "AND JSON_VALUE(data, '$.data.version') = @version ORDER BY publish_time DESC LIMIT 20")
    found = rows(query(gcp, env.project, sql, [p("usecase", usecase), p("env", env.name), p("version", str(version))],
                       kms_key=(env.security or {}).get("bigquery_kms_key")))
    out = []
    for row in found:
        event = json.loads(row["event"] or "{}")
        out.append({"at": (row["published"] or "")[:19], **(event.get("data") or {})})
    return out


@dataclass
class Check:
    """The result of `verify`: problems refuse (or warn), notes are said but never refuse."""
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    approval: dict | None = None

    def deployed(self) -> dict:
        """For model.deployed: a deploy that went on with problems did so under policies.integrity.mode warn, and
        says so with what was wrong (found in QA: it recorded 'refused' and a count for a deploy that happened).
        The problems name files, digests and versions, never data."""
        out = self.summary()
        if self.problems:
            out.update(result="warned: deployed under policies.integrity.mode warn", problems_found=self.problems[:10])
        return out

    def summary(self) -> dict:
        state = "refused" if self.problems else "verified"
        return {"result": state, "problems": len(self.problems),
                **({"approved_at": self.approval.get("at"), "approved_by": self.approval.get("by")}
                   if self.approval else {})}


AUDIT_KEPT_DAYS = 400      # Cloud Audit Logs keep Admin Activity entries this long (the _Required bucket)


def verify(gcp, env: Environment, usecase: str, version: str, folder: str | None, image: str | None,
           first_version_pending: bool = False, model: str | None = None) -> Check:
    """Is the version on file now the one approved? Reads the newest approval of `version` and compares the
    evaluation record, the model files and the serving image with what it recorded."""
    from pipewright.models import gates

    check = Check()
    if first_version_pending:
        return check                       # refused already by the caller, with how to approve it
    try:
        events = approval_events(gcp, env, usecase, version)
    except Exception as e:  # noqa: BLE001 - an unreadable log is a problem: nothing shows the version was approved
        check.problems.append(f"the event log cannot be read, so v{version}'s approval cannot be shown: "
                              f"{str(e).splitlines()[0][:160]}")
        return check
    if events is None:
        check.notes.append("this environment keeps no event log (manifest observability.evidence_dataset), so the "
                           "approval cannot be checked")
        return check
    if not events:
        check.problems.append(f"v{version} has no model.approved event: it became the default outside Pipewright "
                              "(the console, gcloud or the API), so nobody approved it here")
        return check
    approval = check.approval = events[0]
    if model:
        _audit_problem(gcp, env, model, version, approval, check)
    recorded = approval.get("evaluation") or {}
    if recorded.get("sha256"):
        now = record_sha256(gcp, recorded.get("record")) if recorded.get("record") else (
            (gates.record_digest(gcp, folder) or {}).get("sha256"))
        if now is None:
            check.problems.append(f"the evaluation record v{version} was approved on ({recorded.get('record')}) is "
                                  "missing")
        elif now != recorded["sha256"]:
            check.problems.append(f"the evaluation record changed after the approval: sha256 "
                                  f"{recorded['sha256'][:16]} approved, {now[:16]} on file")
    else:
        check.notes.append(f"the approval of v{version} ({approval.get('at')}) predates Pipewright 1.3.1 and "
                           "records no evaluation fingerprint")
    files = approval.get("files")
    if files and folder:
        check.problems += compare_files(gcp, files, folder)
    elif folder:
        check.notes.append("the approval records no model files (approved before Pipewright 1.4.0)")
    check.problems += compare_image(gcp, approval.get("serving_image"), image)
    check.problems += compare_bq_model(gcp, approval.get("bq_model"))
    return check


def check_source(gcp, src_env: Environment, dst_env: Environment, usecase: str, version: str, chosen: dict,
                 loop_case: str | None, log=print) -> Check | None:
    """Before a promotion copies anything: the source version is the one approved in the source environment
    (FR-93). Enforced with the destination's policy. A closed retrain loop's candidate is approved by the loop's
    production approval instead, so only its files are compared with what the loop validated: not here."""
    if loop_case:
        log(f"integrity: v{version} is the candidate of retrain loop {loop_case}: approved by the loop's production "
            "approval, not in the source")
        return None
    check = verify(gcp, src_env, usecase, version, chosen.get("artifactUri"),
                   (chosen.get("containerSpec") or {}).get("imageUri"), model=chosen.get("name"))
    enforce(check, dst_env, f"v{version} ({src_env.name}) is not promoted", log)
    return check


def enforce(check: Check, env: Environment, what: str, log=print,
            reason: str = "what is on file is not what was approved",
            remedy: str = "re-approve the version after review (`pipewright model approve`)") -> None:
    """Refuse (or, under policies.integrity.mode warn, say) what a check found."""
    for note in check.notes:
        log(f"integrity: {note}")
    if not check.problems:
        return
    if policy(env)["mode"] == "warn":
        for p in check.problems:
            log(f"integrity WARNING (policies.integrity.mode warn): {p}")
        return
    raise ConfigError(check.problems, source=f"{what}: {reason}",
                      hint=f"{remedy}, or, for this environment only, set policies.integrity.mode: warn")


# -- what changed, and who (FR-95), and who approved (FR-97) ------------------------------------------------------

DIFF_KEYS = ("decision", "primary_metric", "value")     # top-level keys a diff may show; slices and rows never


def read_generation(gcp, record: str, generation) -> str | None:
    """One generation of an object (the approved evaluation record, kept by bucket versioning). None if gone."""
    bucket, _, name = record.removeprefix("gs://").partition("/")
    r = gcp.session.get(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}",
                        params={"alt": "media", "generation": str(generation)}, timeout=60)
    return r.text if r.status_code == 200 else None


def evaluation_diff(approved: str, now: str) -> list[str]:
    """The metrics and the decision that differ between two evaluation records: names and numbers only. Slices,
    reasons and anything else are counted, never shown (a slice can be named after a data value)."""
    try:
        a, b = json.loads(approved), json.loads(now)
    except ValueError:
        return ["the record is no longer valid JSON"]
    out = []
    for key in DIFF_KEYS:
        if a.get(key) != b.get(key) and (key in a or key in b):
            out.append(f"{key}: {a.get(key)} -> {b.get(key)}")
    ma, mb = a.get("metrics") or {}, b.get("metrics") or {}
    for name in sorted(set(ma) | set(mb)):
        va, vb = ma.get(name), mb.get(name)
        if va != vb and all(isinstance(v, (int, float)) or v is None for v in (va, vb)):
            out.append(f"metrics.{name}: {va} -> {vb}")
    other = sorted(k for k in set(a) | set(b) if k not in DIFF_KEYS and k != "metrics" and a.get(k) != b.get(k))
    if other:
        out.append(f"also changed (not shown: they can hold data values): {', '.join(other)}")
    return out or ["the bytes changed, the metrics and decision did not"]


def _audit(gcp, env: Environment, filter_: str, limit: int = 5) -> list[dict]:
    r = gcp.post("https://logging.googleapis.com/v2/entries:list",
                 {"resourceNames": [f"projects/{env.project}"], "filter": filter_, "orderBy": "timestamp desc",
                  "pageSize": limit})
    if not r.ok:
        raise ConfigError(f"Cloud Audit Logs: HTTP {r.status}")
    return [{"at": e.get("timestamp", "")[:19], "method": (e.get("protoPayload") or {}).get("methodName"),
             "principal": ((e.get("protoPayload") or {}).get("authenticationInfo") or {}).get("principalEmail")}
            for e in (r.body or {}).get("entries", [])]


def _window(at: str, minutes: int) -> tuple[str, str]:
    import datetime

    t = datetime.datetime.fromisoformat(at.replace(" ", "T")[:19]).replace(tzinfo=datetime.timezone.utc)
    d = datetime.timedelta(minutes=minutes)
    return ((t - d).strftime("%Y-%m-%dT%H:%M:%SZ"), (t + d).strftime("%Y-%m-%dT%H:%M:%SZ"))


def who_wrote(gcp, env: Environment, record: str, after: str) -> dict:
    """Who wrote the record after the approval, from Cloud Storage Data Access audit logs (when they are on)."""
    bucket, _, name = record.removeprefix("gs://").partition("/")
    start, _ = _window(after, 0)
    try:
        found = _audit(gcp, env, f'protoPayload.serviceName="storage.googleapis.com" AND '
                                 f'protoPayload.resourceName="projects/_/buckets/{bucket}/objects/{name}" AND '
                                 f'protoPayload.methodName=("storage.objects.create" OR "storage.objects.delete") AND '
                                 f'timestamp>="{start}"')
    except ConfigError as e:
        return {"note": f"not checked: {e}"}
    return {"entries": found} if found else {
        "note": "no Data Access audit entry (Cloud Storage data access logs off, or kept elsewhere)"}


def approval_audit(gcp, env: Environment, model: str, at: str, by: str | None, promoted: bool = False) -> dict:
    """The Admin Activity entry (always on, cannot be turned off) for the change an approval made: the `default`
    alias moved (MergeVersionAliases) or a first version's approval label (UpdateModel). Compares the principal with
    the event's `by`: an event with no entry, or another principal, is flagged. A first version approved with its
    promotion (`promoted`) became the default by its upload (UploadModel, which names the model only in its
    operation): found live on ent-prod, where it was flagged as having no entry."""
    model_id = model.rsplit("/", 1)[-1].split("@")[0]
    start, end = _window(at, 10)
    upload = (f' OR (protoPayload.methodName:"UploadModel" AND operation.id:"models/{model_id}/")' if promoted
              else "")
    try:
        found = _audit(gcp, env, f'protoPayload.serviceName="aiplatform.googleapis.com" AND '
                                 f'((protoPayload.methodName:("MergeVersionAliases" OR "UpdateModel") AND '
                                 f'protoPayload.resourceName:"models/{model_id}"){upload}) AND '
                                 f'timestamp>="{start}" AND timestamp<="{end}"', limit=10)
    except ConfigError as e:
        return {"state": "not checked", "note": str(e)}
    if not found:
        return {"state": "no entry", "note": "no Admin Activity entry for this approval: the event may not be genuine"}
    match = next((e for e in found if by and e["principal"] == by), None)
    if match:
        return {"state": "matched", **match}
    return {"state": "other principal", **found[0],
            "note": f"the audit log names {found[0]['principal']}, the event says {by}"}


def who_made_default(gcp, env: Environment, model: str, version: str, since: str | None) -> dict:
    """A default with no approval on record: who moved the `default` alias to it, from the Admin Activity audit log
    (found live on ent: a version made the default with the API was refused, but nothing said by whom). A first
    version has no such entry: Model Registry made it the default when it was registered."""
    model_id = model.rsplit("/", 1)[-1].split("@")[0]
    start = _window(since, 0)[0] if since else None
    try:
        found = _audit(gcp, env, f'protoPayload.serviceName="aiplatform.googleapis.com" AND '
                                 f'protoPayload.methodName:"MergeVersionAliases" AND '
                                 f'protoPayload.resourceName:"models/{model_id}@{version}" AND '
                                 f'protoPayload.request.versionAliases="default"'
                                 + (f' AND timestamp>="{start}"' if start else ""), limit=1)
    except (ConfigError, ValueError) as e:
        return {"note": f"not checked: {e}"}
    if found:
        return found[0]
    return {"note": "no alias change in the Admin Activity audit log: Model Registry made it the default when it "
                    "was registered (its first version), or the entry is older than the log keeps"}


# -- scoring with an unapproved default (policies.integrity.inference) ---------------------------------------------

def inference_check(gcp, manifest: dict, contract) -> tuple[str, str | None]:
    """(mode, problem): does the default version a scoring run will use have an approval? mode off (the default)
    checks nothing. A first version still pending, or a default with no model.approved event, is a problem."""
    from types import SimpleNamespace

    mode = (((manifest.get("policies") or {}).get("integrity") or {}).get("inference")) or "off"
    if mode == "off":
        return mode, None
    name = (contract.metadata or {}).get("model")
    if not name:
        return mode, None
    api = f"https://{contract.region}-aiplatform.googleapis.com/v1"
    found = gcp.get(f"{api}/projects/{contract.project}/locations/{contract.region}/models",
                    filter=f'display_name="{name}"')
    models = (found.body or {}).get("models", []) if found.ok else []
    if not models:
        return mode, None                                  # nothing registered yet: the run fails on its own
    default = gcp.get(f"{api}/{models[0]['name'].split('@')[0]}@default")
    if not default.ok:
        return mode, f"the default version of {name} cannot be read (HTTP {default.status})"
    version = str((default.body or {}).get("versionId"))
    if ((default.body or {}).get("labels") or {}).get("approval") == "pending":
        return mode, f"v{version} of {name} is the default and labelled approval=pending: nobody has approved it"
    env = SimpleNamespace(name=contract.environment, project=contract.project,
                          observability=manifest.get("observability") or {}, security=manifest.get("security") or {})
    try:
        events = approval_events(gcp, env, contract.usecase, version)
    except Exception as e:  # noqa: BLE001 - the launcher cannot read the log: the approval cannot be shown
        return mode, f"the approval of v{version} cannot be shown: {str(e).splitlines()[0][:120]}"
    if events == []:
        return mode, f"v{version} of {name} has no model.approved event: it became the default outside Pipewright"
    return mode, None


def _audit_problem(gcp, env: Environment, model: str, version: str, approval: dict, check: Check) -> None:
    """QA governance (G3): the approval against the Admin Activity entry its change made. An event anyone with
    publish rights could write is not evidence on its own; the audit log, which nobody can write, is."""
    import datetime

    try:
        audit = approval_audit(gcp, env, model, approval.get("at") or "", approval.get("by"),
                               promoted=bool(approval.get("source_environment")))
    except Exception as e:  # noqa: BLE001 - an unreadable audit log is a note, never a broken deploy
        check.notes.append(f"the approval of v{version} could not be checked against Cloud Audit Logs: "
                           f"{str(e).splitlines()[0][:120]}")
        return
    state = audit.get("state")
    if state == "no entry":
        try:
            at = datetime.datetime.fromisoformat((approval.get("at") or "").replace(" ", "T")[:19]).replace(
                tzinfo=datetime.timezone.utc)
            within = datetime.datetime.now(datetime.timezone.utc) - at < datetime.timedelta(days=AUDIT_KEPT_DAYS)
        except ValueError:
            within = False
        if within:
            check.problems.append(f"the approval of v{version} ({approval.get('at')}, by {approval.get('by')}) has "
                                  "no Admin Activity entry in Cloud Audit Logs: the event may not be genuine")
        else:
            check.notes.append(f"the approval of v{version} is older than Cloud Audit Logs keep: not cross-checked")
    elif state == "other principal":
        check.problems.append(f"the approval of v{version}: {audit.get('note')}")
    elif state == "not checked":
        check.notes.append(f"the approval of v{version} could not be checked against Cloud Audit Logs: "
                           f"{audit.get('note')}")


# -- the retention lock (FR-97) -----------------------------------------------------------------------------------

def _bucket_has_retention(gcp, bucket: str) -> bool:
    r = gcp.session.get(f"{GCS}/b/{bucket}", params={"fields": "objectRetention"}, timeout=60)
    return r.status_code == 200 and ((r.json() or {}).get("objectRetention") or {}).get("mode") == "Enabled"


def lock_contract(gcp, env: Environment, bucket: str, name: str, generation, log=print) -> dict | None:
    """QA governance (G10): a release contract is locked as approved files are (policies.integrity.lock), so
    nobody who may write the bucket can change what a live release runs. None when no lock is configured."""
    import hashlib

    if not isinstance(getattr(env, "policies", None), dict) or policy(env)["lock"]["days"] <= 0:
        return None                                    # no lock configured: nothing to do (as before)
    folder, _, file = name.rpartition("/")
    text, _ = gcs_read_versioned(gcp, bucket, name, generation)
    files = {"folder": f"gs://{bucket}/{folder}", "files": [{"file": file, "generation": int(generation),
                                                             "sha256": hashlib.sha256(text or b"").hexdigest()}]}
    return lock(gcp, env, files, lambda m: log(m.replace("approved files", "contract").replace(
        "approved file", "contract")))


def gcs_read_versioned(gcp, bucket: str, name: str, generation) -> tuple[bytes | None, int]:
    r = gcp.session.get(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}",
                        params={"alt": "media", "generation": str(generation)}, timeout=60)
    return (r.content, int(generation)) if r.status_code == 200 else (None, 0)


def contract_sha256(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def lock(gcp, env: Environment, files: dict | None, log=print) -> dict | None:
    """Locks the approved files for policies.integrity.lock.days. With object retention on the bucket (a bucket
    created with it: Cloud Storage cannot add it later), each file gets retention until then, `unlocked` or
    `locked`. Without it, an `unlocked` lock is a temporary hold on each file (any bucket; released by someone who
    may update objects, with no end date); a `locked` lock cannot be had and is recorded as not done. Returns what
    was done; never fails an approval (the files' fingerprints are recorded either way)."""
    import datetime

    cfg = policy(env)["lock"]
    if not files or cfg["days"] <= 0 or not files.get("files"):
        return None
    until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=cfg["days"])).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    bucket, _, prefix = files["folder"].removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    retention = _bucket_has_retention(gcp, bucket)
    if not retention and cfg["mode"] == "locked":
        reason = ("a locked lock needs object retention on the bucket, which Cloud Storage only sets when the "
                  "bucket is created")
        log(f"integrity: the approved files were not locked: {reason}; their fingerprints are recorded")
        return {"locked": 0, "until": until, "mode": "locked", "method": "none", "error": reason}
    if retention:
        body, method = {"retention": {"mode": "Locked" if cfg["mode"] == "locked" else "Unlocked",
                                      "retainUntilTime": until}}, "retention"
    else:
        body, method = {"temporaryHold": True}, "hold"
    locked, refused = 0, None
    for f in files["files"]:
        name = prefix + f["file"]
        r = gcp.session.patch(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}",
                              params={"generation": str(f["generation"])}, json=body, timeout=60)
        if r.status_code != 200:
            refused = f"HTTP {r.status_code}: {(r.text or '')[:160]}"
            break
        locked += 1
    out = {"locked": locked, "mode": cfg["mode"], "method": method,
           **({"until": until} if method == "retention" else {})}
    if refused:
        log(f"integrity: the approved files could not all be locked ({refused}); their fingerprints are recorded")
        return {**out, "error": refused[:160]}
    log(f"integrity: {locked} approved files locked"
        + (f" until {until} ({cfg['mode']} retention)" if method == "retention" else
           " with a temporary hold (the bucket has no object retention: a hold has no end date, release it when the "
           "version is retired)"))
    return out
