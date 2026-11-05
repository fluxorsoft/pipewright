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
    p = dict((env.policies or {}).get("integrity") or {})
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
    note = None if released_appr else (f"release {release_id} predates phase 6 and does not record its approvers: "
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

    def summary(self) -> dict:
        state = "refused" if self.problems else "verified"
        return {"result": state, "problems": len(self.problems),
                **({"approved_at": self.approval.get("at"), "approved_by": self.approval.get("by")}
                   if self.approval else {})}


def verify(gcp, env: Environment, usecase: str, version: str, folder: str | None, image: str | None,
           first_version_pending: bool = False) -> Check:
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
    recorded = approval.get("evaluation") or {}
    if recorded.get("sha256"):
        now = gates.record_digest(gcp, folder)
        if now is None:
            check.problems.append(f"the evaluation record v{version} was approved on ({recorded.get('record')}) is "
                                  "missing")
        elif now["sha256"] != recorded["sha256"]:
            check.problems.append(f"the evaluation record changed after the approval: sha256 "
                                  f"{recorded['sha256'][:16]} approved, {now['sha256'][:16]} on file")
    else:
        check.notes.append(f"the approval of v{version} ({approval.get('at')}) predates Pipewright 1.3.1 and "
                           "records no evaluation fingerprint")
    files = approval.get("files")
    if files and folder:
        check.problems += compare_files(gcp, files, folder)
    elif folder:
        check.notes.append("the approval records no model files (approved before phase 6)")
    check.problems += compare_image(gcp, approval.get("serving_image"), image)
    return check


def enforce(check: Check, env: Environment, what: str, log=print) -> None:
    """Refuse (or, under policies.integrity.mode warn, say) what a check found."""
    for note in check.notes:
        log(f"integrity: {note}")
    if not check.problems:
        return
    if policy(env)["mode"] == "warn":
        for p in check.problems:
            log(f"integrity WARNING (policies.integrity.mode warn): {p}")
        return
    raise ConfigError(check.problems + ["re-approve the version after review (`pipewright model approve`), or, for "
                                        "this environment only, set policies.integrity.mode: warn"],
                      source=f"{what}: what is on file is not what was approved")


# -- the retention lock (FR-97) -----------------------------------------------------------------------------------

def lock(gcp, env: Environment, files: dict | None, log=print) -> dict | None:
    """Locks the approved files with Cloud Storage object retention for policies.integrity.lock.days. Returns what
    was done; never fails an approval (the files' fingerprints are recorded either way)."""
    import datetime

    cfg = policy(env)["lock"]
    if not files or cfg["days"] <= 0 or not files.get("files"):
        return None
    until = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=cfg["days"])).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    mode = "Locked" if cfg["mode"] == "locked" else "Unlocked"
    bucket, _, prefix = files["folder"].removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    locked, refused = 0, None
    for f in files["files"]:
        name = prefix + f["file"]
        r = gcp.session.patch(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}",
                              params={"generation": str(f["generation"])},
                              json={"retention": {"mode": mode, "retainUntilTime": until}}, timeout=60)
        if r.status_code != 200:
            refused = f"HTTP {r.status_code}: {(r.text or '')[:160]}"
            break
        locked += 1
    if refused:
        log(f"integrity: the approved files could not be locked ({refused}); their fingerprints are recorded. "
            "Object retention must be enabled on the bucket (the starter and blueprint do it for new environments)")
        return {"locked": locked, "until": until, "mode": mode.lower(), "error": refused[:160]}
    log(f"integrity: {locked} approved files locked until {until} ({mode.lower()} retention)")
    return {"locked": locked, "until": until, "mode": mode.lower()}
