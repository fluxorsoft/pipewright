"""pipewright promote: a tested release, and optionally its approved model, from one environment to the next (FR-29).

What must not change is verified, never rebuilt: the component and Ray images are copied by digest; each pipeline
template is compiled for the destination from the source commit and must equal the source template with only the
registry address rewritten; a promoted model is the same files (CRC32C checked), registered in the destination as a
candidate. The configuration is resolved for the destination, whose policies are applied before anything is
copied. The destination contract is written once, the live pointer moved last. CONTEXT.md, section 8.12.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

import yaml

from pipewright.config import Environment, ResolvedUseCase, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, PreconditionFailed, gcs_read, gcs_write
from pipewright.observability import events
from pipewright.release import publish
from pipewright.release.contract import ReleaseContract, contract_name, new_pointer, pointer_name
from pipewright.release.service import _git, _quiet, read_contract, read_pointer
from pipewright.release.verify import verify
from pipewright.tables import sync as tables_sync

GCS = "https://storage.googleapis.com/storage/v1"
_MANIFEST_TYPES = ", ".join([
    "application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json"])
_INDEX_TYPES = ("application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json")


@dataclass
class Promotion:
    contract: ReleaseContract
    contract_uri: str
    previous: str | None
    record: dict
    model_version: str | None = None
    checks: list[str] = field(default_factory=list)
    unchanged: bool = False          # the promotion had been done already: nothing was copied or moved


# ------------------------------------------------------------------ images --

def _split(image: str) -> tuple[str, str, str]:
    """host, repository path, digest of host/path@sha256:..."""
    if "@sha256:" not in image:
        raise ConfigError(f"{image} is not pinned by digest")
    repo, digest = image.split("@", 1)
    host, path = repo.split("/", 1)
    return host, path, digest


def destination_image(image: str, registry: str) -> str:
    """The same image (name and digest) in the destination's Docker registry (host/project/repository)."""
    _, path, digest = _split(image)
    return f"{registry.rstrip('/')}/{path.rsplit('/', 1)[-1]}@{digest}"


class _Sized:
    """A response stream with a known length, so requests streams it without asking the stream for a file
    descriptor (Windows refuses fstat on a socket stream: WinError 6)."""

    def __init__(self, raw, length: str | None):
        self.raw, self.length = raw, int(length or 0)

    def __len__(self) -> int:
        return self.length

    def read(self, size: int = -1) -> bytes:
        return self.raw.read(size)


def copy_image(gcp: Gcp, source: str, dest: str, log=print) -> str:
    """Copies an image between registries through the registry API: the manifest and every blob, byte for byte,
    so the digest cannot change. Returns the destination digest (checked equal to the source's)."""
    shost, spath, digest = _split(source)
    dhost, dpath, ddigest = _split(dest)
    if digest != ddigest:
        raise ConfigError(f"destination {dest} names a different digest than {source}")
    s = gcp.session

    def manifest(host, path, ref):
        r = s.get(f"https://{host}/v2/{path}/manifests/{ref}", headers={"Accept": _MANIFEST_TYPES}, timeout=60)
        if r.status_code != 200:
            raise ConfigError(f"cannot read {host}/{path}@{ref}: {r.status_code} {r.text[:200]}")
        return r.content, r.headers.get("Content-Type", "application/vnd.oci.image.manifest.v1+json")

    def have(host, path, kind, ref):
        return s.head(f"https://{host}/v2/{path}/{kind}/{ref}", headers={"Accept": _MANIFEST_TYPES},
                      timeout=60).status_code == 200

    def copy_blob(blob):
        if have(dhost, dpath, "blobs", blob):
            return
        if shost == dhost:                                   # same registry host: mount instead of copying
            r = s.post(f"https://{dhost}/v2/{dpath}/blobs/uploads/", params={"mount": blob, "from": spath},
                       timeout=120)
            if r.status_code == 201:
                return
        src = s.get(f"https://{shost}/v2/{spath}/blobs/{blob}", stream=True, timeout=600)
        if src.status_code != 200:
            raise ConfigError(f"cannot read blob {blob} of {shost}/{spath}: {src.status_code}")
        start = s.post(f"https://{dhost}/v2/{dpath}/blobs/uploads/", timeout=120)
        if start.status_code != 202:
            raise ConfigError(f"cannot start an upload to {dhost}/{dpath}: {start.status_code} {start.text[:200]}")
        location = start.headers["Location"]
        if location.startswith("/"):
            location = f"https://{dhost}{location}"
        sep = "&" if "?" in location else "?"
        put = s.put(f"{location}{sep}digest={quote(blob)}", data=_Sized(src.raw, src.headers.get("Content-Length")),
                    timeout=1800,
                    headers={"Content-Type": "application/octet-stream",
                             "Content-Length": src.headers.get("Content-Length", "")})
        if put.status_code not in (201, 204):
            raise ConfigError(f"blob {blob} upload to {dhost}/{dpath} failed: {put.status_code} {put.text[:200]}")

    def copy_manifest(ref):
        if have(dhost, dpath, "manifests", ref):
            return
        body, kind = manifest(shost, spath, ref)
        doc = json.loads(body)
        if kind in _INDEX_TYPES or doc.get("manifests"):
            for child in doc.get("manifests", []):
                copy_manifest(child["digest"])
        else:
            for blob in [doc["config"]["digest"], *(layer["digest"] for layer in doc.get("layers", []))]:
                copy_blob(blob)
        r = s.put(f"https://{dhost}/v2/{dpath}/manifests/{ref}", data=body, headers={"Content-Type": kind},
                  timeout=120)
        if r.status_code not in (200, 201):
            raise ConfigError(f"manifest upload to {dhost}/{dpath} failed: {r.status_code} {r.text[:200]}")

    log(f"copying {source} -> {dhost}/{dpath} ...")
    copy_manifest(digest)
    r = s.head(f"https://{dhost}/v2/{dpath}/manifests/{digest}", headers={"Accept": _MANIFEST_TYPES}, timeout=60)
    got = r.headers.get("Docker-Content-Digest")
    if r.status_code != 200 or got != digest:
        raise ConfigError(f"after copying, {dest} has digest {got or 'none'}, expected {digest}")
    return got


# --------------------------------------------------------------- templates --

def _download_template(target, creds) -> str:
    import tempfile

    from kfp.registry import RegistryClient

    repo, package, version = target.template_uri.rsplit("/", 2)
    with tempfile.TemporaryDirectory() as d:
        path = RegistryClient(host=repo, auth=creds).download_pipeline(package, version=version,
                                                                        file_name=f"{d}/template.yaml")
        return Path(path).read_text(encoding="utf-8")


def template_differences(source_text: str, dest_text: str, rewrites: dict[str, str]) -> list[str]:
    """Where the destination's compiled template differs from the source template once each source image address
    is rewritten to the destination's. Empty: the same pipeline, pointing at the destination's copy of the images."""
    for old, new in rewrites.items():
        source_text = source_text.replace(old, new)
    a, b = yaml.safe_load(source_text), yaml.safe_load(dest_text)
    out: list[str] = []

    def walk(x, y, path):
        if len(out) >= 12:
            return
        if isinstance(x, dict) and isinstance(y, dict):
            for k in sorted(set(x) | set(y), key=str):
                if k not in x or k not in y:
                    out.append(f"{path}.{k}: only in the {'destination' if k in y else 'source'}")
                else:
                    walk(x[k], y[k], f"{path}.{k}")
        elif isinstance(x, list) and isinstance(y, list) and len(x) == len(y):
            for i, (p, q) in enumerate(zip(x, y, strict=True)):
                walk(p, q, f"{path}[{i}]")
        elif x != y:
            out.append(f"{path}: {json.dumps(x, default=str)[:80]} != {json.dumps(y, default=str)[:80]}")

    walk(a, b, "template")
    return out


# ------------------------------------------------------------------- model --

def _model_api(env: Environment) -> str:
    return f"https://{env.region}-aiplatform.googleapis.com/v1"


def _find_model(gcp: Gcp, env: Environment, display_name: str) -> dict | None:
    r = gcp.get(f"{_model_api(env)}/projects/{env.project}/locations/{env.region}/models",
                filter=f'display_name="{display_name}"')
    if not r.ok:
        raise ConfigError(f"cannot list models in {env.name}: {r.status} {str(r.body)[:200]}")
    models = r.body.get("models", [])
    return models[0] if models else None


def _versions(gcp: Gcp, env: Environment, model: dict) -> list[dict]:
    r = gcp.get(f"{_model_api(env)}/{model['name']}:listVersions", pageSize=100)
    if not r.ok:
        raise ConfigError(f"cannot list versions of {model['displayName']}: {r.status}")
    return r.body.get("models", [])


def _objects(gcp: Gcp, uri: str) -> list[dict]:
    bucket, _, prefix = uri.removeprefix("gs://").partition("/")
    prefix = prefix.rstrip("/") + "/"
    out, token = [], None
    while True:
        params = {"prefix": prefix, "fields": "items(name,size,crc32c),nextPageToken"}
        if token:
            params["pageToken"] = token
        r = gcp.get(f"{GCS}/b/{bucket}/o", **params)
        if not r.ok:
            raise ConfigError(f"cannot list {uri}: {r.status}")
        out += r.body.get("items", [])
        token = r.body.get("nextPageToken")
        if not token:
            return [dict(o, bucket=bucket, relative=o["name"][len(prefix):]) for o in out]


def _sha256(gcp: Gcp, bucket: str, name: str) -> str:
    h = hashlib.sha256()
    r = gcp.session.get(f"{GCS}/b/{bucket}/o/{quote(name, safe='')}", params={"alt": "media"}, stream=True, timeout=600)
    if r.status_code != 200:
        raise ConfigError(f"cannot read gs://{bucket}/{name}: {r.status_code}")
    for chunk in r.iter_content(1 << 20):
        h.update(chunk)
    return "sha256:" + h.hexdigest()


def copy_model_files(gcp: Gcp, source_uri: str, dest_uri: str, log=print) -> list[dict]:
    """Copies every file under the model's artifact URI (rewrite API: the destination bucket's key applies) and
    checks each copy's CRC32C, which does not depend on the key. Returns [{file, crc32c, size}]."""
    files = _objects(gcp, source_uri)
    if not files:
        raise ConfigError(f"no model files under {source_uri}")
    dbucket, _, dprefix = dest_uri.removeprefix("gs://").partition("/")
    dprefix = dprefix.rstrip("/") + "/"
    copied = []
    for f in files:
        if not f["relative"]:
            continue
        dname = dprefix + f["relative"]
        token, done = None, None
        while done is None:
            params = {"rewriteToken": token} if token else {}
            r = gcp.session.post(f"{GCS}/b/{f['bucket']}/o/{quote(f['name'], safe='')}/rewriteTo/b/{dbucket}/o/"
                                 f"{quote(dname, safe='')}", params=params, json={}, timeout=600)
            if r.status_code != 200:
                raise ConfigError(f"copying {f['name']} failed: {r.status_code} {r.text[:200]}")
            body = r.json()
            token = body.get("rewriteToken")
            done = body.get("resource") if body.get("done") else None
        entry = {"file": f["relative"], "crc32c": done.get("crc32c"), "size": f.get("size")}
        if f.get("crc32c"):
            if done.get("crc32c") != f["crc32c"]:
                raise ConfigError(f"{dname}: checksum {done.get('crc32c')} after copying, {f['crc32c']} before")
        else:
            # Some writers (e.g. a FUSE mount) leave no stored checksum: compare the bytes themselves.
            before, after = _sha256(gcp, f["bucket"], f["name"]), _sha256(gcp, dbucket, dname)
            if before != after:
                raise ConfigError(f"{dname}: SHA-256 {after} after copying, {before} before")
            entry["sha256"] = after
        copied.append(entry)
    log(f"model files: {len(copied)} copied to {dest_uri}, contents verified")
    return copied


def _wait(gcp: Gcp, env: Environment, operation: dict, what: str) -> dict:
    name = operation["name"]
    for _ in range(120):
        if operation.get("done"):
            if operation.get("error"):
                raise ConfigError(f"{what} failed: {operation['error'].get('message')}")
            return operation.get("response", {})
        time.sleep(5)
        operation = gcp.get(f"{_model_api(env)}/{name}").body
    raise ConfigError(f"{what}: still running after 10 minutes ({name})")


def source_version(gcp: Gcp, src_env: Environment, name: str, release_id: str, version: str | None,
                   loop_case: str | None = None) -> tuple[str, dict]:
    """The source version a promotion takes: the default (approved) one, or the given one for a closed retrain loop,
    trained by the release being promoted. Checked before anything is copied."""
    src_model = _find_model(gcp, src_env, name)
    if src_model is None:
        raise ConfigError(f"{name} has no versions in {src_env.name}")
    versions = {v["versionId"]: v for v in _versions(gcp, src_env, src_model)}
    if version is None:
        version = next((k for k, v in versions.items() if "default" in v.get("versionAliases", [])), None)
    chosen = versions.get(str(version))
    if chosen is None:
        raise ConfigError(f"{name} has no version {version} in {src_env.name} (versions: {', '.join(versions)})")
    if "default" not in chosen.get("versionAliases", []) and not loop_case:
        raise ConfigError(f"v{version} of {name} is not the default (approved) version in {src_env.name}: approve it "
                          "there first (`pipewright model approve`), so production gets only what was approved")
    trained_by = (chosen.get("labels") or {}).get("release")
    if trained_by and trained_by != publish._tag(release_id)[:63]:
        raise ConfigError(f"v{version} of {name} was trained by release {trained_by}, not {release_id}: promote the "
                          "release that trained it, or retrain")
    return str(version), chosen


def promote_model(gcp: Gcp, src_env: Environment, dst_env: Environment, uc_dst: ResolvedUseCase,
                  release_id: str, version: str | None, log=print, loop_case: str | None = None) -> dict:
    """The source's approved version, as a new candidate version in the destination's Model Registry.
    loop_case: a closed retrain loop's candidate (FR-48), validated in the source and approved for the destination
    by the loop's production approval; it need not be the source's default."""
    name = uc_dst.model["name"]
    version, chosen = source_version(gcp, src_env, name, release_id, version, loop_case)
    dest_uri = (f"{publish.pipeline_root(dst_env, uc_dst)}/promoted/{src_env.name}/{release_id}/"
                f"v{version}/model")
    files = copy_model_files(gcp, chosen["artifactUri"], dest_uri, log)

    dst_model = _find_model(gcp, dst_env, name)
    from pipewright.models import online

    # FR-76: the destination's own online lookup, never the source's (serving.json in the files names the source's).
    spec = online.container_spec(chosen.get("containerSpec"), uc_dst.ml.get("online_features"))
    body = {"model": {
        "displayName": name, "artifactUri": dest_uri, "containerSpec": spec,
        "versionAliases": [f"promoted-{src_env.name}-v{version}"[:128]],
        "versionDescription": f"promoted from {src_env.name} v{version} (release {release_id})",
        "labels": {"usecase": uc_dst.name[:63], "status": "candidate", "release": publish._tag(release_id)[:63],
                   "promoted-from": src_env.name[:63], "source-version": str(version),
                   **({"pipewright-loop": publish._tag(loop_case)[:63]} if loop_case else {})},
        **({"encryptionSpec": {"kmsKeyName": dst_env.security["kms_key"]}} if dst_env.security.get("kms_key") else {}),
    }}
    if dst_model:
        body["parentModel"] = dst_model["name"]
    r = gcp.post(f"{_model_api(dst_env)}/projects/{dst_env.project}/locations/{dst_env.region}/models:upload", body)
    if not r.ok:
        raise ConfigError(f"model upload in {dst_env.name} failed: {r.status} {str(r.body)[:300]}")
    done = _wait(gcp, dst_env, r.body, f"model upload in {dst_env.name}")
    dst_version = str(done.get("modelVersionId") or "1")
    log(f"model: {name} v{version} ({src_env.name}) -> v{dst_version} ({dst_env.name}), a candidate")
    return {"name": name, "source_version": str(version), "source_artifact_uri": chosen["artifactUri"],
            "artifact_uri": dest_uri, "version": dst_version, "files": files}


def _promote_loop_model(gcp: Gcp, src_env: Environment, dst_env: Environment, uc_dst: ResolvedUseCase,
                        live: ReleaseContract, name: str, version: str, loop_case: str, who: str,
                        log=print) -> Promotion:
    """The release is live in the destination already (the loop's candidate was trained by it): only the model is
    promoted. Repeating it finds the version promoted before."""
    uri = f"gs://{dst_env.artifacts_bucket}/{name}"
    dst_model = _find_model(gcp, dst_env, uc_dst.model["name"])
    for v in _versions(gcp, dst_env, dst_model) if dst_model else []:
        labels = v.get("labels") or {}
        if labels.get("promoted-from") == src_env.name[:63] and labels.get("source-version") == str(version):
            log(f"model: v{version} ({src_env.name}) was promoted already as v{v['versionId']} ({dst_env.name})")
            return Promotion(live, uri, live.release_id, live.promoted_from, str(v["versionId"]), unchanged=True)
    model = _promote_model_files(gcp, src_env, dst_env, uc_dst, live.release_id, version, log, loop_case=loop_case)
    events.emit(events.event("model.promoted", uc_dst.name, dst_env.name, uc_dst.tenant, live.release_id, None,
                             by=who, source_environment=src_env.name, loop=loop_case,
                             model={k: model[k] for k in ("name", "source_version", "version")}),
                dst_env.observability.get("events_topic"), gcp, stream=_quiet(), region=dst_env.region)
    return Promotion(live, uri, live.release_id, {**(live.promoted_from or {}), "model": model}, model["version"])


def _promote_model_files(gcp: Gcp, src_env: Environment, dst_env: Environment, uc_dst: ResolvedUseCase,
                         release_id: str, version: str | None, log=print, loop_case: str | None = None) -> dict:
    """The model's files to the destination: a container model's artifacts, or a BigQuery ML model exported,
    imported and parity checked (bqml/promotion.py, Pipewright 1.1)."""
    if (uc_dst.ml or {}).get("bqml"):
        from pipewright.bqml.promotion import promote_bqml_model

        uc_src = resolve_usecase(Path(uc_dst.source), src_env)
        return promote_bqml_model(gcp, src_env, dst_env, uc_src, uc_dst, release_id, version, log,
                                  loop_case=loop_case)
    return promote_model(gcp, src_env, dst_env, uc_dst, release_id, version, log, loop_case=loop_case)


# --------------------------------------------------------------- promotion --

def _source_release(gcp: Gcp, src_env: Environment, usecase: str, release_id: str | None) -> ReleaseContract:
    pointer, _ = read_pointer(gcp, src_env, usecase)
    if pointer is None:
        raise ConfigError(f"{usecase} has never been released in {src_env.name}")
    known = {pointer.release_id: pointer.contract, **{h["release_id"]: h["contract"] for h in pointer.history}}
    rid = release_id or pointer.release_id
    if rid not in known:
        raise ConfigError(f"release {rid} of {usecase} is not live or in the release history of {src_env.name} "
                          f"(known: {', '.join(known)})")
    return read_contract(gcp, src_env, known[rid])


def _check_checkout(uc: ResolvedUseCase, commit: str | None) -> None:
    if not commit:
        raise ConfigError("the source release has no source commit, so its templates cannot be recompiled here")
    head = _git(uc.directory, "rev-parse", "HEAD")
    if head != commit:
        raise ConfigError(f"check out the source release's commit {commit[:12]} (this checkout is at "
                          f"{(head or 'no commit')[:12]}): the destination's templates are compiled from it")
    if _git(uc.directory, "status", "--porcelain", "--", "."):
        raise ConfigError(f"{uc.directory} has uncommitted changes: promote from a clean checkout of {commit[:12]}")


def promote(usecase_path: Path, src_env: Environment, dst_env: Environment, gcp: Gcp | None = None,
            release_id: str | None = None, model_version: str | None = None, accept_template_change: bool = False,
            skip_tables: bool = False, approve: bool = False, build_dir: Path | None = None, log=print,
            loop_case: str | None = None) -> Promotion:
    """loop_case: the closed retrain loop promotes its validated candidate (model_version), which need not be the
    source's default; with the release live already, only the model is promoted."""
    gcp = gcp or Gcp()
    who = gcp.identity()

    # 1. Allowed source.
    if dst_env.promotion_from != src_env.name:
        raise ConfigError(f"{dst_env.name} takes releases from {dst_env.promotion_from or 'no environment'} "
                          f"(environments/{dst_env.name}.yaml: promotion.from), not {src_env.name}")

    # 2. Destination policies, before anything is copied: resolving for the destination applies its manifest.
    uc_src = resolve_usecase(usecase_path, src_env)
    uc_dst = resolve_usecase(usecase_path, dst_env)
    source = _source_release(gcp, src_env, uc_src.name, release_id)
    rid = source.release_id
    _check_checkout(uc_dst, source.source_commit)
    mode = uc_dst.model.get("promotion", "model")
    policy = [f"{dst_env.name} takes releases from {src_env.name}",
              f"{uc_dst.name} resolves under {dst_env.name}'s manifest (policies, features, Ray limits)",
              f"model promotion: {mode}"]
    log(f"promote {uc_dst.name} release {rid}: {src_env.name} -> {dst_env.name} ({mode})")

    # 3. Idempotent: the destination release ID is the source's; a finished promotion is a no-op.
    name = contract_name(uc_dst.name, rid)
    source_text = json.dumps(json.loads(source.to_json()), sort_keys=True)
    source_sha = "sha256:" + hashlib.sha256(source_text.encode()).hexdigest()
    existing_text, _ = gcs_read(gcp, dst_env.artifacts_bucket, name)
    pointer, generation = read_pointer(gcp, dst_env, uc_dst.name)
    if existing_text:
        existing = ReleaseContract.from_json(existing_text)
        if (existing.promoted_from or {}).get("contract_sha256") != source_sha:
            raise ConfigError(f"{dst_env.name} already has a different release {rid} of {uc_dst.name}: refusing to "
                              "replace it (contracts are write-once)")
        if pointer and pointer.release_id == rid:
            if not (loop_case and model_version and mode == "model"):
                log(f"{uc_dst.name} release {rid} is already live in {dst_env.name}: nothing to do")
                return Promotion(existing, f"gs://{dst_env.artifacts_bucket}/{name}", pointer.release_id,
                                 existing.promoted_from, unchanged=True)
            return _promote_loop_model(gcp, src_env, dst_env, uc_dst, existing, name, model_version, loop_case,
                                       who, log)

    # 4. Templates: compiled for the destination (at the destination's image addresses), compared with the
    # source's, before anything is copied.
    registry = dst_env.artifact_registry["docker"]
    dst_image = destination_image(source.component_image, registry)
    dst_ray = destination_image(source.ray["image"], registry) if source.ray else None
    # Synthetic data may be switched off in the destination (it is not part of what a model release carries): the
    # synthetic pipeline is then left out. Switched on there but not in the source, nothing tested would be promoted.
    if uc_dst.synthetic_image_needed and not source.synthetic:
        raise ConfigError(f"synthetic data is on for {uc_dst.name} in {dst_env.name} but the source release has no "
                          "synthetic pipeline: release it with synthetic data on in the source first")
    dst_synthetic = (tuple(destination_image(source.synthetic[k], registry) for k in ("image", "evaluation_image"))
                     if source.synthetic and uc_dst.synthetic_image_needed else None)
    rewrites = {source.component_image: dst_image, **({source.ray["image"]: dst_ray} if dst_ray else {}),
                **({source.synthetic["image"]: dst_synthetic[0],
                    source.synthetic["evaluation_image"]: dst_synthetic[1]} if dst_synthetic else {})}
    out_dir = build_dir or (uc_dst.directory / ".pipewright" / "promote")
    infos = publish.compile_pinned(uc_dst, dst_image, out_dir, dst_synthetic)
    expected = set(source.pipeline_names) - ({"synthetic"} if source.synthetic and not dst_synthetic else set())
    if set(infos) != expected:
        raise ConfigError(f"the destination compiles pipelines {sorted(infos)}, the source release has "
                          f"{source.pipeline_names}")
    differences = {}
    for pipeline, info in infos.items():
        diff = template_differences(_download_template(source.pipeline(pipeline), gcp.session.credentials),
                                    info.spec_path.read_text(encoding="utf-8"), rewrites)
        if diff:
            differences[pipeline] = diff
    if differences and not accept_template_change:
        lines = [f"{p}: {d}" for p, ds in differences.items() for d in ds]
        raise ConfigError(["the destination's pipelines compile differently from the tested ones (an environment "
                           "override that changes a pipeline's shape, such as tuning.parallel_trials or steps); "
                           "nothing was promoted. Align the overrides, or pass --accept-template-change to promote "
                           "anyway (recorded):", *lines])
    policy.append("templates equal to the source's with the registry address rewritten" if not differences else
                  f"template differences accepted by {who}: {sum(len(d) for d in differences.values())}")

    # Model Registry makes a model's first version its default. Under a manual approval policy that version must be
    # approved explicitly, by the promoter, as part of the promotion (recorded); it is refused before any copy.
    first_version = mode == "model" and _find_model(gcp, dst_env, uc_dst.model["name"]) is None
    manual = uc_dst.approvals.get("promote_model", "manual") != "auto"
    if first_version and manual and not approve:
        raise ConfigError(f"{uc_dst.model['name']} has no version in {dst_env.name} yet, and Model Registry makes a "
                          "model's first version its default: approvals.promote_model is manual there, so approve it "
                          "with this promotion (--approve, recorded as your approval); nothing was promoted")
    if first_version:
        policy.append(f"first version in {dst_env.name}: approved by {who} with the promotion"
                      if manual else f"first version in {dst_env.name}: the default (promote_model auto)")

    # The model version is checked now too (the default, trained by this release), before anything is copied.
    if mode == "model":
        source_version(gcp, src_env, uc_dst.model["name"], rid, model_version, loop_case)

    # 5. The destination's tables, as a release creates them (configuration, additive only).
    if not skip_tables:
        results = tables_sync.sync(uc_dst, dst_env, gcp)
        log(tables_sync.render(results, uc_dst, dry_run=False))
        if any(r.error or r.plan.action == "blocked" for r in results):
            raise ConfigError(f"tables sync in {dst_env.name} failed: fix the table plan; nothing was promoted")

    # 5b. Copy and verify the images.
    copy_image(gcp, source.component_image, dst_image, log)
    if dst_ray:
        copy_image(gcp, source.ray["image"], dst_ray, log)
    if dst_synthetic:
        copy_image(gcp, source.synthetic["image"], dst_synthetic[0], log)
        copy_image(gcp, source.synthetic["evaluation_image"], dst_synthetic[1], log)

    # 6. The model, unchanged.
    model = None
    if mode == "model":
        model = _promote_model_files(gcp, src_env, dst_env, uc_dst, rid, model_version, log, loop_case=loop_case)
        if loop_case:
            policy.append(f"closed retrain loop {loop_case}: v{model_version} validated in {src_env.name}, approved "
                          f"for {dst_env.name} by {who}")

    # 7. Templates uploaded, the contract written once.
    uploaded = {p: publish.upload_template(info, dst_env.artifact_registry["kfp"], uc_dst, rid, gcp.session.credentials)
                for p, info in infos.items()}
    contract = publish.build_contract(uc_dst, dst_env, infos, uploaded, dst_image, rid, source.source_commit,
                                      ray_image=dst_ray, synthetic_images=dst_synthetic)
    record = {
        "environment": src_env.name, "project": src_env.project, "release_id": rid, "contract_sha256": source_sha,
        "contract": f"gs://{src_env.artifacts_bucket}/{contract_name(source.usecase, rid)}",
        "images": rewrites,
        "templates": {p: {"source": source.pipeline(p).template_digest, "destination": uploaded[p][1],
                          "differences": differences.get(p, [])} for p in infos},
        "model": model, "by": who, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "policy": policy,
    }
    contract = ReleaseContract.from_dict({**json.loads(contract.to_json()), "promoted_from": record})
    if not existing_text:
        try:
            gcs_write(gcp, dst_env.artifacts_bucket, name, contract.to_json(), if_generation_match=0)
        except PreconditionFailed:
            raise ConfigError(f"another promotion wrote release {rid} of {uc_dst.name} meanwhile; nothing went live. "
                              "Re-run to check it.") from None
    checks = verify(contract, gcp, gcp.session.credentials)

    # 8. The pointer, last.
    how = events.approval()                           # FR-84: the person behind a CI identity, on the pointer
    moved = pointer.moved_to(rid, name, who, how) if pointer else new_pointer(uc_dst.name, rid, name, who, how)
    try:
        gcs_write(gcp, dst_env.artifacts_bucket, pointer_name(uc_dst.name), moved.to_json(),
                  if_generation_match=generation)
    except PreconditionFailed:
        raise ConfigError(f"the {dst_env.name} pointer of {uc_dst.name} moved while promoting; the contract is "
                          "written but not live. Re-run to finish.") from None
    log(f"live in {dst_env.name}: {rid}" + (f" (was {pointer.release_id})" if pointer else " (first release)"))
    if model and first_version and manual:
        events.emit(events.event("model.approved", uc_dst.name, dst_env.name, uc_dst.tenant, rid, None,
                                 model=model["name"], version=model["version"], previous=None, by=who,
                                 via="promotion", source_environment=src_env.name), dst_env.observability.get(
                                     "events_topic"), gcp, stream=_quiet(), region=dst_env.region)
    events.emit(events.event("release.promoted", uc_dst.name, dst_env.name, uc_dst.tenant, rid, None, by=who,
                             previous=pointer.release_id if pointer else None, source_environment=src_env.name,
                             source_contract_sha256=source_sha, component_image=dst_image,
                             model=({k: model[k] for k in ("name", "source_version", "version")} if model else None)),
                dst_env.observability.get("events_topic"), gcp, stream=_quiet(), region=dst_env.region)
    return Promotion(contract, f"gs://{dst_env.artifacts_bucket}/{name}", pointer.release_id if pointer else None,
                     record, model["version"] if model else None, checks)
