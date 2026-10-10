"""pipewright release / rollback / status.

release: tables sync -> component image -> compile + upload templates -> contract -> verify -> move pointer
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from pipewright.build import images
from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, PreconditionFailed, gcs_read, gcs_write
from pipewright.observability import events
from pipewright.release import publish
from pipewright.release.contract import Pointer, ReleaseContract, contract_name, new_pointer, pointer_name
from pipewright.release.verify import verify
from pipewright.tables import sync as tables_sync


def _git(directory: Path, *args: str) -> str | None:
    git = shutil.which("git")
    if not git:
        return None
    r = subprocess.run([git, "-C", str(directory), *args], capture_output=True, text=True, encoding="utf-8",
                       errors="replace")
    return r.stdout.strip() if r.returncode == 0 else None


def _quiet():
    """CLI events: the log line goes to a null stream (the terminal shows the human message); the Pub/Sub
    message is what tools consume."""
    import io

    return io.StringIO()


def release_id_for(directory: Path) -> tuple[str, str | None]:
    """(release_id, source_commit). Uncommitted changes get a -dirty-<time> suffix so they never look clean."""
    sha = _git(directory, "rev-parse", "HEAD")
    if not sha:
        return f"local-{int(time.time())}", None
    rid = sha[:12]
    dirty = _git(directory, "status", "--porcelain", "--", ".")
    if dirty:
        rid += f"-dirty-{int(time.time())}"
    return rid, sha


def _token(gcp: Gcp) -> str:
    import google.auth.transport.requests

    creds = gcp.session.credentials
    if not creds.valid:
        creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def read_pointer(gcp: Gcp, env: Environment, usecase: str) -> tuple[Pointer | None, int]:
    text, generation = gcs_read(gcp, env.artifacts_bucket, pointer_name(usecase))
    return (Pointer.from_json(text) if text else None), generation


def read_contract(gcp: Gcp, env: Environment, name: str) -> ReleaseContract:
    text, _ = gcs_read(gcp, env.artifacts_bucket, name)
    if text is None:
        raise ConfigError(f"contract not found: gs://{env.artifacts_bucket}/{name}")
    return ReleaseContract.from_json(text)


@dataclass
class ReleaseResult:
    contract: ReleaseContract
    contract_uri: str
    pointer_uri: str
    previous: str | None
    checks: list[str]


def _ray_image(uc: ResolvedUseCase, env: Environment, release_id: str, gcp: Gcp, log) -> str:
    """The use case's Ray image, on the base image the platform set for its tenant."""
    from pipewright.ray_clusters import settings as ray_settings

    settings = ray_settings.platform_settings(env.ray or None, (env.tenants.get(uc.tenant) or {}).get("ray"),
                                              private=bool(env.security.get("network")))
    gpu = any(c.get("accelerator") for c in uc.ray["steps"].values())
    base = ray_settings.base_image(settings, env.region, gpu)
    log(f"building Ray image on {base} (docker)...")
    image = images.build_and_push_ray(uc, env.artifact_registry["docker"], release_id, _token(gcp), base, gcp=gcp,
                                      log=log)
    log(f"Ray image: {image}")
    return image


def release(uc: ResolvedUseCase, env: Environment, gcp: Gcp | None = None, component_image: str | None = None,
            skip_tables: bool = False, build_dir: Path | None = None, log=print) -> ReleaseResult:
    gcp = gcp or Gcp()
    who = gcp.identity()
    release_id, commit = release_id_for(uc.directory)
    log(f"release {uc.name} -> {env.name} as {release_id}")

    pointer, generation = read_pointer(gcp, env, uc.name)

    if not skip_tables:
        results = tables_sync.sync(uc, env, gcp)
        log(tables_sync.render(results, uc, dry_run=False))
        if any(r.error or r.plan.action == "blocked" for r in results):
            raise ConfigError("tables sync failed: fix the table plan before releasing")

    if component_image:
        log(f"component image (given): {component_image}")
    else:
        log("building component image (docker)...")
        component_image = images.build_and_push(uc, env.artifact_registry["docker"], release_id, _token(gcp),
                                                gcp=gcp, log=log)
        log(f"component image: {component_image}")
    ray_image = _ray_image(uc, env, release_id, gcp, log) if uc.ray else None

    # Contracts are write-once (evidence points at them). The same commit with the same images and templates is a
    # retry: reuse the contract. The same commit with anything different (e.g. a Pipewright upgrade that changes an
    # image or how templates compile) is a new release. Templates are compared by digest, the sha256 of the file.
    out_dir = build_dir or (uc.directory / ".pipewright" / "build")
    infos = publish.compile_pinned(uc, component_image, out_dir)
    digests = {n: "sha256:" + hashlib.sha256(info.spec_path.read_bytes()).hexdigest() for n, info in infos.items()}

    def same(c: ReleaseContract) -> bool:
        released = {n: c.pipeline(n).template_digest for n in c.pipeline_names}
        return (c.component_image == component_image and (c.ray or {}).get("image") == ray_image
                and released == digests and c.pipeline_root == publish.pipeline_root(env, uc))

    existing_text, _ = gcs_read(gcp, env.artifacts_bucket, contract_name(uc.name, release_id))
    existing = ReleaseContract.from_json(existing_text) if existing_text else None
    if existing is not None and not same(existing):
        what = "|".join([component_image, ray_image or "", publish.pipeline_root(env, uc),
                         *(f"{n}={d}" for n, d in sorted(digests.items()))])
        release_id = f"{release_id}-{hashlib.sha256(what.encode()).hexdigest()[:8]}"
        log(f"release {uc.name}: same commit, new images or templates: released as {release_id}")
        existing_text, _ = gcs_read(gcp, env.artifacts_bucket, contract_name(uc.name, release_id))
        existing = ReleaseContract.from_json(existing_text) if existing_text else None
        if existing is not None and not same(existing):
            raise ConfigError(f"contract {release_id} of {uc.name} exists but differs from this build")

    name = contract_name(uc.name, release_id)
    if existing is not None:
        contract = existing
        log(f"contract {release_id} already published with these images and templates: reusing it")
    else:
        uploaded = {}
        for pipeline, info in infos.items():
            uploaded[pipeline] = publish.upload_template(info, env.artifact_registry["kfp"], uc, release_id,
                                                         gcp.session.credentials)
            log(f"template ({pipeline}): {uploaded[pipeline][0]}")
        contract = publish.build_contract(uc, env, infos, uploaded, component_image, release_id, commit,
                                          ray_image=ray_image)
        try:
            gcs_write(gcp, env.artifacts_bucket, name, contract.to_json(), if_generation_match=0)
        except PreconditionFailed:
            raise ConfigError(f"contract {release_id} of {uc.name} was published by another release while this one "
                              "ran; nothing went live. Re-run the release.") from None
    contract_uri = f"gs://{env.artifacts_bucket}/{name}"
    log(f"contract: {contract_uri}")

    checks = verify(contract, gcp, gcp.session.credentials)
    for c in checks:
        log(f"  verified: {c}")

    moved = pointer.moved_to(release_id, name, who) if pointer else new_pointer(uc.name, release_id, name, who)
    try:
        gcs_write(gcp, env.artifacts_bucket, pointer_name(uc.name), moved.to_json(), if_generation_match=generation)
    except PreconditionFailed:
        raise ConfigError(f"another release of {uc.name} moved the pointer while this one ran; nothing went live. "
                          "Re-run the release.") from None
    pointer_uri = f"gs://{env.artifacts_bucket}/{pointer_name(uc.name)}"
    log(f"live: {pointer_uri} -> {release_id}" + (f" (was {pointer.release_id})" if pointer else " (first release)"))
    events.emit(events.event("release.live", uc.name, env.name, uc.tenant, release_id, None, by=who,
                             previous=pointer.release_id if pointer else None, pipelines=contract.pipeline_names,
                             component_image=component_image), env.observability.get("events_topic"), gcp,
                stream=_quiet(), region=env.region)
    return ReleaseResult(contract, contract_uri, pointer_uri, pointer.release_id if pointer else None, checks)


def rollback(usecase: str, env: Environment, gcp: Gcp | None = None, to: str | None = None, log=print) -> Pointer:
    gcp = gcp or Gcp()
    pointer, generation = read_pointer(gcp, env, usecase)
    if pointer is None:
        raise ConfigError(f"{usecase} has never been released in {env.name}")
    if not pointer.history:
        raise ConfigError(f"{usecase} has no previous release to roll back to")
    target = next((h for h in pointer.history if h["release_id"] == to), None) if to else pointer.history[0]
    if target is None:
        known = ", ".join(h["release_id"] for h in pointer.history)
        raise ConfigError(f"release {to!r} not in the history of {usecase} (known: {known})")

    contract = read_contract(gcp, env, target["contract"])
    contract.validate()
    verify(contract, gcp, gcp.session.credentials)

    moved = pointer.moved_to(target["release_id"], target["contract"], gcp.identity())
    moved.history = [h for h in moved.history if h["release_id"] != target["release_id"]] or moved.history
    try:
        gcs_write(gcp, env.artifacts_bucket, pointer_name(usecase), moved.to_json(), if_generation_match=generation)
    except PreconditionFailed:
        raise ConfigError("the pointer changed while rolling back; nothing changed. Re-run.") from None
    backwards = any(h["release_id"] == target["release_id"] for h in pointer.history[:1])
    events.emit(events.event("release.rolled_back", usecase, env.name, contract.tenant, target["release_id"], None,
                             previous=pointer.release_id, by=gcp.identity()), env.observability.get("events_topic"),
                gcp, stream=_quiet(), region=env.region)
    log(f"{usecase} in {env.name}: {pointer.release_id} -> {target['release_id']}"
        + (" (rollback)" if backwards and not to else ""))
    return moved
