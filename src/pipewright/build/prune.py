"""pipewright images prune (Pipewright 1.2.0, FR-87): delete old images from an environment's docker repository,
never one a release may still need.

Every release builds and pushes its images (tagged <release>-<context hash>) and nothing removed them, so a
registry grows with every release. A blanket cleanup policy would break rollback and replay, which run the images
of earlier releases. Prune keeps:

  - every image a contract in any use case's pointer (the live release and its history) pins by digest, and
  - the newest --keep images of each package, whatever references them;

and deletes the rest, in the packages releases use: an image in a package no contract names (a team's own) is
never touched. Promotion copies images into the destination's own registry, so each environment is pruned on its
own (environments that share one repository are read together). It is a dry run unless --apply is given; a
pointer or contract that cannot be read stops it before anything is deleted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import quote

from pipewright.config import Environment
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, error_text, gcs_list, gcs_read
from pipewright.release.contract import CONTRACTS_PREFIX, Pointer

AR = "https://artifactregistry.googleapis.com/v1"


@dataclass
class Image:
    package: str
    digest: str
    tags: list[str]
    uploaded: str
    size: int


@dataclass
class PrunePlan:
    repository: str
    keep: list[Image] = field(default_factory=list)
    delete: list[Image] = field(default_factory=list)
    reasons: dict = field(default_factory=dict)           # digest -> why it is kept
    deleted: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def size(self, images: list[Image]) -> int:
        return sum(i.size for i in images)


def _repository(env: Environment) -> tuple[str, str, str, str]:
    """(host, project, location, repository) of the environment's docker repository."""
    host, project, repo = env.artifact_registry["docker"].split("/", 2)
    return host, project, host.removesuffix("-docker.pkg.dev"), repo


def referenced(gcp: Gcp, env: Environment) -> tuple[dict[str, str], set[str]]:
    """(digest -> 'usecase release', packages) for every image a pointer's contracts (live and history) pin."""
    out: dict[str, str] = {}
    packages: set[str] = set()
    for name in gcs_list(gcp, env.artifacts_bucket, f"{CONTRACTS_PREFIX}/"):
        if not name.endswith("/current.json"):
            continue
        text, _ = gcs_read(gcp, env.artifacts_bucket, name)
        pointer = Pointer.from_json(text)
        for contract in dict.fromkeys([pointer.contract, *(h.get("contract") for h in pointer.history)]):
            if not contract:
                continue
            body, _ = gcs_read(gcp, env.artifacts_bucket, contract)
            if body is None:
                continue                              # a contract deleted by hand pins nothing any more
            data = json.loads(body)
            images = [data.get("component_image"), (data.get("ray") or {}).get("image"),
                      (data.get("synthetic") or {}).get("image"), (data.get("synthetic") or {}).get("evaluation_image")]
            for image in images:
                if image and "@sha256:" in image:
                    out.setdefault(image.split("@", 1)[1], f"{pointer.usecase} {data.get('release_id')}")
                    packages.add(image.split("@", 1)[0].split("/", 3)[-1])
    return out, packages


def images(gcp: Gcp, env: Environment) -> list[Image]:
    """Every image in the environment's docker repository, newest first."""
    _, project, location, repo = _repository(env)
    url = f"{AR}/projects/{project}/locations/{location}/repositories/{repo}/dockerImages"
    out, token = [], None
    while True:
        r = gcp.get(url, pageSize=500, **({"pageToken": token} if token else {}))
        if not r.ok:
            raise ConfigError(f"cannot list {env.artifact_registry['docker']}: {error_text(r)}")
        for i in (r.body or {}).get("dockerImages", []):
            ref = i["uri"].split("/", 3)[-1]                       # <package>@sha256:...
            package, _, digest = ref.partition("@")
            out.append(Image(package, digest, i.get("tags") or [], i.get("uploadTime", ""),
                             int(i.get("imageSizeBytes") or 0)))
        token = (r.body or {}).get("nextPageToken")
        if not token:
            return sorted(out, key=lambda i: i.uploaded, reverse=True)


def plan(gcp: Gcp, env: Environment, keep: int = 3, sharing: list[Environment] = ()) -> PrunePlan:
    """sharing: the other environments that push to the same docker repository; their releases count too."""
    pinned, packages = referenced(gcp, env)
    for other in sharing:
        theirs, their_packages = referenced(gcp, other)
        pinned = {**theirs, **pinned}
        packages |= their_packages
    p = PrunePlan(env.artifact_registry["docker"])
    newest: dict[str, int] = {}
    for image in images(gcp, env):
        if image.package not in packages:
            p.keep.append(image)
            p.reasons[image.digest] = "not a package releases use"
            continue
        newest[image.package] = newest.get(image.package, 0) + 1
        if image.digest in pinned:
            p.keep.append(image)
            p.reasons[image.digest] = f"release {pinned[image.digest]}"
        elif newest[image.package] <= keep:
            p.keep.append(image)
            p.reasons[image.digest] = f"one of the newest {keep}"
        else:
            p.delete.append(image)
    return p


def apply(gcp: Gcp, env: Environment, p: PrunePlan) -> PrunePlan:
    _, project, location, repo = _repository(env)
    for image in p.delete:
        url = (f"{AR}/projects/{project}/locations/{location}/repositories/{repo}/packages/"
               f"{quote(image.package, safe='')}/versions/{image.digest}")
        r = gcp.delete(url, force="true")                           # force: its tags go with it
        if r.ok or r.status == 404:
            p.deleted.append(f"{image.package}@{image.digest}")
        else:
            p.errors.append(f"{image.package}@{image.digest[:19]}: {error_text(r)}")
    return p


def _gb(n: int) -> str:
    return f"{n / 1e9:.1f} GB"


def render(p: PrunePlan, applied: bool) -> str:
    # Image sizes count shared layers once per image; the repository stores each layer once (doctor's figure).
    lines = [f"Images in {p.repository}: {len(p.keep) + len(p.delete)} ({_gb(p.size(p.keep) + p.size(p.delete))} "
             "counting shared layers in every image)"]
    for i in p.keep:
        lines.append(f"  KEEP    {i.package}@{i.digest[:19]}  {_gb(i.size)}  {i.uploaded[:10]}  "
                     f"({p.reasons.get(i.digest)})")
    verb = "DELETED" if applied else "DELETE?"
    for i in p.delete:
        if not applied or f"{i.package}@{i.digest}" in p.deleted:
            lines.append(f"  {verb} {i.package}@{i.digest[:19]}  {_gb(i.size)}  {i.uploaded[:10]}")
    for e in p.errors:
        lines.append(f"  FAIL    {e}")
    lines.append(f"{'freed' if applied else 'would free'} up to {_gb(p.size(p.delete))} (less where layers are shared)"
                 + ("" if applied else "; run again with --apply to delete"))
    return "\n".join(lines)
