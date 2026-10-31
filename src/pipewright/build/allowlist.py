"""Approved image allow-list (FR-54): the base images and package indexes a use case's images may be built from.

The platform sets it per environment in the manifest (`policies.images`):

    base_images         patterns (fnmatch) every base image must match: the component, Ray and synthetic images
    package_indexes     the pip indexes requirement files may name; the first is used when a file names none, and
                        direct references (`pkg @ https://...`, `git+...`, local paths) are refused
    default_base_image  the base for the component and synthetic images instead of python:3.11-slim (a mirror)

Without `policies.images` nothing changes: images build from their usual bases and PyPI. `validate` reports the
problems, `release` refuses them before building, and the release contract records the base images by digest.
"""

from __future__ import annotations

import fnmatch
import shlex
from pathlib import Path

DEFAULT_BASE = "python:3.11-slim"
_INDEX = ("-i", "--index-url", "--extra-index-url", "-f", "--find-links")
_INCLUDE = ("-r", "--requirement", "-c", "--constraint")
_DIRECT = ("git+", "hg+", "svn+", "bzr+", "http://", "https://", "file:", "./", "../", "/")


def policy(env) -> dict:
    return ((getattr(env, "policies", None) or {}).get("images")) or {}


def component_base(pol: dict) -> str:
    return pol.get("default_base_image") or DEFAULT_BASE


def _norm(url: str) -> str:
    return url.strip().rstrip("/")


def sources(text: str, base: Path | None = None, seen: set | None = None) -> tuple[list[str], list[str]]:
    """(indexes, direct references) a requirements text names, following -r/-c includes next to `base`."""
    indexes: list[str] = []
    direct: list[str] = []
    seen = seen if seen is not None else set()
    for raw in text.splitlines():
        line = raw.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        try:
            tokens = shlex.split(line, posix=True)
        except ValueError:
            tokens = line.split()
        first = tokens[0]
        name, _, value = first.partition("=")
        if name in _INDEX or name in _INCLUDE:
            value = value or (tokens[1] if len(tokens) > 1 else "")
            if name in _INDEX:
                indexes.append(value)
            elif base is not None and value:
                path = (base / value).resolve()
                if path not in seen and path.is_file():
                    seen.add(path)
                    more, refs = sources(path.read_text(encoding="utf-8"), path.parent, seen)
                    indexes += more
                    direct += refs
            continue
        if first in ("-e", "--editable") or " @ " in line or line.startswith(_DIRECT):
            direct.append(line)
    return indexes, direct


def problems(pol: dict, bases: dict[str, str], requirements: dict[str, tuple[str, Path | None]]) -> list[str]:
    """bases: {what: image}; requirements: {what: (text, folder for includes)}."""
    out: list[str] = []
    allowed = pol.get("base_images")
    if allowed:
        for what, image in bases.items():
            if not any(fnmatch.fnmatchcase(image, p) for p in allowed):
                out.append(f"policies.images: the {what} is built from {image}, which is not an approved base image "
                           f"({', '.join(allowed)})")
    indexes = pol.get("package_indexes")
    if indexes is not None:
        known = {_norm(i) for i in indexes}
        for what, (text, folder) in requirements.items():
            named, direct = sources(text, folder)
            for i in named:
                if _norm(i) not in known:
                    out.append(f"policies.images: {what} names the package index {i}, which is not approved "
                               f"({', '.join(indexes) or 'none'})")
            for d in direct:
                out.append(f"policies.images: {what} has a direct reference ({d}): with approved package indexes, "
                           "requirements name packages, installed from those indexes")
    return out


def with_index(text: str, pol: dict) -> str:
    """The requirements text with the first approved index when it names none (pip's default is PyPI)."""
    indexes = pol.get("package_indexes")
    if not indexes:
        return text
    named, _ = sources(text)
    if any(n for n in named):
        return text
    return f"--index-url {indexes[0]}\n" + text


def usecase_bases(uc, env, pol: dict) -> dict[str, str]:
    """The base image of each image a release of the use case builds."""
    out = {"component image": getattr(uc, "base_image", None) or component_base(pol)}
    if getattr(uc, "ray", None):
        from pipewright.ray_clusters import settings as ray_settings

        settings = ray_settings.platform_settings(env.ray or None, (env.tenants.get(uc.tenant) or {}).get("ray"),
                                                  private=bool(env.security.get("network")))
        gpu = any(c.get("accelerator") for c in uc.ray["steps"].values())
        out["Ray image"] = ray_settings.base_image(settings, env.region, gpu)
    if getattr(uc, "synthetic_image_needed", False):
        out["synthetic engine image"] = out["synthetic evaluation image"] = component_base(pol)
    return out


def usecase_requirements(uc) -> dict[str, tuple[str, Path | None]]:
    files = {"components.requirements": uc.requirements or (uc.components_dir / "requirements.txt"
                                                             if getattr(uc, "components_dir", None) else None),
             "components.ray_requirements": getattr(uc, "ray_requirements", None),
             "synthetic.requirements": getattr(uc, "synthetic_requirements", None)}
    return {what: (Path(p).read_text(encoding="utf-8"), Path(p).parent) for what, p in files.items()
            if p and Path(p).is_file()}


def check(uc, env) -> list[str]:
    """What `validate` and `release` refuse under the environment's `policies.images` (nothing without one)."""
    pol = policy(env)
    if not pol:
        return []
    return problems(pol, usecase_bases(uc, env, pol), usecase_requirements(uc))
