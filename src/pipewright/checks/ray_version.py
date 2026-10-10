"""Ray version alignment between a use case's component image (the Ray client) and its clusters.

A Ray step submits work from the component image to a Ray on Vertex AI cluster. The client and the cluster must run
the same Ray release, so components/requirements.txt has to pin `ray[default]` to the platform's version, and
include `google-cloud-aiplatform[ray]` (it resolves `vertex_ray://` cluster addresses).
"""

from __future__ import annotations

from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement

from pipewright.checks.kfp_version import KfpCheck, requirements_path
from pipewright.config import ResolvedUseCase
from pipewright.ray_clusters.settings import RAY_RELEASES


def _requirements(path: Path, seen: set | None = None) -> list[tuple[Requirement, str]]:
    """Every requirement in a file, following `-r other.txt` includes next to it."""
    seen = seen or set()
    if not path.is_file() or path.resolve() in seen:
        return []
    seen.add(path.resolve())
    out = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-r ", "--requirement ")):
            out += _requirements(path.parent / line.split(maxsplit=1)[1], seen)
            continue
        if line.startswith("-"):
            continue
        try:
            out.append((Requirement(line), line))
        except InvalidRequirement:
            continue
    return out


def check(uc: ResolvedUseCase) -> KfpCheck:
    if not uc.ray:
        return KfpCheck(True, "no Ray steps", skipped=True)
    release = RAY_RELEASES.get(uc.ray["ray_version"])
    path = requirements_path(uc)
    rel = path.name if path.parent == uc.components_dir else str(path)
    want = f"ray[default]=={release}"
    found = {r.name.lower().replace("_", "-"): (r, line) for r, line in _requirements(path)}
    problems = []
    ray = found.get("ray")
    if ray is None:
        problems.append(f"add `{want}`")
    elif not (str(ray[0].specifier).startswith("==") and ray[0].specifier.contains(release)):
        problems.append(f"`{ray[1]}` must pin `{want}` (the clusters run Ray {release})")
    aiplatform = found.get("google-cloud-aiplatform")
    if aiplatform is None or "ray" not in aiplatform[0].extras:
        problems.append("add the `ray` extra to google-cloud-aiplatform (`google-cloud-aiplatform[ray]==<version>`)")
    if problems:
        return KfpCheck(False, f"{rel}: the Ray steps need the Ray client: {'; '.join(problems)}")
    return KfpCheck(True, f"Ray client pinned to {release}, matching the clusters")
