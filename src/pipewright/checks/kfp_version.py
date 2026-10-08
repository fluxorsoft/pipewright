"""KFP version alignment between Pipewright's compiler and a use case's component image.

Pipewright compiles the pipeline with its own `kfp`; each step runs the `kfp` executor installed from the use
case's components/requirements.txt. The two must share major.minor, so the requirement has to pin it.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from packaging.requirements import InvalidRequirement, Requirement
from packaging.version import Version

from pipewright.config import ResolvedUseCase


@dataclass
class KfpCheck:
    ok: bool
    message: str
    skipped: bool = False


def compiler_version() -> str | None:
    try:
        return version("kfp")
    except PackageNotFoundError:
        return None


def requirements_path(uc: ResolvedUseCase) -> Path:
    return uc.requirements or (uc.components_dir / "requirements.txt")


def find_kfp_requirement(path: Path) -> tuple[Requirement | None, str | None]:
    """(requirement, original line) for kfp; follows `-r other.txt` includes next to the file."""
    if not path.is_file():
        return None, None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-r ", "--requirement ")):
            req, src = find_kfp_requirement(path.parent / line.split(maxsplit=1)[1])
            if req:
                return req, src
            continue
        if line.startswith("-"):
            continue                                   # --extra-index-url and other pip options
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        if req.name.lower().replace("_", "-") == "kfp":
            return req, line
    return None, None


def _allows(spec, v: str) -> bool:
    return spec.contains(v, prereleases=False)


def check(uc: ResolvedUseCase, compiler: str | None = None) -> KfpCheck:
    compiler = compiler or compiler_version()
    path = requirements_path(uc)
    rel = path.name if path.parent == uc.components_dir else str(path)
    if compiler is None:
        return KfpCheck(True, "skipped: kfp is not installed here (install pipewright-mlops[gcp])", skipped=True)
    cv = Version(compiler)
    major, minor = cv.major, cv.minor
    want = f"kfp=={major}.{minor}.*"

    if not path.is_file():
        return KfpCheck(False, f"no requirements file for the component image (looked for {path})")
    req, line = find_kfp_requirement(path)
    if req is None:
        return KfpCheck(False, f"{rel} has no kfp requirement: add `{want}` "
                               f"(Pipewright compiles with kfp {compiler})")
    spec = req.specifier
    same = any(_allows(spec, f"{major}.{minor}.{z}") for z in range(0, 100))
    others = [f"{major}.{m}.{z}" for m in range(max(0, minor - 10), minor + 11) if m != minor for z in (0, 5, 50)]
    others += [f"{major + 1}.0.0"] + ([f"{major - 1}.99.0"] if major > 0 else [])
    leaks = [v for v in others if _allows(spec, v)]

    if not same:
        return KfpCheck(False, f"`{line}` in {rel} does not match Pipewright's compiler (kfp {compiler}): "
                               f"pin `{want}`")
    if leaks or not str(spec):
        return KfpCheck(False, f"`{line}` in {rel} allows kfp versions other than {major}.{minor}.x "
                               f"(e.g. {leaks[0] if leaks else 'any'}): pin `{want}` to match Pipewright's "
                               f"compiler (kfp {compiler})")
    return KfpCheck(True, f"`{line}` matches Pipewright's compiler (kfp {compiler})")
