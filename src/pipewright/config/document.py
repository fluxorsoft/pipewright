"""Assembles one use case document from several files.

usecases/<name>/usecase.yaml           identity, tenant, pipelines, `include:` patterns
usecases/<name>/<included files>       one or more spec sections each; a section lives in exactly one file
usecases/<name>/environments/<env>.yaml  per-environment overrides (same as spec.environments.<env>)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pipewright.config.io import load_yaml
from pipewright.config.merge import merge_parts
from pipewright.errors import ConfigError

USECASES_DIR = "usecases"
USECASE_FILE = "usecase.yaml"
OVERRIDES_DIR = "environments"


@dataclass
class UseCaseDocument:
    doc: dict
    path: Path
    sources: list[str] = field(default_factory=list)       # every file that contributed, relative to the folder
    origins: dict[str, str] = field(default_factory=dict)  # spec section -> file it came from


def discover_usecases(root: Path) -> dict[str, Path]:
    base = Path(root) / USECASES_DIR
    return {p.parent.name: p for p in sorted(base.glob(f"*/{USECASE_FILE}"))}


def load_usecase_document(path: Path) -> UseCaseDocument:
    path = Path(path)
    folder = path.parent
    doc = load_yaml(path)
    if not isinstance(doc, dict) or not isinstance(doc.get("spec"), dict):
        raise ConfigError("expected a UseCase document with a `spec` mapping", source=str(path))
    spec = doc["spec"]
    problems: list[str] = []
    origins = {key: USECASE_FILE for key in spec}
    sources = [USECASE_FILE]

    for pattern in spec.get("include", []):
        files = sorted(p for p in folder.glob(pattern) if p.is_file() and p.resolve() != path.resolve())
        if not files:
            problems.append(f"include pattern {pattern!r} matched no files")
        for f in files:
            rel = f.relative_to(folder).as_posix()
            part = load_yaml(f)
            if part is None:
                continue
            if not isinstance(part, dict):
                problems.append(f"{rel}: must be a mapping of spec sections")
                continue
            merge_parts(spec, part, rel, origins, problems)
            sources.append(rel)

    env_dir = folder / OVERRIDES_DIR
    if env_dir.is_dir():
        inline = spec.setdefault("environments", {})
        for f in sorted(env_dir.glob("*.yaml")):
            rel = f.relative_to(folder).as_posix()
            name = f.stem
            if name in inline:
                problems.append(f"overrides for {name!r} are in both usecase.yaml and {rel}: keep one")
                continue
            content = load_yaml(f) or {}
            if not isinstance(content, dict):
                problems.append(f"{rel}: must be a mapping of overrides")
                continue
            inline[name] = content
            sources.append(rel)
        if not inline:
            spec.pop("environments")

    if problems:
        raise ConfigError(problems, source=str(path))
    return UseCaseDocument(doc, path, sources, origins)
