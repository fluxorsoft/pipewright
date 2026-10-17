"""Environment file + environment manifest -> Environment."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from pipewright.config.io import load_yaml, read_text
from pipewright.config.schema import prune_unknown, validate
from pipewright.errors import ConfigError

ENVIRONMENTS_DIR = "environments"

# Fields an Environment file may set explicitly; when a manifest is also used they must agree with it.
_ENV_FIELDS = ("project", "region", "artifact_registry", "artifacts_bucket")


@dataclass(frozen=True)
class Environment:
    name: str
    project: str
    region: str
    artifact_registry: dict
    artifacts_bucket: str
    orchestrator: str = "vertex"
    blueprint: str | None = None
    landing_bucket: str | None = None
    launcher: dict = field(default_factory=dict)
    policies: dict = field(default_factory=dict)
    tenants: dict = field(default_factory=dict)
    observability: dict = field(default_factory=dict)
    security: dict = field(default_factory=dict)  # private profiles: kms_key, network, reserved_ip_ranges
    approver_groups: dict = field(default_factory=dict)   # name -> {topic, subscription}: retrain decisions
    ray: dict = field(default_factory=dict)       # Ray clusters: mode, version, image, network attachment, limits
    features: dict = field(default_factory=dict)  # feature tables: required (tenants list their datasets)
    kubernetes: dict = field(default_factory=dict)  # orchestrator kfp: endpoint, ca_certificate, namespace
    schedules: dict = field(default_factory=dict)   # backend (cloud_scheduler | kubernetes), namespace, image
    manifest_ignored: tuple = ()                  # manifest keys this version does not know (R-05): ignored
    promotion_from: str | None = None
    manifest_source: str | None = None
    source: str = ""

    @property
    def blueprint_name(self) -> str | None:
        return self.blueprint.split("@", 1)[0] if self.blueprint else None


def check_requires(manifest: dict, source: str) -> None:
    """The manifest's `requires` (a version specifier such as ">=0.5"): the Pipewright it needs. A platform sets it
    when a newer key is a policy an older version must not silently ignore."""
    spec = manifest.get("requires") if isinstance(manifest, dict) else None
    if not spec:
        return
    from packaging.specifiers import InvalidSpecifier, SpecifierSet
    from packaging.version import Version

    from pipewright import __version__

    try:
        wanted = SpecifierSet(str(spec))
    except InvalidSpecifier:
        raise ConfigError(f"requires: {spec!r} is not a version specifier (for example \">=0.5\")",
                          source=source) from None
    if __version__ == "0.0.0":
        return                                   # a source checkout without an installed version
    if Version(Version(__version__).base_version) not in wanted:
        raise ConfigError(f"this environment needs Pipewright {spec}, and this is {__version__}: upgrade it (and "
                          "the version pinned in CI)", source=source)


def load_manifest(ref: str, base_dir: Path) -> tuple[dict, str, list[str]]:
    """The manifest, validated, with the keys this version does not know removed (returned third)."""
    text, name = read_text(ref, base_dir)
    try:
        manifest = json.loads(text)
    except json.JSONDecodeError as e:
        raise ConfigError(f"manifest is not valid JSON: {e}", source=name) from None
    check_requires(manifest, name)
    manifest, ignored = prune_unknown(manifest, "manifest")
    validate(manifest, "manifest", name)
    return manifest, name, ignored


def environment_from_manifest(ref: str, base_dir: Path = Path(".")) -> Environment:
    """An Environment from the manifest alone (platform services with no MLOps repo: launcher, evidence worker)."""
    manifest, source, ignored = load_manifest(ref, base_dir)
    return Environment(
        name=manifest["environment"], project=manifest["project"], region=manifest["region"],
        artifact_registry=manifest["artifact_registry"], artifacts_bucket=manifest["artifacts_bucket"],
        orchestrator=manifest.get("orchestrator", "vertex"), blueprint=manifest.get("blueprint"),
        landing_bucket=manifest.get("landing_bucket"), launcher=manifest.get("launcher", {}),
        policies=manifest.get("policies", {}), tenants=manifest.get("tenants", {}),
        observability=manifest.get("observability") or {}, security=manifest.get("security") or {},
        approver_groups=manifest.get("approver_groups") or {}, ray=manifest.get("ray") or {},
        features=manifest.get("features") or {}, kubernetes=manifest.get("kubernetes") or {},
        schedules=manifest.get("schedules") or {},
        manifest_ignored=tuple(ignored),
        manifest_source=source, source=source,
    )


def load_environment(root: Path, name: str) -> Environment:
    path = Path(root) / ENVIRONMENTS_DIR / f"{name}.yaml"
    misnamed = path.with_suffix(".yml")
    if not path.exists() and misnamed.exists():
        raise ConfigError(f"{ENVIRONMENTS_DIR}/{misnamed.name}: Pipewright reads .yaml files; rename it to "
                          f"{ENVIRONMENTS_DIR}/{name}.yaml")
    doc = load_yaml(path)
    validate(doc, "environment", str(path))
    spec = doc["spec"]
    problems = []

    if doc["metadata"]["name"] != name:
        problems.append(f"metadata.name is {doc['metadata']['name']!r} but the file is {path.name}")

    manifest, manifest_source, ignored = {}, None, []
    if "manifest" in spec:
        manifest, manifest_source, ignored = load_manifest(spec["manifest"], path.parent)
        if manifest["environment"] != name:
            problems.append(f"manifest is for environment {manifest['environment']!r}, not {name!r}")
        for key in _ENV_FIELDS:
            if key in spec and spec[key] != manifest[key]:
                problems.append(f"spec.{key} is {spec[key]!r} but the manifest says {manifest[key]!r}")

    if problems:
        raise ConfigError(problems, source=str(path))

    def pick(key):
        return spec.get(key, manifest.get(key))

    return Environment(
        name=name,
        project=pick("project"),
        region=pick("region"),
        artifact_registry=pick("artifact_registry"),
        artifacts_bucket=pick("artifacts_bucket"),
        orchestrator=manifest.get("orchestrator", "vertex"),
        blueprint=manifest.get("blueprint"),
        landing_bucket=manifest.get("landing_bucket"),
        launcher=manifest.get("launcher", {}),
        policies=manifest.get("policies", {}),
        tenants=manifest.get("tenants", {}),
        observability=manifest.get("observability") or {},
        security=manifest.get("security") or {},
        approver_groups=manifest.get("approver_groups") or {},
        ray=manifest.get("ray") or {},
        features=manifest.get("features") or {},
        kubernetes=manifest.get("kubernetes") or {},
        schedules=manifest.get("schedules") or {},
        manifest_ignored=tuple(ignored),
        promotion_from=spec.get("promotion", {}).get("from"),
        manifest_source=manifest_source,
        source=str(path),
    )
