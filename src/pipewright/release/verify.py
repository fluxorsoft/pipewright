"""Release gate: a contract may only go live if everything it references exists and agrees with it."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import yaml

from pipewright.errors import ConfigError
from pipewright.gcp import Gcp
from pipewright.release.contract import PipelineTarget, ReleaseContract

_MANIFEST_TYPES = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def _with_retries(fn, attempts: int = 4, first_delay: float = 2.0):
    """Retries transient network failures (connection resets, timeouts) with backoff."""
    import requests

    delay = first_delay
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == attempts:
                raise
            time.sleep(delay)
            delay *= 2


def image_exists(gcp: Gcp, image: str) -> bool:
    repo, ref = image.split("@", 1) if "@" in image else image.rsplit(":", 1)
    host, path = repo.split("/", 1)
    r = _with_retries(lambda: gcp.session.head(f"https://{host}/v2/{path}/manifests/{ref}",
                                               headers={"Accept": _MANIFEST_TYPES}, timeout=30))
    return r.status_code == 200


def _verify_template(contract: ReleaseContract, target: PipelineTarget, creds, passed: list[str]) -> list[str]:
    from kfp.registry import RegistryClient

    problems = []
    repo, package, version = target.template_uri.rsplit("/", 2)
    try:
        with tempfile.TemporaryDirectory() as d:
            path = _with_retries(lambda: RegistryClient(host=repo, auth=creds).download_pipeline(
                package, version=version, file_name=f"{d}/template.yaml"))
            spec = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        images = {e["container"]["image"] for e in spec["deploymentSpec"]["executors"].values() if "container" in e}
        params = sorted(spec["root"].get("inputDefinitions", {}).get("parameters", {}))
        if images != {contract.component_image}:
            problems.append(f"{target.name}: template images {sorted(images)} != contract image")
        if params != sorted(target.parameters):
            problems.append(f"{target.name}: template parameters differ from the contract")
        if spec["pipelineInfo"]["name"] != target.pipeline_name:
            problems.append(f"{target.name}: template pipeline name differs from the contract")
        if not problems:
            passed.append(f"{target.name} template {package}/{version[:19]}... matches the contract")
    except Exception as e:  # noqa: BLE001 - anything here blocks the release
        problems.append(f"{target.name}: template not retrievable: {e}")
    return problems


def verify(contract: ReleaseContract, gcp: Gcp, creds) -> list[str]:
    """Returns the checks that passed; raises ConfigError listing every problem."""
    passed, problems = [], []
    try:
        contract.validate()
        passed.append(f"contract v{contract.contract_version} is valid")
    except ConfigError as e:
        problems += e.problems

    if image_exists(gcp, contract.component_image):
        passed.append("component image exists (by digest)")
    else:
        problems.append(f"component image not found: {contract.component_image}")
    if contract.ray:
        if image_exists(gcp, contract.ray["image"]):
            passed.append(f"Ray image exists (by digest), for {', '.join(contract.ray['steps'])}")
        else:
            problems.append(f"Ray image not found: {contract.ray['image']}")

    for name in contract.pipeline_names:
        target = contract.pipeline(name)
        problems += _verify_template(contract, target, creds, passed)

    if problems:
        raise ConfigError(problems, source=f"verify {contract.usecase}/{contract.release_id}")
    return passed
