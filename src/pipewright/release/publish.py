"""Compiles a use case's pipelines against a pinned component image, uploads the templates, builds the contract."""

from __future__ import annotations

import re
from pathlib import Path

from pipewright.build.compile import TemplateInfo, check_parameters, compile_all, static_parameters
from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.release.contract import CONTRACT_VERSION, ReleaseContract, now

Uploaded = dict  # pipeline name -> (template_uri, template_digest)



def pipeline_root(env, uc) -> str:
    """Where a use case's runs write their artifacts: the tenant's own prefix of the artifacts bucket, so a tenant's
    runner can be granted that prefix only (R-04)."""
    return f"gs://{env.artifacts_bucket}/pipeline-root/{uc.tenant}/{uc.name}"

def _tag(value: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")[:120]


def compile_pinned(uc: ResolvedUseCase, component_image: str, out_dir: Path,
                   synthetic_images: tuple[str, str] | None = None) -> dict[str, TemplateInfo]:
    """Every pipeline of the use case, with `steps` applied. The synthetic pipeline runs on the synthetic images
    (engine, evaluation)."""
    from pipewright.synthetic import PIPELINE as SYNTHETIC

    if SYNTHETIC in uc.pipelines and not synthetic_images:
        raise ConfigError("the synthetic pipeline needs the use case's synthetic images")
    infos = compile_all(uc, component_image=component_image, out_dir=out_dir,
                        **({"synthetic_images": synthetic_images} if synthetic_images else {}))
    for name, info in infos.items():
        expected = set(synthetic_images) if name == SYNTHETIC else {component_image}
        if info.images != expected:
            raise ConfigError(f"{name} template uses unexpected images: {sorted(info.images)}")
    problems, _ = check_parameters(uc, infos)
    if problems:
        raise ConfigError(problems, source=f"pipelines of {uc.name}")
    return infos


def upload_template(info: TemplateInfo, kfp_repository: str, uc: ResolvedUseCase, release_id: str,
                    creds) -> tuple[str, str]:
    """Returns (template_uri, template_digest). The training template keeps the bare use case tags."""
    from kfp.registry import RegistryClient

    stem = uc.name if info.pipeline == "training" else f"{uc.name}-{info.pipeline}"
    client = RegistryClient(host=kfp_repository, auth=creds)
    package, version = client.upload_pipeline(
        file_name=str(info.spec_path),
        tags=[_tag(f"{stem}-{release_id}"), _tag(f"{stem}-latest")],
        extra_headers={"description": f"{uc.name} {info.pipeline} release {release_id}"},
    )
    return f"{kfp_repository.rstrip('/')}/{package}/{version}", version


def build_contract(uc: ResolvedUseCase, env: Environment, infos: dict[str, TemplateInfo], uploaded: Uploaded,
                   component_image: str, release_id: str, source_commit: str | None,
                   ray_image: str | None = None,
                   synthetic_images: tuple[str, str] | None = None) -> ReleaseContract:
    experiment = uc.ml["training"]["experiment"]["name"] if uc.ml else None
    pipelines = {
        name: {"pipeline_name": info.name, "template_uri": uploaded[name][0], "template_digest": uploaded[name][1],
               "parameters": sorted(info.parameters), "static_parameters": static_parameters(uc, info),
               "experiment": experiment if name == "training" else None}
        for name, info in infos.items()
    }
    training = pipelines["training"]
    contract = ReleaseContract(
        contract_version=CONTRACT_VERSION,
        release_id=release_id,
        usecase=uc.name,
        tenant=uc.tenant,
        environment=env.name,
        blueprint=env.blueprint,
        orchestrator=env.orchestrator,
        project=env.project,
        region=env.region,
        pipeline_name=training["pipeline_name"],
        template_uri=training["template_uri"],
        template_digest=training["template_digest"],
        component_image=component_image,
        pipeline_root=pipeline_root(env, uc),
        runner_service_account=uc.resources["runner_service_account"],
        parameters=training["parameters"],
        static_parameters=training["static_parameters"],
        triggers={
            "landing_prefix": uc.resources.get("landing_prefix") if uc.triggers.get("landing") else None,
            "landing_suffixes": (uc.triggers.get("landing") or {}).get("suffixes", []),
            "landing_pipeline": (uc.triggers.get("landing") or {}).get("pipeline", "training"),
            "api": bool(uc.triggers.get("api")),
            "schedules": {s["name"]: {k: v for k, v in s.items() if k != "name"} for s in uc.triggers["schedules"]},
        },
        source_commit=source_commit,
        created_at=now(),
        labels=dict(uc.labels),
        pipelines=pipelines,
        metadata={"owner": uc.owner, "objective": uc.objective, "description": uc.description,
                  "contacts": list(uc.contacts), "model": uc.model["name"]},
        ray=({"image": ray_image, "ray_version": uc.ray["ray_version"], "steps": uc.ray["steps"],
              # the mode the release was built for (a standing release names its tenant's cluster)
              **({"mode": "standing", "standing": uc.ray.get("standing")} if uc.ray.get("mode") == "standing" else {})}
             if uc.ray and ray_image else {}),
        synthetic=({"image": synthetic_images[0], "evaluation_image": synthetic_images[1],
                    "purpose": uc.synthetic["purpose"], "output_table": uc.synthetic["output_table"]}
                   if synthetic_images and uc.synthetic else {}),
    )
    contract.validate()
    return contract
