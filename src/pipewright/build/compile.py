"""Compiles a use case's KFP pipelines, applies step settings, and reads the parameters each template accepts."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import yaml

from pipewright.build.spec_patch import apply_steps, task_names
from pipewright.config import ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.synthetic import EVALUATION_IMAGE_ENV as SYNTHETIC_EVALUATION_IMAGE_ENV
from pipewright.synthetic import IMAGE_ENV as SYNTHETIC_IMAGE_ENV

# Parameters the launcher always supplies at run time.
LAUNCHER_PARAMETERS = ("project", "region", "source_uri", "run_id")
# Optional parameter: when a template declares it, Pipewright passes the resolved ML sections (dict).
ML_CONFIG_PARAMETER = "ml_config"
PLACEHOLDER_IMAGE = "pipewright.invalid/compile-check@sha256:" + "0" * 64


@dataclass
class TemplateInfo:
    name: str                 # pipelineInfo.name
    parameters: dict          # name -> {"type": ..., "default": ..., "optional": bool}
    images: set
    spec_path: Path
    pipeline: str = "training"
    tasks: list | None = None


def _import(uc: ResolvedUseCase, pipeline: str):
    ref = uc.pipelines[pipeline]
    search = [str(ref.file.parent), str(uc.components_dir)]
    # Drop cached copies of this use case's modules, and of any module with the same name from another use case
    # compiled earlier in this process (two use cases from one example share `ingest`, `train`, ...): a cached
    # module keeps the COMPONENT_IMAGE it was imported with.
    local_names = {p.stem for d in search for p in Path(d).glob("*.py")}
    for name, module in list(sys.modules.items()):
        f = getattr(module, "__file__", None)
        if (f and any(Path(f).resolve().parent == Path(d) for d in search)) or name in local_names:
            del sys.modules[name]
    module_name = f"pipewright_{uc.name.replace('-', '_')}_{pipeline}"
    spec = importlib.util.spec_from_file_location(module_name, ref.file)
    module = importlib.util.module_from_spec(spec)
    sys.path[:0] = search
    write_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True           # never leave __pycache__ in the team's repo
    try:
        spec.loader.exec_module(module)
    except Exception as e:  # noqa: BLE001 - surface any import error to the user
        raise ConfigError(f"pipeline file failed to import: {type(e).__name__}: {e}", source=str(ref.file)) from e
    finally:
        sys.dont_write_bytecode = write_bytecode
        for d in search:
            if d in sys.path:
                sys.path.remove(d)
    fn = getattr(module, ref.function, None)
    if fn is None:
        raise ConfigError(f"function {ref.function!r} not found in {ref.file.name}")
    return fn


def compile_pipeline(uc: ResolvedUseCase, pipeline: str = "training", component_image: str = PLACEHOLDER_IMAGE,
                     output: Path | None = None, steps: dict | None = None,
                     synthetic_images: tuple[str, str] = (PLACEHOLDER_IMAGE, PLACEHOLDER_IMAGE)) -> TemplateInfo:
    """Compiles one pipeline. `steps` (a subset of uc.steps) is applied to the compiled template. The synthetic
    pipeline Pipewright provides runs on the use case's synthetic images (engine, evaluation), every other on its
    component image."""
    try:
        from kfp import compiler
    except ImportError:
        raise ConfigError("compiling pipelines needs the GCP extra: pip install 'pipewright-mlops[gcp]'") from None
    if pipeline not in uc.pipelines:
        raise ConfigError(f"{uc.name} has no {pipeline} pipeline")

    # Components read COMPONENT_IMAGE at import time; drop cached copies so the image takes effect.
    # PIPEWRIGHT_ML_CONFIG lets pipeline code read compile-time settings (e.g. ParallelFor parallelism).
    os.environ["COMPONENT_IMAGE"] = component_image
    os.environ[SYNTHETIC_IMAGE_ENV], os.environ[SYNTHETIC_EVALUATION_IMAGE_ENV] = synthetic_images
    os.environ["PIPEWRIGHT_ML_CONFIG"] = json.dumps(uc.ml_config(), default=str)
    fn = _import(uc, pipeline)
    out = output or Path(tempfile.mkdtemp(prefix="pipewright-")) / f"{uc.name}-{pipeline}.yaml"
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        compiler.Compiler().compile(fn, package_path=str(out))
    except Exception as e:  # noqa: BLE001
        raise ConfigError(f"{pipeline} pipeline failed to compile: {type(e).__name__}: {e}",
                          source=str(uc.pipelines[pipeline].file)) from e

    doc = yaml.safe_load(out.read_text(encoding="utf-8"))
    if steps:
        apply_steps(doc, steps)
        out.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    params = {
        name: {"type": p.get("parameterType"), "default": p.get("defaultValue"),
               "optional": bool(p.get("isOptional")) or "defaultValue" in p}
        for name, p in doc["root"].get("inputDefinitions", {}).get("parameters", {}).items()
    }
    images = {e["container"]["image"] for e in doc["deploymentSpec"]["executors"].values() if "container" in e}
    return TemplateInfo(doc["pipelineInfo"]["name"], params, images, out, pipeline, task_names(doc))


def compile_all(uc: ResolvedUseCase, component_image: str = PLACEHOLDER_IMAGE,
                out_dir: Path | None = None,
                synthetic_images: tuple[str, str] = (PLACEHOLDER_IMAGE, PLACEHOLDER_IMAGE)) -> dict[str, TemplateInfo]:
    """Compiles every pipeline, then applies each `steps` entry to the pipelines that contain that task."""
    plain = {name: compile_pipeline(uc, name, component_image,
                                    (out_dir / f"{uc.name}-{name}.yaml") if out_dir else None,
                                    synthetic_images=synthetic_images)
             for name in uc.pipelines}
    unknown = [s for s in uc.steps if not any(s in info.tasks for info in plain.values())]
    if unknown:
        all_tasks = sorted({t for info in plain.values() for t in info.tasks})
        raise ConfigError(f"steps: no task named {', '.join(unknown)} (tasks: {', '.join(all_tasks)})")
    infos = {}
    for name, info in plain.items():
        mine = {s: v for s, v in uc.steps.items() if s in info.tasks}
        infos[name] = (compile_pipeline(uc, name, component_image, info.spec_path, mine, synthetic_images)
                       if mine else info)
    return infos


def compile_usecase(uc: ResolvedUseCase, component_image: str = PLACEHOLDER_IMAGE,
                    output: Path | None = None) -> TemplateInfo:
    """The training pipeline (older single-pipeline entry point)."""
    return compile_pipeline(uc, "training", component_image, output)


def static_parameters(uc: ResolvedUseCase, info: TemplateInfo) -> dict:
    """What a release fixes for one pipeline: the use case's parameters it accepts, plus ml_config if declared."""
    params = {k: v for k, v in uc.parameters.items() if k in info.parameters}
    if ML_CONFIG_PARAMETER in info.parameters:
        params[ML_CONFIG_PARAMETER] = uc.ml_config()
    return params


def check_parameters(uc: ResolvedUseCase, infos) -> tuple[list[str], list[str]]:
    """Returns (problems, notes). `infos` is one TemplateInfo or a dict of them (all pipelines of the use case)."""
    infos = {infos.pipeline: infos} if isinstance(infos, TemplateInfo) else infos
    problems, notes = [], []
    for name, info in infos.items():
        prefix = f"{name} pipeline: " if len(infos) > 1 else ""
        for p in LAUNCHER_PARAMETERS:
            if p not in info.parameters:
                problems.append(f"{prefix}template has no parameter {p!r}; the launcher always passes it")
        supplied = set(LAUNCHER_PARAMETERS) | set(static_parameters(uc, info))
        for pname, p in info.parameters.items():
            if pname not in supplied and not p["optional"]:
                problems.append(f"{prefix}template parameter {pname!r} has no default and nothing supplies it")
            elif pname not in supplied:
                notes.append(f"{prefix}{pname} uses the pipeline default {p['default']!r}")
    for p in uc.parameters:
        if not any(p in info.parameters for info in infos.values()):
            problems.append(f"parameters.{p}: the pipeline has no such parameter")
    return problems, notes
