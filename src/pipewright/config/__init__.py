"""ML-persona configuration: loading, validation, resolution.

io            read YAML and gs:// text
merge         deep merge (overrides) and strict merge (include files)
references    ${...} substitution
environment   environment file + manifest
document      one use case assembled from usecase.yaml, include files and environments/<env>.yaml
ml            ML lifecycle sections: defaults and cross-checks
models        resolved objects
resolve       orchestration
"""

from pipewright.config.document import discover_usecases, load_usecase_document, run_in
from pipewright.config.environment import Environment, environment_from_manifest, load_environment, load_manifest
from pipewright.config.merge import deep_merge
from pipewright.config.models import PipelineRef, ResolvedUseCase, Table
from pipewright.config.references import interpolate
from pipewright.config.resolve import resolve_usecase

__all__ = ["Environment", "PipelineRef", "ResolvedUseCase", "Table", "deep_merge", "discover_usecases",
           "environment_from_manifest", "interpolate", "load_environment", "load_manifest", "load_usecase_document",
           "resolve_usecase", "run_in"]
