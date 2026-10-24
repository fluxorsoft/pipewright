"""Release contract v3 and per-use-case contract pointers.

v3 adds `pipelines` (training, inference, ...): one compiled template per pipeline, all released together.
The top-level template fields always describe the training pipeline, so v2 contracts still read the same way.
Optional `ray` (no version change; older launchers ignore it): the release's Ray image by digest, the Ray version and
each Ray step's cluster ({step: {head_machine, worker_machine, workers, accelerator, max_minutes}}). The launcher
builds a step's cluster from this, never from the request (pipewright.ray_clusters).
Optional `synthetic` (no version change; older launchers ignore it): the synthetic pipeline's images by digest
({image, evaluation_image}) and what it releases ({purpose, output_table}), when synthetic data is enabled.

gs://<artifacts>/contracts/<usecase>/<release_id>.json   one contract per release (never changed)
gs://<artifacts>/contracts/<usecase>/current.json        pointer: which release is live, plus history

The launcher reads only pointers; ML releases move them. Moves use a GCS generation precondition, so two
releases cannot overwrite each other silently.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from pipewright.errors import ConfigError

CONTRACT_VERSION = "3"
SUPPORTED_VERSIONS = ("2", "3")
LAUNCHER_PARAMETERS = ("project", "region", "source_uri", "run_id")
# Optional pipeline parameters a run request may set, run by run (the closed retrain loop, FR-48): the version a
# training run compares with and the one a scoring run uses. Passed only to a pipeline that declares them.
RUN_OVERRIDES = ("champion_version", "model_version")
CONTRACTS_PREFIX = "contracts"
POINTER_HISTORY = 20


def contract_name(usecase: str, release_id: str) -> str:
    return f"{CONTRACTS_PREFIX}/{usecase}/{release_id}.json"


def pointer_name(usecase: str) -> str:
    return f"{CONTRACTS_PREFIX}/{usecase}/current.json"


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class PipelineTarget:
    """One runnable pipeline of a release."""
    name: str                         # training, inference, ...
    pipeline_name: str
    template_uri: str
    template_digest: str
    parameters: list[str]
    static_parameters: dict
    experiment: str | None = None

    def problems(self) -> list[str]:
        out = []
        missing = [p for p in LAUNCHER_PARAMETERS if p not in self.parameters]
        if missing:
            out.append(f"{self.name}: template does not accept launcher parameters: {missing}")
        unknown = [p for p in self.static_parameters if p not in self.parameters]
        if unknown:
            out.append(f"{self.name}: static parameters not accepted by the template: {unknown}")
        clash = [p for p in self.static_parameters if p in LAUNCHER_PARAMETERS]
        if clash:
            out.append(f"{self.name}: static parameters may not set launcher parameters: {clash}")
        if not self.template_digest.startswith("sha256:") or not self.template_uri.endswith(self.template_digest):
            out.append(f"{self.name}: template_uri must reference the template by its sha256 digest")
        return out


@dataclass(frozen=True)
class ReleaseContract:
    contract_version: str
    release_id: str
    usecase: str
    tenant: str
    environment: str
    blueprint: str | None
    orchestrator: str
    project: str
    region: str
    pipeline_name: str
    template_uri: str
    template_digest: str
    component_image: str
    pipeline_root: str
    runner_service_account: str
    parameters: list[str]
    static_parameters: dict
    triggers: dict
    source_commit: str | None = None
    created_at: str = ""
    labels: dict = field(default_factory=dict)
    pipelines: dict = field(default_factory=dict)     # v3: name -> PipelineTarget fields (without name)
    # Who owns it and what it is for (owner, objective, description, contacts, model): read by tools that have no
    # checkout of the MLOps repo, e.g. evidence packs. Optional; older launchers ignore it.
    metadata: dict = field(default_factory=dict)
    ray: dict = field(default_factory=dict)
    # Set when the release came through `pipewright promote`: the source release, digests, model, who, policy.
    promoted_from: dict = field(default_factory=dict)
    synthetic: dict = field(default_factory=dict)     # the synthetic pipeline's images and purpose (FR-49)

    @classmethod
    def from_dict(cls, data: dict) -> ReleaseContract:
        known = set(cls.__dataclass_fields__)
        optional = {"source_commit", "created_at", "labels", "blueprint", "pipelines", "metadata", "ray",
                    "promoted_from", "synthetic"}
        missing = sorted(known - optional - set(data))
        if missing:
            raise ConfigError(f"contract is missing fields: {missing}")
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def from_json(cls, text: str) -> ReleaseContract:
        return cls.from_dict(json.loads(text))

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)

    @property
    def pipeline_names(self) -> list[str]:
        if not self.pipelines:
            return ["training"]
        return sorted(self.pipelines, key=lambda n: (n != "training", n))     # training first

    def images_for(self, name: str) -> set[str]:
        """The images a pipeline's template may use: the synthetic pipeline's own two, or the component image."""
        if name == "synthetic" and self.synthetic:
            return {self.synthetic["image"], self.synthetic["evaluation_image"]}
        return {self.component_image}

    def pipeline(self, name: str = "training") -> PipelineTarget:
        if name in self.pipelines:
            return PipelineTarget(name=name, **self.pipelines[name])
        if name == "training" and not self.pipelines:      # v2: the top-level template is the only pipeline
            return PipelineTarget("training", self.pipeline_name, self.template_uri, self.template_digest,
                                  self.parameters, self.static_parameters)
        raise ConfigError(f"{self.usecase} release {self.release_id} has no {name} pipeline "
                          f"(has: {', '.join(self.pipeline_names)})")

    def validate(self) -> None:
        problems = []
        if self.contract_version not in SUPPORTED_VERSIONS:
            problems.append(f"contract_version {self.contract_version!r} is not supported (expected one of "
                            f"{list(SUPPORTED_VERSIONS)})")
        if self.contract_version == "3" and "training" not in self.pipelines:
            problems.append("pipelines.training is required")
        if "training" in self.pipeline_names:
            for name in self.pipeline_names:
                problems += self.pipeline(name).problems()
        if self.pipelines and "training" in self.pipelines:
            top = PipelineTarget("training", self.pipeline_name, self.template_uri, self.template_digest,
                                 self.parameters, self.static_parameters, self.pipelines["training"].get("experiment"))
            if top != self.pipeline("training"):
                problems.append("top-level template fields must describe pipelines.training")
        wanted = {s.get("pipeline", "training") for s in self.triggers.get("schedules", {}).values()}
        if self.triggers.get("landing_prefix") is not None:
            wanted.add(self.triggers.get("landing_pipeline", "training"))
        for w in sorted(wanted - set(self.pipeline_names)):
            problems.append(f"a trigger starts pipeline {w!r}, which this release does not have")
        if "@sha256:" not in self.component_image:
            problems.append("component_image must be pinned by digest")
        if self.ray:
            if "@sha256:" not in (self.ray.get("image") or ""):
                problems.append("ray.image must be pinned by digest")
            if not self.ray.get("steps"):
                problems.append("ray.steps is empty: a release with a Ray image has at least one Ray step")
        if self.synthetic:
            for key in ("image", "evaluation_image"):
                if "@sha256:" not in (self.synthetic.get(key) or ""):
                    problems.append(f"synthetic.{key} must be pinned by digest")
            if "synthetic" not in self.pipeline_names:
                problems.append("synthetic images without a synthetic pipeline")
        elif "synthetic" in self.pipeline_names:
            problems.append("a synthetic pipeline needs synthetic.image and synthetic.evaluation_image")
        if not self.pipeline_root.startswith("gs://"):
            problems.append("pipeline_root must be a gs:// URI")
        if self.orchestrator not in ("vertex", "kfp"):
            problems.append(f"unknown orchestrator {self.orchestrator!r}")
        if problems:
            raise ConfigError(problems, source=f"contract {self.usecase}/{self.release_id}")


@dataclass
class Pointer:
    usecase: str
    release_id: str
    contract: str                    # object name of the contract
    updated_at: str
    updated_by: str | None = None
    history: list[dict] = field(default_factory=list)   # newest first: {release_id, contract, updated_at}

    @classmethod
    def from_json(cls, text: str) -> Pointer:
        data = json.loads(text)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    def moved_to(self, release_id: str, contract: str, by: str | None) -> Pointer:
        entry = {"release_id": self.release_id, "contract": self.contract, "updated_at": self.updated_at}
        return Pointer(self.usecase, release_id, contract, now(), by, ([entry] + self.history)[:POINTER_HISTORY])


def new_pointer(usecase: str, release_id: str, contract: str, by: str | None) -> Pointer:
    return Pointer(usecase, release_id, contract, now(), by, [])
