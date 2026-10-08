"""Resolved configuration objects."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Table:
    name: str
    schema_path: Path
    fields: list
    partition: dict | None = None
    cluster: list = field(default_factory=list)
    description: str | None = None


@dataclass(frozen=True)
class PipelineRef:
    name: str          # training | inference
    file: Path
    function: str


@dataclass(frozen=True)
class ResolvedUseCase:
    name: str
    environment: str
    owner: str
    tenant: str
    resources: dict
    directory: Path
    pipelines: dict                 # name -> PipelineRef
    components_dir: Path
    requirements: Path | None
    framework: str
    triggers: dict
    parameters: dict
    steps: dict                     # task name -> {cpu, memory, gpu, retries, cache}
    tables: dict
    retention_days: int | None
    model: dict
    approvals: dict
    labels: dict
    contacts: list
    source: str
    description: str = ""
    objective: str = ""
    ml: dict = field(default_factory=dict)     # resolved ML sections, passed to pipelines as `ml_config`
    tests_command: str | None = None
    sources: list = field(default_factory=list)
    platform: dict = field(default_factory=dict)  # launcher_url, project, region, bigquery_dataset, events_topic,
    #                                               run_requests_topic, kms_key
    outputs: dict = field(default_factory=dict)   # store, gcs {format, external_tables, uri}, tables {name: fields}

    # Older single-pipeline accessors (training).
    @property
    def pipeline_file(self) -> Path:
        return self.pipelines["training"].file

    @property
    def pipeline_function(self) -> str:
        return self.pipelines["training"].function

    @property
    def resources_per_step(self) -> dict:
        return self.steps

    def ml_config(self) -> dict:
        """The document pipelines receive (as the `ml_config` parameter) when their template accepts it."""
        return {"usecase": self.name, "model_name": self.model["name"], "environment": self.environment,
                "description": self.description, "objective": self.objective, "owner": self.owner,
                "platform": dict(self.platform), "outputs": dict(self.outputs), **self.ml}

    def to_dict(self) -> dict:
        def rel(p: Path | None) -> str | None:
            if p is None:
                return None
            try:
                return Path(os.path.relpath(p, self.directory)).as_posix()
            except ValueError:  # different drive on Windows
                return p.as_posix()

        return {
            "usecase": self.name,
            "environment": self.environment,
            "owner": self.owner,
            "description": self.description or None,
            "tenant": self.tenant,
            "sources": self.sources,
            "resources": self.resources,
            "pipelines": {n: {"file": rel(p.file), "function": p.function} for n, p in self.pipelines.items()},
            "components": {"dir": rel(self.components_dir), "requirements": rel(self.requirements)},
            "framework": self.framework,
            "triggers": self.triggers,
            "parameters": self.parameters,
            "steps": self.steps,
            **self.ml,
            "outputs": {
                "store": self.outputs.get("store", "bigquery"),
                "gcs": (self.outputs.get("gcs") if self.outputs.get("store") in ("gcs", "both") else None),
                "bigquery": {
                    "dataset": self.resources.get("bigquery_dataset"),
                    "tables": {
                        t.name: {
                            "schema": rel(t.schema_path),
                            "columns": len(t.fields),
                            "partition": t.partition,
                            "cluster": t.cluster or None,
                        }
                        for t in self.tables.values()
                    },
                    "retention_days": self.retention_days,
                }
            },
            "model": self.model,
            "approvals": self.approvals,
            "tests": {"command": self.tests_command} if self.tests_command else None,
            "labels": self.labels,
        }
