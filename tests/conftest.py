"""Builds a small client MLOps repo in a temp folder for each test."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

MANIFEST = {
    "apiVersion": "pipewright/v1",
    "environment": "dev",
    "blueprint": "private-managed@1.0.0",
    "orchestrator": "vertex",
    "project": "acme-ml-dev",
    "region": "europe-west2",
    "artifact_registry": {
        "docker": "europe-west2-docker.pkg.dev/acme-ml-dev/ml-images",
        "kfp": "https://europe-west2-kfp.pkg.dev/acme-ml-dev/ml-pipelines",
    },
    "artifacts_bucket": "acme-ml-dev-artifacts",
    "landing_bucket": "acme-ml-dev-landing",
    "launcher": {
        "url": "https://launcher.example",
        "service_account": "ml-launcher@acme-ml-dev.iam.gserviceaccount.com",
    },
    "policies": {},
    "tenants": {
        "churn": {
            "runner_service_account": "ml-runner-churn@acme-ml-dev.iam.gserviceaccount.com",
            "bigquery_dataset": "churn",
            "landing_prefix": "incoming/churn/",
            "online_serving": True,
        }
    },
}

USECASE = {
    "apiVersion": "pipewright/v1",
    "kind": "UseCase",
    "metadata": {"name": "churn", "owner": "team-retention", "labels": {"product": "retention"}},
    "spec": {
        "tenant": "churn",
        "pipeline": {"file": "pipeline.py", "function": "churn_training_pipeline"},
        "components": {"dir": "components/", "requirements": "components/requirements.txt"},
        "framework": "pytorch",
        "triggers": {
            "landing": {"suffixes": [".csv"]},
            "schedules": [{"name": "nightly", "cron": "0 2 * * *", "timezone": "Europe/London"}],
            "api": True,
        },
        "parameters": {"auc_threshold": 0.75, "epochs": 40},
        "resources_per_step": {"train-model": {"cpu": 4, "memory": "8G"}},
        "outputs": {
            "bigquery": {
                "tables": {
                    "predictions": {
                        "schema": "schemas/predictions.json",
                        "partition": {"field": "scored_at", "type": "DAY"},
                        "cluster": ["customer_id"],
                    },
                    "model_metrics": "schemas/model_metrics.json",
                },
                "retention_days": 400,
            }
        },
        "model": {"name": "churn-classifier", "serving": {"enabled": True, "target": "vertex"}},
        "approvals": {
            "promote_model": "manual",
            "deploy_model": "manual",
            "approvers": ["group:ml-leads@acme.example"],
        },
        "environments": {
            "dev": {
                "parameters": {"epochs": 5},
                "triggers": {"schedules": []},
                "model": {"serving": {"enabled": False}},
                "approvals": {"deploy_model": "auto"},
            },
            "prod": {"model": {"serving": {"replicas": {"min": 2, "max": 4}}}},
        },
    },
}

PREDICTIONS = [
    {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "churn_probability", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "scored_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]
METRICS = [{"name": "run_id", "type": "STRING", "mode": "REQUIRED"}, {"name": "auc", "type": "FLOAT"}]


class Repo:
    def __init__(self, root: Path):
        self.root = root
        self.usecase = copy.deepcopy(USECASE)
        self.manifests = {"dev": copy.deepcopy(MANIFEST)}
        self.environments = {"dev": {"manifest": "dev.manifest.json"}}

    def add_env(self, name: str, **manifest_changes):
        m = copy.deepcopy(MANIFEST)
        m["environment"] = name
        m.update(manifest_changes)
        self.manifests[name] = m
        self.environments[name] = {"manifest": f"{name}.manifest.json"}

    def write(self) -> Path:
        env_dir = self.root / "environments"
        env_dir.mkdir(parents=True, exist_ok=True)
        for name, spec in self.environments.items():
            doc = {"apiVersion": "pipewright/v1", "kind": "Environment", "metadata": {"name": name}, "spec": spec}
            (env_dir / f"{name}.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
        for name, manifest in self.manifests.items():
            (env_dir / f"{name}.manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        uc = self.root / "usecases" / "churn"
        (uc / "components").mkdir(parents=True, exist_ok=True)
        (uc / "schemas").mkdir(exist_ok=True)
        (uc / "pipeline.py").write_text("def churn_training_pipeline(): ...\n", encoding="utf-8")
        from pipewright.checks.kfp_version import compiler_version

        kfp = compiler_version() or "2.17.0"
        (uc / "components" / "requirements.txt").write_text(f"kfp=={kfp}\ntorch\n", encoding="utf-8")
        (uc / "schemas" / "predictions.json").write_text(json.dumps(PREDICTIONS), encoding="utf-8")
        (uc / "schemas" / "model_metrics.json").write_text(json.dumps(METRICS), encoding="utf-8")
        (uc / "usecase.yaml").write_text(yaml.safe_dump(self.usecase, sort_keys=False), encoding="utf-8")
        return uc / "usecase.yaml"


@pytest.fixture
def repo(tmp_path) -> Repo:
    return Repo(tmp_path)
