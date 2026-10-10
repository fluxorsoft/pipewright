"""Checks a built Pipewright wheel: version, and every non-Python file the CLI, launcher and images need.

    python scripts/check_package.py dist/            # used by .github/workflows/ci.yml
"""

from __future__ import annotations

import sys
import tomllib
import zipfile
from pathlib import Path

REQUIRED = [
    "pipewright/config/schemas/usecase.schema.json",
    "pipewright/config/schemas/environment.schema.json",
    "pipewright/config/schemas/manifest.schema.json",
    "pipewright/config/schemas/reference.yaml",
    "pipewright/build/templates/ray.Dockerfile",
    "pipewright/build/templates/components.Dockerfile",
    "pipewright/build/templates/pipewright_runtime.py",
    "pipewright/scaffold/ci_templates/action.yml",
    "pipewright/scaffold/ci_templates/pipewright-pr.yml",
    "pipewright/scaffold/ci_templates/pipewright-release.yml",
    "pipewright/scaffold/ci_templates/pipewright-operate.yml",
    "pipewright/examples/churn/usecase.yaml.tmpl",
    "pipewright/examples/churn/training_pipeline.py",
    "pipewright/examples/churn/inference_pipeline.py",
    "pipewright/examples/churn/components/requirements.txt",
    "pipewright/examples/churn/data/sample_customers.csv",
    "pipewright/examples/minimal/example.yaml",
    "pipewright/examples/minimal/usecase.yaml.tmpl",
    "pipewright/examples/ray/example.yaml",
    "pipewright/examples/ray/usecase.yaml.tmpl",
    "pipewright/examples/ray/components/ray_tune_job.py",
    "pipewright/examples/features/example.yaml",
    "pipewright/examples/features/usecase.yaml.tmpl",
    "pipewright/examples/features/config/data.yaml.tmpl",
    "pipewright/examples/features/data/load_tables.sh",
]
FORBIDDEN = ("__pycache__", ".pyc", ".pipewright/", ".pytest_cache")


def main(dist: str) -> int:
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
    wheels = sorted(Path(dist).glob("pipewright_mlops-*-py3-none-any.whl"))
    if len(wheels) != 1:
        print(f"expected one wheel in {dist}, found {[w.name for w in wheels]}")
        return 1
    wheel = wheels[0]
    problems = []
    if not wheel.name.startswith(f"pipewright_mlops-{version}-"):
        problems.append(f"{wheel.name} does not carry the pyproject version {version}")
    names = set(zipfile.ZipFile(wheel).namelist())
    problems += [f"missing {n}" for n in REQUIRED if n not in names]
    problems += [f"should not ship {n}" for n in sorted(names) if any(f in n for f in FORBIDDEN)]
    for p in problems:
        print(f"FAIL  {p}")
    print(f"{wheel.name}: {len(names)} files, {'OK' if not problems else f'{len(problems)} problem(s)'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "dist"))
