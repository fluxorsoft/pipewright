"""pipewright ci init: write the GitHub Actions workflows an app team runs Pipewright with.

    .github/actions/pipewright-setup/action.yml   Python, uv, Workload Identity Federation, Pipewright
    .github/pipewright-constraints.txt            the kfp Pipewright was tested with (Pipewright 1.2)
    .github/workflows/pipewright-pr.yml           pull requests: validate, test, doctor, dry runs
    .github/workflows/pipewright-release.yml      merge to main: release changed use cases, env by env
    .github/workflows/pipewright-operate.yml      by hand: run, status, rollback, approve, deploy, canary
    .github/workflows/pipewright-decisions.yml    every 15 minutes: pending decisions; advances retrain loops
    .github/workflows/pipewright-gate.yml         one decision, approved by its GitHub environment's reviewers

Each environment is a GitHub environment with the variables PIPEWRIGHT_PIP_SPEC and, with Workload Identity
Federation, GCP_WIF_PROVIDER and PIPEWRIGHT_SERVICE_ACCOUNT (optional: PIPEWRIGHT_PIP_INDEX,
PIPEWRIGHT_SETUP_TOOLS=off, PIPEWRIGHT_SCHEDULES=off). Required reviewers on a GitHub environment are the approval
gate for releases and operations there.

Decisions (FR-56): pipewright-decisions runs in <environment>-decisions (the same variables, no reviewers) and
dispatches a pipewright-gate run per pending decision. A gate runs in <environment>, or <environment>-<group> for
a retrain case's approver group; its required reviewers decide, and the decision's event records them.

Self-hosted runners: `--runs-on self-hosted,linux,mlops` puts those labels on every job. On a GCE or GKE runner
with a service account attached, leave GCP_WIF_PROVIDER empty and the runner's own identity is used.
"""

from __future__ import annotations

import re
from importlib import resources
from pathlib import Path

from pipewright.errors import ConfigError
from pipewright.scaffold.new_usecase import NAME

FILES = {
    "action.yml": ".github/actions/pipewright-setup/action.yml",
    "pipewright-pr.yml": ".github/workflows/pipewright-pr.yml",
    "pipewright-release.yml": ".github/workflows/pipewright-release.yml",
    "pipewright-operate.yml": ".github/workflows/pipewright-operate.yml",
    "pipewright-decisions.yml": ".github/workflows/pipewright-decisions.yml",
    "pipewright-gate.yml": ".github/workflows/pipewright-gate.yml",
}

RELEASE_JOB = """  release-@@ENV@@:
    needs: [@@NEEDS@@]
    if: needs.changes.outputs.usecases != '[]'
    runs-on: @@RUNS_ON@@
    environment: @@ENV@@
    strategy:
      fail-fast: false
      max-parallel: 2
      matrix:
        usecase: ${{ fromJSON(needs.changes.outputs.usecases) }}
    env:
      ENV: @@ENV@@
      UC: ${{ matrix.usecase }}
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/pipewright-setup
        with:
          wif_provider: ${{ vars.GCP_WIF_PROVIDER }}
          service_account: ${{ vars.PIPEWRIGHT_SERVICE_ACCOUNT }}
          pip_spec: ${{ vars.PIPEWRIGHT_PIP_SPEC }}
          pip_index: ${{ vars.PIPEWRIGHT_PIP_INDEX }}
          setup_tools: ${{ vars.PIPEWRIGHT_SETUP_TOOLS }}
      - name: Tables
        run: pipewright tables sync --env "$ENV" --usecase "$UC" | tee -a "$GITHUB_STEP_SUMMARY"
      - name: Release
        run: pipewright release --env "$ENV" --usecase "$UC" --skip-tables | tee -a "$GITHUB_STEP_SUMMARY"
      - name: Schedules
        if: vars.PIPEWRIGHT_SCHEDULES != 'off'
        run: pipewright schedules sync --env "$ENV" --usecase "$UC" | tee -a "$GITHUB_STEP_SUMMARY"
      - name: Alerts
        run: pipewright alerts sync --env "$ENV" --usecase "$UC" | tee -a "$GITHUB_STEP_SUMMARY"
"""


def _template(name: str) -> str:
    return resources.files("pipewright.scaffold").joinpath("ci_templates", name).read_text(encoding="utf-8")


def runs_on(labels: list[str] | None) -> str:
    """`ubuntu-latest`, or a flow list of self-hosted runner labels."""
    labels = [label.strip() for label in (labels or []) if label.strip()]
    if not labels:
        return "ubuntu-latest"
    bad = [label for label in labels if not re.fullmatch(r"[A-Za-z0-9_.-]+", label)]
    if bad:
        raise ConfigError(f"runner labels may hold letters, digits, '.', '_' and '-': {bad}")
    return "[" + ", ".join(labels) + "]"


def render(environments: list[str], labels: list[str] | None = None) -> dict[str, str]:
    """{relative path: content}. Environments are in promotion order (first: where PRs are checked)."""
    if not environments:
        raise ConfigError("give at least one environment, e.g. --environments dev,prod")
    bad = [e for e in environments if not NAME.match(e)]
    if bad or len(set(environments)) != len(environments):
        raise ConfigError(f"environment names must be unique, lowercase letters, digits and hyphens: {environments}")
    jobs, previous = [], "changes"
    for env in environments:
        jobs.append(RELEASE_JOB.replace("@@ENV@@", env).replace("@@NEEDS@@", previous if previous == "changes"
                                                                      else f"changes, {previous}"))
        previous = f"release-{env}"
    values = {"@@FIRST_ENV@@": environments[0], "@@ENV_CHOICES@@": ", ".join(environments),
              "@@ENV_ORDER@@": " -> ".join(environments), "@@RELEASE_JOBS@@": "\n".join(jobs).rstrip(),
              "@@RUNS_ON@@": runs_on(labels)}
    out = {}
    for name, path in FILES.items():
        text = _template(name)
        for key, value in values.items():
            text = text.replace(key, value)
        out[path] = text
    from pipewright.checks.kfp_version import constraints

    out[".github/pipewright-constraints.txt"] = constraints()       # the setup action installs with it (FR-81)
    custom = [label for label in (labels or []) if label.strip() and label.strip() != "self-hosted"]
    if custom:   # actionlint (and editors) need to know self-hosted runner labels
        out[".github/actionlint.yaml"] = ("# Generated by `pipewright ci init`: labels of the self-hosted runners.\n"
                                          "self-hosted-runner:\n  labels:\n"
                                          + "".join(f"    - {label.strip()}\n" for label in custom))
    return out


def init(root: Path, environments: list[str], force: bool = False, labels: list[str] | None = None) -> list[Path]:
    root = Path(root)
    if not (root / "usecases").is_dir() and not (root / "environments").is_dir():
        raise ConfigError(f"{root} does not look like an MLOps repo (no usecases/ or environments/)")
    missing = [e for e in environments if not (root / "environments" / f"{e}.yaml").is_file()]
    if missing:
        raise ConfigError(f"no environments/{{name}}.yaml for: {', '.join(missing)}")
    files = render(environments, labels)
    existing = [p for p in files if (root / p).exists()]
    if existing and not force:
        raise ConfigError(f"already present (use --force to replace): {', '.join(existing)}")
    written = []
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
        written.append(path)
    return written
