"""pipewright ci init: write the GitHub Actions workflows an app team runs Pipewright with.

    .github/actions/pipewright-setup/action.yml   Python, uv, Workload Identity Federation, Pipewright
    .github/pipewright-constraints.txt            the kfp Pipewright was tested with (Pipewright 1.2.0)
    .github/workflows/pipewright-pr.yml           pull requests: validate, test, doctor, dry runs
    .github/workflows/pipewright-release.yml      merge to main: release changed use cases, env by env
    .github/workflows/pipewright-operate.yml      by hand: run, status, rollback, approve, deploy, canary
    .github/workflows/pipewright-decisions.yml    on a schedule: pending decisions; advances retrain loops
    .github/workflows/pipewright-gate.yml         one decision, approved by its GitHub environment's reviewers
    .github/workflows/pipewright-promote.yml      by hand: promote to an environment with promotion.from (1.2.0)

Environments with promotion.from (Pipewright 1.2.0, FR-80) get no release job: their releases come from the source
environment through pipewright-promote, in the destination's GitHub environment, and the operate workflow has a
promote action. The decisions workflow runs hourly in working hours (UTC) by default; --decisions-cron changes it
(every 15 minutes uses up a private repository's free Actions minutes).

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
PROMOTE_FILE = ".github/workflows/pipewright-promote.yml"   # only with an environment that has promotion.from

# Hourly in working hours (UTC, Monday to Friday): every 15 minutes, as in 1.1, uses up a private repository's free
# Actions minutes (Pipewright 1.2.0, FR-80).
DECISIONS_CRON = "17 7-18 * * 1-5"

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


def _cron(value: str) -> str:
    if not re.fullmatch(r"[0-9*/,-]+( [0-9*/,-]+){4}", value or ""):
        raise ConfigError(f"--decisions-cron: five cron fields, e.g. {DECISIONS_CRON!r}, not {value!r}")
    return value


def render(environments: list[str], labels: list[str] | None = None, promotions: dict[str, str] | None = None,
           decisions_cron: str = DECISIONS_CRON) -> dict[str, str]:
    """{relative path: content}. Environments are in promotion order (first: where PRs are checked); promotions
    maps an environment to its promotion.from: it gets the promote workflow instead of a release job."""
    if not environments:
        raise ConfigError("give at least one environment, e.g. --environments dev,prod")
    bad = [e for e in environments if not NAME.match(e)]
    if bad or len(set(environments)) != len(environments):
        raise ConfigError(f"environment names must be unique, lowercase letters, digits and hyphens: {environments}")
    promotions = {e: s for e, s in (promotions or {}).items() if e in environments}
    if environments[0] in promotions:
        raise ConfigError(f"{environments[0]} is where pull requests are checked and releases start: it cannot have "
                          f"promotion.from ({promotions[environments[0]]})")
    jobs, previous = [], "changes"
    for env in environments:
        if env in promotions:
            continue
        jobs.append(RELEASE_JOB.replace("@@ENV@@", env).replace("@@NEEDS@@", previous if previous == "changes"
                                                                      else f"changes, {previous}"))
        previous = f"release-{env}"
    released = [e for e in environments if e not in promotions]
    promote_cases = "\n".join(f"            {e}) SOURCE={s} ;;" for e, s in promotions.items())
    values = {"@@FIRST_ENV@@": environments[0], "@@ENV_CHOICES@@": ", ".join(environments),
              "@@ENV_ORDER@@": " -> ".join(released), "@@RELEASE_JOBS@@": "\n".join(jobs).rstrip(),
              "@@RUNS_ON@@": runs_on(labels), "@@DECISIONS_CRON@@": _cron(decisions_cron),
              "@@PROMOTED_CHOICES@@": ", ".join(promotions),
              "@@PROMOTIONS@@": ", ".join(f"{s} -> {e}" for e, s in promotions.items()),
              "@@PROMOTE_CASES@@": promote_cases,
              "@@PROMOTE_ACTION@@": "\n          - promote" if promotions else "",
              "@@PROMOTE_OPERATE@@": (
                  "            promote)         case \"$ENV\" in "
                  + " ".join(f"{e}) SOURCE={s} ;;" for e, s in promotions.items())
                  + " *) echo \"$ENV has no promotion.from\"; exit 1 ;; esac\n"
                  "                             commit=$(pipewright promote --from \"$SOURCE\" --to \"$ENV\" --usecase "
                  "\"$UC\" ${VERSION:+--release \"$VERSION\"} --print-source-commit)\n"
                  "                             git checkout -q --detach \"$commit\"   # the release's own commit\n"
                  "                             p promote --from \"$SOURCE\" --to \"$ENV\" --usecase \"$UC\" "
                  "${VERSION:+--release \"$VERSION\"} ;;\n") if promotions else ""}
    out = {}
    files = {**FILES, **({"pipewright-promote.yml": PROMOTE_FILE} if promotions else {})}
    for name, path in files.items():
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


def promotions(root: Path, environments: list[str]) -> dict[str, str]:
    """{environment: promotion.from} from environments/<env>.yaml."""
    import yaml

    out = {}
    for e in environments:
        spec = (yaml.safe_load((Path(root) / "environments" / f"{e}.yaml").read_text(encoding="utf-8")) or {})
        source = ((spec.get("spec") or {}).get("promotion") or {}).get("from")
        if source:
            out[e] = source
    return out


def init(root: Path, environments: list[str], force: bool = False, labels: list[str] | None = None,
         decisions_cron: str = DECISIONS_CRON) -> list[Path]:
    root = Path(root)
    if not (root / "usecases").is_dir() and not (root / "environments").is_dir():
        raise ConfigError(f"{root} does not look like an MLOps repo (no usecases/ or environments/)")
    missing = [e for e in environments if not (root / "environments" / f"{e}.yaml").is_file()]
    if missing:
        raise ConfigError(f"no environments/{{name}}.yaml for: {', '.join(missing)}")
    files = render(environments, labels, promotions(root, environments), decisions_cron)
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
