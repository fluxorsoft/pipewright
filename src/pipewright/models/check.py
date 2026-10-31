"""pipewright model check (Pipewright 1.2, FR-72): a version against today's gates, slices and fairness.

`model list` shows each version against the gates in force, but a version that predates a gate was never measured
for it (the ebook's Chapter 10: `[gates FAIL: slice gate auc: the evaluation record has no slices]`). Each training
run now measures the champion's slices and fairness on the same test rows as the challenger (the champion entry of
its evaluation record, `_slices` and `_fairness`), so a champion trained before a gate is measured by the next
challenger. `model check` reads the version's own record, fills what it lacks from the newest challenger's
measurement of it, and says which gates it passes and fails, and what is still unmeasured. Read only.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError
from pipewright.models import gates


@dataclass
class CheckResult:
    version: str
    problems: list[str]
    measured_by: str | None = None          # the challenger version whose evaluation measured this one
    filled: list[str] = field(default_factory=list)
    worst_series: tuple | None = None       # a forecast's worst series: (series, metric, value) (FR-69)


def merged(own: dict, challenger: dict | None) -> tuple[dict, list[str]]:
    """The version's record with the slices and fairness a challenger measured for it, where its own lacks them."""
    record, filled = dict(own), []
    champion = (challenger or {}).get("champion") or {}
    for key in ("slices", "fairness"):
        if not record.get(key) and champion.get(f"_{key}"):
            record[key] = champion[f"_{key}"]
            filled.append(key)
    return record, filled


def check(uc: ResolvedUseCase, env: Environment, creds, version: str) -> CheckResult:
    from pipewright.gcp import Gcp
    from pipewright.models import registry

    gcp = Gcp(creds)
    resource = registry._resource(uc, env, creds)
    own = gates.read_record(gcp, registry.artifact_uri(resource, version, uc))
    if own is None:
        raise ConfigError(f"v{version} of {uc.model['name']} has no evaluation record: nothing to check")
    newer = [v for v in registry.versions(uc, env, creds, check_gates=False, limit=50)
             if registry._number(v.version_id) > registry._number(version)]
    challenger, by = None, None
    for v in sorted(newer, key=lambda v: registry._number(v.version_id), reverse=True):
        rec = gates.read_record(gcp, registry.artifact_uri(resource, v.version_id, uc)) or {}
        champ = rec.get("champion") or {}
        if str(champ.get("_version")) == str(version) and (champ.get("_slices") or champ.get("_fairness")):
            challenger, by = rec, v.version_id
            break
    record, filled = merged(own, challenger)
    evaluation = uc.ml["evaluation"]
    metric = next((g["metric"] for g in evaluation.get("gates") or [] if g.get("per_series")),
                  evaluation.get("primary_metric"))
    worst = gates.worst_series(record, metric, metric in set(evaluation.get("lower_is_better") or ["mae", "rmse",
                                                                                                   "mape"]))
    return CheckResult(str(version), gates.check(record, evaluation), by if filled else None, filled,
                       (worst[0], metric, worst[1]) if worst else None)


def render(uc: ResolvedUseCase, result: CheckResult) -> str:
    head = f"{uc.model['name']} v{result.version} ({uc.environment}) against today's gates: " + (
        "PASS" if not result.problems else f"FAIL ({len(result.problems)})")
    lines = [head, *(f"  FAIL  {p}" for p in result.problems)]
    if result.worst_series:
        name, metric, value = result.worst_series
        lines.append(f"  worst series  {name}: {metric} {value:.4f}")
    if result.filled:
        lines.append(f"  note  {' and '.join(result.filled)} measured on v{result.measured_by}'s test rows, where "
                     f"v{result.version} was the champion")
    if any("no slices" in p or "not measured" in p for p in result.problems):
        lines.append("  next  train a challenger: its evaluation measures the champion's slices and fairness on the "
                     "same test rows, then check again")
    return "\n".join(lines)
