"""Release gates for synthetic data: defaults per purpose, and the check Pipewright runs on every candidate.

Standalone (no Pipewright imports): the synthetic image runs it in the pipeline, the CLI runs it on an evaluation
record. Privacy gates may only be made stricter than the purpose's defaults; quality gates may be set either way.

Defaults come from the FR-49 spike (MOSTLY AI, SDV and synthcity on a 5,000-row and a 30,000-row table):
independent data gives about 5% of rows closer to a training row than the holdout's 5th percentile, and the
attack risks are Anonymeter's upper confidence bounds.
"""

from __future__ import annotations

# metric -> (direction, test-data default, sharing default). "min": the value must be at least the gate.
GATES = {
    "fidelity":       ("min", 0.90, 0.85),   # SDMetrics quality score (column shapes and column-pair trends)
    "tstr_drop":      ("max", 0.05, 0.08),   # primary metric: trained on real minus trained on synthetic
    "exact_copies":   ("max", 0, 0),         # synthetic rows identical to a training row
    "dcr_ratio":      ("min", 0.9, 1.0),     # median distance to training: synthetic over holdout
    "closer_than_p5": ("max", 0.10, 0.05),   # share of synthetic rows nearer than the holdout's 5th percentile
    "singling_out":   ("max", 0.30, 0.20),   # Anonymeter risk, upper confidence bound
    "linkability":    ("max", 0.10, 0.05),
    "inference":      ("max", 0.50, 0.25),   # the label and declared sensitive columns as secrets
}
PRIVACY = {"exact_copies", "dcr_ratio", "closer_than_p5", "singling_out", "linkability", "inference"}
PURPOSES = ("test-data", "sharing")


def defaults(purpose: str) -> dict[str, dict]:
    """{metric: {"min" | "max": value}} for a purpose."""
    column = 1 if purpose == "test-data" else 2
    return {m: {g[0]: g[column]} for m, g in GATES.items()}


def effective(purpose: str, overrides: dict | None, problems: list[str] | None = None) -> dict[str, dict]:
    """The purpose's defaults with the use case's overrides. A privacy gate looser than the default is a problem."""
    out = defaults(purpose)
    for metric, gate in (overrides or {}).items():
        where = f"synthetic.gates.{metric}"
        if metric not in GATES:
            if problems is not None:
                problems.append(f"{where}: unknown gate (known: {', '.join(GATES)})")
            continue
        direction, base = GATES[metric][0], out[metric][GATES[metric][0]]
        if set(gate) != {direction}:
            if problems is not None:
                problems.append(f"{where}: give {direction} (the only bound this gate has)")
            continue
        value = gate[direction]
        looser = value < base if direction == "min" else value > base
        if metric in PRIVACY and looser and problems is not None:
            problems.append(f"{where}: {direction} {value} is looser than the {purpose} default ({base}); privacy "
                            "gates may only be made stricter")
            continue
        out[metric] = {direction: value}
    return out


def check(metrics: dict, gates: dict[str, dict]) -> list[str]:
    """Failed gates as readable reasons; a metric the evaluation did not produce fails its gate."""
    failed = []
    for metric, gate in gates.items():
        value = metrics.get(metric)
        for direction, bound in gate.items():
            if value is None:
                failed.append(f"{metric}: not measured")
            elif direction == "min" and value < bound:
                failed.append(f"{metric} {value:.4g} is below {bound}")
            elif direction == "max" and value > bound:
                failed.append(f"{metric} {value:.4g} is above {bound}")
    return failed


def select(candidates: list[dict], gates: dict[str, dict]) -> tuple[int | None, list[dict]]:
    """The best candidate that passes every gate: highest fidelity, then smallest TSTR drop. Never the best score
    alone (the spike's highest-fidelity candidate nearly copied its training rows). Returns (index or None, one
    {name, passed, reasons} per candidate)."""
    verdicts = []
    for c in candidates:
        reasons = c.get("errors") or check(c.get("metrics") or {}, gates)
        verdicts.append({"name": c.get("name"), "passed": not reasons, "reasons": reasons})
    passing = [i for i, v in enumerate(verdicts) if v["passed"]]
    if not passing:
        return None, verdicts

    def key(i):
        m = candidates[i]["metrics"]
        return (m.get("fidelity") or 0.0, -(m.get("tstr_drop") or 0.0))

    return max(passing, key=key), verdicts
