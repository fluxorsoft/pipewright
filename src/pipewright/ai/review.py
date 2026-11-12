"""The review bundle (Pipewright 1.3.0): a pull request description a person can judge.

`pipewright review --env <env> [--base main]` compares the working tree with a base revision and writes Markdown:
which use cases changed, what a release would change in Google Cloud (tables, schedules, alerts, serving), how the
changed settings moved, the checks (validate and the judgement checks), where the features come from, and a section
for the judgement calls the author (often an AI agent) made, with reasons. Reviewers read decisions, not diffs.

Offline: it reads files and Git only. `pipewright mcp`'s `plan` tool, or the commands' `--dry-run`, show the live plan.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

WATCHED = ("data.contract", "data.label", "data.split", "data.features_from", "features", "data_protection",
           "evaluation.gates", "evaluation.champion", "evaluation.slice_gates", "evaluation.fairness",
           "monitoring", "retraining", "inference.output_table", "ab_test", "feature_contract")
MODEL_WATCHED = ("serving",)                # FR-129: model.serving (capture, pool, lookup, scale to zero) moves too


def _git(root: Path, *args: str) -> str:
    out = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, encoding="utf-8",
                         stdin=subprocess.DEVNULL)   # stdin is the MCP channel inside `pipewright mcp`
    return out.stdout if out.returncode == 0 else ""


def changed_files(root: Path, base: str) -> list[str]:
    files = set(_git(root, "diff", "--name-only", base).split())
    files |= set(_git(root, "ls-files", "--others", "--exclude-standard").split())
    return sorted(f for f in files if f)


def _base_tree(root: Path, base: str) -> Path | None:
    """The base revision's environments/ and usecases/ in a temporary folder (None when base does not exist)."""
    names = _git(root, "ls-tree", "-r", "--name-only", base).split()
    if not names:
        return None
    tmp = Path(tempfile.mkdtemp(prefix="pipewright-review-"))
    for n in names:
        if not (n.startswith("environments/") or n.startswith("usecases/")):
            continue
        blob = subprocess.run(["git", "show", f"{base}:{n}"], cwd=root, capture_output=True,
                              stdin=subprocess.DEVNULL)
        if blob.returncode == 0:
            (tmp / n).parent.mkdir(parents=True, exist_ok=True)
            (tmp / n).write_bytes(blob.stdout)
    return tmp


def _flat(d, prefix: str) -> dict:
    """{dotted.key: value} for every leaf under prefix (lists are leaves)."""
    if isinstance(d, dict) and d:
        out = {}
        for k, v in d.items():
            out.update(_flat(v, f"{prefix}.{k}"))
        return out
    return {prefix: d}


def _section(ml: dict, dotted: str):
    node = ml
    for part in dotted.split("."):
        node = (node or {}).get(part) if isinstance(node, dict) else None
    return node


def _moves(old_ml: dict | None, new_ml: dict, watched: tuple = WATCHED, prefix: str = "") -> list[str]:
    lines = []
    for key in watched:
        old, new = (_section(old_ml, key) if old_ml else None), _section(new_ml, key)
        if old == new:
            continue
        fo, fn = _flat(old, prefix + key), _flat(new, prefix + key)
        if old is None and isinstance(new, dict) and len(fn) > 8:   # FR-129 live test: a section switched on listed
            lines.append(f"- `{prefix + key}`: switched on ({len(fn)} settings with their defaults; "  # 30+ defaults
                         "what it changes is listed above)")
            continue
        for k in sorted(set(fo) | set(fn)):
            if fo.get(k) != fn.get(k):
                a, b = fo.get(k), fn.get(k)
                if a is None:
                    lines.append(f"- `{k}`: set to `{b}`")
                elif b is None:
                    lines.append(f"- `{k}`: removed (was `{a}`)")
                else:
                    lines.append(f"- `{k}`: `{a}` -> `{b}`")
    return lines


def _cloud_changes(old, new) -> list[str]:
    out = []
    old_tables = set((old.tables or {}).keys()) if old else set()
    for t in sorted(set(new.tables or {}) - old_tables):
        out.append(f"+ table `{t}` in `{new.resources.get('bigquery_dataset')}`")
    old_sched = {s["name"] for s in ((old.triggers or {}).get("schedules") or [])} if old else set()
    for s in (new.triggers or {}).get("schedules") or []:
        if s["name"] not in old_sched:
            out.append(f"+ schedule `{s['name']}` ({s.get('cron', '')} {s.get('timezone', '')}".rstrip() + ")")
    alerts_old = ((old.ml.get("monitoring") or {}).get("alerts") if old else None) or {}
    alerts_new = (new.ml.get("monitoring") or {}).get("alerts") or {}
    if alerts_new != alerts_old and (alerts_new.get("notify") or alerts_new.get("events")):
        out.append("~ alert policies (after the merge: `pipewright alerts sync`)")
    serving_old = ((old.model or {}).get("serving") or {}).get("enabled") if old else False
    if ((new.model or {}).get("serving") or {}).get("enabled") and not serving_old:
        out.append("+ online serving (an endpoint, once `deploy-model` is approved)")
    out += _changes_since_14(old, new)
    out.append("= no IAM, network or project changes (Pipewright never makes them)")
    return out


def _on(section) -> bool:
    return bool(isinstance(section, dict) and section.get("enabled"))


def _changes_since_14(old, new) -> list[str]:
    """FR-129: what the 1.4 settings would change, so a reviewer sees an A/B test, serving health, capture, a pool or
    a new feature contract as a change to production and not as a few lines of YAML."""
    out = []
    o_ml, n_ml = (old.ml if old else {}), new.ml
    o_srv = ((old.model or {}).get("serving") or {}) if old else {}
    n_srv = (new.model or {}).get("serving") or {}
    o_ab, n_ab = o_ml.get("ab_test") or {}, n_ml.get("ab_test") or {}
    if _on(n_ab) and (not _on(o_ab) or o_ab.get("name") != n_ab.get("name")):
        trt = ((n_ab.get("arms") or {}).get("treatment") or {}).get("model_version", "?")
        bp = (n_ab.get("allocation") or {}).get("treatment_bp")
        out.append(f"+ A/B test `{n_ab.get('name')}` ({n_ab.get('mode', 'ab')}): treatment v{trt}"
                   + (f" for {bp / 100:g}% of `{n_ab.get('unit')}`" if bp else "") + "; a person starts it "
                   "(`pipewright ab start`, a deploy of the treatment for online use cases) and approver groups "
                   f"{', '.join(((n_ab.get('decision') or {}).get('approvals') or {}).get('required') or []) or '-'} "
                   "decide it")
    elif _on(o_ab) and not _on(n_ab):
        out.append(f"- A/B test `{o_ab.get('name')}` switched off (a running test is stopped by a person: `ab stop`)")
    o_mon, n_mon = o_ml.get("monitoring") or {}, n_ml.get("monitoring") or {}
    if _on(n_mon.get("health")) and not _on(o_mon.get("health")):
        out.append("+ serving health: alert policies per deployed version (errors, latency, replicas, scale-ups), "
                   "after the merge with `pipewright alerts sync` or the next `deploy-model`")
    cap_o, cap_n = o_srv.get("capture") or {}, n_srv.get("capture") or {}
    if _on(cap_n) and (not _on(cap_o) or cap_o != cap_n):
        from pipewright.serving import capture as capture_mod

        table = capture_mod.source_table(new.name, n_srv.get("target"))
        out.append(f"+ request capture into `{table}`: a sample of {cap_n.get('sample_rate', '?')} of "
                   f"live calls, kept {cap_n.get('retention_days', 30)} days (requests hold features of real people)")
    if _on(n_mon.get("online")) and not _on(o_mon.get("online")):
        out.append("+ online monitoring: the use case's monitor steps on the live window, on "
                   f"`{(n_mon.get('online') or {}).get('schedule', '?')}`")
    if n_srv.get("pool") != o_srv.get("pool") and (n_srv.get("pool") or o_srv.get("pool")):
        out.append(f"~ serving moves {'to pool `' + n_srv['pool'] + '`' if n_srv.get('pool') else 'off its pool'}: "
                   "the next deploy changes the pool, which restarts its machines (its neighbours are told)")
    if n_srv.get("scale_to_zero") and not o_srv.get("scale_to_zero"):
        out.append("+ scale to zero: calls during a scale-up are refused and dropped (2 to 3 minutes measured)")
    fc_o, fc_n = (o_ml.get("feature_contract") or {}), (n_ml.get("feature_contract") or {})
    if fc_n and fc_n.get("version") != fc_o.get("version"):
        out.append(f"~ feature contract {fc_o.get('version', 'none')} -> {fc_n.get('version')}: the next training "
                   "reads the new definitions; the served model keeps its approved ones until a new version is "
                   "approved")
    return out


def _features(uc) -> str:
    spec = (uc.ml.get("data") or {}).get("features_from")
    if not spec:
        return ("from the use case's own prepared table (allowed to start; feature tables or Feature Store groups are "
                "the recommended pattern)")
    return "from " + "; ".join(f"`{s.get('feature_group') or s.get('table')}` ({', '.join(s.get('features') or [])})"
                               for s in spec.get("sources") or [])


def bundle(root: Path, env, base: str = "main") -> str:
    from pipewright.config import load_environment, resolve_usecase
    from pipewright.config.document import discover_usecases
    from pipewright.errors import ConfigError

    root = Path(root)
    files = changed_files(root, base)
    names = sorted({f.split("/")[1] for f in files if f.startswith("usecases/") and f.count("/") >= 2})
    base_root = _base_tree(root, base)
    found = discover_usecases(root)
    old_found = discover_usecases(base_root) if base_root and (base_root / "usecases").is_dir() else {}
    old_env = None
    if base_root and (base_root / "environments" / f"{env.name}.yaml").exists():
        try:
            old_env = load_environment(base_root, env.name)
        except ConfigError:
            old_env = None
    L = [f"## Review bundle: {', '.join(names) or 'no use case changed'} ({env.name})", "",
         f"Compared with `{base}`. Files changed: {len(files)}.", ""]
    all_ok = True
    for name in names:
        if name not in found:
            L += [f"### {name}", "", "- removed in this change", ""]
            continue
        try:
            new = resolve_usecase(found[name], env)
        except ConfigError as e:
            all_ok = False
            L += [f"### {name}", "", "**Does not validate:**", "", "```", str(e), "```", ""]
            continue
        old, base_note = None, None
        if name in old_found and old_env:
            try:
                old = resolve_usecase(old_found[name], old_env)
            except ConfigError as e:      # FR-129 live test: said "(new)" with no reason, and every setting as new
                base_note = (f"_`{base}`'s version does not resolve, so it is compared as new: "
                             f"{str(e).splitlines()[0].split(': ', 1)[-1][:200]}_")
        L += [f"### {name}{' (new)' if old is None and not base_note else ''}", "",
              *([base_note, ""] if base_note else []), "**What a release would change in Google Cloud**", ""]
        L += [f"- {c}" for c in _cloud_changes(old, new)]
        L += ["", "**Checks**", "", "- validate: passes" + (f", {len(new.warnings)} warning(s)" if new.warnings
                                                               else ", no warnings")]
        L += [f"  - {w}" for w in new.warnings]
        L += ["", f"**Features:** {_features(new)}.", ""]
        moves = _moves(old.ml if old else None, new.ml) + _moves(old.model if old else None, new.model or {},
                                                                  MODEL_WATCHED, "model.")
        if moves and old is not None:
            L += ["**Settings that moved**", "", *moves, ""]
    if base_root:
        shutil.rmtree(base_root, ignore_errors=True)
    from pipewright.ai import context

    ok, msg = context.check(root, env)
    L += ["**Context pack:** " + ("current" if ok else msg), "",
          "### Judgement calls", "",
          "_The author (or the AI agent) lists each decision a reviewer should weigh, with its reason: the split, the "
          "features chosen or missing, gates set or changed, warnings left standing and why._", "",
          "- ", "",
          "_Generated by `pipewright review`. The author verified it; a person decides." +
          ("_" if all_ok else " Some use cases do not validate: fix them before review._")]
    return "\n".join(L) + "\n"
