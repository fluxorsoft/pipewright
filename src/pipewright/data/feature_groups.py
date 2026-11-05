"""pipewright features register (Pipewright phase 6, FR-75): the data team registers feature groups.

A feature group is a feature table registered in Agent Platform's Feature Store: use cases then read it by name
(`data.features_from.sources[].feature_group`) and online stores serve it. The spec file says, per group, the table,
its entity columns and its features with a description each:

    customer_activity:
      table: pw_retail.customer_activity        # dataset.table in the environment's project, or project.dataset.table
      entity: [customer_id]
      description: Monthly purchase behaviour per customer
      features:
        recency_days: Days since the last purchase
        orders_12m: Orders in the last twelve months

Safe to repeat: a group or feature that exists is left as it is (never changed or deleted). Run it as the data team,
the identity that owns the feature tables. Messages name groups, tables and features, never values.
"""

from __future__ import annotations

import time
from pathlib import Path

import yaml

from pipewright.config import Environment
from pipewright.errors import ConfigError


def load_spec(path: Path) -> dict[str, dict]:
    try:
        doc = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"cannot read {path}: {e}") from None
    if not isinstance(doc, dict) or not doc:
        raise ConfigError(f"{path}: expected feature groups by name (table, entity, description, features)")
    problems = []
    for name, spec in doc.items():
        if not isinstance(spec, dict):
            problems.append(f"{name}: expected table, entity, description and features")
            continue
        for key in ("table", "entity", "features"):
            if not spec.get(key):
                problems.append(f"{name}: {key} is required")
        if spec.get("table") and str(spec["table"]).count(".") not in (1, 2):
            problems.append(f"{name}: table must be dataset.table or project.dataset.table")
        if spec.get("features") and not isinstance(spec["features"], dict):
            problems.append(f"{name}: features maps each feature to its description")
        if not str(name).replace("_", "").isalnum() or not str(name)[:1].isalpha():
            problems.append(f"{name}: a feature group name has letters, digits and underscores, starting with a letter")
    if problems:
        raise ConfigError(problems, source=str(path))
    return doc


def _table(env: Environment, table: str) -> str:
    return f"bq://{table}" if table.count(".") == 2 else f"bq://{env.project}.{table}"


def register(gcp, env: Environment, specs: dict[str, dict], names: list[str] | None = None, dry_run: bool = False,
             log=print, poll: float = 5.0) -> list[dict]:
    """Creates each missing feature group and feature. Returns what was done, per group."""
    chosen = names or list(specs)
    unknown = [n for n in chosen if n not in specs]
    if unknown:
        raise ConfigError(f"not in the spec file: {', '.join(unknown)} (it has {', '.join(specs)})")
    api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"

    def wait(resp, what: str):
        if not resp.ok:
            raise ConfigError(f"{what}: HTTP {resp.status} {str(resp.body)[:200]}")
        op = resp.body or {}
        for _ in range(240):
            if op.get("done"):
                if op.get("error"):
                    raise ConfigError(f"{what}: {op['error'].get('message')}")
                return
            time.sleep(poll)
            op = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{op['name']}").body or {}
        raise ConfigError(f"{what}: still running after 20 minutes")

    out = []
    for name in chosen:
        spec = specs[name]
        source = _table(env, spec["table"])
        exists = gcp.get(f"{api}/featureGroups/{name}")
        created = []
        if exists.status == 404:
            if not dry_run:
                wait(gcp.post(f"{api}/featureGroups", {
                    "description": spec.get("description") or name,
                    "bigQuery": {"bigQuerySource": {"inputUri": source}, "entityIdColumns": list(spec["entity"])}},
                    featureGroupId=name), f"feature group {name}")
            log(f"feature group {name}: {'would be ' if dry_run else ''}created over {source.removeprefix('bq://')}")
            have = set()
        elif exists.ok:
            listed_source = (((exists.body or {}).get("bigQuery") or {}).get("bigQuerySource") or {}).get("inputUri")
            if listed_source and listed_source != source:
                raise ConfigError(f"feature group {name} exists over {listed_source}, not {source}: it is left as it "
                                  "is (change it in the console, or register under another name)")
            log(f"feature group {name}: exists")
            listed = gcp.get(f"{api}/featureGroups/{name}/features", pageSize=1000)
            have = ({f["name"].rsplit("/", 1)[-1] for f in (listed.body or {}).get("features", [])}
                    if listed.ok else set())
        else:
            raise ConfigError(f"feature group {name}: HTTP {exists.status} {str(exists.body)[:200]}")
        for feature, description in spec["features"].items():
            if feature in have:
                continue
            if not dry_run:
                wait(gcp.post(f"{api}/featureGroups/{name}/features", {"description": description or feature},
                              featureId=feature), f"feature {name}.{feature}")
            created.append(feature)
        log(f"  {len(spec['features'])} features" + (f", {'would add' if dry_run else 'added'}: {', '.join(created)}"
                                                     if created else ", all registered already"))
        out.append({"feature_group": name, "table": source, "existed": exists.ok, "features_added": created})
    return out
