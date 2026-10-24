"""pipewright lineage: a model version back to its run, release, rows and sources (FR-55).

lineage.json (written by the register step, next to the evaluation record) holds what only the run knows; this adds
the release contract it ran (commit, template and image digests), the row count and read time of its snapshot or
BigQuery ML rows table, and the version's registry state. A promoted version leads to the test version it came from
(its `source-version` and `promoted-from` labels). Approvals and deployments are `model.*` events: Fleet View joins
them by model and version.
"""

from __future__ import annotations

import json

from pipewright.config import Environment, ResolvedUseCase
from pipewright.errors import ConfigError


def _read_gcs(gcp, uri: str) -> dict | None:
    from pipewright.gcp.storage import gcs_read

    bucket, _, name = uri.removeprefix("gs://").partition("/")
    text, _ = gcs_read(gcp, bucket, name)
    return json.loads(text) if text else None


def _rows(gcp, env: Environment, table: str, run_id: str | None) -> dict:
    """Row count (and read time, for a snapshot) of the rows a run trained on; {} when the table is gone."""
    from pipewright.gcp.bigquery import query, rows

    where = " WHERE run_id = @run" if run_id else ""
    read = ", CAST(MIN(ingested_at) AS STRING) AS read_at" if run_id else ""
    params = [{"name": "run", "parameterType": {"type": "STRING"}, "parameterValue": {"value": run_id}}] \
        if run_id else []
    try:
        found = rows(query(gcp, env.project, f"SELECT COUNT(*) AS n{read} FROM `{table}`{where}", params,
                           kms_key=env.security.get("bigquery_kms_key")))
    except Exception as e:  # noqa: BLE001 - dropped by retention, or no access: say so
        return {"error": str(e).splitlines()[0][:200]}
    return {"rows": int(found[0]["n"]), **({"read_at": found[0].get("read_at")} if run_id else {})} if found else {}


def lineage(uc: ResolvedUseCase, env: Environment, gcp, version: str | None = None) -> dict:
    from google.cloud import aiplatform

    from pipewright.models import registry
    from pipewright.release.contract import contract_name
    from pipewright.release.service import read_contract

    resource = registry._resource(uc, env, gcp.session.credentials)
    found = registry.versions(uc, env, gcp.session.credentials, check_gates=False, limit=100)
    if version is None:
        version = next((v.version_id for v in found if v.default), None)
        if version is None:
            raise ConfigError(f"{uc.model['name']} has no default version in {env.name}: give --version")
    this = next((v for v in found if str(v.version_id) == str(version)), None)
    if this is None:
        raise ConfigError(f"{uc.model['name']} has no version {version} in {env.name}")
    model = aiplatform.Model(model_name=f"{resource}@{version}")
    folder = registry.record_location(uc, model.uri, this.labels)
    out: dict = {"usecase": uc.name, "environment": env.name, "model": uc.model["name"], "version": str(version),
                 "default": this.default, "created": this.created, "labels": this.labels,
                 "records": folder}
    labels = this.labels or {}
    if labels.get("promoted-from"):
        out["promoted_from"] = {"environment": labels["promoted-from"], "version": labels.get("source-version")}
    record = _read_gcs(gcp, f"{folder.rstrip('/')}/lineage.json") if folder else None
    if record is None:
        out["lineage"] = None
        out["note"] = ("no lineage.json: trained before Pipewright 1.1, or by a register step that does not write "
                       "one (see the example's register.py)")
        return out
    out["lineage"] = record
    release_id = record.get("release")
    if release_id:
        try:
            c = read_contract(gcp, env, contract_name(uc.name, release_id))
            out["release"] = {"release_id": c.release_id, "commit": c.source_commit, "created_at": c.created_at,
                              "component_image": c.component_image,
                              "templates": {n: p.get("template_digest") for n, p in (c.pipelines or {}).items()}
                              or {c.pipeline_name: c.template_digest},
                              **({"promoted_from": c.promoted_from} if c.promoted_from else {})}
        except ConfigError:
            out["release"] = {"release_id": release_id,
                              "note": f"no contract in {env.name}: the run ran in "
                                      f"{labels.get('promoted-from') or 'another environment'}"}
    rows = (record.get("data") or {}).get("rows") or {}
    snapshot = rows.get("snapshot")
    if snapshot:
        out["snapshot"] = {**snapshot, **_rows(gcp, env, snapshot["table"], snapshot["run_id"])}
    if rows.get("table"):
        out["rows_table"] = {"table": rows["table"], **_rows(gcp, env, rows["table"], None)}
    return out


def render(info: dict) -> str:
    lines = [f"{info['model']} v{info['version']} in {info['environment']}"
             + (" (default)" if info.get("default") else "") + f", created {info.get('created') or '?'}"]
    if info.get("promoted_from"):
        p = info["promoted_from"]
        lines.append(f"  promoted from {p['environment']} v{p.get('version')}: "
                     f"`pipewright lineage --env {p['environment']} --version {p.get('version')}` goes on")
    record = info.get("lineage")
    if not record:
        lines.append(f"  {info.get('note')}")
        return "\n".join(lines)
    lines.append(f"  run      {record.get('run_id')}  (recorded {record.get('recorded_at')})")
    release = info.get("release") or {"release_id": record.get("release")}
    lines.append(f"  release  {release.get('release_id')}" + (f"  commit {release['commit']}"
                                                              if release.get("commit") else "")
                 + (f"  ({release['note']})" if release.get("note") else ""))
    for name, digest in (release.get("templates") or {}).items():
        lines.append(f"           template {name}: {digest}")
    if release.get("component_image"):
        lines.append(f"           image {release['component_image']}")
    config = record.get("config") or {}
    lines.append(f"  config   ml_config sha256 {config.get('ml_config_sha256')}")
    for name, digest in (config.get("sql") or {}).items():
        lines.append(f"           {name}.sql sha256 {digest}")
    data = record.get("data") or {}
    lines.append(f"  data     {data.get('source_uri')}" + (f" (replay of run {data['replay_of']})"
                                                           if data.get("replay_of") else ""))
    if info.get("snapshot"):
        s = info["snapshot"]
        lines.append(f"  rows     snapshot {s['table']} run {s['run_id']}: "
                     + (s["error"] if s.get("error") else f"{s.get('rows')} rows, read {s.get('read_at')}"))
    if info.get("rows_table"):
        t = info["rows_table"]
        lines.append(f"  rows     {t['table']}: " + (t["error"] if t.get("error") else f"{t.get('rows')} rows"))
    sources = data.get("sources") or {}
    if sources.get("entity_table"):
        lines.append(f"  sources  entity table {sources['entity_table']}")
    for src in (sources.get("features") or {}).get("sources") or []:
        what = src.get("feature_group") or src.get("table")
        lines.append(f"           features {what}: {', '.join(src.get('features') or [])}")
    if sources.get("labels"):
        lb = sources["labels"]
        lines.append(f"           labels {lb.get('table')}, window {lb.get('window_days')} days")
    ev = record.get("evaluation") or {}
    lines.append(f"  eval     {ev.get('decision')}: {ev.get('primary_metric')} {ev.get('value')}"
                 + (f" vs champion v{ev['champion'].get('version')} {ev['champion'].get('value')}"
                    if ev.get("champion") else "") + f"  ({ev.get('record')})")
    return "\n".join(lines)
