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
    labels = dict(getattr(model, "labels", None) or this.labels or {})      # the listing leaves labels out
    folder = registry.record_location(uc, model.uri, labels)
    out: dict = {"usecase": uc.name, "environment": env.name, "model": uc.model["name"], "version": str(version),
                 "default": this.default, "created": this.created, "labels": labels, "records": folder}
    if labels.get("promoted-from"):
        out["promoted_from"] = {"environment": labels["promoted-from"], "version": labels.get("source-version")}
    record = _read_gcs(gcp, f"{folder.rstrip('/')}/lineage.json") if folder else None
    if record is None:
        out["lineage"] = None
        out["note"] = ("no lineage.json: trained before Pipewright 1.1, or by a register step that does not write "
                       "one (see the example's register.py)")
        out.update(decisions(gcp, env, uc.name, str(version), labels.get("release")))
        # Phase 6: the checks still run. A version that lost its labels (a BigQuery ML model changed in BigQuery
        # resyncs its registry entry without them, seen live) is found by the record its approval names.
        approved = next((d["evaluation"]["record"] for d in reversed(out.get("decisions") or [])
                         if (d.get("evaluation") or {}).get("record")), None)
        folder = folder or (approved.rsplit("/", 1)[0] + "/" if approved else None)
        if not labels and approved:
            out["note"] += ("; the version has no labels now (a BigQuery ML model changed in BigQuery loses them): "
                            "found by its approval's record")
        check_evaluations(gcp, out, folder)
        check_integrity(gcp, env, uc, out, resource, model)
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
    out.update(decisions(gcp, env, uc.name, str(version), (out.get("release") or {}).get("release_id")
                         or labels.get("release")))
    if snapshot:
        out.update(fingerprints(gcp, env, record.get("run_id"), (record.get("data") or {}).get("replay_of")))
    check_evaluations(gcp, out, folder)
    check_integrity(gcp, env, uc, out, resource, model)
    return out


def check_integrity(gcp, env: Environment, uc: ResolvedUseCase, out: dict, resource: str, model) -> None:
    """Phase 6: the version against its approval (files, image, BigQuery ML model), and each approval against Cloud
    Audit Logs (FR-97). Read-only; never fails lineage."""
    from pipewright.models import integrity

    spec = getattr(getattr(model, "gca_resource", None), "container_spec", None)
    try:
        check = integrity.verify(gcp, env, uc.name, out["version"], getattr(model, "uri", None) or None,
                                 getattr(spec, "image_uri", None) or None)
        out["integrity"] = {**check.summary(), "problems_found": check.problems, "notes": check.notes}
        if check.approval and check.approval.get("lock"):
            out["integrity"]["lock"] = check.approval["lock"]
        if not check.approval and check.problems and out.get("default"):
            out["integrity"]["made_default"] = integrity.who_made_default(gcp, env, resource, out["version"],
                                                                          out.get("created"))
    except Exception as e:  # noqa: BLE001 - lineage still shows everything else
        out["integrity"] = {"result": f"not checked: {str(e).splitlines()[0][:160]}"}
    for d in out.get("decisions") or []:
        if d.get("event") == "model.approved":
            d["audit"] = integrity.approval_audit(gcp, env, resource, d["at"], d.get("by"))
            if d.get("evaluation_now") in ("changed", "missing") and (d.get("evaluation") or {}).get("record"):
                d["changed_by"] = integrity.who_wrote(gcp, env, d["evaluation"]["record"], d["at"])


def check_evaluations(gcp, out: dict, folder: str | None) -> None:
    """For each approval that recorded its evaluation (Pipewright 1.3.1), whether the record is still the one the
    reviewer was judged on: `unchanged`, `changed` (another digest now) or `missing`."""
    judged = [d for d in out.get("decisions") or [] if (d.get("evaluation") or {}).get("sha256")]
    if not judged:
        return
    from pipewright.models import gates

    try:
        now = gates.record_digest(gcp, folder)
    except Exception as e:  # noqa: BLE001 - say what could not be checked
        for d in judged:
            d["evaluation_now"] = f"not checked: {str(e).splitlines()[0][:120]}"
        return
    for d in judged:
        d["evaluation_now"] = ("missing" if now is None else
                               "unchanged" if now["sha256"] == d["evaluation"]["sha256"] else "changed")
        if d["evaluation_now"] in ("changed", "missing"):
            explain_change(gcp, d)


def explain_change(gcp, d: dict) -> None:
    """FR-95: what changed in an approved evaluation record (metrics and decision only) and who wrote it."""
    from pipewright.gcp import gcs_read
    from pipewright.models import integrity

    ev = d["evaluation"]
    record, generation = ev.get("record"), ev.get("generation")
    if not record:
        return
    try:
        approved = integrity.read_generation(gcp, record, generation) if generation else None
        bucket, _, name = record.removeprefix("gs://").partition("/")
        now, _ = gcs_read(gcp, bucket, name)
        if approved is None:
            d["evaluation_diff"] = ["the approved version of the record is no longer kept (bucket versioning off, or "
                                    "past its retention)"]
        elif now is not None:
            d["evaluation_diff"] = integrity.evaluation_diff(approved, now)
    except Exception as e:  # noqa: BLE001 - say what could not be shown
        d["evaluation_diff"] = [f"not shown: {str(e).splitlines()[0][:120]}"]


def fingerprints(gcp, env: Environment, run_id: str | None, replay_of: str | None) -> dict:
    """FR-85: the run's rows fingerprint from the event log and, for a replay, the original run's, compared."""
    dataset = (env.observability or {}).get("evidence_dataset")
    if not dataset or not run_id:
        return {}
    from pipewright.gcp.bigquery import query, rows

    runs = [r for r in (run_id, replay_of) if r]
    sql = (f"SELECT JSON_VALUE(data, '$.run_id') AS run, JSON_VALUE(data, '$.data.fingerprint') AS fingerprint "
           f"FROM `{env.project}.{dataset}.events` WHERE JSON_VALUE(data, '$.event_type') IN "
           "('data.snapshot.written', 'data.snapshot.replayed') AND JSON_VALUE(data, '$.run_id') IN UNNEST(@runs) "
           "AND JSON_VALUE(data, '$.data.fingerprint') IS NOT NULL")
    try:
        found = rows(query(gcp, env.project, sql, [{"name": "runs", "parameterType": {
            "type": "ARRAY", "arrayType": {"type": "STRING"}}, "parameterValue": {
            "arrayValues": [{"value": r} for r in runs]}}], kms_key=env.security.get("bigquery_kms_key")))
    except Exception:  # noqa: BLE001 - lineage still shows everything else
        return {}
    by_run = {r["run"]: r["fingerprint"] for r in found}
    current = by_run.get(run_id)
    out = {"rows": current}
    if replay_of:
        original = by_run.get(replay_of)
        out.update(original=original, identical=None if not (current and original) else current == original)
    return {"fingerprint": out} if current or replay_of else {}


# Decisions about a version (Pipewright 1.2.0, FR-84): who approved, promoted, deployed or rolled it back, and how each
# was approved, from the environment's event log (manifest observability.evidence_dataset, table `events`).
VERSION_EVENTS = ("model.approved", "model.deployed", "model.undeployed")
RELEASE_EVENTS = ("release.live", "release.promoted", "release.rolled_back")


def decisions(gcp, env: Environment, usecase: str, version: str, release: str | None) -> dict:
    dataset = (env.observability or {}).get("evidence_dataset")
    if not dataset:
        return {"decisions_note": "no event log in this environment (manifest observability.evidence_dataset): who "
                                  "approved, promoted or deployed this version was not kept"}
    from pipewright.gcp.bigquery import query, rows

    def p(name, value):
        return {"name": name, "parameterType": {"type": "STRING"}, "parameterValue": {"value": value or ""}}

    sql = (f"SELECT CAST(publish_time AS STRING) AS published, TO_JSON_STRING(data) AS event "
           f"FROM `{env.project}.{dataset}.events` "
           "WHERE JSON_VALUE(data, '$.usecase') = @usecase AND JSON_VALUE(data, '$.environment') = @env AND ("
           f"(JSON_VALUE(data, '$.event_type') IN UNNEST({list(VERSION_EVENTS)}) "
           "AND JSON_VALUE(data, '$.data.version') = @version) OR "
           f"(JSON_VALUE(data, '$.event_type') IN UNNEST({list(RELEASE_EVENTS)}) AND JSON_VALUE(data, '$.release') = "
           "@release)) ORDER BY publish_time LIMIT 100")
    try:
        found = rows(query(gcp, env.project, sql, [p("usecase", usecase), p("env", env.name), p("version", version),
                                                   p("release", release)],
                           kms_key=env.security.get("bigquery_kms_key")))
    except Exception as e:  # noqa: BLE001 - no access, or the table is not there: say so
        return {"decisions_note": f"cannot read the event log {dataset}.events: {str(e).splitlines()[0][:160]}"}
    out = []
    for row in found:
        event = json.loads(row["event"] or "{}")
        data = event.get("data") or {}
        out.append({"at": (row["published"] or "")[:19], "event": event.get("event_type"), "by": data.get("by"),
                    "approval": data.get("approval"), "version": data.get("version"),
                    "release": event.get("release"), "endpoint": data.get("endpoint"),
                    "traffic_percent": data.get("traffic_percent"),
                    # Pipewright 1.3.1: the evaluation record an approval was judged on, by digest
                    **({"evaluation": data["evaluation"]} if isinstance(data.get("evaluation"), dict) else {})})
    return {"decisions": out}


def _how(approval: dict | None) -> str:
    if not approval:
        return ""
    if approval.get("via") == "github":
        who = ", ".join(r.get("login", r) if isinstance(r, dict) else str(r) for r in approval.get("reviewers") or [])
        return (f"GitHub {approval.get('actor') or ''}" + (f", reviewed by {who}" if who else "")
                + (f", {approval['run_url']}" if approval.get("run_url") else ""))
    return "outside CI" if approval.get("via") == "outside-ci" else str(approval.get("via") or "")


def render(info: dict) -> str:
    lines = [f"{info['model']} v{info['version']} in {info['environment']}"
             + (" (default)" if info.get("default") else "") + f", created {info.get('created') or '?'}"]
    if info.get("promoted_from"):
        p = info["promoted_from"]
        lines.append(f"  promoted from {p['environment']} v{p.get('version')}: `pipewright lineage --env "
                     f"{p['environment']} --usecase {info.get('usecase')} --version {p.get('version')}` goes on")
    record = info.get("lineage")
    if not record:
        lines.append(f"  {info.get('note')}")
        _render_decisions(info, lines)
        _render_integrity(info, lines)
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
    fp = info.get("fingerprint") or {}
    if fp.get("rows") or "original" in fp:
        same = fp.get("identical")
        verdict = {True: "identical", None: "not recorded", False: "DIFFERENT (rows erased or changed since)"}[same]
        lines.append(f"           fingerprint {fp.get('rows') or 'not recorded'}"
                     + ("" if "original" not in fp else f"; the original run's rows: {verdict}"))
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
    _render_decisions(info, lines)
    _render_integrity(info, lines)
    return "\n".join(lines)


def _render_integrity(info: dict, lines: list[str]) -> None:
    i = info.get("integrity")
    if not i:
        return
    lines.append(f"  integrity {i.get('result')}" + (f": {i['problems']} problem(s)" if i.get("problems") else ""))
    for p in i.get("problems_found") or []:
        lines.append(f"           {p}")
    for n in i.get("notes") or []:
        lines.append(f"           note: {n}")
    made = i.get("made_default")
    if made:
        lines.append(f"           made the default by {made['principal']} at {made['at']} ({made['method']}, "
                     "Admin Activity audit log)" if made.get("principal") else f"           {made.get('note')}")
    if i.get("lock"):
        lk = i["lock"]
        how = ("on a temporary hold (no end date)" if lk.get("method") == "hold" else
               f"locked until {lk.get('until')} ({lk.get('mode')} retention)" if lk.get("until") else "not locked")
        lines.append(f"           {lk.get('locked', 0)} approved files {how}"
                     + (f"; {lk['error']}" if lk.get("error") else ""))


def _render_decisions(info: dict, lines: list[str]) -> None:
    if info.get("decisions_note"):
        lines.append(f"  decided  {info['decisions_note']}")
    for i, d in enumerate(info.get("decisions") or []):
        what = {"model.approved": f"v{d.get('version')} made the champion",
                "model.deployed": f"v{d.get('version')} deployed to {d.get('endpoint')} "
                                  f"({d.get('traffic_percent')}% of traffic)",
                "model.undeployed": f"v{d.get('version')} undeployed",
                "release.live": f"release {d.get('release')} live",
                "release.promoted": f"release {d.get('release')} promoted here",
                "release.rolled_back": f"rolled back to release {d.get('release')}"}.get(d["event"], d["event"])
        how = _how(d.get("approval"))
        lines.append(f"  {'decided' if i == 0 else '       '}  {d['at']}  {what}, by {d.get('by') or '?'}"
                     + (f" ({how})" if how else ""))
        ev = d.get("evaluation") or {}
        if ev.get("sha256"):
            state = {"unchanged": "unchanged since", "missing": "MISSING now",
                     "changed": "CHANGED since: the record on file is not the one approved"}.get(
                d.get("evaluation_now"), d.get("evaluation_now") or "not checked")
            lines.append(f"           judged on evaluation sha256 {ev['sha256'][:16]} (generation "
                         f"{ev.get('generation')}), {state}")
        for change in d.get("evaluation_diff") or []:
            lines.append(f"             {change}")
        for entry in ((d.get("changed_by") or {}).get("entries") or [])[:3]:
            lines.append(f"             written by {entry['principal']} at {entry['at']} ({entry['method']})")
        audit = d.get("audit") or {}
        if audit:
            lines.append("           audit log: " + {
                "matched": f"{audit.get('method')} by {audit.get('principal')} at {audit.get('at')}",
                "other principal": f"MISMATCH: {audit.get('note')}",
                "no entry": f"NO ENTRY: {audit.get('note')}"}.get(audit.get("state"), audit.get("note") or "?"))
