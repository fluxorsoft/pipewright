"""Pipewright 1.4.1 (FR-103, CONTEXT section 8.17): feature definitions, fingerprinted and compared.

An approved model reads its features from `data.features_from` sources (feature tables or feature groups). This
module reads what each source is *now* (its BigQuery object: a view's or materialized view's SQL, its schema; for a
table built by a query, the SQL of the job that last wrote it, or its scheduled query) and fingerprints it, so the
use case's `feature_contract`, an approval and every later check can say whether the definition is the one approved.

Found live (docs/evidence/fr103-feature-definitions):
- the fingerprint is the SQL with whitespace normalised plus the schema as names, types and modes: etags, modified
  times, DDL and descriptions move on harmless edits;
- a table built by a query carries no SQL: the job history (`built_tables`) or its scheduled query has it;
- definitions are values-free: messages name sources, columns, kinds, digests and versions, never a value.

A view's SQL names projects; the source's own project is replaced by `{project}` before hashing, so the same
definition in test and in prod has the same fingerprint (promotion compares across projects).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from pipewright.errors import ConfigError

BQ = "https://bigquery.googleapis.com/bigquery/v2"
DT = "https://bigquerydatatransfer.googleapis.com/v1"
REGISTRY = "https://{region}-aiplatform.googleapis.com/v1/projects/{project}/locations/{region}/featureGroups/{group}"
KINDS = ("view", "materialized_view", "table", "scheduled_query")
_TYPES = {"VIEW": "view", "MATERIALIZED_VIEW": "materialized_view"}
# CREATE [OR REPLACE] [TEMP] TABLE [IF NOT EXISTS] <name> [(columns)] [PARTITION/CLUSTER/OPTIONS ...] AS <query>
_CTAS = re.compile(r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
                   r"`?[\w.\-:]+`?.*?\bAS\s*(?=\(|SELECT\b|WITH\b)")
_LEADING_COMMENTS = re.compile(r"^\s*(?:/\*.*?\*/\s*|--[^\n]*\n\s*)+", re.S)


def sha256(value) -> str:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def normalise_sql(sql: str | None, project: str | None = None) -> str | None:
    """The SQL a feature is computed by, as compared: a CREATE TABLE ... AS prefix removed (a job's statement and a
    declared query compare equal), leading comments removed (dbt and schedulers add them), whitespace collapsed, a
    trailing semicolon dropped, and the source's own project written `{project}`."""
    if not sql:
        return None
    s = _LEADING_COMMENTS.sub("", sql)
    s = _CTAS.sub("", s, count=1)
    s = " ".join(s.split()).rstrip(";").strip()
    if project:
        s = s.replace(f"`{project}.", "`{project}.").replace(f"`{project}`.", "`{project}`.")
        s = re.sub(rf"(?<![\w-]){re.escape(project)}\.", "{project}.", s)
    return s


def schema_of(fields: list[dict] | None, prefix: str = "") -> list[str]:
    """`name TYPE MODE` per column, nested records flattened (`a.b`): never descriptions, which move on harmless
    edits and are dropped when a view is re-created (found live)."""
    out = []
    for f in fields or []:
        name = f"{prefix}{f['name']}"
        out.append(f"{name} {f.get('type', '?')} {f.get('mode') or 'NULLABLE'}")
        if f.get("fields"):
            out += schema_of(f["fields"], name + ".")
    return out


def parse_ref(ref: str) -> tuple[str, str, str]:
    p = ref.removeprefix("bq://").strip("`").split(".")
    if len(p) != 3:
        raise ConfigError(f"{ref}: expected project.dataset.table")
    return p[0], p[1], p[2]


# -- what a source is now ------------------------------------------------------------------------------------------

def source_name(src: dict) -> str:
    """The name a source has in the contract: its feature group, or its table's ID (the same in every environment,
    where the project and dataset may differ)."""
    if src.get("feature_group"):
        return str(src["feature_group"])
    return parse_ref(str(src.get("table") or ""))[2]


def resolve(gcp, src: dict, project: str, region: str) -> str:
    """The BigQuery object behind a features_from source: its table, or a feature group's BigQuery source."""
    if not src.get("feature_group"):
        return ".".join(parse_ref(str(src["table"])))
    url = REGISTRY.format(region=src.get("region") or region, project=src.get("project") or project,
                          group=src["feature_group"])
    r = gcp.get(url)
    if not r.ok:
        raise ConfigError(f"feature group {src['feature_group']} cannot be read (HTTP {r.status})")
    uri = (((r.body or {}).get("bigQuery") or {}).get("bigQuerySource") or {}).get("inputUri")
    if not uri:
        raise ConfigError(f"feature group {src['feature_group']} has no BigQuery source")
    return ".".join(parse_ref(uri))


def read(gcp, ref: str) -> dict:
    """{object, kind, sql, schema, location}: one free `tables.get`."""
    p, d, t = parse_ref(ref)
    r = gcp.get(f"{BQ}/projects/{p}/datasets/{d}/tables/{t}")
    if not r.ok:
        raise ConfigError(f"{ref} cannot be read (HTTP {r.status})")
    body = r.body or {}
    kind = _TYPES.get(body.get("type"), "table")
    sql = ((body.get("view") or body.get("materializedView") or {}).get("query")) if kind != "table" else None
    return {"object": ref, "kind": kind, "sql": sql, "schema": schema_of((body.get("schema") or {}).get("fields")),
            "location": body.get("location")}


def fingerprint(obj: dict) -> dict:
    project = parse_ref(obj["object"])[0]
    sql = normalise_sql(obj.get("sql"), project)
    return {"object": obj["object"], "kind": obj["kind"], "sql_sha256": sha256(sql) if sql else None,
            "schema_sha256": sha256(obj["schema"])}


def built_sql(gcp, ref: str, location: str | None, kms_key: str | None = None) -> dict:
    """The newest successful job that wrote a table (INFORMATION_SCHEMA.JOBS_BY_PROJECT, kept 180 days; needs
    bigquery.jobs.listAll; at least 20 MiB billed). {"sql_sha256", "job", "at"} or {} when none is found."""
    from pipewright.gcp.bigquery import query, rows

    p, d, t = parse_ref(ref)
    region = (location or "US").lower()
    sql = (f"SELECT job_id, CAST(creation_time AS STRING) AS at, query FROM `{p}`.`region-{region}`."
           "INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE state = 'DONE' AND error_result IS NULL "
           "AND destination_table.dataset_id = @d AND destination_table.table_id = @t "
           "AND statement_type IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'MERGE', 'SELECT') "
           "ORDER BY creation_time DESC LIMIT 1")
    params = [{"name": n, "parameterType": {"type": "STRING"}, "parameterValue": {"value": v}}
              for n, v in (("d", d), ("t", t))]
    found = rows(query(gcp, p, sql, params, location=location, kms_key=kms_key))
    if not found:
        return {}
    return {"sql_sha256": sha256(normalise_sql(found[0]["query"], p)), "job": found[0]["job_id"],
            "at": found[0]["at"]}


def scheduled_sql(gcp, name: str, project: str) -> str | None:
    """A scheduled query's SQL, normalised and hashed (free; bigquery.transfers.get)."""
    r = gcp.get(f"{DT}/{name}")
    if not r.ok:
        raise ConfigError(f"scheduled query {name} cannot be read (HTTP {r.status})")
    sql = ((r.body or {}).get("params") or {}).get("query")
    return sha256(normalise_sql(sql, project)) if sql else None


# -- the contract ----------------------------------------------------------------------------------------------------

def _version(v: str | None) -> tuple[int, int, int]:
    try:
        a, b, c = (int(x) for x in str(v or "0.0.0").split("."))
        return a, b, c
    except ValueError:
        raise ConfigError(f"feature_contract.version must be MAJOR.MINOR.PATCH, not {v!r}") from None


def classify(old: dict | None, new: dict) -> tuple[str | None, list[str]]:
    """How the contract changes: (major | minor | patch | None, what changed). A source removed, a column removed or
    retyped is major (models trained on it break); changed SQL, a new column, kind or source is minor; feature
    descriptions are a patch."""
    old_src = {s["name"]: s for s in (old or {}).get("sources") or []}
    new_src = {s["name"]: s for s in new.get("sources") or []}
    level, what = None, []
    order = {None: 0, "patch": 1, "minor": 2, "major": 3}

    def up(lv: str, why: str):
        nonlocal level
        what.append(why)
        if order[lv] > order[level]:
            level = lv

    for name in old_src.keys() - new_src.keys():
        up("major", f"{name}: removed")
    for name in new_src.keys() - old_src.keys():
        up("minor", f"{name}: added")
    for name in sorted(old_src.keys() & new_src.keys()):
        a, b = old_src[name], new_src[name]
        cols_a = {c.split(" ")[0]: c for c in a.get("schema") or []}
        cols_b = {c.split(" ")[0]: c for c in b.get("schema") or []}
        for col in sorted(cols_a.keys() - cols_b.keys()):
            up("major", f"{name}: column {col} removed")
        for col in sorted(cols_a.keys() & cols_b.keys()):
            if cols_a[col] != cols_b[col]:
                up("major", f"{name}: column {col} is now {cols_b[col].split(' ', 1)[1]} "
                            f"(was {cols_a[col].split(' ', 1)[1]})")
        for col in sorted(cols_b.keys() - cols_a.keys()):
            up("minor", f"{name}: column {col} added")
        if a.get("kind") != b.get("kind"):
            up("minor", f"{name}: now a {b.get('kind')} (was a {a.get('kind')})")
        if a.get("sql_sha256") != b.get("sql_sha256"):
            up("minor", f"{name}: its SQL changed (sha256 {str(a.get('sql_sha256'))[:12]} -> "
                        f"{str(b.get('sql_sha256'))[:12]})")
        if (a.get("features") or {}) != (b.get("features") or {}):
            up("patch", f"{name}: feature descriptions or types changed")
    return level, what


def bump(version: str | None, level: str | None) -> str:
    major, minor, patch = _version(version)
    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    if level == "patch":
        return f"{major}.{minor}.{patch + 1}"
    return f"{major}.{minor}.{patch}"


def contract_digest(section: dict) -> str:
    """What an approval and a run record: the version and each source's kind and fingerprints (not descriptions)."""
    return sha256({"version": section.get("version"),
                   "sources": sorted([[s["name"], s.get("kind"), s.get("sql_sha256"), s.get("schema_sha256")]
                                      for s in section.get("sources") or []])})


def _declared_sql(entry: dict, root: Path | None, project: str) -> str | None:
    """A built table's declared SQL file (relative to the use case folder), normalised and hashed."""
    if not entry.get("sql_file"):
        return None
    if root is None:
        return entry.get("sql_sha256")          # a released contract: its recorded digest stands
    path = Path(root) / entry["sql_file"]
    try:
        return sha256(normalise_sql(path.read_text(encoding="utf-8"), project))
    except OSError as e:
        raise ConfigError(f"feature_contract: {entry['sql_file']} cannot be read: {e}") from None


def live(gcp, features_from: dict, project: str, region: str) -> list[dict]:
    """Each features_from source as it is now: {name, object, kind, sql_sha256, schema_sha256, schema, features}."""
    out = []
    for src in (features_from or {}).get("sources") or []:
        ref = resolve(gcp, src, project, region)
        obj = read(gcp, ref)
        fp = fingerprint(obj)
        cols = {c.split(" ")[0]: c.split(" ")[1] for c in obj["schema"]}
        feats = {f: {"type": cols.get(f, "?")} for f in src.get("features") or cols}
        out.append({"name": source_name(src), **fp, "schema": obj["schema"], "features": feats,
                    "location": obj.get("location")})
    return out


def build(gcp, features_from: dict, project: str, region: str, existing: dict | None, root: Path | None) -> dict:
    """The feature_contract section from the live sources, keeping the existing contract's declarations (sql_file,
    scheduled_query, descriptions) and setting the version by what changed. Returns {"section", "level", "changes"}."""
    if not (features_from or {}).get("sources"):
        raise ConfigError("the use case reads no feature tables (data.features_from): a feature contract covers the "
                          "sources it lists")
    before = {s["name"]: s for s in (existing or {}).get("sources") or []}
    sources = []
    for now in live(gcp, features_from, project, region):
        old = before.get(now["name"], {})
        entry = {"name": now["name"], "kind": now["kind"], "sql_sha256": now["sql_sha256"],
                 "schema_sha256": now["schema_sha256"], "schema": now["schema"]}
        for key in ("sql_file", "scheduled_query"):
            if old.get(key):
                entry[key] = old[key]
        if now["kind"] == "table":
            declared = _declared_sql(entry, root, project)
            if entry.get("scheduled_query"):
                declared = scheduled_sql(gcp, entry["scheduled_query"], project)
            entry["sql_sha256"] = declared
        descriptions = {k: (v or {}).get("description") for k, v in (old.get("features") or {}).items()}
        entry["features"] = {f: {**v, **({"description": descriptions[f]} if descriptions.get(f) else {})}
                             for f, v in now["features"].items()}
        sources.append(entry)
    section = {"version": (existing or {}).get("version") or "1.0.0", "sources": sources}
    if not existing:
        return {"section": section, "level": "new", "changes": [f"{s['name']}: added" for s in sources]}
    level, changes = classify(existing, section)
    section["version"] = bump(existing.get("version"), level)
    return {"section": section, "level": level, "changes": changes}


# -- the check -------------------------------------------------------------------------------------------------------

def compare(expected: list[dict], now: list[dict], built: dict | None = None) -> tuple[list[str], list[str]]:
    """(problems, notes): each expected source (from a contract or an approval) against what it is now. `built`
    maps a table source's name to the job-history result {"sql_sha256", "job"} (built_tables on), or is None."""
    problems, notes = [], []
    live_by = {s["name"]: s for s in now}
    for e in expected:
        name, cur = e["name"], live_by.get(e["name"])
        if cur is None:
            problems.append(f"feature source {name}: no longer read by the use case")
            continue
        if cur.get("error"):
            problems.append(f"feature source {name}: {cur['error']}")
            continue
        if e.get("kind") and cur.get("kind") != e.get("kind"):
            problems.append(f"feature source {name}: now a {cur.get('kind')}, approved as a {e.get('kind')}")
        if e.get("schema_sha256") and cur.get("schema_sha256") != e["schema_sha256"]:
            level, what = classify({"sources": [{**e, "features": None}]}, {"sources": [{**cur, "features": None}]})
            detail = "; ".join(w.split(": ", 1)[1] for w in what if "column" in w) or "its schema changed"
            problems.append(f"feature source {name}: {detail} (schema sha256 {e['schema_sha256'][:12]} approved, "
                            f"{str(cur.get('schema_sha256'))[:12]} now)")
        if cur.get("kind") in ("view", "materialized_view"):
            if e.get("sql_sha256") and cur.get("sql_sha256") != e["sql_sha256"]:
                problems.append(f"feature source {name}: its SQL changed (sha256 {e['sql_sha256'][:12]} approved, "
                                f"{str(cur.get('sql_sha256'))[:12]} now)")
        elif e.get("sql_sha256"):
            b = (built or {}).get(name)
            if built is None:
                notes.append(f"feature source {name} is a table built by a query: checked by schema only "
                             "(policies.integrity.features.built_tables is off)")
            elif not b:
                notes.append(f"feature source {name}: no job that wrote it in the last 180 days, so its SQL cannot "
                             "be shown")
            elif b.get("error"):
                problems.append(f"feature source {name}: {b['error']}")
            elif b.get("sql_sha256") != e["sql_sha256"]:
                problems.append(f"feature source {name}: last built by SQL sha256 {b['sql_sha256'][:12]}"
                                f"{' (' + b['job'] + ')' if b.get('job') else ''}, approved "
                                f"{e['sql_sha256'][:12]}")
    return problems, notes


def check(gcp, expected: list[dict], features_from: dict | None, project: str, region: str,
          built_tables: bool = False, kms_key: str | None = None) -> tuple[list[str], list[str], list[dict]]:
    """(problems, notes, live) for a contract's or an approval's sources. With an approval's sources (each has an
    `object`), the objects it recorded are read; with a contract's, the use case's features_from sources."""
    now: list[dict] = []
    if all(e.get("object") for e in expected):
        for e in expected:
            try:
                obj = read(gcp, e["object"])
                now.append({"name": e["name"], **fingerprint(obj), "location": obj.get("location")})
            except ConfigError as err:
                now.append({"name": e["name"], "error": str(err)})
    else:
        try:
            now = live(gcp, features_from or {}, project, region)
        except ConfigError as err:
            return [f"feature sources cannot be read: {err}"], [], []
    built = None
    if built_tables:
        built = {}
        for e in expected:
            cur = next((n for n in now if n["name"] == e["name"]), None)
            if not cur or cur.get("error") or cur.get("kind") != "table" or not e.get("sql_sha256"):
                continue
            try:
                if e.get("scheduled_query"):
                    built[e["name"]] = {"sql_sha256": scheduled_sql(gcp, e["scheduled_query"],
                                                                    parse_ref(cur["object"])[0]),
                                        "job": e["scheduled_query"].rsplit("/", 1)[-1]}
                else:
                    built[e["name"]] = built_sql(gcp, cur["object"], cur.get("location"), kms_key)
            except ConfigError as err:
                built[e["name"]] = {"error": f"its build cannot be read: {str(err)[:160]}"}
    problems, notes = compare(expected, now, built)
    return problems, notes, now


def approval_record(section: dict, now: list[dict]) -> dict:
    """The approval's `features`: the contract version and digest, and each source's object and fingerprints as
    checked (a table built by a query keeps the contract's declared SQL digest)."""
    by = {s["name"]: s for s in now}
    sources = []
    for s in section.get("sources") or []:
        cur = by.get(s["name"], {})
        sources.append({"name": s["name"], "object": cur.get("object"), "kind": s.get("kind"),
                        "sql_sha256": s.get("sql_sha256"), "schema_sha256": s.get("schema_sha256"),
                        **({"scheduled_query": s["scheduled_query"]} if s.get("scheduled_query") else {})})
    return {"contract_version": section.get("version"), "contract_sha256": contract_digest(section),
            "sources": sources}
