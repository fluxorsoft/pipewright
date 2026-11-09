"""`model.serving.online_store` -> the lookup a served model carries (FR-104).

Pipewright 1.1 (FR-51) resolves `model.serving.features: online` to `{store, key, views}`: the tenant's Bigtable
online store. A use case that also sets `model.serving.online_store.type` to `bigquery`, `cloudsql` or `http` gets a
lookup of that type instead, with a `type` key; without it nothing changes. The environment decides which types are
there (manifest `features.online_store.types`), the use case picks one.

Every type serves one table per use case and environment, one row per entity, its columns named as the model's
features. Pipewright's sync builds it (bigquery and cloudsql); with `http` the team's store answers for it.
"""

from __future__ import annotations

import re

from pipewright.online import TYPES, sizing

EXPORT_KEEP = 3
_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def service_name(tenant: str, usecase: str) -> str:
    """The Cloud Run lookup service of a use case: `pwl-<tenant>-<usecase>`, within Cloud Run's 49 characters. The
    launcher creates only names with the tenant's prefix (spike (d): Cloud Run cannot scope a runner by name)."""
    raw = re.sub(r"[^a-z0-9-]+", "-", f"pwl-{tenant}-{usecase}".lower()).strip("-")
    if len(raw) > 49:
        import hashlib

        raw = raw[:42].rstrip("-") + "-" + hashlib.sha256(raw.encode()).hexdigest()[:6]
    return raw


def table_name(usecase: str) -> str:
    """The default table a use case's online features live in: `pw_online_<usecase>`."""
    return "pw_online_" + re.sub(r"[^a-z0-9_]+", "_", usecase.lower()).strip("_")


def export_prefix(env, tenant: str, usecase: str, spec: dict | None = None) -> str:
    """Where a use case's Parquet generations go: export.uri, or the tenant's prefix in the artifacts bucket."""
    uri = ((spec or {}).get("export") or {}).get("uri")
    return (uri.rstrip("/") if uri else f"gs://{env.artifacts_bucket}/online-export/{tenant}/{usecase}")


def environment_store(env) -> dict:
    return ((env.features or {}).get("online_store")) or {}


def resolve(spec: dict, ml: dict, env, tenant: str, usecase: str, problems: list[str]) -> dict | None:
    """The lookup for `model.serving.online_store` (a type other than bigtable), or None with problems."""
    where = "model.serving.online_store"
    kind = spec.get("type", "bigtable")
    if kind not in TYPES:
        problems.append(f"{where}.type {kind!r}: one of {', '.join(TYPES)}")
        return None
    store = environment_store(env)
    offered = store.get("types") or ["bigtable"]
    if kind not in offered:
        problems.append(f"{where}.type {kind}: {env.name} offers {', '.join(offered)} (manifest "
                        "features.online_store.types, set by the platform team)")
    ff = (ml.get("data") or {}).get("features_from") or {}
    key = ((ff.get("entity") or {}).get("columns")) or []
    if len(key) != 1:
        problems.append(f"{where}: needs one entity column (data.features_from.entity.columns has {len(key)})")
        return None
    include = ((ml.get("features") or {}).get("include")) or []
    served = {}
    for src in ff.get("sources") or []:
        prefix = src.get("prefix", "")
        for f in src.get("features") or []:
            served[prefix + f] = prefix + f
    missing = [f for f in include if f not in served]
    if missing:
        problems.append(f"{where}: model features no source in data.features_from provides: {', '.join(missing)}")
    features = include or sorted(served)
    for name in features:
        if not _IDENT.match(name) and kind == "cloudsql":
            problems.append(f"{where}: feature {name!r} is not a PostgreSQL column name (lower case, digits, _)")
    export = {"enabled": False, "keep": EXPORT_KEEP, "readers": [], **(spec.get("export") or {})}
    # 1.4.2: a reader as an email or as a member (serviceAccount:...), as lookup.callers takes; kept as the email
    export["readers"] = [r.removeprefix("serviceAccount:") for r in export.get("readers") or []]
    if export.get("uri"):
        mine = f"gs://{env.artifacts_bucket}/online-export/{tenant}/"
        if export["uri"].startswith(f"gs://{env.artifacts_bucket}/") and not export["uri"].startswith(mine):
            problems.append(f"{where}.export.uri: in the artifacts bucket a tenant writes only under {mine}")
        if not export["uri"].startswith("gs://"):
            problems.append(f"{where}.export.uri: a gs:// location")
    out = {"type": kind, "key": key[0], "features": features}
    if export["enabled"]:
        out["export"] = {"uri": export_prefix(env, tenant, usecase, spec), "keep": export["keep"],
                         "readers": export["readers"]}
    if kind == "bigquery":
        bq = store.get("bigquery") or {}
        if not bq.get("bi_engine_gib"):
            problems.append(f"{where}.type bigquery: {env.name} has no BI Engine reservation (manifest "
                            "features.online_store.bigquery.bi_engine_gib). Without it every lookup is billed as a "
                            "full table read, so Pipewright does not serve from BigQuery without one")
        dataset = ((env.tenants.get(tenant) or {}).get("bigquery_dataset"))
        table = (spec.get("bigquery") or {}).get("table") or table_name(usecase)
        out.update(project=env.project, location=env.region, table=f"{env.project}.{dataset}.{table}",
                   timeout_ms=(spec.get("bigquery") or {}).get("timeout_ms", 1000))
        if (env.security or {}).get("bigquery_kms_key"):     # CMEK on the lookups' results (found live in ent)
            out["kms_key"] = env.security["bigquery_kms_key"]
    elif kind == "cloudsql":
        sql = store.get("cloudsql") or {}
        if not sql.get("instance"):
            problems.append(f"{where}.type cloudsql: {env.name} has no Cloud SQL instance (manifest "
                            "features.online_store.cloudsql.instance)")
        cs = spec.get("cloudsql") or {}
        schema, table = cs.get("schema", "public"), cs.get("table") or table_name(usecase)
        for label, ident in (("schema", schema), ("table", table)):
            if not _IDENT.match(ident):
                problems.append(f"{where}.cloudsql.{label} {ident!r}: lower case letters, digits and _, up to 63")
        lookup = {**sizing.LOOKUP_DEFAULTS, **(((store.get("lookup") or {}).get("defaults")) or {}),
                  **(spec.get("lookup") or {})}
        provider = lookup.get("provider", "pipewright")
        out.update(instance=sql.get("instance"), database=sql.get("database", "features"), schema=schema,
                   table=table, provider=provider)
        if provider == "pipewright":
            share = None
            limit = sizing.default_connections(str((sql.get("settings") or {}).get("tier", "db-custom-1-3840")))
            if limit:
                share = int(limit * 0.8) - 5            # room for the sync, Cloud SQL's own backends and others
            problems += sizing.lookup_problems(lookup, (store.get("lookup") or {}).get("limits"),
                                               f"{where}.lookup", share)
            out["lookup"] = {k: lookup[k] for k in sizing.LOOKUP_DEFAULTS}
            out.update(service=service_name(tenant, usecase), project=env.project, region=env.region)
            gateway = _gateway(lookup, env, usecase, f"{where}.lookup", problems)
            if gateway:
                out.update(mode="gateway", gateway=gateway)
        elif provider == "own":
            if lookup.get("mode", "lookup") == "gateway":
                problems.append(f"{where}.lookup.mode gateway: the gateway is Pipewright's lookup service "
                                "(provider pipewright)")
            sized = [k for k in (spec.get("lookup") or {}) if k in sizing.LOOKUP_DEFAULTS]
            if sized:
                problems.append(f"{where}.lookup: {', '.join(sized)} size Pipewright's lookup service; with "
                                "provider own the team runs its own, so remove them")
            if not lookup.get("reader"):
                problems.append(f"{where}.lookup.reader: with provider own, name the service account of the tenant "
                                "that reads the table (Pipewright grants it SELECT)")
            out["reader"] = lookup.get("reader")
        else:
            problems.append(f"{where}.lookup.provider {provider!r}: pipewright or own")
    elif kind == "http":
        h = spec.get("http") or {}
        if not h.get("url", "").startswith("https://"):
            problems.append(f"{where}.http.url: the team's lookup, an https:// URL answering POST /lookup")
        out.update(url=h.get("url"), audience=h.get("audience") or h.get("url"),
                   timeout_ms=h.get("timeout_ms", 1000))
        mine = spec.get("lookup") or {}
        if mine.get("mode") == "gateway":
            # The Pipewright custom solution (1.4.2): the team's own lookup service inside the network, which an
            # endpoint cannot reach, behind Pipewright's gateway. The gateway is a Pipewright lookup service whose
            # features come from the team's POST /lookup instead of Cloud SQL.
            lookup = {**sizing.LOOKUP_DEFAULTS, **(((store.get("lookup") or {}).get("defaults")) or {}), **mine}
            sized = {k: lookup[k] for k in sizing.LOOKUP_DEFAULTS if k != "pool_size"}
            problems += sizing.lookup_problems(lookup, (store.get("lookup") or {}).get("limits"), f"{where}.lookup")
            gateway = _gateway(lookup, env, usecase, f"{where}.lookup", problems)
            if gateway:
                out.update(mode="gateway", gateway=gateway, provider="pipewright", lookup=sized,
                           service=service_name(tenant, usecase), project=env.project, region=env.region)
        elif mine:
            problems.append(f"{where}.lookup: with type http only mode gateway and the gateway's size; the team's "
                            "store answers lookups itself")
    if kind not in ("cloudsql", "http") and ((spec.get("lookup") or {}).get("mode") or "lookup") != "lookup":
        problems.append(f"{where}.lookup.mode gateway: with Cloud SQL (type cloudsql) or the team's own store (type "
                        "http); BigQuery and Bigtable are Google APIs, which endpoints reach directly")
    if kind in ("bigquery", "cloudsql"):
        out["sync"] = {"mode": (spec.get("sync") or {}).get("mode", "full")}
    return out


GATEWAY_RETRIES = 3
GATEWAY_KEYS = ("mode", "retries", "callers")


def _gateway(lookup: dict, env, usecase: str, where: str, problems: list[str]) -> dict | None:
    """FR-106 (1.4.2): `lookup.mode: gateway`. Callers send entity IDs to the lookup service inside the network; it
    reads Cloud SQL and calls the endpoint as its own identity (an endpoint cannot reach into the network, spike
    2026-11-08). Off by default: without it nothing changes."""
    mode = lookup.get("mode", "lookup")
    if mode not in ("lookup", "gateway"):
        problems.append(f"{where}.mode {mode!r}: lookup or gateway")
        return None
    if mode != "gateway":
        for k in ("retries", "callers"):
            if k in lookup:
                problems.append(f"{where}.{k}: only with mode gateway")
        return None
    callers = list(lookup.get("callers") or [])
    if not callers:
        problems.append(f"{where}.callers: with mode gateway, name who may call it (serviceAccount:, group: or "
                        "user: members get run.invoker on the lookup service)")
    from pipewright.serving import endpoints

    return {"retries": int(lookup.get("retries", GATEWAY_RETRIES)), "callers": callers,
            "endpoint": endpoints.declared(env, usecase) or {"name": f"{usecase}-endpoint"},
            "private": endpoints.is_private(env, usecase)}

