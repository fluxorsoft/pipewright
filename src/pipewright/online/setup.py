"""The Cloud SQL online store's one-time setup (FR-104): `pipewright features online-setup --manifest ...`.

Run by the platform as its online store admin identity (an IAM database user given cloudsqlsuperuser with
`gcloud sql users assign-roles`, or Terraform's database_roles; no password user exists). Spike (d): whoever creates
the database owns its `public` schema, so the admin creates it, never a tenant. Then, per tenant whose manifest entry
has a sync job:

- its runner (the sync identity) may connect and create schemas, and create tables in `public` (the default schema;
  the docs advise a schema per team). Postgres ownership then keeps tenants apart: a table or schema belongs to the
  runner that created it, and the sync refuses to write one another identity owns.
- its lookup identity may connect; the sync grants it SELECT on the tables it writes.

Idempotent: run again after a tenant is added. Names only in its output.
"""

from __future__ import annotations

from pipewright.errors import ConfigError
from pipewright.online.server import iam_user


def plan(manifest_tenants: dict, database: str) -> list[str]:
    sql = ["REVOKE ALL ON SCHEMA public FROM PUBLIC", f'REVOKE CREATE ON DATABASE "{database}" FROM PUBLIC']
    for _name, tenant in sorted(manifest_tenants.items()):
        feats = (tenant or {}).get("features") or {}
        if not feats.get("sync_job"):
            continue
        runner = tenant.get("runner_service_account")
        if runner:
            r = iam_user(runner)
            sql += [f'GRANT CONNECT, CREATE ON DATABASE "{database}" TO "{r}"',
                    f'GRANT USAGE, CREATE ON SCHEMA public TO "{r}"']
        if feats.get("lookup_service_account"):
            sql.append(f'GRANT CONNECT ON DATABASE "{database}" TO "{iam_user(feats["lookup_service_account"])}"')
    return sql


def run(env, gcp, connect_admin, log=print, dry_run: bool = False) -> list[str]:
    """connect_admin(database) opens a connection as the admin identity."""
    store = ((env.features or {}).get("online_store") or {}).get("cloudsql") or {}
    if not store.get("instance"):
        raise ConfigError(f"{env.name} has no Cloud SQL online store (manifest features.online_store.cloudsql)")
    database = store.get("database", "features")
    statements = plan(env.tenants, database)
    if dry_run:
        for s in statements:
            log(f"would run: {s}")
        return statements
    admin = connect_admin("postgres")
    if not admin.run("SELECT 1 FROM pg_database WHERE datname = :d", d=database):
        admin.run(f'CREATE DATABASE "{database}"')
        log(f"created database {database}")
    conn = connect_admin(database)
    missing = []
    for s in statements:
        try:
            conn.run(s)
        except Exception as e:  # noqa: BLE001 - a tenant's IAM user not yet created: said, the rest still run
            if "does not exist" in str(e):
                missing.append(s.rsplit('"', 2)[-2])
                continue
            raise
    log(f"online store {database}: {len(statements) - len(missing)} grants applied")
    if missing:
        log("IAM database users not created yet (Terraform creates them): " + ", ".join(sorted(set(missing))))
    return statements
