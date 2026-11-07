"""FR-104: the Cloud SQL online store's one-time setup."""
from types import SimpleNamespace

from pipewright.online import setup

TENANTS = {"churn": {"runner_service_account": "runner-churn@acme.iam.gserviceaccount.com",
                     "features": {"sync_job": "j", "lookup_service_account": "pwl-churn@acme.iam.gserviceaccount.com"}},
           "other": {"runner_service_account": "runner-other@acme.iam.gserviceaccount.com"}}


def test_grants_per_tenant_with_a_sync_job_only():
    sql = setup.plan(TENANTS, "features")
    assert sql[:2] == ["REVOKE ALL ON SCHEMA public FROM PUBLIC", 'REVOKE CREATE ON DATABASE "features" FROM PUBLIC']
    assert 'GRANT CONNECT, CREATE ON DATABASE "features" TO "runner-churn@acme.iam"' in sql
    assert 'GRANT USAGE, CREATE ON SCHEMA public TO "runner-churn@acme.iam"' in sql
    assert 'GRANT CONNECT ON DATABASE "features" TO "pwl-churn@acme.iam"' in sql
    assert not any("runner-other" in s for s in sql)


class Conn:
    def __init__(self, exists=True, missing=()):
        self.sql, self.exists, self.missing = [], exists, missing

    def run(self, sql, **p):
        self.sql.append(sql)
        if sql.startswith("SELECT 1 FROM pg_database"):
            return [[1]] if self.exists else []
        if any(m in sql for m in self.missing):
            raise RuntimeError(f'role "{self.missing[0]}" does not exist')
        return []


def test_the_admin_creates_the_database_and_a_missing_user_is_said():
    conn, said = Conn(exists=False, missing=("pwl-churn@acme.iam",)), []
    env = SimpleNamespace(name="dev", tenants=TENANTS, features={"online_store": {"cloudsql": {
        "instance": "acme:europe-west2:features"}}})
    setup.run(env, None, lambda db: conn, log=said.append)
    assert 'CREATE DATABASE "features"' in conn.sql
    assert any("not created yet" in s and "pwl-churn@acme.iam" in s for s in said)
