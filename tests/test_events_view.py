"""FR-134: the CLI reads a tenant's own view of the event log (Enterprise's pipewright_events) where the manifest
names it, so the team's CI identity needs no access to every team's events; the launcher keeps the table."""
import json
from types import SimpleNamespace

import pytest

from pipewright.models import approvals, integrity

VIEW = "acme-ml-test.pw_retail.pipewright_events"


def env(view=VIEW):
    return SimpleNamespace(name="test", project="acme-ml-test", region="r", security={},
                           observability={"evidence_dataset": "pw_evidence"},
                           tenants={"retail": {"events_view": view} if view else {}, "fraud": {}})


@pytest.fixture
def sql(monkeypatch):
    approvals.reset()
    approvals.use_store(None, None)
    seen = []

    def query(gcp, project, text, params, kms_key=None):
        seen.append(text)
        if "pipewright_events" in text and "release.live" in text:
            return [{"sha": "abc"}]
        if "pipewright_events" in text:
            return [{"published": "2026-11-18 10:00:00", "event_type": "model.approved", "usecase": "churn",
                     "details": json.dumps({"version": "3", "by": "ana@example.com"})}]
        raise RuntimeError("Access Denied: Table acme-ml-test:pw_evidence.events")

    monkeypatch.setattr("pipewright.gcp.bigquery.query", query)
    monkeypatch.setattr("pipewright.gcp.bigquery.rows", lambda r: r)
    yield seen
    approvals.reset()


def test_the_cli_reads_the_tenants_view(sql):
    approvals.know("churn", "retail")                  # resolve_usecase records it
    got = approvals.events(None, env(), "model.approved", "churn", "3")
    assert got and got[0]["by"] == "ana@example.com" and got[0]["at"] == "2026-11-18 10:00:00"
    assert all("pipewright_events" in s and "pw_evidence" not in s for s in sql)
    assert "event_type IN (" in sql[0] and "environment = @env" in sql[0]


def test_a_use_case_the_process_did_not_resolve_reads_the_table(sql):
    """The launcher resolves no use case from a checkout: it reads the table, as it is granted to."""
    with pytest.raises(RuntimeError, match="Access Denied"):
        approvals.events(None, env(), "model.approved", "churn", "3")
    assert "pw_evidence.events" in sql[0]


def test_a_tenant_without_a_view_reads_the_table_as_before(sql):
    approvals.know("churn", "retail")
    with pytest.raises(RuntimeError):
        approvals.events(None, env(view=None), "model.approved", "churn", "3")
    assert "pw_evidence.events" in sql[0]


def test_the_contract_check_reads_the_view_too(sql, monkeypatch):
    approvals.know("churn", "retail")
    monkeypatch.setattr(integrity, "contract_sha256", lambda text: "abc")
    monkeypatch.setattr(integrity, "rollback_features", lambda *a, **k: None)
    check = integrity.contract_check(None, env(), "churn", "r1", "{}")
    assert check.problems == [] and "pipewright_events" in sql[-1] and "JSON_VALUE(details" in sql[-1]


def test_the_manifest_takes_a_tenants_events_view():
    import copy

    from pipewright.config.schema import validate
    from tests.conftest import MANIFEST

    manifest = copy.deepcopy(MANIFEST)
    tenant = next(iter(manifest["tenants"]))
    manifest["tenants"][tenant]["events_view"] = VIEW
    validate(manifest, "manifest", "m.json")
