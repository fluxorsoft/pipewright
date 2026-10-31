"""FR-85: a replay can be shown to read the same rows, and the example trains on them in the same order."""
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pd = pytest.importorskip("pandas")
sys.path.insert(0, str(Path(__file__).parents[1] / "src" / "pipewright" / "build" / "templates"))
import pipewright_runtime as rt  # noqa: E402

from pipewright.models import lineage  # noqa: E402

FIELDS = [{"name": "customer_id", "type": "STRING"}, {"name": "snapshot_date", "type": "DATE"},
          {"name": "orders", "type": "INTEGER"}, {"name": "spend", "type": "FLOAT"},
          {"name": "run_id", "type": "STRING"}, {"name": "ingested_at", "type": "TIMESTAMP"}]


def original():
    import datetime as dt
    return pd.DataFrame({"customer_id": ["2", "1", "1"],
                         "snapshot_date": [dt.date(2011, 9, 1), dt.date(2011, 8, 1), dt.date(2011, 9, 1)],
                         "orders": [3, 1, 2], "spend": [10.5, None, 3.25]})


def test_the_same_rows_in_another_order_and_with_bigquerys_types_fingerprint_equal():
    a = original()
    # What a replay gets back: dates as strings, integers as floats, another order, the snapshot's own columns.
    b = a.iloc[[2, 0, 1]].assign(snapshot_date=lambda d: pd.to_datetime(d["snapshot_date"]).dt.strftime("%Y-%m-%d"),
                                 orders=lambda d: d["orders"].astype(float), run_id="r9", ingested_at="2026-10-31")
    key = ["customer_id", "snapshot_date"]
    assert rt.rows_fingerprint(a, FIELDS, key) == rt.rows_fingerprint(b, FIELDS, key)


def test_an_erased_or_changed_row_changes_the_fingerprint():
    a, key = original(), ["customer_id", "snapshot_date"]
    assert rt.rows_fingerprint(a.iloc[1:], FIELDS, key) != rt.rows_fingerprint(a, FIELDS, key)     # erased
    assert rt.rows_fingerprint(a.assign(spend=[10.5, None, 3.26]), FIELDS, key) != rt.rows_fingerprint(a, FIELDS, key)


def test_lineage_says_whether_a_replay_read_the_original_rows(monkeypatch):
    env = SimpleNamespace(project="p", security={}, observability={"evidence_dataset": "pw_evidence"})
    answers = {"r2": "sha256:aa", "r1": "sha256:aa"}
    monkeypatch.setattr("pipewright.gcp.bigquery.query", lambda *a, **k: {})
    monkeypatch.setattr("pipewright.gcp.bigquery.rows", lambda res: [{"run": k, "fingerprint": v}
                                                                    for k, v in answers.items()])
    assert lineage.fingerprints(None, env, "r2", "r1")["fingerprint"]["identical"] is True
    answers["r2"] = "sha256:bb"                                                  # the ebook's Chapter 17 erasure
    out = lineage.fingerprints(None, env, "r2", "r1")
    assert out["fingerprint"]["identical"] is False
    text = lineage.render({"model": "m", "version": "2", "environment": "test", "snapshot": {
        "table": "t", "run_id": "r2", "rows": 3, "read_at": "x"}, "lineage": {"run_id": "r2"}, **out})
    assert "fingerprint sha256:bb; the original run's rows: DIFFERENT" in text
    del answers["r1"]                                                            # before Pipewright 1.2
    assert lineage.fingerprints(None, env, "r2", "r1")["fingerprint"]["identical"] is None
