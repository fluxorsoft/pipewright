"""FR-65: the synthetic pipeline reads labels like training. With data.label_source, an entity table without the
label column gets its labels from the label window; a source that holds the label is read as in 1.x."""
from types import SimpleNamespace

from pipewright.build.templates import pipewright_runtime as rt

LABELS = {"table": "bq://sales.purchases", "time_column": "purchased_at", "window_days": 20,
          "key": ["customer_id"], "cutoff_column": "snapshot_date"}


def config(label_source=LABELS):
    return {"data": {"label": {"column": "churned"}, **({"label_source": label_source} if label_source else {})}}


def client_with(monkeypatch, *columns):
    table = SimpleNamespace(schema=[SimpleNamespace(name=c) for c in columns])
    monkeypatch.setattr(rt, "bigquery_client", lambda *a, **k: SimpleNamespace(get_table=lambda ref: table))


def test_the_entity_table_gets_its_labels(monkeypatch):
    client_with(monkeypatch, "customer_id", "snapshot_date", "tenure_months")
    assert rt.needs_labels("bq://crm.customers", "p", config())


def test_rows_labelled_already_are_read_as_before(monkeypatch):
    client_with(monkeypatch, "customer_id", "snapshot_date", "churned")
    assert not rt.needs_labels("bq://ml.churn_snapshot", "p", config())


def test_without_a_label_source_or_bigquery_nothing_changes(monkeypatch):
    client_with(monkeypatch, "customer_id")
    assert not rt.needs_labels("bq://crm.customers", "p", config(None))
    assert not rt.needs_labels("gs://b/rows.parquet", "p", config())
    assert not rt.needs_labels("snapshot://r1", "p", config())
