"""GA QA T10.3: with capture on, the erasure report says capture holds request features and prediction IDs, never the
key, so it ages out with its retention instead of being found by key."""
from types import SimpleNamespace

from pipewright.data import erase


def uc(capture):
    return SimpleNamespace(name="churn", environment="test", model={"serving": {"capture": capture}},
                           ml={"data_protection": {"erasure": {"key_column": "customer_id"}}})


def test_the_note_appears_only_with_capture_on(monkeypatch):
    monkeypatch.setattr(erase, "targets", lambda u: ("customer_id", []))
    on = erase.render(uc({"enabled": True, "retention_days": 7}), [], 1, False)
    assert "online capture (7 days)" in on and "never the key" in on
    assert "online capture" not in erase.render(uc({}), [], 1, False)
