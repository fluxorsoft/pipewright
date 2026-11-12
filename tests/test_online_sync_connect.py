"""The sync's Cloud SQL connection says what to do when the store is not set up (1.4.3 GA QA, live: a rebuilt
environment's sync failed with a raw 'database "features" does not exist' traceback)."""
from types import SimpleNamespace

import pg8000.exceptions
import pg8000.native
import pytest

from pipewright.errors import ConfigError
from pipewright.online import server, sync


def _connect(monkeypatch, error):
    def fail(*a, **k):
        raise error
    monkeypatch.setattr(pg8000.native, "Connection", fail)
    monkeypatch.setattr(server, "ssl_context", lambda ca: None)
    gcp = SimpleNamespace(get=lambda url, **k: SimpleNamespace(ok=True, body={"ipAddresses": [
        {"type": "PRIVATE", "ipAddress": "10.0.0.5"}], "serverCaCert": {"cert": "x"}}),
        identity=lambda: "run@p.iam.gserviceaccount.com", access_token=lambda: "tok")
    env = SimpleNamespace(features={"online_store": {"cloudsql": {}}})
    return sync.connector(env, gcp, {"instance": "p:r:db", "database": "features"})


def test_a_missing_database_says_to_run_the_platform_setup(monkeypatch):
    connect = _connect(monkeypatch, pg8000.exceptions.DatabaseError(
        {"S": "FATAL", "C": "3D000", "M": 'database "features" does not exist'}))
    with pytest.raises(ConfigError, match="online-setup"):
        connect()


def test_other_database_errors_are_raised_as_they_are(monkeypatch):
    connect = _connect(monkeypatch, pg8000.exceptions.DatabaseError({"S": "FATAL", "C": "28000", "M": "denied"}))
    with pytest.raises(pg8000.exceptions.DatabaseError):
        connect()
