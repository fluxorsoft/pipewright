"""FR-104: Pipewright's Cloud SQL lookup service."""
import http.client
import json
import threading
from http.server import ThreadingHTTPServer

import pytest

from pipewright.online import server

CFG = {"host": "10.0.0.3", "database": "features", "schema": "team_churn", "table": "pw_online_churn",
       "key": "customer_id", "features": ["tenure", "tickets"], "pool_size": 2, "user": "lookup@acme.iam"}


class FakeConn:
    opened = 0

    def __init__(self, rows):
        FakeConn.opened += 1
        self.rows, self.sql = rows, []

    def run(self, sql, **params):
        self.sql.append((sql, params))
        if isinstance(self.rows, Exception):
            raise self.rows
        return [r for r in self.rows if r[0] in params["ids"]]


def serve(lookup):
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.handler_for(lookup))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def post(httpd, body, path="/lookup"):
    c = http.client.HTTPConnection("127.0.0.1", httpd.server_address[1], timeout=5)
    data = json.dumps(body).encode()
    c.request("POST", path, body=data, headers={"Content-Type": "application/json"})
    r = c.getresponse()
    return r.status, json.loads(r.read())


def test_lookup_answers_the_http_contract_with_quoted_names(capsys):
    conns = []
    lk = server.Lookup(CFG, connect=lambda: conns.append(FakeConn([("c-1", 12, None), ("c-2", 3, 1.5)])) or conns[-1],
                       tokens=server.Tokens(lambda: "t"))
    assert lk.sql == ('SELECT "customer_id", "tenure", "tickets" FROM "team_churn"."pw_online_churn" '
                      'WHERE "customer_id" = ANY(:ids)')
    httpd = serve(lk)
    try:
        assert post(httpd, {"entity_ids": ["c-1", "c-9"]}) == (200, {"features": {"c-1": {"tenure": 12.0,
                                                                                         "tickets": None}}})
        assert post(httpd, {"entity_ids": "c-1"})[0] == 400
        assert post(httpd, {"entity_ids": ["x"] * 1001})[0] == 400
        assert post(httpd, {}, path="/other")[0] == 404
    finally:
        httpd.shutdown()
    out = capsys.readouterr().out
    assert "c-1" not in out and '"asked": 2' in out and '"found": 1' in out     # counts, never IDs or values


def test_the_pool_is_bounded_and_a_broken_connection_is_replaced():
    FakeConn.opened = 0
    state = {"rows": RuntimeError("server closed the connection")}
    lk = server.Lookup(CFG, connect=lambda: FakeConn(state["rows"]), tokens=server.Tokens(lambda: "t"))
    with pytest.raises(RuntimeError):
        lk.fetch(["c-1"])
    state["rows"] = [("c-1", 1, 2)]
    assert lk.fetch(["c-1"]) == {"c-1": {"tenure": 1.0, "tickets": 2.0}}
    assert FakeConn.opened == 2 and lk.pool.qsize() == 2


def test_a_database_error_is_503_with_its_class_only():
    lk = server.Lookup(CFG, connect=lambda: FakeConn(RuntimeError("password authentication failed for c-1")),
                       tokens=server.Tokens(lambda: "t"))
    httpd = serve(lk)
    try:
        status, body = post(httpd, {"entity_ids": ["c-1"]})
    finally:
        httpd.shutdown()
    assert status == 503 and body == {"error": "the database did not answer (RuntimeError)"}


def test_names_must_be_identifiers():
    with pytest.raises(ValueError, match="not a PostgreSQL identifier"):
        server.Lookup({**CFG, "table": 'x"; DROP TABLE y'}, connect=lambda: None)
    with pytest.raises(ValueError, match="not PostgreSQL column names"):
        server.Lookup({**CFG, "features": ["ok", "Bad-Name"]}, connect=lambda: None)


def test_iam_user_and_tokens():
    assert server.iam_user("pwl-churn@acme.iam.gserviceaccount.com") == "pwl-churn@acme.iam"
    calls = []
    t = server.Tokens(lambda: calls.append(1) or "tok")
    assert t.get() == t.get() == "tok" and len(calls) == 1


def test_tokens_follow_their_expiry_not_a_fixed_reuse():
    """Found live (1.4.2): the metadata server's token may have minutes left; reusing it for 25 minutes sent expired
    tokens (the endpoint answered 401). Refresh five minutes before it expires."""
    from pipewright.online.server import Tokens

    now = {"t": 1000.0}
    issued = iter([{"access_token": "A", "expires_in": 420}, {"access_token": "B", "expires_in": 3599}])
    tok = Tokens(fetch=lambda: next(issued), clock=lambda: now["t"])
    assert tok.get() == "A"
    now["t"] += 119
    assert tok.get() == "A"                     # 2 minutes in, 5 minutes left: still good
    now["t"] += 2
    assert tok.get() == "B"                     # within five minutes of expiry: a new one
