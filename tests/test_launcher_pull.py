"""The launcher as a pull worker (launcher.runtime gke): pulled messages go through the same routes as pushes, a 2xx
answer acknowledges them, and a pulled approver decision is trusted as a push from Pub/Sub is."""

import base64
import json

from pipewright.launcher import pull


class FakeClient:
    def __init__(self, status):
        self.status, self.calls = status, []

    def post(self, path, json=None, environ_base=None):
        self.calls.append((path, json, environ_base))

        class R:
            status_code = self.status

            def get_data(self, as_text=False):
                return "x"

            def get_json(self, silent=False):
                return {"status": "submitted"}
        return R()


def received(body):
    return {"ackId": "a1", "message": {"messageId": "m1", "data": base64.b64encode(json.dumps(body).encode()).decode(),
                                       "attributes": {"usecase": "churn"}}}


def test_a_pulled_message_goes_to_its_route_as_a_push_envelope():
    client = FakeClient(200)
    assert pull.handle(client, "/pubsub/run", received({"usecase": "churn"}), "projects/p/subscriptions/s")
    path, body, environ = client.calls[0]
    assert path == "/pubsub/run" and body["subscription"] == "projects/p/subscriptions/s"
    assert body["message"]["message_id"] == "m1" and environ == {pull.PULLED: True}
    assert not pull.handle(FakeClient(503), "/pubsub/run", received({}), "projects/p/subscriptions/s")


def test_a_pulled_decision_is_trusted_and_a_direct_call_without_identity_is_not():
    from pipewright.launcher.app import create_app

    class Reg:
        manifest = {"environment": "dev", "launcher": {"service_account": "launcher@p.iam.gserviceaccount.com"},
                    "approver_groups": {}}
        errors = {}

        def get(self, name):
            return None

        def live(self):
            return []

    app = create_app(Reg())
    c = app.test_client()
    env = {"message": {"data": base64.b64encode(b'{"usecase": "churn"}').decode()}, "subscription": "projects/p/s"}
    pulled = c.post("/retraining/decide", json=env, environ_base={pull.PULLED: True})
    assert pulled.status_code == 200 and pulled.get_json()["reason"] == "not an approver group subscription"
    pushed = c.post("/retraining/decide", json=env)        # no token: not Pub/Sub as the launcher
    assert pushed.status_code == 403
