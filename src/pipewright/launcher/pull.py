"""The launcher as a pull worker (manifest launcher.runtime: gke, the private-gke blueprint's second option).

    python -m pipewright.launcher.pull                 # the worker (a Deployment in the cluster)
    python -m pipewright.launcher.pull --tick          # retrain decisions: expiry and escalation (a CronJob)
    python -m pipewright.launcher.pull --tick --digest # the weekly digest (a CronJob)

Pub/Sub cannot push into a private cluster, so the launcher pulls instead. The subscriptions are the ones Cloud Run is
pushed from (landing files, run requests and Ray requests, approver groups' decisions), without their push
configuration; PIPEWRIGHT_SUBSCRIPTIONS maps each to the route that handles it ({"projects/.../subscriptions/x":
"/pubsub"}). Each message goes to that route in the same envelope a push uses, so the handlers, their checks and their
answers are the same as on Cloud Run: a 2xx answer acknowledges the message, anything else leaves it for redelivery
(and the dead-letter topic after the subscription's attempts).

A pulled message is trusted as a push from Pub/Sub is: the worker read it from the subscription as the launcher
identity. That is marked in the WSGI environment (PULLED), which no HTTP client can set. The worker serves no API:
its only port answers /health (the last successful pull), for the kubelet.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PULLED = "pipewright.pulled"           # WSGI environ key: the request came from a subscription this worker pulled
log = logging.getLogger("pipewright.launcher")
_last_pull = {"t": time.time()}


def envelope(received: dict, subscription: str) -> dict:
    """A pulled message in the push format the routes read."""
    message = dict(received.get("message") or {})
    message.setdefault("message_id", message.get("messageId"))
    return {"message": message, "subscription": subscription}


def _pubsub_host(region: str | None) -> str:
    return f"https://{region}-pubsub.googleapis.com/v1" if region else "https://pubsub.googleapis.com/v1"


def handle(client, path: str, received: dict, subscription: str) -> bool:
    """Runs one message through its route; True when it should be acknowledged."""
    r = client.post(path, json=envelope(received, subscription), environ_base={PULLED: True})
    ok = 200 <= r.status_code < 300
    message_id = (received.get("message") or {}).get("messageId")
    if ok:
        answer = r.get_json(silent=True) or {}
        status = answer.get("status") or ",".join(str(x.get("status")) for x in answer.get("results") or []) or "ok"
        log.info("pulled %s on %s: %s %s", message_id, path, r.status_code, status)
    else:
        log.warning("pulled %s on %s: %s %s (left for redelivery)", message_id, path, r.status_code,
                    r.get_data(as_text=True)[:200])
    return ok


def pull_loop(app, subscription: str, path: str, region: str | None, stop: threading.Event) -> None:
    from pipewright.gcp import Gcp

    gcp, client, base = Gcp(), app.test_client(), _pubsub_host(region)     # one session and client per thread
    while not stop.is_set():
        try:
            r = gcp.session.post(f"{base}/{subscription}:pull", json={"maxMessages": 10}, timeout=90)
            if r.status_code != 200:
                log.warning("pull %s: %s %s", subscription, r.status_code, r.text[:200])
                time.sleep(10)
                continue
            _last_pull["t"] = time.time()
            received = (r.json() or {}).get("receivedMessages") or []
            acks, nacks = [], []
            for m in received:
                (acks if handle(client, path, m, subscription) else nacks).append(m["ackId"])
            if acks:
                gcp.session.post(f"{base}/{subscription}:acknowledge", json={"ackIds": acks}, timeout=30)
            if nacks:   # back now, with the subscription's retry backoff and dead-letter policy
                gcp.session.post(f"{base}/{subscription}:modifyAckDeadline",
                                 json={"ackIds": nacks, "ackDeadlineSeconds": 0}, timeout=30)
        except Exception as e:  # noqa: BLE001 - a network blip must not stop the worker
            log.warning("pull %s failed: %s", subscription, e)
            time.sleep(10)


class _Health(BaseHTTPRequestHandler):
    max_silence = 300

    def do_GET(self):  # noqa: N802 - the http.server API
        fresh = time.time() - _last_pull["t"] < self.max_silence
        self.send_response(200 if self.path == "/health" and fresh else 503)
        self.end_headers()
        self.wfile.write(b"ok" if fresh else b"no successful pull recently")

    def log_message(self, *args):
        return


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(prog="python -m pipewright.launcher.pull")
    p.add_argument("--tick", action="store_true", help="run the retrain decision tick once and exit")
    p.add_argument("--digest", action="store_true", help="with --tick: the weekly digest")
    args = p.parse_args(argv)

    from pipewright.launcher import app as launcher_app

    app = launcher_app.app
    if app is None:
        print("MANIFEST_URI is not set", file=sys.stderr)
        return 2
    client = app.test_client()
    if args.tick:
        r = client.post("/retraining/tick", json={"digest": True} if args.digest else {}, environ_base={PULLED: True})
        print(r.get_data(as_text=True))
        return 0 if 200 <= r.status_code < 300 else 1

    routes = json.loads(os.environ.get("PIPEWRIGHT_SUBSCRIPTIONS") or "{}")
    if not routes:
        print("PIPEWRIGHT_SUBSCRIPTIONS is empty: nothing to pull", file=sys.stderr)
        return 2
    region = os.environ.get("PIPEWRIGHT_PUBSUB_REGION")
    stop = threading.Event()
    for subscription, path in routes.items():
        threading.Thread(target=pull_loop, args=(app, subscription, path, region, stop), daemon=True,
                         name=f"pull {subscription.rsplit('/', 1)[-1]}").start()
        log.info("pulling %s for %s", subscription, path)
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), _Health)
    try:
        server.serve_forever()
    finally:
        stop.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
