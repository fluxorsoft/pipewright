"""Pipewright's capture receiver for KServe's logger (Pipewright 1.4.3, FR-98 stage 2). Standard library only.

KServe's logger (the InferenceService's `logger`, mode all) posts each request and each answer as a CloudEvent. The
receiver answers at once and never makes a prediction wait: it samples (by the request ID, so a request and its
answer are kept or dropped together), caps a day's records, puts each kept event in a bounded queue, and a worker
prints one JSON line per event on stdout. A log sink per tenant copies the lines into the tenant's dataset, where the
table takes the dataset's key (spike 2: no new grant; writing BigQuery from the pod was refused 403).

    SAMPLE_RATE   share of requests kept (default 1)          MAX_PER_DAY  records kept a day (default 100000)
    QUEUE         queue size (default 10000)                  PORT         (default 8080)

One receiver serves a tenant's serving namespace; each use case's sample rate and cap come in its logger URL
(`deploy-model` writes `?rate=0.05&max=100000`), and the day's count is kept per InferenceService.

GET /stats: received, kept, sampled_out, capped, dropped (queue full), written. The launcher compares them with the
model's request counter (coverage: serving.capture_gap, serving.capture_silent). A receiver that is down loses
records, never predictions: KServe's agent fails open (spike 2: 40 of 40 calls answered).
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MARKER = "pipewright_online_log"           # the log sink's filter: jsonPayload.pipewright_online_log:*


class Receiver:
    def __init__(self, sample_rate: float = 1.0, max_per_day: int = 100000, size: int = 10000, out=None):
        self.sample_rate, self.max_per_day = float(sample_rate), int(max_per_day)
        self.q: queue.Queue = queue.Queue(maxsize=size)
        self.stats = {"received": 0, "kept": 0, "sampled_out": 0, "capped": 0, "dropped": 0, "written": 0}
        self.day, self.today = time.strftime("%Y-%m-%d", time.gmtime()), {}
        self.lock = threading.Lock()
        self.out = out or (lambda line: print(line, flush=True))

    def _bump(self, key: str, n: int = 1) -> None:
        with self.lock:
            self.stats[key] += n

    @staticmethod
    def sampled(request_id: str, rate: float) -> bool:
        if rate >= 1:
            return True
        h = int(hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF
        return h < rate

    def take(self, headers: dict, raw: bytes, params: dict | None = None) -> str:
        """One CloudEvent: 'kept', 'sampled_out', 'capped' or 'dropped'. params: the logger URL's rate and max."""
        self._bump("received")
        params = params or {}
        try:
            rate = float(params.get("rate", self.sample_rate))
            cap = int(params.get("max", self.max_per_day))
        except ValueError:
            rate, cap = self.sample_rate, self.max_per_day
        ce_id = headers.get("ce-id", "")
        if not self.sampled(ce_id, rate):
            self._bump("sampled_out")
            return "sampled_out"
        isvc = headers.get("ce-inferenceservicename", "")
        with self.lock:
            day = time.strftime("%Y-%m-%d", time.gmtime())
            if day != self.day:
                self.day, self.today = day, {}
            if self.today.get(isvc, 0) >= cap:
                self.stats["capped"] += 1
                return "capped"
            self.today[isvc] = self.today.get(isvc, 0) + 1
        try:
            body = json.loads(raw or b"null")
        except ValueError:
            body = raw.decode("utf-8", "replace")
        event = {"ce_id": ce_id, "ce_type": headers.get("ce-type", ""),
                 "isvc": headers.get("ce-inferenceservicename", ""), "namespace": headers.get("ce-namespace", ""),
                 "component": headers.get("ce-component", ""), "endpoint": headers.get("ce-endpoint", ""),
                 "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 "body": body if isinstance(body, str) else json.dumps(body, separators=(",", ":"))}
        try:
            self.q.put_nowait(event)
        except queue.Full:
            self._bump("dropped")
            return "dropped"
        self._bump("kept")
        return "kept"

    def drain_once(self, wait: float = 1.0) -> int:
        try:
            event = self.q.get(timeout=wait)
        except queue.Empty:
            return 0
        self.out(json.dumps({"severity": "INFO", "message": "pipewright online log", MARKER: event}))
        self._bump("written")
        return 1

    def run_worker(self) -> None:
        while True:
            self.drain_once()


def serve(r: Receiver, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({**r.stats, "queued": r.q.qsize(), "sample_rate": r.sample_rate,
                               "max_per_day": r.max_per_day}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            self.send_response(200)                    # first: a prediction never waits for the record
            self.send_header("Content-Length", "0")
            self.end_headers()
            from urllib.parse import parse_qsl, urlsplit

            r.take({k.lower(): v for k, v in self.headers.items()}, raw, dict(parse_qsl(urlsplit(self.path).query)))

        def log_message(self, *a):
            pass

    for _ in range(2):
        threading.Thread(target=r.run_worker, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


def main() -> None:
    serve(Receiver(float(os.environ.get("SAMPLE_RATE", "1")), int(os.environ.get("MAX_PER_DAY", "100000")),
                   int(os.environ.get("QUEUE", "10000"))), int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
