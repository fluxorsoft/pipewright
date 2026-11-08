"""Spike FR-92/105/106 model: runs in the prebuilt PyTorch image (TorchServe, service_envelope=json).

An instance is either a feature list (8 numbers) -> {"score", "model"}, or {"probe": [targets]} -> the outcome of
reaching each target from inside the endpoint (the egress test). Counts and outcomes only, never values in logs.
"""
import json
import math
import os
import socket
import time
import urllib.error
import urllib.request

from ts.torch_handler.base_handler import BaseHandler

TAG = os.environ.get("MODEL_TAG", "v1")
FILE_TAG = "v3-file"
W = {"v1": [0.02, -0.01, 0.001, 0.3, -0.2, 0.1, 0.05, -0.004],
     "v3env": [0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "v2": [0.03, -0.02, 0.002, 0.2, -0.1, 0.2, 0.04, -0.003]}[TAG]


def _probe(t):
    start = time.perf_counter()
    try:
        if "tcp" in t:
            host, port = t["tcp"].rsplit(":", 1)
            socket.create_connection((host, int(port)), timeout=5).close()
            out = "connected"
        else:
            req = urllib.request.Request(t["url"], method="GET")
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    out = f"http {r.status}"
            except urllib.error.HTTPError as e:
                out = f"http {e.code}"
    except Exception as e:  # noqa: BLE001 - the spike records any failure
        out = f"{type(e).__name__}: {str(e)[:120]}"
    return {"name": t["name"], "outcome": out, "ms": round((time.perf_counter() - start) * 1000, 1)}


class Handler(BaseHandler):
    def initialize(self, context):
        self.initialized = True

    def handle(self, data, context):
        out = []
        for item in data:
            x = item.get("body", item) if isinstance(item, dict) and "body" in item else item
            if isinstance(x, (bytes, bytearray)):
                x = json.loads(x)
            if isinstance(x, dict) and "probe" in x:
                out.append({"model": TAG, "file": FILE_TAG, "probe": [_probe(t) for t in x["probe"]]})
                continue
            z = sum(w * float(v) for w, v in zip(W, x))
            out.append({"score": round(1 / (1 + math.exp(-z)), 6), "model": TAG, "file": FILE_TAG})
        return out
