"""The gateway as an A/B router (FR-57, phase B). Standard library only, like the server.

    mode ab:      each entity is assigned by the test's hash (the same as batch scoring and SQL); the control's gateway
                  scores control units, the treatment's scores treatment units; the answer says each unit's arm.
    mode shadow:  the control answers every call; a copy is scored by the treatment in the background and only a
                  comparison is logged (how many, the mean absolute difference, how often they agree); nothing acts on
                  the treatment's answers.
    router-only:  for use cases whose callers send features (D3): `instances` and `units` instead of entity IDs.

Exposures: one line per unit and call (`pipewright_ab_exposure`: test, arm, unit, the answering deployed model,
the release) written through the Cloud Logging API in batches, off the request path; the platform's log sink copies
them into the tenant's dataset (spike S4: none lost, all there within about five minutes). Where the platform allows
the feed, a second line (`pipewright_ab_feed`) carries a keyed hash of the unit instead of the unit.

The breaker: the treatment's last `window` calls; more than `max_error_rate` failed and the treatment's units go to
the control at once (this process), a `ab_breaker_tripped` line is logged, and the launcher's tick stops the test.
An answer from a model other than the arm's deployment is counted (`mismatched_model`) and logged.

Counts: once a minute each instance logs its totals since it started (`event: counts`: calls and instances sent to
each arm, unassigned units, mismatched answers, exposure lines dropped or not written). The launcher's tick reads
them: the share checks block a decision on any loss, and the treatment's calls are compared with what its endpoint
answered (Cloud Monitoring), which shows calls that bypassed the router.
"""

from __future__ import annotations

import collections
import hashlib
import hmac
import json
import os
import queue
import threading
import time
import urllib.request

SPACE = 2**56
LOG_API = "https://logging.googleapis.com/v2/entries:write"


def arm(name: str, unit, treatment_bp: int) -> str:
    """The same assignment as pipewright.abtest.assign (a test keeps them equal)."""
    if unit is None or unit == "":
        return "control"
    bucket = int.from_bytes(hashlib.sha256(f"{name}:{unit}".encode()).digest()[:7], "big")
    return "treatment" if bucket < int(treatment_bp) * SPACE // 10000 else "control"


class ExposureLog:
    """Batched log writes off the request path. write(entries) never blocks a call; a full queue drops and counts."""

    def __init__(self, project: str, token, labels: dict, opener=None, batch: int = 200, wait: float = 1.0,
                 capacity: int = 20000):
        self.project, self._token, self.labels = project, token, labels
        self._open = opener or urllib.request.urlopen
        self.batch, self.wait = batch, wait
        self.q: queue.Queue = queue.Queue(maxsize=capacity)
        self.dropped = self.written = self.failed = 0
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()

    def put(self, log_name: str, payload: dict) -> None:
        try:
            self.q.put_nowait((log_name, payload))
        except queue.Full:
            self.dropped += 1

    def _run(self):
        while True:
            items = [self.q.get()]
            deadline = time.time() + self.wait
            while len(items) < self.batch and time.time() < deadline:
                try:
                    items.append(self.q.get(timeout=max(deadline - time.time(), 0.01)))
                except queue.Empty:
                    break
            if items[0] is None:
                return
            by_log: dict[str, list] = collections.defaultdict(list)
            for item in items:
                if item is not None:
                    by_log[item[0]].append(item[1])
            for name, payloads in by_log.items():
                self._send(name, payloads)
            if any(i is None for i in items):
                return

    def _send(self, name: str, payloads: list[dict]) -> None:
        body = {"logName": f"projects/{self.project}/logs/{name}", "resource": {"type": "global"},
                "labels": self.labels, "entries": [{"jsonPayload": p, "severity": "INFO"} for p in payloads]}
        req = urllib.request.Request(LOG_API, method="POST", data=json.dumps(body).encode(),
                                     headers={"Authorization": "Bearer " + self._token(),
                                              "Content-Type": "application/json"})
        try:
            with self._open(req, timeout=30) as r:
                r.read()
            self.written += len(payloads)
        except Exception as e:  # noqa: BLE001 - counted and reported; a call is never failed for its log line
            self.failed += len(payloads)
            print(json.dumps({"severity": "ERROR", "message": "exposure log write failed", "lines": len(payloads),
                              "error": type(e).__name__}), flush=True)

    def close(self, timeout: float = 10):
        self.q.put(None)
        self._t.join(timeout)


class Breaker:
    def __init__(self, window: int = 200, max_error_rate: float = 0.05, minimum: int = 20):
        self.calls: collections.deque = collections.deque(maxlen=window)
        self.max_error_rate, self.minimum = max_error_rate, min(minimum, window)
        self.tripped_at: float | None = None
        self._lock = threading.Lock()

    def record(self, ok: bool) -> bool:
        """True when this call trips the breaker."""
        with self._lock:
            self.calls.append(ok)
            if self.tripped_at is None and len(self.calls) >= self.minimum:
                rate = 1 - sum(self.calls) / len(self.calls)
                if rate > self.max_error_rate:
                    self.tripped_at = time.time()
                    return True
            return False

    @property
    def error_rate(self) -> float:
        return (1 - sum(self.calls) / len(self.calls)) if self.calls else 0.0


class ABRouter:
    """Wraps the control's and the treatment's gateways. cfg (from the launcher, the running test):
    {name, mode, treatment_bp, state, release, usecase, tenant, project, treatment_deployed_model,
     breaker: {window, max_error_rate}, feed: {exposures, key}}"""

    def __init__(self, cfg: dict, control, treatment, log: ExposureLog | None, token=None, opener=None):
        self.cfg, self.control, self.treatment, self.log = cfg, control, treatment, log
        self._token, self._open = token, opener or urllib.request.urlopen
        b = cfg.get("breaker") or {}
        self.breaker = Breaker(int(b.get("window", 200)), float(b.get("max_error_rate", 0.05)))
        self.counts = collections.Counter()
        feed = cfg.get("feed") or {}
        key = feed.get("key") or (os.environ.get(feed["key_env"]) if feed.get("key_env") else None)
        if not key and feed.get("key_secret") and feed.get("exposures") and token is not None:
            key = self._secret(feed["key_secret"])          # on pods (KServe): read from Secret Manager at start
        self._feed_key = key.encode() if key and (cfg.get("feed") or {}).get("exposures") else None
        self.usecase = getattr(control, "usecase", cfg.get("usecase", ""))
        self.instance = os.environ.get("K_REVISION", "local") + "-" + os.urandom(4).hex()
        self._beat_every = float(cfg.get("counts_every", 60))
        if self.log is not None and self._beat_every > 0:
            threading.Thread(target=self._beats, daemon=True).start()
        follow = cfg.get("follow") or {}
        if follow.get("bucket") and self._token is not None:
            self.follow()                                   # the state now, then every `every` seconds
            threading.Thread(target=self._follows, args=(float(follow.get("every", 30)),), daemon=True).start()

    # ---------------------------------------------------------------------- following the test (KServe) --
    def follow(self) -> None:
        """The test's state as its record says (a small file the test writes on every change, in the tenant's
        online-sync prefix): a pause, a lower allocation or a stop takes effect here within `every` seconds, on
        every pod, without anyone changing the router. A file that is gone means the test ended: control only."""
        from urllib.parse import quote

        f = self.cfg["follow"]
        url = f"https://storage.googleapis.com/storage/v1/b/{f['bucket']}/o/{quote(f['object'], safe='')}?alt=media"
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + self._token()})
        try:
            with self._open(req, timeout=10) as r:
                state = json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print(json.dumps({"severity": "ERROR", "message": "ab state not read", "status": e.code}), flush=True)
                return
            state = {"state": "stopped", "treatment_bp": 0}
        except Exception as e:  # noqa: BLE001 - the last state stays until the next read
            print(json.dumps({"severity": "ERROR", "message": "ab state not read", "error": type(e).__name__}),
                  flush=True)
            return
        changed = {k: state.get(k) for k in ("state", "treatment_bp") if state.get(k) != self.cfg.get(k)}
        if changed:
            self.cfg.update(changed)
            print(json.dumps({"severity": "INFO", "message": "ab state", "ab_test": self.cfg["name"], **changed}),
                  flush=True)

    def _follows(self, every: float):
        while True:
            time.sleep(every)
            self.follow()

    def _secret(self, name: str) -> str | None:
        import base64

        try:
            req = urllib.request.Request(f"https://secretmanager.googleapis.com/v1/{name}/versions/latest:access",
                                         headers={"Authorization": "Bearer " + self._token()})
            with self._open(req, timeout=10) as r:
                return base64.b64decode(json.loads(r.read())["payload"]["data"]).decode()
        except Exception as e:  # noqa: BLE001 - without the key, no feed lines; exposures still go to the dataset
            print(json.dumps({"severity": "ERROR", "message": "the feed key was not read: no feed exposures",
                              "error": type(e).__name__}), flush=True)
            return None

    def _beats(self):
        while True:
            time.sleep(self._beat_every)
            self.beat()

    def beat(self) -> None:
        """The instance's totals since it started, one line (the tick takes differences between lines)."""
        if self.log is None:
            return
        self.log.put("pipewright_ab_exposure", {
            "ab_test": self.cfg["name"], "event": "counts", "usecase": self.usecase, "instance": self.instance,
            **{k: self.counts.get(k, 0) for k in ("control_calls", "treatment_calls", "control_instances",
                                                  "treatment_instances", "control_errors", "treatment_errors",
                                                  "control_ms", "treatment_ms", "unassigned", "mismatched_model")},
            "dropped": getattr(self.log, "dropped", 0) + getattr(self.log, "failed", 0),
            "breaker_tripped": self.breaker.tripped_at is not None, "at": time.time()})

    # --------------------------------------------------------------------------------------------- assignment --
    def _bp(self) -> int:
        if self.cfg.get("state") != "running" or self.breaker.tripped_at is not None:
            return 0
        return int(self.cfg.get("treatment_bp", 0))

    def split(self, units: list[str]) -> dict[str, str]:
        bp = self._bp() if self.cfg.get("mode", "ab") == "ab" else 0
        return {u: arm(self.cfg["name"], u, bp) for u in units}

    def _exposures(self, arms: dict[str, str], deployed: dict[str, str | None]) -> None:
        now = time.time()
        for unit, a in arms.items():
            if not unit or unit == "None":
                self.counts["unassigned"] += 1
                continue
            if self.log is None:
                continue
            self.log.put("pipewright_ab_exposure", {
                "ab_test": self.cfg["name"], "arm": a, "unit": unit, "deployed_model": deployed.get(a),
                "usecase": self.usecase, "release": self.cfg.get("release"), "at": now})
            if self._feed_key:
                ref = hmac.new(self._feed_key, unit.encode(), hashlib.sha256).hexdigest()[:32]
                self.log.put("pipewright_ab_feed", {"ab_test": self.cfg["name"], "arm": a, "unit_ref": ref,
                                                    "usecase": self.usecase, "tenant": self.cfg.get("tenant"),
                                                    "at": now})

    def _call(self, a: str, fn, payload):
        """An arm's answer, counted (calls, instances, errors, time); the treatment's also by the breaker. A failure
        is raised so the caller gets the error (units in the treatment are not silently scored by the control)."""
        self.counts[f"{a}_calls"] += 1
        self.counts[f"{a}_instances"] += len(payload)
        t = time.perf_counter()
        try:
            out = fn(payload)
        except Exception:
            self.counts[f"{a}_errors"] += 1
            if a == "treatment" and self.breaker.record(False):
                self._tripped()
            raise
        finally:
            self.counts[f"{a}_ms"] += int((time.perf_counter() - t) * 1000)
        if a == "treatment" and self.breaker.record(True):
            self._tripped()
        return out

    def _treatment_call(self, fn, payload):
        return self._call("treatment", fn, payload)

    def _tripped(self):
        print(json.dumps({"severity": "WARNING", "message": "ab_breaker_tripped", "ab_test": self.cfg["name"],
                          "usecase": self.usecase, "error_rate": round(self.breaker.error_rate, 4),
                          "window": len(self.breaker.calls)}), flush=True)
        if self.log is not None:
            self.log.put("pipewright_ab_exposure", {"ab_test": self.cfg["name"], "event": "breaker_tripped",
                                                    "usecase": self.usecase,
                                                    "error_rate": round(self.breaker.error_rate, 4),
                                                    "at": time.time()})

    def _check_model(self, a: str, deployed: str | None) -> None:
        expected = self.cfg.get("treatment_deployed_model") if a == "treatment" else None
        if a == "treatment" and expected and deployed and str(deployed) != str(expected):
            self.counts["mismatched_model"] += 1
            print(json.dumps({"severity": "WARNING", "message": "ab_mismatched_model", "ab_test": self.cfg["name"],
                              "expected": expected, "answered": deployed}), flush=True)

    # ---------------------------------------------------------------------------------- entity IDs (gateway) --
    def predict(self, ids: list[str]) -> tuple[dict, dict]:
        if self.cfg.get("mode") == "shadow":
            return self._shadow(ids)
        arms = self.split(ids)
        groups = {a: [i for i in ids if arms[i] == a] for a in ("control", "treatment")}
        body = {"predictions": {}, "not_found": [], "arms": {}, "ab_test": self.cfg["name"],
                "deployed_model": {}, "model_version": {}}
        fields = {"asked": len(ids), "found": 0, "attempts": 0, "control": len(groups["control"]),
                  "treatment": len(groups["treatment"]), "ab_test": self.cfg["name"]}
        deployed = {}
        for a in ("control", "treatment"):
            if not groups[a]:
                continue
            g = self.control if a == "control" else self.treatment
            out, f = self._call(a, g.predict, groups[a])
            body["predictions"].update(out["predictions"])
            body["not_found"] += out["not_found"]
            body["deployed_model"][a], body["model_version"][a] = out.get("deployed_model"), out.get("model_version")
            deployed[a] = out.get("deployed_model")
            self._check_model(a, deployed[a])
            for i in out["predictions"]:
                body["arms"][i] = a
            fields["found"] += f["found"]
            fields["attempts"] += f["attempts"]
        self._exposures({i: arms[i] for i in body["arms"]}, deployed)
        return body, fields

    def _shadow(self, ids: list[str]) -> tuple[dict, dict]:
        body, fields = self.control.predict(ids)
        threading.Thread(target=self._shadow_copy, args=(list(ids), dict(body.get("predictions") or {})),
                         daemon=True).start()
        return body, {**fields, "ab_test": self.cfg["name"], "shadow": True}

    def _shadow_copy(self, ids: list[str], control: dict) -> None:
        t = time.perf_counter()
        try:
            (out, _), ok, err = self._treatment_call(self.treatment.predict, ids), True, None
        except Exception as e:  # noqa: BLE001 - a shadow failure is measured, never seen by the caller
            out, ok, err = {"predictions": {}}, False, type(e).__name__
        self._shadow_line(control, out.get("predictions") or {}, ok, err, t)

    # ---------------------------------------------------------------------------- router-only (request mode) --
    def predict_instances(self, instances: list, units: list[str]) -> tuple[dict, dict]:
        """Callers send features (D3): the instances and the unit of each, in order."""
        if len(units) != len(instances):
            raise ValueError("units: one per instance")
        if self.cfg.get("mode") == "shadow":
            out = self.control.call(instances)
            threading.Thread(target=self._shadow_instances, args=(instances, units, out), daemon=True).start()
            return {**out, "ab_test": self.cfg["name"]}, {"asked": len(units), "shadow": True}
        arms = self.split(units)
        preds: list = [None] * len(units)
        deployed = {}
        for a in ("control", "treatment"):
            idx = [k for k, u in enumerate(units) if arms[u] == a]
            if not idx:
                continue
            g = self.control if a == "control" else self.treatment
            batch = [instances[k] for k in idx]
            out = self._call(a, g.call, batch)
            deployed[a] = out.get("deployedModelId")
            self._check_model(a, deployed[a])
            for k, p in zip(idx, out.get("predictions") or [], strict=False):
                preds[k] = p
        self._exposures({u: arms[u] for u in units}, deployed)
        return ({"predictions": preds, "arms": [arms[u] for u in units], "ab_test": self.cfg["name"],
                 "deployed_model": deployed},
                {"asked": len(units), "control": sum(a == "control" for a in arms.values()),
                 "treatment": sum(a == "treatment" for a in arms.values()), "ab_test": self.cfg["name"]})

    def _shadow_instances(self, instances, units, control_out):
        t = time.perf_counter()
        try:
            out, ok, err = self._treatment_call(self.treatment.call, instances), True, None
        except Exception as e:  # noqa: BLE001 - a shadow failure is measured, never seen by the caller
            out, ok, err = {"predictions": []}, False, type(e).__name__
        cmap = dict(zip(units, control_out.get("predictions") or [], strict=False))
        tmap = dict(zip(units, out.get("predictions") or [], strict=False))
        self._shadow_line(cmap, tmap, ok, err, t)

    def _shadow_line(self, control: dict, treatment: dict, ok: bool, err: str | None, started: float) -> None:
        """One comparison line per shadowed call: how many, the mean absolute difference, how often they agree."""
        diffs, agree = [], 0
        for u, v in treatment.items():
            try:
                a_, b_ = _score(control.get(u)), _score(v)
            except (TypeError, ValueError):
                continue
            diffs.append(abs(a_ - b_))
            agree += (a_ >= 0.5) == (b_ >= 0.5)
        if self.log is not None:
            self.log.put("pipewright_ab_exposure", {
                "ab_test": self.cfg["name"], "event": "shadow", "usecase": self.usecase, "ok": ok,
                "n": len(diffs), "mean_abs_diff": round(sum(diffs) / len(diffs), 6) if diffs else None,
                "agreement": round(agree / len(diffs), 6) if diffs else None,
                "ms": round((time.perf_counter() - started) * 1000, 1), **({} if ok else {"error": err}),
                "at": time.time()})


def _score(v) -> float:
    """A prediction as one number: a probability, a score, or the positive class's share from a list or dict."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, list) and v and isinstance(v[-1], (int, float)):
        return float(v[-1])
    if isinstance(v, dict):
        for k in ("score", "probability", "value", "scores"):
            if k in v:
                return _score(v[k])
    raise ValueError("not a number")
