"""OpenTelemetry export from the launcher (FR-127), off by default.

Everything telemetry does lives in this package. Until `configure` finds `observability.otlp` in the manifest (or, on
an environment without a blueprint and without that block, the standard `OTEL_EXPORTER_OTLP_ENDPOINT`), every call
here returns at once and nothing is imported: no SDK, no cost. The SDK and the OTLP exporter come with the `otel`
extra (`pipewright-mlops[otel]`), which the launcher image installs.

The invariant (STABILITY.md): telemetry never fails a run, changes an outcome or a refusal, modifies evidence or
blocks the launcher. Every call below swallows its own errors; exports run in the background from bounded queues that
drop when full. The contract (names, types, units, attributes) is `pipewright.telemetry.catalogue`.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass

from pipewright.telemetry import catalogue

log = logging.getLogger("pipewright.telemetry")

EXPORT_TIMEOUT_MS = 10_000          # an export gives up after 10 s (decision 14)
SHUTDOWN_FLUSH_MS = 5_000           # Cloud Run gives 10 s after SIGTERM
SPAN_QUEUE = 2048
SPAN_BATCH = 512
SPAN_DELAY_MS = 5000
DIAGNOSTIC_EVERY_S = 60


@dataclass(frozen=True)
class Settings:
    endpoint: str
    headers: dict
    sample_ratio: float
    interval_s: int
    source: str                     # manifest | environment


class _State:
    def __init__(self):
        self.on = False
        self.tracer = None
        self.instruments: dict = {}
        self.providers: list = []
        self.usecases: set[str] = set()
        self.tenants: set[str] = set()
        self.series: set = set()
        self.over_budget = False
        self.failed_exports = 0
        self.last_diagnostic = 0.0
        self.cases = None               # a callable: {usecase: open cases}
        self.primed: set[str] = set()   # use cases whose counters were created at 0
        self.followed = None            # the manifest's otlp setting last seen (follow)
        self.lock = threading.Lock()


_s = _State()


# ---------------------------------------------------------------------------------------------- settings --

def _plain_allowed(endpoint: str) -> bool:
    """Plain http only to a sidecar on the same instance or a Collector inside the cluster or network."""
    from urllib.parse import urlparse

    host = (urlparse(endpoint).hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1") or host.endswith((".svc", ".svc.cluster.local", ".internal"))


def settings(manifest: dict, env: dict | None = None, secret=None) -> Settings | None:
    """What to export to, or None (off). Precedence (decision of 2026-11-18): the manifest's `observability.otlp`
    (only the platform writes it); otherwise `OTEL_EXPORTER_OTLP_ENDPOINT`, but only where the environment has no
    blueprint (Enterprise always exports to its gateway, set by Terraform, so nothing can bypass the approved
    egress). Raises ValueError on an invalid endpoint, so `validate` and `doctor` can say why."""
    env = os.environ if env is None else env
    obs = (manifest or {}).get("observability") or {}
    otlp = obs.get("otlp")
    if otlp is None and obs.get("otlp_endpoint"):
        otlp = {"endpoint": obs["otlp_endpoint"]}           # the key reserved since 1.0
    source = "manifest"
    if otlp is None:
        if (manifest or {}).get("blueprint") or not env.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
            return None
        otlp, source = {"endpoint": env["OTEL_EXPORTER_OTLP_ENDPOINT"]}, "environment"
    if otlp.get("enabled") is False:
        return None
    endpoint = str(otlp.get("endpoint") or "").rstrip("/")
    if not endpoint.startswith(("https://", "http://")):
        raise ValueError(f"observability.otlp.endpoint {endpoint!r}: an http(s) URL (OTLP over HTTP)")
    if endpoint.startswith("http://") and not _plain_allowed(endpoint):
        raise ValueError(f"observability.otlp.endpoint {endpoint}: a remote endpoint must use https; plain http only "
                         "to a sidecar on localhost or a Collector inside the cluster")
    headers = {}
    if otlp.get("headers_secret"):
        text = secret(otlp["headers_secret"]) if secret else ""
        from urllib.parse import unquote

        # the OpenTelemetry form (OTEL_EXPORTER_OTLP_HEADERS): key=value pairs, values URL-encoded, so a vendor's
        # "Authorization=Basic%20..." is sent as "Basic ..." (found in the phase 11 round with Grafana Cloud's value)
        for part in (text or "").strip().split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                headers[unquote(k.strip())] = unquote(v.strip())
    ratio = float(otlp.get("sample_ratio", 1.0))
    if not 0 <= ratio <= 1:
        raise ValueError("observability.otlp.sample_ratio: between 0 and 1")
    return Settings(endpoint, headers, ratio, int(otlp.get("interval_seconds", 60)), source)


def _secret(gcp):
    def read(name: str) -> str:
        import base64

        r = gcp.get(f"https://secretmanager.googleapis.com/v1/{name}:access")
        if not r.ok:
            raise RuntimeError(f"secret {name} not readable ({r.status})")
        return base64.b64decode(r.body["payload"]["data"]).decode()
    return read


# --------------------------------------------------------------------------------------------- set up --

def _otlp_key(manifest: dict) -> str:
    obs = (manifest or {}).get("observability") or {}
    return json.dumps([obs.get("otlp"), obs.get("otlp_endpoint")], sort_keys=True, default=str)


def follow(manifest: dict, version: str, gcp=None) -> bool:
    """The launcher re-reads its manifest every minute; telemetry follows it on (found in the phase 11 Enterprise
    round: the gateway and the manifest are written after the launcher's revision starts, so the launcher began
    without telemetry and, with min_instances above 0, never restarted). A setting that changes while telemetry is
    on is applied at the launcher's next restart, never by swapping exporters under live traffic. Never raises."""
    try:
        key = _otlp_key(manifest)
        if key == _s.followed:
            return False
        _s.followed = key
        if _s.on:
            log.warning("telemetry: observability.otlp changed; the launcher applies it at its next restart")
            return False
        return configure(manifest, version, gcp)
    except Exception:  # noqa: BLE001
        log.debug("telemetry: manifest not followed", exc_info=True)
        return False


def configure(manifest: dict, version: str, gcp=None, env: dict | None = None) -> bool:
    """Start exporting if the manifest says so. Never raises: a telemetry problem is logged, and the launcher runs."""
    try:
        _s.followed = _otlp_key(manifest)
    except Exception:  # noqa: BLE001
        pass
    try:
        cfg = settings(manifest, env, secret=_secret(gcp) if gcp else None)
    except Exception as e:  # noqa: BLE001 - a bad setting turns telemetry off, never the launcher
        log.warning("telemetry off: %s", e)
        return False
    if cfg is None:
        return False
    try:
        _start(cfg, manifest, version)
    except ImportError:
        log.warning("telemetry off: observability.otlp is set but the otel extra is not installed "
                    "(pip install 'pipewright-mlops[otel]')")
        return False
    except Exception as e:  # noqa: BLE001
        log.warning("telemetry off: %s", str(e).splitlines()[0][:200])
        return False
    _s.tenants = set((manifest.get("tenants") or {}).keys())
    log.info("telemetry: exporting to %s (%s), sample ratio %s", cfg.endpoint, cfg.source, cfg.sample_ratio)
    return True


def _start(cfg: Settings, manifest: dict, version: str, span_exporter=None, metric_reader=None) -> None:
    """span_exporter, metric_reader: in-memory ones in tests; the OTLP exporters otherwise."""
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    resource = Resource.create({"service.name": "pipewright-launcher", "service.version": version,
                                "deployment.environment": str(manifest.get("environment") or "")})
    spans = _Counting(span_exporter or OTLPSpanExporter(endpoint=f"{cfg.endpoint}/v1/traces", headers=cfg.headers,
                                                        timeout=EXPORT_TIMEOUT_MS / 1000))
    tp = TracerProvider(resource=resource, sampler=ParentBased(TraceIdRatioBased(cfg.sample_ratio)))
    tp.add_span_processor(BatchSpanProcessor(spans, max_queue_size=SPAN_QUEUE, max_export_batch_size=SPAN_BATCH,
                                             schedule_delay_millis=SPAN_DELAY_MS,
                                             export_timeout_millis=EXPORT_TIMEOUT_MS))
    metrics = _Counting(OTLPMetricExporter(endpoint=f"{cfg.endpoint}/v1/metrics", headers=cfg.headers,
                                           timeout=EXPORT_TIMEOUT_MS / 1000))
    reader = metric_reader or PeriodicExportingMetricReader(metrics, export_interval_millis=cfg.interval_s * 1000,
                                                            export_timeout_millis=EXPORT_TIMEOUT_MS)
    views = [View(instrument_name="pipewright.trigger.latency",
                  aggregation=ExplicitBucketHistogramAggregation(list(catalogue.LATENCY_BUCKETS_S)))]
    mp = MeterProvider(resource=resource, metric_readers=[reader], views=views)
    meter = mp.get_meter("pipewright.launcher", version)
    inst = {}
    for m in catalogue.METRICS:
        if m.kind == "counter":
            inst[m.name] = meter.create_counter(m.name, unit=m.unit, description=m.description)
        elif m.kind == "histogram":
            inst[m.name] = meter.create_histogram(m.name, unit=m.unit, description=m.description)
        elif m.kind == "gauge":
            inst[m.name] = meter.create_observable_gauge(m.name, callbacks=[_observe_cases], unit=m.unit,
                                                         description=m.description)
    _s.tracer = tp.get_tracer("pipewright.launcher", version)
    _s.instruments = inst
    _s.providers = [tp, mp]
    _s.on = True
    try:
        _prime_checks()
    except Exception:  # noqa: BLE001
        log.debug("telemetry: checks not primed", exc_info=True)
    import atexit

    atexit.register(shutdown)


class _Counting:
    """Wraps an exporter: counts failed exports and says so once a minute in the launcher's log, which keeps working
    when OTLP does not (self-observability, decision 14)."""

    def __init__(self, inner):
        self.inner = inner

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def export(self, batch, *a, **k):
        try:
            result = self.inner.export(batch, *a, **k)
        except Exception:  # noqa: BLE001 - an export that raises is a failed export, nothing more
            result = None
        if result is None or getattr(result, "name", "") not in ("SUCCESS",):
            _failed(len(batch) if hasattr(batch, "__len__") else 1)
        return result if result is not None else _failure_result(self.inner)


def _failure_result(inner):
    """The FAILURE value of the exporter's own result type (metric exporters carry a preferred temporality)."""
    if hasattr(inner, "_preferred_temporality"):
        from opentelemetry.sdk.metrics.export import MetricExportResult

        return MetricExportResult.FAILURE
    from opentelemetry.sdk.trace.export import SpanExportResult

    return SpanExportResult.FAILURE


def _failed(n: int) -> None:
    with _s.lock:
        _s.failed_exports += n
        now = time.monotonic()
        if now - _s.last_diagnostic < DIAGNOSTIC_EVERY_S:
            return
        _s.last_diagnostic, count = now, _s.failed_exports
    log.warning(json.dumps({"message": "telemetry export failing: runs are not affected",
                            "pipewright_telemetry": {"failed_items_so_far": count}}))


def shutdown(deadline_ms: int = SHUTDOWN_FLUSH_MS) -> None:
    """A bounded flush, then stop: everything, flush and the providers' own shutdown, within one deadline (Cloud Run
    allows 10 s after SIGTERM). Found in the phase 11 round: against a hung endpoint the providers' shutdown exported
    every queued batch with its own timeout, 85 s in all. It now runs in a daemon thread that is abandoned at the
    deadline, so a telemetry backend can never hold the launcher's exit."""
    providers, _s.providers, _s.on = list(_s.providers), [], False
    if not providers:
        return

    def stop():
        for p in providers:
            try:
                if hasattr(p, "force_flush"):
                    p.force_flush(deadline_ms)
                p.shutdown()
            except Exception:  # noqa: BLE001
                pass

    t = threading.Thread(target=stop, name="pipewright-telemetry-shutdown", daemon=True)
    t.start()
    t.join(deadline_ms / 1000)


def reset() -> None:
    """For tests: off, nothing kept."""
    global _s
    _s = _State()


# ------------------------------------------------------------------------------------------- metrics --

def know(usecase: str | None = None, tenant: str | None = None) -> None:
    """Names the launcher has seen in a live release are allowed values; anything else is `other`."""
    if usecase:
        _s.usecases.add(usecase)
    if tenant:
        _s.tenants.add(tenant)


PRIME_SHARE = 0.5                   # priming may fill at most half the series budget; real traffic keeps the rest


def _triggers_of(triggers: dict) -> list[tuple[str, str | None]]:
    """(trigger, pipeline or None) pairs a use case's live release can produce."""
    out = []
    if triggers.get("landing_prefix"):
        out.append(("landing", triggers.get("landing_pipeline") or "training"))
    for sched in (triggers.get("schedules") or {}).values():
        out.append(("schedule", (sched or {}).get("pipeline", "training")))
    if triggers.get("api"):
        out += [("api", None), ("request", None)]
    return list(dict.fromkeys(out))


def prime(usecase: str, triggers: dict | None) -> int:
    """Create the counters a use case can increment at 0 (FR-127, found in the dashboards' live test): a backend's
    increase() cannot see a new series' first sample, so without this the first refusal of each kind is lost.
    Only the triggers the live release configures, with every refusal reason; once per use case; never past
    PRIME_SHARE of the series budget. Returns the series created."""
    if not _s.on or not usecase or usecase in _s.primed:
        return 0
    try:
        _s.primed.add(usecase)
        know(usecase)
        pairs = _triggers_of(triggers or {})
        kinds = list(dict.fromkeys(t for t, _ in pairs))
        reasons = [r for r in catalogue.REASONS if r != "other"]
        wanted = [("pipewright.runs.started", {"usecase": usecase, "pipeline": p, "trigger": t}) for t, p in pairs if p]
        wanted += [("pipewright.runs.deduplicated", {"usecase": usecase, "trigger": t}) for t in kinds]
        wanted += [("pipewright.runs.refused", {"usecase": usecase, "trigger": t, "reason": r})
                   for t in kinds for r in reasons]
        with _s.lock:
            room = int(catalogue.SERIES_BUDGET * PRIME_SHARE) - len(_s.series)
        if len(wanted) > room:
            log.info("telemetry: %s not primed (%d series would pass the priming share of the budget)", usecase,
                     len(wanted))
            return 0
        for name, values in wanted:
            _s.instruments[name].add(0, _attrs(_metric(name), values))
        return len(wanted)
    except Exception:  # noqa: BLE001 - never fail the caller
        log.debug("telemetry: %s not primed", usecase, exc_info=True)
        return 0


def _prime_checks() -> None:
    m = _metric("pipewright.tick.failures")
    for check in catalogue.CHECKS:
        if check != "other":
            _s.instruments[m.name].add(0, _attrs(m, {"check": check}))


def _attrs(metric: catalogue.Metric, values: dict) -> dict:
    out = {}
    for a in metric.attributes:
        v = values.get(a)
        allowed = catalogue.METRIC_VALUES.get(a)
        known = _s.usecases if a == "usecase" else _s.tenants if a == "tenant" else set(allowed or ())
        out[a] = v if v in known else "other"
    key = (metric.name, tuple(sorted(out.items())))
    with _s.lock:
        if key not in _s.series:
            if len(_s.series) >= catalogue.SERIES_BUDGET:
                if not _s.over_budget:
                    _s.over_budget = True
                    log.warning("telemetry: %s active series reached; new combinations are recorded as other",
                                catalogue.SERIES_BUDGET)
                return {a: "other" for a in out}
            _s.series.add(key)
    return out


def _metric(name: str) -> catalogue.Metric:
    return next(m for m in catalogue.METRICS if m.name == name)


def count(name: str, **values) -> None:
    if not _s.on:
        return
    try:
        _s.instruments[name].add(1, _attrs(_metric(name), values))
    except Exception:  # noqa: BLE001 - never fail the caller
        log.debug("telemetry: %s not recorded", name, exc_info=True)


def trigger_latency(seconds: float, trigger: str, tenant: str) -> None:
    if not _s.on or seconds is None or seconds < 0:
        return
    try:
        m = _metric("pipewright.trigger.latency")
        _s.instruments[m.name].record(float(seconds), _attrs(m, {"trigger": trigger, "tenant": tenant}))
    except Exception:  # noqa: BLE001
        log.debug("telemetry: latency not recorded", exc_info=True)


def cases_source(fn) -> None:
    """A callable returning {usecase: open retrain cases}, read at each export."""
    _s.cases = fn


def _observe_cases(options=None):
    from opentelemetry.metrics import Observation

    if not _s.cases:
        return []
    try:
        found = _s.cases() or {}
    except Exception:  # noqa: BLE001 - a store that cannot be read gives no reading this time
        return []
    m = _metric("pipewright.cases.open")
    return [Observation(int(n), _attrs(m, {"usecase": uc})) for uc, n in found.items()]


# --------------------------------------------------------------------------------------------- traces --

def _span_attrs(values: dict) -> dict:
    out = {}
    for k, v in values.items():
        key = k if k.startswith(("pipewright.", "exception.")) else f"pipewright.{k}"
        if key in catalogue.SPAN_ATTRIBUTES and v is not None:
            out[key] = str(v)[:256]
    return out


class _NoSpan:
    def set(self, **values):
        return None

    def error(self, e: BaseException):
        return None


class _Span:
    def __init__(self, span):
        self.span = span

    def set(self, **values):
        try:
            self.span.set_attributes(_span_attrs(values))
        except Exception:  # noqa: BLE001
            pass

    def error(self, e: BaseException):
        """The error's type always; its message only when Pipewright wrote it (the value-free rule of FR-132)."""
        try:
            from opentelemetry.trace import Status, StatusCode

            values = {"exception.type": type(e).__name__}
            if getattr(e, "pipewright_value_free", False):
                values["exception.message"] = str(e).splitlines()[0][:256]
            self.span.set_attributes(_span_attrs(values))
            self.span.set_status(Status(StatusCode.ERROR))
        except Exception:  # noqa: BLE001
            pass


@contextlib.contextmanager
def span(name: str, incoming: str | None = None, **values):
    """A span around a stage of the launcher. `incoming`: a caller's traceparent, recorded as a link only: it never
    becomes the parent and never sets a team, use case or decision (decision 12)."""
    if not _s.on:
        yield _NoSpan()
        return
    try:
        links = []
        if incoming:
            ctx = _context_of(incoming)
            if ctx is not None:
                from opentelemetry.trace import Link, get_current_span

                sc = get_current_span(ctx).get_span_context()
                if sc.is_valid:
                    links = [Link(sc)]
        cm = _s.tracer.start_as_current_span(name, links=links, attributes=_span_attrs(values),
                                             record_exception=False, set_status_on_exception=False)
    except Exception:  # noqa: BLE001
        yield _NoSpan()
        return
    with cm as s:
        handle = _Span(s)
        try:
            yield handle
        except BaseException as e:
            handle.error(e)
            raise


def _context_of(traceparent: str):
    try:
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        return TraceContextTextMapPropagator().extract({"traceparent": traceparent})
    except Exception:  # noqa: BLE001 - invalid context: a new trace, no error
        return None


def traceparent() -> str | None:
    """The current span as a W3C traceparent (55 characters: it fits a run label), or None."""
    if not _s.on:
        return None
    try:
        from opentelemetry.trace import get_current_span

        sc = get_current_span().get_span_context()
        if not sc.is_valid:
            return None
        return f"00-{sc.trace_id:032x}-{sc.span_id:016x}-{int(sc.trace_flags):02x}"
    except Exception:  # noqa: BLE001
        return None


def label_of(tp: str | None) -> str | None:
    """A traceparent as a run label value (lowercase hex and dashes already; checked)."""
    import re

    return tp if tp and re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", tp) else None


def run_spans(tp: str, run: dict, steps: list[dict]) -> int:
    """After a run finishes: one span for the run and one per step, from the orchestrator's own start and end times,
    parented to the trigger that started it (decision 12). `run`: {start, end, state, usecase, pipeline, run_id,
    job_id}; `steps`: [{name, start, end, state}], times as datetimes. Returns the spans written."""
    if not _s.on or not label_of(tp):
        return 0
    try:
        from opentelemetry.trace import Status, StatusCode, set_span_in_context

        def ns(t):
            return int(t.timestamp() * 1e9)

        parent = _context_of(tp)
        r = _s.tracer.start_span("pipewright.run", context=parent, start_time=ns(run["start"]),
                                 attributes=_span_attrs({k: run.get(k) for k in
                                                         ("usecase", "pipeline", "run_id", "job_id", "state")}))
        written = 1
        rctx = set_span_in_context(r)
        for st in steps:
            if not st.get("start") or not st.get("end"):
                continue
            s = _s.tracer.start_span("pipewright.step", context=rctx, start_time=ns(st["start"]),
                                     attributes=_span_attrs({"step": st.get("name"), "state": st.get("state")}))
            if str(st.get("state", "")).upper() in ("FAILED", "ERROR"):
                s.set_status(Status(StatusCode.ERROR))
            s.end(end_time=ns(st["end"]))
            written += 1
        if str(run.get("state", "")).upper().endswith(("FAILED", "CANCELLED")):
            r.set_status(Status(StatusCode.ERROR))
        r.end(end_time=ns(run["end"]))
        return written
    except Exception:  # noqa: BLE001
        log.debug("telemetry: run spans not written", exc_info=True)
        return 0


def is_on() -> bool:
    return _s.on
