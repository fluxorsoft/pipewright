"""FR-127: OpenTelemetry export from the launcher, off by default (the founder's architecture review, 2026-11-18)."""
import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewright import telemetry
from pipewright.telemetry import catalogue

pytest.importorskip("opentelemetry.sdk")


@pytest.fixture
def otel():
    """Telemetry on, exporting to memory."""
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    telemetry.reset()
    spans, reader = InMemorySpanExporter(), InMemoryMetricReader()
    cfg = telemetry.Settings("http://localhost:4318", {}, 1.0, 60, "manifest")
    telemetry._start(cfg, {"environment": "test"}, "1.4.5", span_exporter=spans, metric_reader=reader)
    yield spans, reader
    telemetry.shutdown()
    telemetry.reset()


def points(reader, name):
    out = []
    data = reader.get_metrics_data()
    for rm in (data.resource_metrics if data else []):
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == name:
                    out += [(dict(p.attributes), getattr(p, "value", None) if hasattr(p, "value") else p.count)
                            for p in m.data.data_points]
    return out


def finished(spans):
    telemetry._s.providers[0].force_flush()
    return spans.get_finished_spans()


# ---------------------------------------------------------------------------------------------- off --

def test_off_without_settings_and_every_call_is_a_no_op():
    telemetry.reset()
    assert telemetry.settings({"observability": {}}, env={}) is None
    assert not telemetry.configure({"observability": {}}, "1.4.5", env={})
    telemetry.count("pipewright.runs.started", usecase="churn")          # nothing raised, nothing kept
    with telemetry.span("pipewright.run.start", usecase="churn") as s:
        s.set(outcome="submitted")
    assert telemetry.traceparent() is None and not telemetry.is_on()


# ------------------------------------------------------------------------------------------ settings --

def test_settings_precedence_and_validation():
    m = {"observability": {"otlp": {"endpoint": "https://otel.example.com", "sample_ratio": 0.5}}}
    s = telemetry.settings(m, env={"OTEL_EXPORTER_OTLP_ENDPOINT": "https://elsewhere.example.com"})
    assert s.endpoint == "https://otel.example.com" and s.source == "manifest" and s.sample_ratio == 0.5
    assert telemetry.settings({"observability": {"otlp_endpoint": "https://a.example.com"}}, env={}).endpoint == \
        "https://a.example.com"                                           # the key reserved since 1.0
    # the standard variables only where the manifest says nothing and there is no blueprint
    assert telemetry.settings({}, env={"OTEL_EXPORTER_OTLP_ENDPOINT": "https://b.example.com"}).source == "environment"
    assert telemetry.settings({"blueprint": "private-managed"},
                              env={"OTEL_EXPORTER_OTLP_ENDPOINT": "https://b.example.com"}) is None
    assert telemetry.settings({"observability": {"otlp": {"endpoint": "https://x.example.com", "enabled": False}}},
                              env={}) is None
    with pytest.raises(ValueError, match="must use https"):
        telemetry.settings({"observability": {"otlp": {"endpoint": "http://collector.example.com"}}}, env={})
    for ok in ("http://localhost:4318", "http://otel-gateway.observability.svc.cluster.local:4318"):
        assert telemetry.settings({"observability": {"otlp": {"endpoint": ok}}}, env={}).endpoint == ok
    with pytest.raises(ValueError, match="sample_ratio"):
        telemetry.settings({"observability": {"otlp": {"endpoint": "https://x.example.com", "sample_ratio": 2}}},
                           env={})


def test_headers_come_from_a_secret_never_the_manifest():
    m = {"observability": {"otlp": {"endpoint": "https://x.example.com",
                                    "headers_secret": "projects/p/secrets/otel/versions/1"}}}
    s = telemetry.settings(m, env={}, secret=lambda name: "Authorization=Bearer t0ken, X-Team=ml")
    assert s.headers == {"Authorization": "Bearer t0ken", "X-Team": "ml"}


def test_a_bad_setting_turns_telemetry_off_not_the_launcher():
    telemetry.reset()
    assert telemetry.configure({"observability": {"otlp": {"endpoint": "ftp://x"}}}, "1.4.5", env={}) is False


# ------------------------------------------------------------------------------------- the contract --

def test_the_contract_doc_is_current():
    doc = Path(__file__).resolve().parents[1] / "docs" / "TELEMETRY.md"
    assert doc.read_text(encoding="utf-8") == catalogue.render(), (
        "regenerate: python -m pipewright.telemetry.catalogue > docs/TELEMETRY.md")


def test_every_metric_is_named_typed_and_bounded():
    for m in catalogue.METRICS:
        assert m.name.startswith("pipewright.") and m.kind in ("counter", "histogram", "gauge") and m.unit
        assert all(a in catalogue.METRIC_VALUES for a in m.attributes)
        assert not {"run_id", "case_id", "release", "file", "user"} & set(m.attributes)
    latency = next(m for m in catalogue.METRICS if m.kind == "histogram")
    assert "usecase" not in latency.attributes                  # buckets multiply series (decision 11)


# --------------------------------------------------------------------------------------- metrics --

def test_counts_with_values_outside_their_lists_as_other(otel):
    _, reader = otel
    telemetry.know("churn", "retail")
    telemetry.count("pipewright.runs.started", usecase="churn", pipeline="training", trigger="landing")
    telemetry.count("pipewright.runs.started", usecase="planted-by-a-request", pipeline="training", trigger="x")
    telemetry.count("pipewright.runs.refused", usecase="churn", trigger="api", reason="'; DROP TABLE")
    got = points(reader, "pipewright.runs.started")
    assert ({"usecase": "churn", "pipeline": "training", "trigger": "landing"}, 1) in got
    assert ({"usecase": "other", "pipeline": "training", "trigger": "other"}, 1) in got
    assert points(reader, "pipewright.runs.refused")[0][0]["reason"] == "other"


def test_the_series_budget_holds(otel, monkeypatch):
    _, reader = otel
    monkeypatch.setattr(catalogue, "SERIES_BUDGET", 3)
    for i in range(10):
        telemetry.know(f"uc{i}", "retail")
        telemetry.count("pipewright.runs.deduplicated", usecase=f"uc{i}", trigger="api")
    got = points(reader, "pipewright.runs.deduplicated")
    assert len(got) <= 4 and any(a["usecase"] == "other" for a, _ in got)


def test_latency_has_the_contracts_buckets_and_no_use_case(otel):
    _, reader = otel
    telemetry.know("churn", "retail")
    telemetry.trigger_latency(42.0, "landing", "retail")
    data = reader.get_metrics_data()
    m = next(m for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics
             if m.name == "pipewright.trigger.latency")
    p = m.data.data_points[0]
    assert list(p.explicit_bounds) == list(catalogue.LATENCY_BUCKETS_S) and dict(p.attributes) == {
        "trigger": "landing", "tenant": "retail"}


# ---------------------------------------------------------------------------------------- spans --

def test_only_allowlisted_attributes_and_value_free_messages(otel):
    spans, _ = otel
    with telemetry.span("pipewright.run.start", usecase="churn", file_name="customers-of-ana@example.com.csv",
                        sql="SELECT * FROM t") as s:
        s.error(ValueError("row 17: ana@example.com"))
    with telemetry.span("pipewright.run.start", usecase="churn") as s:
        e = ValueError("no live release for churn")
        e.pipewright_value_free = True
        s.error(e)
    a, b = finished(spans)
    assert set(a.attributes) == {"pipewright.usecase", "exception.type"}             # no file name, no SQL
    assert "exception.message" not in a.attributes                                  # a message Pipewright did not write
    assert b.attributes["exception.message"] == "no live release for churn"


def test_a_callers_trace_is_a_link_never_the_parent(otel):
    spans, _ = otel
    incoming = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    with telemetry.span("pipewright.trigger", incoming=incoming, trigger="api"):
        tp = telemetry.traceparent()
    (s,) = finished(spans)
    assert s.parent is None and s.links[0].context.trace_id == int("a" * 32, 16)
    assert f"{s.context.trace_id:032x}" != "a" * 32 and telemetry.label_of(tp) == tp and len(tp) == 55
    with telemetry.span("pipewright.trigger", incoming="garbage"):                  # invalid: a new trace, no error
        pass


def test_run_and_step_spans_from_the_orchestrators_times(otel):
    spans, _ = otel
    with telemetry.span("pipewright.trigger", trigger="landing"):
        tp = telemetry.traceparent()
    t0 = datetime(2026, 11, 18, 10, 0, tzinfo=timezone.utc)
    n = telemetry.run_spans(tp, {"start": t0, "end": t0 + timedelta(minutes=9), "state": "PIPELINE_STATE_FAILED",
                                 "usecase": "churn", "pipeline": "training", "run_id": "r1"},
                            [{"name": "ingest-data", "start": t0, "end": t0 + timedelta(minutes=2),
                              "state": "SUCCEEDED"},
                             {"name": "train", "start": t0 + timedelta(minutes=2), "end": t0 + timedelta(minutes=9),
                              "state": "FAILED"}])
    assert n == 3
    done = finished(spans)
    trigger = next(s for s in done if s.name == "pipewright.trigger")
    rest = [s for s in done if s is not trigger]
    run = next(s for s in rest if s.name == "pipewright.run")
    steps = [s for s in rest if s.name == "pipewright.step"]
    assert run.parent.span_id == trigger.context.span_id and all(s.parent.span_id == run.context.span_id
                                                                   for s in steps)
    assert run.end_time - run.start_time == 9 * 60 * 10 ** 9 and not run.status.is_ok


# ---------------------------------------------------------------------------------- the launcher --

def _envelope(name="incoming/customers.csv"):
    data = {"bucket": "landing", "name": name, "generation": "7",
            "timeCreated": (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()}
    return {"message": {"messageId": "m1", "attributes": {"eventType": "OBJECT_FINALIZE", "bucketId": "landing",
                                                          "objectId": name, "objectGeneration": "7"},
                        "data": base64.b64encode(json.dumps(data).encode()).decode()}}


def test_the_launcher_measures_without_changing_anything(otel, monkeypatch):
    pytest.importorskip("flask")
    from pipewright import adapters
    from pipewright.adapters.vertex import AlreadySubmitted
    from pipewright.launcher.app import create_app
    from tests.test_launcher import CHURN, NIMBUS, FakeRegistry

    spans, reader = otel
    calls = []

    def fake_submit(contract, target, params, job_id, labels, security=None, manifest=None, gcp=None):
        if job_id in [c[0] for c in calls]:
            raise AlreadySubmitted(job_id)
        calls.append((job_id, labels))
        return "job"

    monkeypatch.setattr(adapters, "submit", fake_submit)
    client = create_app(FakeRegistry([CHURN, NIMBUS])).test_client()
    first = client.post("/pubsub", json=_envelope("incoming/nimbus/jan.csv"))
    again = client.post("/pubsub", json=_envelope("incoming/nimbus/jan.csv"))
    assert first.json["results"][0]["status"] == "submitted" and again.json["results"][0]["status"] == "duplicate"
    assert points(reader, "pipewright.runs.started") == [
        ({"usecase": "churn-nimbus", "pipeline": "training", "trigger": "landing"}, 1)]
    assert points(reader, "pipewright.runs.deduplicated") == [({"usecase": "churn-nimbus", "trigger": "landing"}, 1)]
    (lat,) = points(reader, "pipewright.trigger.latency")
    assert lat[0] == {"trigger": "landing", "tenant": "nimbus"}
    label = calls[0][1]["pipewright-trace"]                              # the run carries its trigger's trace
    names = [s.name for s in finished(spans)]
    assert names.count("pipewright.trigger") == 2 and names.count("pipewright.run.start") == 2
    trig = next(s for s in finished(spans) if s.name == "pipewright.trigger")
    assert label.split("-")[1] == f"{trig.context.trace_id:032x}" or label.split("-")[1] in {
        f"{s.context.trace_id:032x}" for s in finished(spans)}
    assert not any("jan.csv" in str(v) for s in finished(spans) for v in s.attributes.values())   # no file names


def test_a_broken_exporter_never_reaches_the_run(monkeypatch):
    """The invariant: an export that fails (the Collector is down) changes nothing for the caller."""
    from opentelemetry.sdk.trace.export import SpanExporter

    class Broken(SpanExporter):
        def export(self, spans):
            raise ConnectionError("collector down")

        def shutdown(self):
            pass

    telemetry.reset()
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    telemetry._start(telemetry.Settings("http://localhost:4318", {}, 1.0, 60, "manifest"), {}, "1.4.5",
                     span_exporter=Broken(), metric_reader=InMemoryMetricReader())
    with telemetry.span("pipewright.run.start", usecase="churn"):
        pass
    telemetry._s.providers[0].force_flush()          # the export fails inside; nothing raised here
    assert telemetry._s.failed_exports >= 1
    telemetry.shutdown()
    telemetry.reset()


def test_the_tick_writes_spans_for_traced_runs_only(otel):
    from pipewright.gcp.session import Response
    from pipewright.launcher import watch

    spans, _ = otel
    with telemetry.span("pipewright.trigger", trigger="landing"):
        tp = telemetry.traceparent()
    job = {"name": "projects/p/locations/r/pipelineJobs/churn-r1", "state": "PIPELINE_STATE_SUCCEEDED",
           "startTime": "2026-11-18T10:00:00Z", "endTime": "2026-11-18T10:05:00Z",
           "labels": {"pipewright-usecase": "churn", "pipewright-pipeline": "training", "run-id": "r1",
                      "pipewright-trace": tp}}
    untraced = {**job, "name": "projects/p/locations/r/pipelineJobs/x", "labels": {"pipewright-usecase": "x"}}
    detail = {**job, "jobDetail": {"taskDetails": [
        {"taskName": "churn-driver", "startTime": "2026-11-18T10:00:00Z", "endTime": "2026-11-18T10:00:05Z"},
        {"taskName": "ingest-data", "startTime": "2026-11-18T10:00:05Z", "endTime": "2026-11-18T10:02:00Z",
         "state": "SUCCEEDED"}]}}

    class G:
        def get(self, url, **params):
            return Response(200, {"pipelineJobs": [job, untraced]} if "filter" in params else detail)

    assert watch.trace_runs(G(), "p", "r", datetime(2026, 11, 18, tzinfo=timezone.utc)) == 2   # the run + 1 step
    names = sorted(s.name for s in finished(spans))
    assert names == ["pipewright.run", "pipewright.step", "pipewright.trigger"]
