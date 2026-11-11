"""The event catalogue lists every event the code sends (Pipewright 1.4.3): a new event without a catalogue entry
fails here, so the docs, `pipewright events` and the personas' subscription filters never drift."""

import json
import re
from pathlib import Path

from pipewright.alerts import sync
from pipewright.observability import catalogue, events

ROOT = Path(__file__).resolve().parent.parent
SOURCES = [p for d in ("src/pipewright", "examples") for p in (ROOT / d).rglob("*.py")]

# A literal event type passed to an emitter: events.event("x.y", ...), _event("x.y", ...), emit_event("x.y", ...),
# service.emit(env, uc, gcp, "x.y", ...), or a (type, contract, data) tuple a watch returns.
CALLS = [
    re.compile(r"\b(?:events\.event|_event|emit_event|self\._event)\(\s*\"([a-z_]+\.[a-z_.-]+)\""),
    re.compile(r"\bemit\((?:[^()\"]*,\s*)?\"([a-z_]+\.[a-z_.-]+)\""),
    re.compile(r"\(\"([a-z_]+\.[a-z_.-]+)\",\s*c\b"),
    re.compile(r"\"(model\.lock\.(?:renewed|failed))\""),
    re.compile(r"\{\"paused\": \"(ab_test\.paused)\", \"running\": \"(ab_test\.resumed)\""),
]


def sent() -> set[str]:
    found = set()
    for path in SOURCES:
        text = path.read_text(encoding="utf-8")
        for pattern in CALLS:
            for m in pattern.finditer(text):
                found.update(g for g in m.groups() if g)
    return found


def test_every_event_sent_is_in_the_catalogue():
    missing = sorted(sent() - set(catalogue.BY_TYPE) - set(catalogue.REQUEST_KINDS))
    assert not missing, f"add these to observability/catalogue.py: {missing}"


def test_scan_finds_the_events():
    # the scan itself works: it sees events from the launcher, the CLI, the steps and the A/B service
    assert {"run.submitted", "release.live", "training.evaluated", "ab_test.started", "model.lock.renewed"} <= sent()


def test_dynamic_families_are_listed():
    from pipewright.loop import driver
    from pipewright.retraining import cases

    text = Path(driver.__file__).read_text(encoding="utf-8")
    states = set(re.findall(r"_step\(\s*\"([a-z-]+)\"", text)) | {"paused", "resumed"}
    states |= {"validation-failed"} if "validation-failed" in text else set()
    assert {f"loop.{s}" for s in states} <= set(catalogue.BY_TYPE)
    assert {"retrain.declined", "retrain.snoozed"} <= set(catalogue.BY_TYPE)
    assert cases.ACTIONS                                     # the retrain states come from these actions


def test_every_emailable_and_decision_event_exists():
    alias = {"training.rejected"}                            # an email filter on training.evaluated
    assert set(sync.EVENTS) - alias <= set(catalogue.BY_TYPE)
    assert set(events.DECISIONS) <= set(catalogue.BY_TYPE)


def test_entries_are_well_formed():
    assert len(catalogue.BY_TYPE) == len(catalogue.EVENTS)
    for e in catalogue.EVENTS:
        assert e.family in catalogue.FAMILIES, e.type
        assert e.personas and set(e.personas) <= set(catalogue.PERSONAS), e.type
        assert "—" not in e.when and e.when.endswith("."), e.type


def test_persona_filters_are_short():
    for persona in catalogue.PERSONAS:
        assert len(catalogue.persona_filter(persona).encode()) < 256


def test_family_filter():
    f = catalogue.subscription_filter(catalogue.select(family="ab_test"))
    assert f == 'hasPrefix(attributes.event_type, "ab_test.")'
    one = catalogue.subscription_filter([catalogue.BY_TYPE["run.failed"]])
    assert one == 'attributes.event_type = "run.failed"'


def test_persona_attributes():
    assert catalogue.attributes("run.failed") == {"pw_ml": "1", "pw_sre": "1"}
    assert catalogue.attributes("no.such") == {}


def test_the_runtime_knows_the_step_events_personas():
    import importlib.util

    spec = importlib.util.spec_from_file_location("rt", ROOT / "src/pipewright/build/templates/pipewright_runtime.py")
    rt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rt)
    want = {e.type: tuple(sorted(e.personas)) for e in catalogue.EVENTS if e.source == "pipeline step"}
    assert {k: tuple(sorted(v)) for k, v in rt.EVENT_PERSONAS.items()} == want


def test_published_messages_carry_persona_attributes():
    sent_bodies = []

    class G:
        def post(self, url, body):
            sent_bodies.append(body)

            class R:
                ok, status = True, 200
            return R()

    e = events.event("run.failed", "uc", "test", "t", None, None)
    import io
    assert events.emit(e, "projects/p/topics/events", G(), stream=io.StringIO())
    attrs = sent_bodies[0]["messages"][0]["attributes"]
    assert attrs["event_type"] == "run.failed" and attrs["pw_sre"] == "1" and attrs["pw_ml"] == "1"
    assert "pw_governance" not in attrs


def test_events_doc_is_current():
    doc = (ROOT / "docs" / "EVENTS.md").read_text(encoding="utf-8")
    assert doc == catalogue.markdown(), "regenerate: python -m pipewright.observability.catalogue > docs/EVENTS.md"


def test_concurrent_events_stay_one_json_line_each():
    """1.4.3 battle test: two launcher threads emitting at once interleaved their lines (kept as text, not JSON)."""
    import io
    import threading

    from pipewright.observability import events

    out = io.StringIO()
    threads = [threading.Thread(target=lambda i=i: [events.emit(events.event("serving.recovered", "u", "e",
                                                                                 n=i, pad="x" * 5000), stream=out)
                                                    for _ in range(20)]) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lines = out.getvalue().splitlines()
    assert len(lines) == 160 and all(json.loads(line)["pipewright_event"]["event_type"] == "serving.recovered"
                                     for line in lines)


def test_the_brief_has_only_what_the_event_has_and_both_copies_agree():
    """1.4.3 battle test: emails printed (null) for fields an event does not have."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("rt2", ROOT / "src/pipewright/build/templates/pipewright_runtime.py")
    rt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rt)
    data = {"signal": "errors", "deployed_model": None, "threshold": 0.05, "top_movers": [{"feature": "a"}],
            "drifted": ["a", "b"], "shares": {"a": 1}}
    text = events.brief(data, None, "r1")
    assert text == "signal: errors\nthreshold: 0.05\ndrifted: a, b\nrelease: r1"
    assert rt.brief(data, None, "r1") == text
    assert "brief" in events.event("serving.recovered", "u", "e", signal="errors")["data"]
