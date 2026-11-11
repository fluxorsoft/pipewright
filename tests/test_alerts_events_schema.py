"""Every event the schema accepts in monitoring.alerts.events is one alerts sync can email (GA QA:
model.registration_refused was refused by the schema though a team must hear of it). Decision and platform events
are added by sync itself, so they are not in the list."""
import json
from pathlib import Path

from pipewright.alerts import sync

SCHEMA = Path(__file__).parents[1] / "src/pipewright/config/schemas/usecase.schema.json"


def _enums(node, out):
    if isinstance(node, dict):
        if node.get("type") == "array" and isinstance(node.get("items"), dict) and "monitoring.alert" in (
                node["items"].get("enum") or []):
            out.append(set(node["items"]["enum"]))
        for v in node.values():
            _enums(v, out)
    elif isinstance(node, list):
        for v in node:
            _enums(v, out)
    return out


def test_schema_and_alerts_sync_agree():
    found = _enums(json.loads(SCHEMA.read_text(encoding="utf-8")), [])
    known = set(sync.EVENTS) | set(sync.SPLIT)
    assert found and all(e <= known for e in found)
    assert all("model.registration_refused" in e for e in found)
