"""Model Armor screening for Pipewright Assist: the question, each text field a tool returns, and the answer.

Screening is a second layer. The first is structure: tool text is handed to the model as data, and the owner, the
rule and the advice come from fixed lists. Each field is screened on its own because, in the design's spike, Model
Armor missed an instruction planted inside a whole record and caught it in the field alone.

Off (no template): nothing is called, and every answer says it was not screened.
"""

from __future__ import annotations

import re

_TEMPLATE = re.compile(r"^projects/[^/]+/locations/([^/]+)/templates/[^/]+$")


class Screen:
    """screen(kind, text) -> None when the text passes or screening is off, else a short reason."""

    def __init__(self, template: str | None, gcp=None):
        self.template = template if template and _TEMPLATE.match(template) else None
        self._gcp = gcp
        self.calls = 0

    @property
    def on(self) -> bool:
        return self.template is not None

    def _url(self, verb: str) -> str:
        location = _TEMPLATE.match(self.template).group(1)
        host = "modelarmor.googleapis.com" if location == "global" else f"modelarmor.{location}.rep.googleapis.com"
        return f"https://{host}/v1/{self.template}:{verb}"

    def screen(self, kind: str, text: str) -> str | None:
        """kind: 'question' or 'tool' (screened as a prompt), 'answer' (screened as a model response)."""
        if not self.on or not text or not text.strip():
            return None
        if self._gcp is None:
            from pipewright.gcp.session import Gcp

            self._gcp = Gcp()
        self.calls += 1
        if kind == "answer":
            r = self._gcp.post(self._url("sanitizeModelResponse"), {"modelResponseData": {"text": text}})
        else:
            r = self._gcp.post(self._url("sanitizeUserPrompt"), {"userPromptData": {"text": text}})
        if not r.ok:
            # screening that cannot run is said, never silently skipped
            return f"screening could not run (Model Armor answered {r.status})"
        result = (r.body or {}).get("sanitizationResult") or {}
        if result.get("filterMatchState") != "MATCH_FOUND":
            return None
        matched = sorted(name for name, f in (result.get("filterResults") or {}).items() if _matched(f))
        return "Model Armor matched: " + (", ".join(matched) or "a filter")


def _matched(filter_result) -> bool:
    """A filter's result object says MATCH_FOUND somewhere in its own matchState."""
    if isinstance(filter_result, dict):
        return any((k == "matchState" and v == "MATCH_FOUND") or _matched(v) for k, v in filter_result.items())
    if isinstance(filter_result, list):
        return any(_matched(v) for v in filter_result)
    return False
