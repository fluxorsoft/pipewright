"""The record of answers: every question Pipewright Assist was asked and what it answered.

Open source: a local file beside the tool-call log (.pipewright/assist-log.jsonl). It is for the person's own
transparency, not evidence (the person can edit it). Pipewright Enterprise keeps the record in the evidence dataset,
written by its hosted tool server.

The question is kept as typed. A typed question can contain personal data: `pipewright assist explain` says so.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

FILE = "assist-log.jsonl"


class Record:
    def __init__(self, root: Path):
        self.path = Path(root) / ".pipewright" / FILE

    def write(self, entry: dict, now: datetime.datetime | None = None) -> dict:
        now = now or datetime.datetime.now(datetime.timezone.utc)
        entry = {"time": now.isoformat(timespec="seconds"), **entry}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
        return entry

    def entries(self) -> list[dict]:
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out

    def asked_this_month(self, environment: str, team: str | None, now: datetime.datetime | None = None) -> int:
        """Questions that reached the model this calendar month, for one team in one environment."""
        month = (now or datetime.datetime.now(datetime.timezone.utc)).strftime("%Y-%m")
        return sum(1 for e in self.entries()
                   if str(e.get("time", "")).startswith(month) and e.get("environment") == environment
                   and e.get("team") == team and e.get("model_calls"))
