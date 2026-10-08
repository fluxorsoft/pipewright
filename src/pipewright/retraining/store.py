"""Cases in the artifacts bucket. The launcher is the only writer; readers are `pipewright retrain` and evidence.

gs://<artifacts>/cases/<usecase>/<case_id>.json   one case (rewritten as it moves)
gs://<artifacts>/cases/<usecase>/current.json     {"open": <case_id> | null, "last_case", "last_approved_at"}

Writes use GCS generation preconditions; `update` re-reads and re-applies on a lost race, so two launcher
instances handling messages for the same use case cannot overwrite each other.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from pipewright.gcp import Gcp, PreconditionFailed, gcs_list, gcs_read, gcs_write

CASES_PREFIX = "cases"
ATTEMPTS = 5


def case_name(usecase: str, case_id: str) -> str:
    return f"{CASES_PREFIX}/{usecase}/{case_id}.json"


def pointer_name(usecase: str) -> str:
    return f"{CASES_PREFIX}/{usecase}/current.json"


class CaseStore:
    def __init__(self, gcp: Gcp, bucket: str):
        self.gcp, self.bucket = gcp, bucket

    def _read(self, name: str) -> tuple[dict | None, int]:
        text, generation = gcs_read(self.gcp, self.bucket, name)
        return (json.loads(text) if text else None), generation

    def pointer(self, usecase: str) -> tuple[dict, int]:
        p, generation = self._read(pointer_name(usecase))
        return p or {"open": None, "last_case": None, "last_approved_at": None}, generation

    def get(self, usecase: str, case_id: str) -> dict | None:
        return self._read(case_name(usecase, case_id))[0]

    def current(self, usecase: str) -> dict | None:
        p, _ = self.pointer(usecase)
        return self.get(usecase, p["open"]) if p.get("open") else None

    def list(self, usecase: str) -> list[dict]:
        names = [n for n in gcs_list(self.gcp, self.bucket, f"{CASES_PREFIX}/{usecase}/")
                 if n.endswith(".json") and not n.endswith("/current.json")]
        cases = [c for c in (self._read(n)[0] for n in names) if c]
        return sorted(cases, key=lambda c: c.get("created_at") or "", reverse=True)

    def usecases(self) -> list[str]:
        return sorted({n.split("/")[1] for n in gcs_list(self.gcp, self.bucket, f"{CASES_PREFIX}/")
                       if n.count("/") >= 2})

    def write(self, case: dict, generation: int | None) -> int:
        return gcs_write(self.gcp, self.bucket, case_name(case["usecase"], case["case_id"]),
                         json.dumps(case, indent=2, sort_keys=True), if_generation_match=generation)

    def update(self, usecase: str, change: Callable[[dict | None, dict], tuple[dict | None, dict | None]]):
        """change(current_case, pointer) -> (case to write or None, new pointer or None). Retried on a lost race;
        returns the last change's result."""
        last: Exception | None = None
        for _ in range(ATTEMPTS):
            pointer, pointer_gen = self.pointer(usecase)
            current, case_gen = (None, 0)
            if pointer.get("open"):
                current, case_gen = self._read(case_name(usecase, pointer["open"]))
            case, new_pointer = change(current, dict(pointer))
            try:
                if case is not None:
                    same = current is not None and case["case_id"] == current["case_id"]
                    self.write(case, case_gen if same else 0)
                if new_pointer is not None and new_pointer != pointer:
                    gcs_write(self.gcp, self.bucket, pointer_name(usecase), json.dumps(new_pointer, indent=2),
                              if_generation_match=pointer_gen)
                return case, new_pointer
            except PreconditionFailed as e:
                last = e
        raise last or RuntimeError("case update failed")

    def update_case(self, usecase: str, case_id: str, change: Callable[[dict], dict | None]) -> dict | None:
        """change(case) -> case to write or None, for a case that may not be current (decisions on old cases)."""
        last: Exception | None = None
        for _ in range(ATTEMPTS):
            case, generation = self._read(case_name(usecase, case_id))
            if case is None:
                return None
            new = change(case)
            if new is None:
                return case
            try:
                self.write(new, generation)
                return new
            except PreconditionFailed as e:
                last = e
        raise last or RuntimeError("case update failed")
