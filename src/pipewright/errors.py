"""Errors that collect every problem found, so users fix them in one pass."""

from __future__ import annotations

import re


class ConfigError(Exception):
    """One or more problems in Pipewright configuration."""

    def __init__(self, problems: list[str] | str, source: str | None = None):
        self.problems = [problems] if isinstance(problems, str) else list(problems)
        self.source = source
        super().__init__(self.__str__())

    def __str__(self) -> str:
        head = f"{self.source}: " if self.source else ""
        if len(self.problems) == 1:
            return head + self.problems[0]
        return head + f"{len(self.problems)} problems\n" + "\n".join(f"  - {p}" for p in self.problems)


# Value-free errors (NFR-18, D-65): a data engine's message can quote the rows it failed on ("Bad int64 value: abc",
# "Invalid date: '2026-13-01'"). Messages from data paths name the column and the rule, never the value: quoted
# literals and what follows "value:" are replaced; `backticked` identifiers stay. Kept in step with
# pipewright_runtime.value_free (a test checks), which runs standalone in component images.
_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_AFTER_VALUE = re.compile(r"(?i)\b(values?|got|found|literal)(\s*[:=]\s*)(?![`'\"])[^\s,;)\]]+")
HIDDEN = "..."


def value_free(text: str) -> str:
    """The message with data values taken out."""
    text = _QUOTED.sub(lambda m: m.group(0)[0] + HIDDEN + m.group(0)[0], str(text))
    return _AFTER_VALUE.sub(lambda m: m.group(1) + m.group(2) + HIDDEN, text)
