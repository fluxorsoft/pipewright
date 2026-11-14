"""Errors that collect every problem found, so users fix them in one pass."""

from __future__ import annotations

import re


class ConfigError(Exception):
    """One or more problems in Pipewright configuration."""

    def __init__(self, problems: list[str] | str, source: str | None = None, hint: str | None = None):
        self.problems = [problems] if isinstance(problems, str) else list(problems)
        self.source = source
        self.hint = hint                     # how to fix it: said after the problems, never counted as one
        super().__init__(self.__str__())

    def __str__(self) -> str:
        head = f"{self.source}: " if self.source else ""
        fix = f"\n  fix: {self.hint}" if self.hint else ""
        if len(self.problems) == 1:
            return head + self.problems[0] + fix
        return head + f"{len(self.problems)} problems\n" + "\n".join(f"  - {p}" for p in self.problems) + fix


# Value-free errors (NFR-18, D-65): a data engine's message can quote the rows it failed on ("Bad int64 value: abc",
# "Invalid date: '2026-13-01'"). Messages from data paths name the column and the rule, never the value: quoted
# literals and what follows "value:" are replaced; `backticked` identifiers stay. Kept in step with
# pipewright_runtime.value_free (a test checks), which runs standalone in component images.
_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_AFTER_VALUE = re.compile(r"(?i)\b(values?|got|found|literal)(\s*[:=]\s*)(?![`'\"])[^\s,;)\]]+")
HIDDEN = "..."
# Python and pandas type names are not data (1.4.4): "unsupported operand type(s) for /: 'float' and 'str'" keeps them.
TYPE_NAMES = frozenset((
    "int", "float", "str", "bool", "bytes", "complex", "NoneType", "list", "dict", "tuple", "set", "frozenset",
    "object", "function", "module", "type", "datetime", "date", "time", "timedelta", "Decimal", "Timestamp",
    "Timedelta", "NaTType", "Series", "DataFrame", "Index", "ndarray", "int8", "int16", "int32", "int64", "uint8",
    "uint16", "uint32", "uint64", "float16", "float32", "float64", "bool_", "str_", "object_", "datetime64",
    "timedelta64", "category", "Int64", "Float64", "boolean", "string"))


def value_free(text: str) -> str:
    """The message with data values taken out."""
    def quoted(m):
        q = m.group(0)
        return q if q[1:-1] in TYPE_NAMES else q[0] + HIDDEN + q[0]

    return _AFTER_VALUE.sub(lambda m: m.group(1) + m.group(2) + HIDDEN, _QUOTED.sub(quoted, str(text)))
