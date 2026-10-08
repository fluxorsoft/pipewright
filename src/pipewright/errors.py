"""Errors that collect every problem found, so users fix them in one pass."""

from __future__ import annotations


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
