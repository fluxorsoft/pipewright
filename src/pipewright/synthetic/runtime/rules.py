"""Column rules and contract-only generation: rows from the data contract alone, no real data, no ML.

Standalone (no Pipewright imports, standard library only): the CLI uses it for development and CI data, the synthetic
image applies the same rules to a learned engine's output (new keys, never real ones; personal-data columns never
learned).

Rules (synthetic.columns.<name>):
    {rule: key, prefix: "SYN-"}                         new unique keys (strings with the prefix, or 1, 2, 3...)
    {rule: fake, provider: email|name|phone|uuid|text}  made-up personal data
    {rule: constant, value: ...}
    {rule: drop}                                        always null
    {rule: time, from: 2025-01-31, to: 2025-12-31, step: day|month_end}
    {rule: values, values: [...], weights: [...]}       a categorical with chosen frequencies
    {rule: range, min: 0, max: 10}                      uniform within the bounds
"""

from __future__ import annotations

import calendar
import random
import uuid
from datetime import date, datetime, timedelta

RULES = ("key", "fake", "constant", "drop", "time", "values", "range")
PROVIDERS = ("email", "name", "phone", "uuid", "text")
_FIRST = ("Alex", "Sam", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Jamie", "Avery", "Quinn", "Robin", "Drew")
_LAST = ("Smith", "Jones", "Patel", "Garcia", "Chen", "Nguyen", "Kumar", "Brown", "Silva", "Novak", "Okafor", "Kim")
_WORDS = ("alpha", "bravo", "delta", "echo", "lima", "nova", "orbit", "pixel", "quartz", "sierra", "tango", "zephyr")


def problems_for(name: str, rule: dict, column: dict | None) -> list[str]:
    """What is wrong with one column rule (column: the contract's definition, or None if not in the contract)."""
    where = f"synthetic.columns.{name}"
    kind = rule.get("rule")
    out = []
    if column is None:
        out.append(f"{where}: column {name!r} is not in data.contract.columns")
    if kind not in RULES:
        return [*out, f"{where}: rule {kind!r} is not one of {', '.join(RULES)}"]
    ctype = (column or {}).get("type")
    if kind == "fake" and rule.get("provider") not in PROVIDERS:
        out.append(f"{where}: provider must be one of {', '.join(PROVIDERS)}")
    if kind == "fake" and ctype and ctype != "string":
        out.append(f"{where}: fake values are strings; the column is {ctype}")
    if kind == "constant" and "value" not in rule:
        out.append(f"{where}: give the value")
    if kind == "time":
        if ctype and ctype not in ("date", "timestamp"):
            out.append(f"{where}: rule time needs a date or timestamp column")
        try:
            if _date(rule["from"]) > _date(rule["to"]):
                out.append(f"{where}: from is after to")
        except (KeyError, ValueError, TypeError):
            out.append(f"{where}: give from and to as YYYY-MM-DD")
        if rule.get("step", "day") not in ("day", "month_end"):
            out.append(f"{where}: step must be day or month_end")
    if kind == "values":
        values, weights = rule.get("values") or [], rule.get("weights")
        if not values:
            out.append(f"{where}: give values")
        if weights is not None and (len(weights) != len(values) or any(w < 0 for w in weights) or not sum(weights)):
            out.append(f"{where}: weights must be one non-negative number per value, not all zero")
    if kind == "range":
        if "min" not in rule or "max" not in rule or rule["min"] > rule["max"]:
            out.append(f"{where}: needs min <= max")
        elif ctype and ctype not in ("integer", "float"):
            out.append(f"{where}: rule range needs an integer or float column")
    if kind == "key" and ctype and ctype not in ("string", "integer"):
        out.append(f"{where}: keys are strings or integers; the column is {ctype}")
    return out


def _date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _month_ends(start: date, end: date) -> list[date]:
    out, y, m = [], start.year, start.month
    while True:
        d = date(y, m, calendar.monthrange(y, m)[1])
        if d > end:
            return out
        if d >= start:
            out.append(d)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def column_values(name: str, rule: dict, column: dict, n: int, rng: random.Random, start: int = 1) -> list:
    """n values of one column under one rule. `start` numbers keys (shards use different starts)."""
    kind, ctype = rule["rule"], column.get("type", "string")
    if kind == "key":
        if ctype == "integer":
            return list(range(start, start + n))
        prefix = rule.get("prefix", "SYN-")
        return [f"{prefix}{i:08d}" for i in range(start, start + n)]
    if kind == "drop":
        return [None] * n
    if kind == "constant":
        return [rule["value"]] * n
    if kind == "fake":
        return [_fake(rule["provider"], rng) for _ in range(n)]
    if kind == "values":
        return rng.choices(rule["values"], weights=rule.get("weights"), k=n)
    if kind == "range":
        if ctype == "integer":
            return [rng.randint(int(rule["min"]), int(rule["max"])) for _ in range(n)]
        return [round(rng.uniform(rule["min"], rule["max"]), 4) for _ in range(n)]
    # time
    lo, hi = _date(rule["from"]), _date(rule["to"])
    if rule.get("step") == "month_end":
        grid = _month_ends(lo, hi) or [hi]
        days = [rng.choice(grid) for _ in range(n)]
    else:
        span = (hi - lo).days
        days = [lo + timedelta(days=rng.randint(0, span)) for _ in range(n)]
    if ctype == "timestamp":
        return [datetime(d.year, d.month, d.day, rng.randint(0, 23), rng.randint(0, 59)).isoformat() for d in days]
    return [d.isoformat() for d in days]


def _fake(provider: str, rng: random.Random) -> str:
    first, last = rng.choice(_FIRST), rng.choice(_LAST)
    if provider == "name":
        return f"{first} {last}"
    if provider == "email":
        return f"{first.lower()}.{last.lower()}{rng.randint(1, 9999)}@example.com"     # reserved domain (RFC 2606)
    if provider == "phone":
        return f"+44 7700 900{rng.randint(0, 999):03d}"                    # Ofcom's drama range (07700 900000-999)
    if provider == "uuid":
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))
    return " ".join(rng.choice(_WORDS) for _ in range(rng.randint(2, 5)))


def default_rule(column: dict, label: dict | None, unique: set[str]) -> dict:
    """Contract-only rule for a column with no synthetic.columns entry: from the contract's bounds."""
    name, ctype = column["name"], column["type"]
    if label and name == label.get("column") and label.get("values"):
        return {"rule": "values", "values": list(label["values"])}
    if column.get("allowed"):
        return {"rule": "values", "values": list(column["allowed"])}
    if name in unique:
        return {"rule": "key"}
    if ctype in ("integer", "float"):
        lo = column.get("min", 0)
        hi = column.get("max", lo + 100)
        return {"rule": "range", "min": lo, "max": hi}
    if ctype == "boolean":
        return {"rule": "values", "values": [True, False]}
    if ctype in ("date", "timestamp"):
        today = date.today()
        return {"rule": "time", "from": date(today.year - 1, 1, 1).isoformat(), "to": today.isoformat()}
    return {"rule": "fake", "provider": "text"}


def contract_rows(contract: dict, label: dict | None, rules: dict, rows: int, seed: int = 42) -> list[dict]:
    """Rows from the data contract alone. Null fractions follow each column's max_null_fraction (required columns
    and keys are never null)."""
    rng = random.Random(seed)
    unique = set(contract.get("unique") or [])
    data = {}
    for column in contract["columns"]:
        rule = rules.get(column["name"]) or default_rule(column, label, unique)
        values = column_values(column["name"], rule, column, rows, rng)
        nulls = column.get("max_null_fraction", 0) if not column.get("required") and rule["rule"] != "key" else 0
        if nulls:
            values = [None if rng.random() < nulls / 2 else v for v in values]     # within the limit, not at it
        data[column["name"]] = values
    return [{name: data[name][i] for name in data} for i in range(rows)]


def apply_rules(records: list[dict], contract: dict, rules: dict, seed: int = 42) -> list[dict]:
    """A learned engine's rows with the use case's rules applied (keys, personal data, constants)."""
    columns = {c["name"]: c for c in contract["columns"]}
    rng = random.Random(seed)
    for name, rule in rules.items():
        values = column_values(name, rule, columns.get(name, {"name": name}), len(records), rng)
        for row, v in zip(records, values, strict=True):
            row[name] = v
    return records
