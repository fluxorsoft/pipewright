"""Q1c: which signals move on harmless changes (label, description, field description, whitespace, new data) and
on real definition changes. Signals: etag, lastModifiedTime, the defining SQL, schema (names+types), schema with
descriptions, INFORMATION_SCHEMA.TABLES ddl."""
import hashlib
from h import *
H = lambda v: hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()[:10]
NAMES = ("f_spend_v", "f_spend_mv", "f_spend_t")
ddl_bytes = []

def sig(t):
    d = table(t)
    fields = (d.get("schema") or {}).get("fields", [])
    return {"etag": d.get("etag"), "modified": d.get("lastModifiedTime"),
            "sql": H(((d.get("view") or d.get("materializedView") or {}).get("query"))),
            "sql_norm": H(" ".join(((d.get("view") or d.get("materializedView") or {}).get("query") or "").split())),
            "schema": H([(f["name"], f["type"], f.get("mode")) for f in fields]),
            "schema_desc": H(fields), "rows": d.get("numRows")}

def ddl():
    rows, st = q(f"SELECT table_name, ddl FROM `{P}.{DS}`.INFORMATION_SCHEMA.TABLES WHERE table_name IN {NAMES}")
    ddl_bytes.append(st.get("query", {}).get("totalBytesBilled"))
    return {r["table_name"]: H(r["ddl"]) for r in rows}

def snap():
    s = {t: sig(t) for t in NAMES}
    for t, h in ddl().items():
        s[t]["ddl"] = h
    return s

def diff(label, a, b):
    out = {t: [k for k in a[t] if a[t][k] != b[t][k]] for t in NAMES}
    print(f"{label:52s}", " | ".join(f"{t}: {','.join(v) or '-'}" for t, v in out.items()))

FEAT = lambda days, ws=" ": f"""SELECT customer_id, ROUND(AVG(amount), 4) AS avg_spend_90d,{ws}COUNT(*) AS txn_count_90d,
  TIMESTAMP '2026-11-07 00:00:00' AS feature_timestamp
FROM `{P}.{DS}.transactions`
WHERE ts >= TIMESTAMP_SUB(TIMESTAMP '2026-11-07 00:00:00', INTERVAL {days} DAY)
GROUP BY customer_id"""
steps = [
    ("1 label added (all three)", lambda: [call("PATCH", f"{BQ}/datasets/{DS}/tables/{t}", {"labels": {"team": "risk"}}) for t in NAMES]),
    ("2 table description (all three)", lambda: [call("PATCH", f"{BQ}/datasets/{DS}/tables/{t}", {"description": "spend features"}) for t in NAMES]),
    ("3 field description (view, table)", lambda: [call("PATCH", f"{BQ}/datasets/{DS}/tables/{t}", {"schema": {"fields": [
        {**f, **({"description": "mean spend, 90 days"} if f["name"] == "avg_spend_90d" else {})}
        for f in table(t)["schema"]["fields"]]}}) for t in ("f_spend_v", "f_spend_t")]),
    ("4 view re-created, whitespace only", lambda: q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(90, '   ')}")),
    ("5 new source rows; table rebuilt, same SQL", lambda: (q(f"INSERT `{P}.{DS}.transactions` VALUES ('c1', 999.0, TIMESTAMP '2026-11-06 00:00:00')"),
                                                          q(f"CREATE OR REPLACE TABLE `{P}.{DS}.f_spend_t` AS {FEAT(90)}"))),
    ("6 view SQL changed (90 -> 60 days)", lambda: q(f"CREATE OR REPLACE VIEW `{P}.{DS}.f_spend_v` AS {FEAT(60)}")),
    ("7 table rebuilt with changed SQL (90 -> 60)", lambda: q(f"CREATE OR REPLACE TABLE `{P}.{DS}.f_spend_t` AS {FEAT(60)}")),
    ("8 materialized view refreshed (new data)", lambda: q(f"CALL BQ.REFRESH_MATERIALIZED_VIEW('{P}.{DS}.f_spend_mv')")),
]
prev = snap()
print("signals per object that moved (- = none)\n")
for label, act in steps:
    act(); time.sleep(2)
    now = snap(); diff(label, prev, now); prev = now
print("\nINFORMATION_SCHEMA.TABLES bytes billed per query:", sorted(set(ddl_bytes)))
r, _ = q(f"SELECT ddl FROM `{P}.{DS}`.INFORMATION_SCHEMA.TABLES WHERE table_name = 'f_spend_t'")
print("\nCTAS table ddl (what it holds):\n", r[0]["ddl"][:500])
