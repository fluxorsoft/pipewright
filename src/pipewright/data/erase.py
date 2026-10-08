"""pipewright data erase: delete a data subject's rows (data_protection.erasure.key_column) from every table the use
case owns that has that column. Dry run by default: counts first, deletes only with --confirm.

Keys are passed as query parameters and never printed. Not covered (reported, not changed): landing files and
pipeline artifacts in GCS (platform retention applies), and models already trained on the data (retrain them).
"""

from __future__ import annotations

from dataclasses import dataclass

from pipewright.config import Environment, ResolvedUseCase
from pipewright.data import erase_gcs
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, bigquery

_PARAM_TYPES = {"STRING": "STRING", "INTEGER": "INT64", "INT64": "INT64"}


@dataclass
class EraseResult:
    table: str
    rows: int | None
    deleted: bool
    error: str | None = None
    files: int | None = None        # GCS: Parquet files rewritten


def read_keys(keys: list[str] | None, keys_file: str | None) -> list[str]:
    out = list(keys or [])
    if keys_file:
        with open(keys_file, encoding="utf-8") as f:
            out += [line.strip() for line in f if line.strip() and not line.startswith("#")]
    out = sorted(set(out))
    if not out:
        raise ConfigError("give the keys to erase with --key (repeatable) or --keys-file")
    return out


def targets(uc: ResolvedUseCase) -> tuple[str, list[tuple[str, str]]]:
    """(key column, [(table, BigQuery type of the key)])."""
    erasure = (uc.ml.get("data_protection") or {}).get("erasure")
    if not erasure:
        raise ConfigError(f"{uc.name} has no data_protection.erasure.key_column")
    key = erasure["key_column"]
    found = []
    for name, table in uc.tables.items():
        field = next((f for f in table.fields if f["name"] == key), None)
        if field:
            kind = _PARAM_TYPES.get(field["type"].upper())
            if kind is None:
                raise ConfigError(f"{name}.{key} is {field['type']}: erasure supports STRING and INTEGER keys")
            found.append((name, kind))
    if not found:
        raise ConfigError(f"no table of {uc.name} has the erasure key column {key!r}")
    return key, found


def _query(gcp: Gcp, project: str, location: str | None, sql: str, keys: list[str], kind: str,
           kms_key: str | None = None) -> dict:
    params = [{"name": "keys", "parameterType": {"type": "ARRAY", "arrayType": {"type": kind}},
               "parameterValue": {"arrayValues": [{"value": k} for k in keys]}}]
    return bigquery.query(gcp, project, sql, params, location, kms_key)


def erase(uc: ResolvedUseCase, env: Environment, keys: list[str], confirm: bool, gcp: Gcp | None = None,
          ) -> list[EraseResult]:
    gcp = gcp or Gcp()
    key, tables = targets(uc)
    dataset = uc.resources["bigquery_dataset"]
    location = bigquery.dataset_location(gcp, env.project, dataset)
    store = uc.outputs.get("store", "bigquery")
    results = []
    try:
        return _erase_all(gcp, uc, env, dataset, location, tables, key, keys, confirm, store, results)
    finally:
        if confirm:
            import io

            from pipewright.observability import events

            # Counts only: erased keys are personal data and never leave this process.
            events.emit(events.event("data.erased", uc.name, env.name, uc.tenant, None, None, keys=len(keys),
                                     key_column=key, rows={r.table: r.rows for r in results}, by=gcp.identity()),
                        env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)


def _erase_all(gcp, uc, env, dataset, location, tables, key, keys, confirm, store, results):
    for table, kind in tables:
        if store in ("bigquery", "both"):
            results.append(_erase_bigquery(gcp, env, dataset, location, table, key, kind, keys, confirm))
        if store in ("gcs", "both"):
            label = f"{table} (gcs)" if store == "both" else table
            try:
                rows, files = erase_gcs.erase_table(gcp, uc.outputs["gcs"]["uri"], table, key, keys, confirm)
                results.append(EraseResult(label, rows, confirm and rows > 0, files=files))
            except ConfigError as e:
                results.append(EraseResult(label, None, False, str(e)))
    return results


def _erase_bigquery(gcp, env, dataset, location, table, key, kind, keys, confirm) -> EraseResult:
    fq = f"`{env.project}.{dataset}.{table}`"
    try:
        bq_key = env.security.get("bigquery_kms_key")
        res = _query(gcp, env.project, location, f"SELECT COUNT(*) FROM {fq} WHERE {key} IN UNNEST(@keys)", keys, kind,
                     bq_key)
        rows = int(res["rows"][0]["f"][0]["v"])
        if confirm and rows:
            res = _query(gcp, env.project, location, f"DELETE FROM {fq} WHERE {key} IN UNNEST(@keys)", keys, kind,
                         bq_key)
            rows = int(res.get("numDmlAffectedRows", rows))
        return EraseResult(table, rows, confirm and rows > 0)
    except ConfigError as e:
        return EraseResult(table, None, False, str(e))


def render(uc: ResolvedUseCase, results: list[EraseResult], keys: int, confirm: bool) -> str:
    key, _ = targets(uc)
    lines = [f"{'Erasing' if confirm else 'Dry run: erasure of'} {keys} key(s) on {key} for {uc.name} "
             f"({uc.environment})"]
    for r in results:
        if r.error:
            lines.append(f"  FAIL  {r.table}: {r.error}")
        elif confirm:
            files = f" ({r.files} file(s) rewritten)" if r.files else ""
            lines.append(f"  {'DEL ' if r.deleted else 'OK  '}  {r.table}: {r.rows} row(s) deleted{files}")
        else:
            lines.append(f"  ...   {r.table}: {r.rows} row(s) would be deleted")
    if not confirm:
        lines.append("re-run with --confirm to delete")
    lines.append("not covered: landing files and pipeline artifacts in GCS (platform retention), and models trained "
                 "on this data: retrain to remove its influence")
    return "\n".join(lines)
