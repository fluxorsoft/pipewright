"""Erasure in Parquet outputs on GCS (outputs.store gcs or both): every file under a table's prefix is read, and
files holding a key are rewritten without those rows. Each rewrite is conditional on the object's generation, so a
file written meanwhile is never overwritten by an older copy.
"""

from __future__ import annotations

import io
from urllib.parse import quote

from pipewright.errors import ConfigError
from pipewright.gcp import Gcp
from pipewright.gcp.storage import GCS_API, GCS_UPLOAD, gcs_list


def _objects(gcp: Gcp, uri: str, table: str) -> list[str]:
    bucket, _, prefix = f"{uri}{table}/".removeprefix("gs://").partition("/")
    return [n for n in gcs_list(gcp, bucket, prefix) if n.endswith(".parquet") and "/run=_schema/" not in n]


def erase_table(gcp: Gcp, uri: str, table: str, key: str, keys: list[str], confirm: bool) -> tuple[int, int]:
    """(rows matched, files rewritten)."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    bucket = uri.removeprefix("gs://").split("/", 1)[0]
    wanted = set(keys)
    matched = rewritten = 0
    for name in _objects(gcp, uri, table):
        path = f"{GCS_API}/b/{bucket}/o/{quote(name, safe='')}"
        meta = gcp.get(path)
        if not meta.ok:
            raise ConfigError(f"cannot read gs://{bucket}/{name}")
        data = gcp.session.get(path, params={"alt": "media"}, timeout=120).content
        parquet = pq.read_table(io.BytesIO(data))
        if key not in parquet.column_names:
            continue
        hit = pc.is_in(pc.cast(parquet[key], "string"), value_set=pa.array(sorted(wanted)))
        n = pc.sum(hit).as_py() or 0
        matched += n
        if confirm and n:
            kept = parquet.filter(pc.invert(hit))
            buf = io.BytesIO()
            pq.write_table(kept, buf, compression="snappy")
            r = gcp.session.post(f"{GCS_UPLOAD}/b/{bucket}/o",
                                 params={"uploadType": "media", "name": name,
                                         "ifGenerationMatch": meta.body["generation"]},
                                 data=buf.getvalue(), headers={"Content-Type": "application/vnd.apache.parquet"},
                                 timeout=120)
            if r.status_code == 412:
                raise ConfigError(f"gs://{bucket}/{name} changed during erasure: re-run")
            if not r.ok:
                raise ConfigError(f"cannot rewrite gs://{bucket}/{name}: {r.status_code} {r.text[:200]}")
            rewritten += 1
    return matched, rewritten
