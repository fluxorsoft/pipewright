"""Cloud Storage helpers: read, conditional write, list."""

from __future__ import annotations

from pipewright.errors import ConfigError
from pipewright.gcp.session import Gcp, Response, _json, error_text

GCS_API = "https://storage.googleapis.com/storage/v1"
GCS_UPLOAD = "https://storage.googleapis.com/upload/storage/v1"


class PreconditionFailed(Exception):
    """A conditional write lost a race (the object changed since it was read)."""


def gcs_read(gcp: Gcp, bucket: str, name: str) -> tuple[str | None, int]:
    """Returns (text, generation); (None, 0) if the object does not exist."""
    from urllib.parse import quote

    meta = gcp.get(f"{GCS_API}/b/{bucket}/o/{quote(name, safe='')}")
    if meta.status == 404:
        return None, 0
    if not meta.ok:
        raise ConfigError(f"cannot read gs://{bucket}/{name}: {error_text(meta)}")
    r = gcp.session.get(f"{GCS_API}/b/{bucket}/o/{quote(name, safe='')}", params={"alt": "media"}, timeout=30)
    return r.text, int(meta.body["generation"])


def gcs_write(gcp: Gcp, bucket: str, name: str, text: str, content_type: str = "application/json",
              if_generation_match: int | None = None) -> int:
    """Uploads text; with if_generation_match (0 = must not exist) the write only succeeds if nobody else wrote."""
    params = {"uploadType": "media", "name": name}
    if if_generation_match is not None:
        params["ifGenerationMatch"] = str(if_generation_match)
    r = gcp.session.post(f"{GCS_UPLOAD}/b/{bucket}/o", params=params, data=text.encode("utf-8"),
                         headers={"Content-Type": content_type}, timeout=60)
    if r.status_code == 412:
        raise PreconditionFailed(f"gs://{bucket}/{name} changed since it was read")
    if not r.ok:
        raise ConfigError(f"cannot write gs://{bucket}/{name}: {error_text(Response(r.status_code, _json(r)))}")
    return int(r.json()["generation"])


def gcs_list(gcp: Gcp, bucket: str, prefix: str) -> list[str]:
    names, token = [], None
    while True:
        params = {"prefix": prefix, "fields": "items(name),nextPageToken"}
        if token:
            params["pageToken"] = token
        r = gcp.get(f"{GCS_API}/b/{bucket}/o", **params)
        if not r.ok:
            raise ConfigError(f"cannot list gs://{bucket}/{prefix}: {error_text(r)}")
        names += [i["name"] for i in r.body.get("items", [])]
        token = r.body.get("nextPageToken")
        if not token:
            return names
