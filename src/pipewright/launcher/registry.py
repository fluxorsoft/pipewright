"""Live contracts per use case, read from pointers in the artifacts bucket (refreshed every TTL)."""

from __future__ import annotations

import json
import logging
import threading
import time

from pipewright.config.environment import check_requires
from pipewright.config.schema import prune_unknown
from pipewright.errors import ConfigError
from pipewright.gcp import Gcp, gcs_list, gcs_read
from pipewright.release.contract import CONTRACTS_PREFIX, Pointer, ReleaseContract

log = logging.getLogger("pipewright.launcher")


class Registry:
    """Live contracts per use case, from pointers in the artifacts bucket."""

    def __init__(self, gcp: Gcp, manifest_uri: str, ttl: int = 60):
        self.gcp = gcp
        self.ttl = ttl
        self.manifest_uri = manifest_uri
        self.manifest = self._read_manifest()
        self.bucket = self.manifest["artifacts_bucket"]
        self._contracts: dict[str, ReleaseContract] = {}      # object name -> contract (immutable)
        self._live: list[ReleaseContract] = []
        self._errors: dict[str, str] = {}
        self._loaded = 0.0
        self._lock = threading.Lock()

    def _read_manifest(self) -> dict:
        bucket, _, name = self.manifest_uri.removeprefix("gs://").partition("/")
        text, _ = gcs_read(self.gcp, bucket, name)
        if text is None:
            raise ConfigError(f"manifest not found: {self.manifest_uri}")
        manifest = json.loads(text)
        # A manifest that needs a newer Pipewright is refused (the launcher keeps its last good one); keys this
        # version does not know are logged and otherwise ignored (R-05).
        check_requires(manifest, self.manifest_uri)
        _, ignored = prune_unknown(manifest, "manifest")
        if ignored:
            log.warning("manifest keys this launcher does not know, ignored: %s", ", ".join(ignored))
        return manifest

    def _contract(self, name: str) -> ReleaseContract:
        if name not in self._contracts:
            text, _ = gcs_read(self.gcp, self.bucket, name)
            if text is None:
                raise ConfigError(f"contract {name} not found")
            c = ReleaseContract.from_json(text)
            c.validate()
            if c.environment != self.manifest["environment"]:
                raise ConfigError(f"contract is for {c.environment}, launcher serves {self.manifest['environment']}")
            self._contracts[name] = c
        return self._contracts[name]

    def live(self, force: bool = False) -> list[ReleaseContract]:
        with self._lock:
            if not force and time.monotonic() - self._loaded < self.ttl:
                return self._live
            try:
                # New tenants appear without a redeploy; on a read error keep the last good manifest.
                self.manifest = self._read_manifest()
            except Exception as e:  # noqa: BLE001
                log.error("keeping previous manifest: %s", e)
            live, errors = [], {}
            for name in gcs_list(self.gcp, self.bucket, f"{CONTRACTS_PREFIX}/"):
                if not name.endswith("/current.json"):
                    continue
                usecase = name.split("/")[1]
                try:
                    text, _ = gcs_read(self.gcp, self.bucket, name)
                    contract = self._contract(Pointer.from_json(text).contract)
                    if contract.tenant not in self.manifest["tenants"]:
                        raise ConfigError(f"tenant {contract.tenant!r} is not in the manifest")
                    live.append(contract)
                except Exception as e:  # noqa: BLE001 - one broken use case must not stop the others
                    errors[usecase] = str(e)
                    log.error("skipping use case %s: %s", usecase, e)
            self._live, self._errors, self._loaded = live, errors, time.monotonic()
            return live

    def get(self, usecase: str, release: str | None = None) -> ReleaseContract | None:
        """release: the release a run request was sent for (Pipewright 1.1). A newer one than the cached pointer
        names reloads the pointers, so a run right after `pipewright release` gets that release, not the last."""
        found = next((c for c in self.live() if c.usecase == usecase), None)
        if release and (found is None or found.release_id != release):
            found = next((c for c in self.live(force=True) if c.usecase == usecase), None)
        return found

    @property
    def errors(self) -> dict[str, str]:
        return dict(self._errors)
