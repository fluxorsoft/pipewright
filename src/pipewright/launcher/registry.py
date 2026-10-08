"""Live contracts per use case, read from pointers in the artifacts bucket (refreshed every TTL)."""

from __future__ import annotations

import json
import logging
import threading
import time

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
        return json.loads(text)

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

    def get(self, usecase: str) -> ReleaseContract | None:
        return next((c for c in self.live() if c.usecase == usecase), None)

    @property
    def errors(self) -> dict[str, str]:
        return dict(self._errors)
