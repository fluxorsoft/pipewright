"""Reading configuration files: local YAML, and text from gs:// or local paths."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pipewright.errors import ConfigError


def load_yaml(path: Path) -> Any:
    try:
        with Path(path).open(encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"file not found: {path}") from None
    except yaml.YAMLError as e:
        raise ConfigError(f"not valid YAML: {e}", source=str(path)) from None


def read_text(ref: str, base_dir: Path) -> tuple[str, str]:
    """Returns (text, display name) for a gs:// URI or a path relative to base_dir."""
    if ref.startswith("gs://"):
        # Same credentials as every other Pipewright call (ADC, GCLOUD_CONFIG or a token), not the storage
        # library's own defaults.
        from pipewright.gcp import Gcp, gcs_read

        bucket, _, name = ref.removeprefix("gs://").partition("/")
        text, _ = gcs_read(Gcp(), bucket, name)
        if text is None:
            raise ConfigError(f"manifest not found: {ref} (ask the platform team to publish it)")
        return text, ref
    path = (Path(base_dir) / ref).resolve()
    if not path.is_file():
        raise ConfigError(f"manifest not found: {path}")
    return path.read_text(encoding="utf-8"), str(path)
