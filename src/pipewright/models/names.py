"""Which registry model is the use case's (Pipewright FR-124).

Models are found by display name. Before FR-124 two training runs registering a first version at the same time could
each make a model with the name, and every lookup took whichever the API listed first. The rule now: the oldest model
with the name is the use case's model, everywhere; and a first version is registered under a fixed model ID
(`model_id`), so the race cannot make a second model (the second run's upload is refused and becomes a version).
The register step keeps the same rule in pipewright_runtime (registry_model_id, registry_models; a test keeps them
equal).
"""

from __future__ import annotations

import re


def model_id(display_name: str) -> str:
    """The model ID a first version is registered under: a letter first, [a-z0-9-], at most 63 characters."""
    rid = re.sub(r"[^a-z0-9-]", "-", display_name.lower()).strip("-")
    rid = rid if rid[:1].isalpha() else f"m-{rid}"
    return rid[:63].rstrip("-")


def oldest(models: list) -> list:
    """Models with one display name, oldest first, whether SDK objects (create_time) or REST bodies (createTime)."""
    def key(m):
        if isinstance(m, dict):
            return str(m.get("createTime") or ""), str(m.get("name") or "")
        return str(getattr(m, "create_time", "") or ""), str(getattr(m, "resource_name", "") or "")

    return sorted(models or [], key=key)
