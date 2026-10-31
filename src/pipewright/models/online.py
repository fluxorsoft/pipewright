"""Online features on a served model (FR-51, FR-76): which online store a model reads, and from where.

The training step packages the lookup in `serving.json` with the store of the environment it trained in. A promoted
model is the same files, so `serving.json` names the source environment's store. From Pipewright 1.2.0 the store is
never taken from the files when it can be helped: the model's serving container carries the environment's own
lookup in PIPEWRIGHT_ONLINE_FEATURES (set by the register step and replaced by `pipewright promote`), and the
handler prefers it. `deploy-model` and `doctor` refuse a model whose store is in another project than the
environment's: with read access to that project it would serve the other environment's feature values silently.
(The variable is the lookup only, never credentials: the endpoint still reads as the tenant's runner.)
"""

from __future__ import annotations

import json

ENV_VAR = "PIPEWRIGHT_ONLINE_FEATURES"


def env_value(online: dict | None) -> str | None:
    """The variable's value for a resolved `ml_config["online_features"]`, or None when there is none."""
    return json.dumps(online, sort_keys=True, separators=(",", ":")) if online else None


def container_spec(spec: dict | None, online: dict | None) -> dict:
    """A serving container spec (REST form) with PIPEWRIGHT_ONLINE_FEATURES set to `online`, or removed without it.
    Every other variable is kept."""
    spec = dict(spec or {})
    env = [e for e in spec.get("env") or [] if e.get("name") != ENV_VAR]
    value = env_value(online)
    if value:
        env.append({"name": ENV_VAR, "value": value})
    if env:
        spec["env"] = env
    else:
        spec.pop("env", None)
    return spec


def store_project(store: str | None) -> str | None:
    """`projects/<p>/locations/<r>/featureOnlineStores/<s>` -> <p>."""
    parts = (store or "").split("/")
    return parts[1] if len(parts) > 1 and parts[0] == "projects" else None


def packaged_store(env_vars: dict, serving_json: dict | None) -> tuple[str | None, str]:
    """The store a model would read, and where that comes from: its serving container's variable first, then the
    `serving.json` packaged with its files."""
    value = env_vars.get(ENV_VAR)
    if value:
        try:
            return (json.loads(value) or {}).get("store"), "its serving container (PIPEWRIGHT_ONLINE_FEATURES)"
        except ValueError:
            return None, "an unreadable PIPEWRIGHT_ONLINE_FEATURES"
    online = (serving_json or {}).get("online_features") or {}
    return online.get("store"), "serving.json in its files"


def check(expected: dict | None, env_vars: dict, serving_json: dict | None, environment: str) -> str | None:
    """A problem when the model would read another project's online store than the environment's, else None."""
    if not expected:
        return None
    store, source = packaged_store(env_vars, serving_json)
    if store is None:
        return (f"the model carries no online lookup ({source}): train it with model.serving.features online, or "
                "promote it with Pipewright 1.2.0 or later")
    if store != expected.get("store"):
        where = ("it was trained or promoted in another environment" if store_project(store) !=
                 store_project(expected.get("store")) else "the environment's online store has changed since")
        return (f"the model would read the online store {store} ({source}), not {environment}'s "
                f"{expected.get('store')}: {where}. Promote it again with Pipewright 1.2.0 or later, which gives a "
                "promoted model the destination's store, or retrain it here")
    return None
