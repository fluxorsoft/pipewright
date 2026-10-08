"""Load the registry's default model version (the champion) into a Model artifact."""

import os
from typing import NamedTuple

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def load_model(
    project: str,
    region: str,
    model_display_name: str,
    required: bool,
    model: dsl.Output[dsl.Model],
) -> NamedTuple("Champion", [("found", str), ("version", str), ("resource_name", str)]):
    """found is "yes" or "no". With required=True (inference) a missing model fails the run."""
    import logging
    import os
    from collections import namedtuple

    from google.cloud import aiplatform, storage

    logging.basicConfig(level=logging.INFO)
    out = namedtuple("Champion", ["found", "version", "resource_name"])
    aiplatform.init(project=project, location=region)
    os.makedirs(model.path, exist_ok=True)
    existing = aiplatform.Model.list(filter=f'display_name="{model_display_name}"', order_by="create_time desc")
    if not existing:
        if required:
            raise RuntimeError(f"no model {model_display_name!r} in the registry: run the training pipeline first")
        logging.info("no champion yet for %s", model_display_name)
        return out("no", "", "")
    default = aiplatform.Model(model_name=existing[0].resource_name)    # resolves to the default version
    bucket_name, _, prefix = default.uri.removeprefix("gs://").partition("/")
    client = storage.Client(project=project)
    copied = []
    for name in ("model.pt", "features.json", "training_stats.json"):
        blob = client.bucket(bucket_name).blob(f"{prefix.rstrip('/')}/{name}")
        if blob.exists():
            blob.download_to_filename(os.path.join(model.path, name))
            copied.append(name)
    if "model.pt" not in copied:
        raise RuntimeError(f"default version of {model_display_name} has no model.pt at {default.uri}")
    model.metadata.update({"resourceName": default.resource_name, "versionId": default.version_id,
                           "artifactUri": default.uri, "files": copied})
    logging.info("champion %s version %s (%s)", default.resource_name, default.version_id, copied)
    return out("yes", str(default.version_id), default.resource_name)
