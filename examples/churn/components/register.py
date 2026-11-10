"""Register: upload an approved model as a new version in the Model Registry, with a model card.

registry.set_default (usecase.yaml):
  on_approval   the new version becomes the default (the champion inference uses) at once
  manual        it is registered as a candidate; `pipewright model approve` makes it the default
"""

import os

from kfp import dsl

COMPONENT_IMAGE = os.environ.get("COMPONENT_IMAGE", "mlops-components:dev")
BASE_IMAGE = "python:3.11-slim"

# Prebuilt TorchServe runtime; serves <artifact_uri>/model.mar. Keep its minor version in step with torch in
# requirements.txt.
# Agent Platform's prebuilt serving images are used by tag (it refuses one given by digest). The approval records the
# digest this tag resolves to, and deploy-model refuses the version if the tag has moved since (Pipewright phase 6).
SERVING_IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/pytorch-cpu.2-4:latest"


def model_card(ml_config: dict, report: dict, run_id: str, source_uri: str) -> str:
    """Markdown model card from model_card.* and the evaluation report."""
    card = ml_config.get("model_card", {})
    m = report["metrics"]
    lines = [f"# {ml_config['model_name']}", "",
             f"Use case `{ml_config['usecase']}`, owner {ml_config.get('owner', '')}, environment "
             f"{ml_config['environment']}, run `{run_id}`.", ""]
    if ml_config.get("description"):
        lines += [ml_config["description"], ""]
    if ml_config.get("objective"):
        lines += [f"**Objective.** {ml_config['objective']}", ""]
    for key, title in (("intended_use", "Intended use"), ("out_of_scope", "Out of scope"),
                       ("limitations", "Limitations"), ("ethical_considerations", "Ethical considerations")):
        if card.get(key):
            lines += [f"## {title}", "", card[key].strip(), ""]
    features = ", ".join(ml_config["features"]["include"])
    lines += ["## Training data", "", f"Source `{source_uri}`. Features: {features}.",
              f"Split: {ml_config['data']['split']['strategy']}. Personal data columns (never features): "
              f"{', '.join(ml_config['data_protection']['personal_data_columns']) or 'none'}.", "",
              "## Evaluation (test split)", "", "| metric | value |", "|---|---|"]
    lines += [f"| {k} | {v:.4f} |" for k, v in m.items() if v is not None]
    lines += ["", f"Decision threshold {report['threshold']}; calibration error {report['calibration']['ece']:.4f}.",
              "", "Decision: " + report["decision"] + ". " + "; ".join(report["reasons"]), ""]
    if report.get("fairness"):
        lines += ["## Fairness (test split)", "", "| attribute | measure | value |", "|---|---|---|"]
        for attr, entry in report["fairness"].items():
            lines += [f"| {attr} | {k} | {'not measured' if v is None else f'{v:.4f}'} |"
                      for k, v in entry.items() if k != "groups"]
        lines.append("")
    if report.get("feature_importance"):
        lines += ["## Feature importance (permutation)", "", "| feature | drop |", "|---|---|"]
        lines += [f"| {k} | {v:.4f} |" for k, v in report["feature_importance"].items()]
        lines.append("")
    return "\n".join(lines)


@dsl.component(base_image=BASE_IMAGE, target_image=COMPONENT_IMAGE)
def register_model(
    project: str,
    region: str,
    model: dsl.Input[dsl.Model],
    report: dsl.Input[dsl.Artifact],
    model_display_name: str,
    run_id: str,
    source_uri: str,
    ml_config: dict,
    vertex_model: dsl.Output[dsl.Artifact],
    card: dsl.Output[dsl.Markdown],
):
    import json
    import logging
    import re

    from google.cloud import aiplatform, storage

    logging.basicConfig(level=logging.INFO)
    aiplatform.init(project=project, location=region)
    with open(report.path) as f:
        evaluation = json.load(f)
    # A replay is a check, not a new model. Since rows are sorted before training (Pipewright 1.2.0) a replay with
    # the same release reproduces its run exactly, so it ties the champion it was trained as, and a tie passes the
    # champion rule: registering it would add a copy of a model that already exists (found re-running the book).
    champ = evaluation.get("champion") or {}
    key = ml_config["evaluation"]["primary_metric"]
    new, old = (evaluation.get("metrics") or {}).get(key), champ.get(key)
    if source_uri.startswith("snapshot://") and new is not None and old is not None and new == old:
        logging.info("replay of %s reproduced the champion exactly (%s %.4f, champion version %s): a replay is a "
                     "check, so nothing is registered", source_uri.removeprefix("snapshot://"), key, new,
                     champ.get("_version", "?"))
        with open(card.path, "w") as f:
            f.write(f"Replay of {source_uri}: reproduced the champion exactly; not registered.\n")
        from pipewright_runtime import emit_event

        # Said as an event (Pipewright FR-124): the launcher's tick reports an approved run that registered nothing,
        # and a replay is the one approved run that rightly registers nothing.
        emit_event("model.registration_skipped", ml_config, run_id, model=model_display_name, reason="replay",
                   source_uri=source_uri, primary_metric=key, primary_value=new)
        return

    text = model_card(ml_config, evaluation, run_id, source_uri)
    with open(card.path, "w") as f:
        f.write(text)
    # Stored with the model files, so the card travels with every version.
    bucket_name, _, prefix = model.uri.removeprefix("gs://").partition("/")
    storage.Client(project=project).bucket(bucket_name).blob(f"{prefix}/model_card.md").upload_from_string(
        text, content_type="text/markdown")
    blob = storage.Client(project=project).bucket(bucket_name).blob(f"{prefix}/evaluation.json")
    blob.upload_from_string(json.dumps(evaluation, indent=2), content_type="application/json")

    # Pipewright 1.4.1 (FR-101): the files as Agent Platform copies them now, at upload: it serves that copy, so an
    # approval can be shown to cover exactly what serves (names, generations and digests; never contents).
    from pipewright_runtime import folder_files

    registered_files = folder_files(model.uri, project)
    from google.api_core.exceptions import Conflict
    from pipewright_runtime import registry_model_id, registry_models

    # The oldest model with the name is the use case's model (Pipewright FR-124): two runs registering a first version
    # at once each found none and each made a model, and Pipewright read only one of them. A first version is now
    # made under a fixed model ID, so the second run gets a conflict and becomes a version of the first one's model.
    existing = registry_models(aiplatform, model_display_name)
    alias = re.sub(r"[^a-z0-9-]", "-", f"run-{run_id}".lower())[:127]
    primary = ml_config["evaluation"]["primary_metric"]
    value = evaluation["metrics"].get(primary)
    online = ml_config.get("online_features")
    manual = ml_config["registry"]["set_default"] == "manual"

    def upload(parent):
        first = parent is None
        make_default = not manual or first
        return make_default, aiplatform.Model.upload(
            display_name=model_display_name,
            artifact_uri=model.uri,
            serving_container_image_uri=SERVING_IMAGE,
            # The environment's online lookup on the container, which the handler prefers over serving.json, so a
            # promoted copy can be given the destination's (Pipewright 1.2.0, FR-76).
            # On a shared pool (Pipewright 1.4.2, FR-105) none: the pool's variables are the first model's, so a pool
            # model's settings stay in serving.json with its files (deploy-model refuses a pool model that sets any).
            serving_container_environment_variables=(
                {"PIPEWRIGHT_ONLINE_FEATURES": json.dumps(online, sort_keys=True, separators=(",", ":"))}
                if online and not (ml_config.get("serving") or {}).get("pool") else None),
            parent_model=parent,
            model_id=registry_model_id(model_display_name) if first else None,
            is_default_version=make_default,
            version_aliases=[alias],
            version_description=f"run {run_id}, {primary}={value:.4f}" if value is not None else f"run {run_id}",
            encryption_spec_key_name=ml_config["platform"].get("kms_key"),   # CMEK in private environments
            labels={"run_id": alias[:63], "pipeline": "training", "usecase": ml_config["usecase"][:63],
                    "status": "champion" if make_default else "candidate",
                    # A first version is the default by itself; under a manual policy nobody has approved it yet
                    # (Pipewright 1.2.0, FR-83): `pipewright model approve` records the approval.
                    **({"approval": "pending"} if first and manual else {}),
                    # the release that trained it: `pipewright promote` promotes a model only with that release
                    **({"release": re.sub(r"[^a-z0-9-]", "-", os.environ["PIPEWRIGHT_RELEASE"].lower())[:63]}
                       if os.environ.get("PIPEWRIGHT_RELEASE") else {})},
            sync=True,
        )

    try:
        make_default, registered = upload(existing[0].resource_name if existing else None)
    except Conflict:
        if existing:
            raise
        # another run made the first version a moment ago: this one is a version of that model
        logging.info("another run registered the first %s version at the same time: registering this run as a "
                     "version of that model", model_display_name)
        import time

        for _ in range(12):                    # the winner's upload may still be finishing
            existing = registry_models(aiplatform, model_display_name)
            if existing:
                break
            time.sleep(10)
        if not existing:
            raise
        make_default, registered = upload(existing[0].resource_name)
    vertex_model.uri = f"https://{region}-aiplatform.googleapis.com/v1/{registered.versioned_resource_name}"
    vertex_model.metadata.update({"resourceName": registered.resource_name, "versionId": registered.version_id,
                                  "versionedResourceName": registered.versioned_resource_name,
                                  "default": make_default})
    from pipewright_runtime import emit_event

    emit_event("model.registered", ml_config, run_id, model=model_display_name, version=registered.version_id,
               default=make_default, primary_metric=primary, primary_value=value, files=registered_files)
    # Lineage (Pipewright 1.1): the release, config digest, rows, sources and evaluation, next to the model files.
    from pipewright_runtime import lineage_record, write_lineage

    write_lineage(ml_config, run_id, model.uri, lineage_record(
        ml_config, run_id, source_uri, {"name": model_display_name, "version": str(registered.version_id)},
        evaluation, f"gs://{bucket_name}/{prefix}/evaluation.json"))
    logging.info("registered %s version %s (%s)", registered.resource_name, registered.version_id,
                 "default" if make_default else "candidate: run `pipewright model approve` to make it the default")
