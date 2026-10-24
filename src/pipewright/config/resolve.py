"""UseCase (+ include files + environment overrides) + Environment -> ResolvedUseCase."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pipewright.config.document import discover_usecases, load_usecase_document
from pipewright.config.environment import Environment, load_environment, load_manifest
from pipewright.config.merge import deep_merge
from pipewright.config.ml import resolve_ml
from pipewright.config.models import PipelineRef, ResolvedUseCase, Table
from pipewright.config.references import interpolate, interpolate_tree
from pipewright.config.schema import validate
from pipewright.errors import ConfigError
from pipewright.runs import sources

__all__ = ["Environment", "ResolvedUseCase", "Table", "deep_merge", "discover_usecases", "interpolate",
           "load_environment", "load_manifest", "resolve_usecase"]

_SERVING_DEFAULTS = {"enabled": False, "target": "vertex", "machine": "n1-standard-2", "replicas": {"min": 1, "max": 1}}
_APPROVAL_DEFAULTS = {"promote_model": "manual", "deploy_model": "manual", "approvers": []}
_CRON = re.compile(r"^\S+(\s+\S+){4}$")


def _load_table_schema(path: Path, problems: list[str], label: str) -> list:
    if not path.is_file():
        problems.append(f"{label}: schema file not found: {path.name}")
        return []
    try:
        fields = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        problems.append(f"{label}: {path.name} is not valid JSON ({e})")
        return []
    if not isinstance(fields, list) or not all(isinstance(f, dict) and "name" in f and "type" in f for f in fields):
        problems.append(f"{label}: {path.name} must be a BigQuery schema: a list of {{name, type, mode}} fields")
        return []
    return fields


def _resources(tenant: str, explicit: dict, env: Environment, problems: list[str]) -> dict:
    resources: dict[str, Any] = {}
    if env.tenants:
        if tenant not in env.tenants:
            problems.append(f"tenant {tenant!r} is not in environment {env.name!r}: ask the platform team to add it")
            return resources
        from_manifest = env.tenants[tenant]
        for key in ("runner_service_account", "bigquery_dataset", "landing_prefix", "outputs_uri"):
            mv, ev = from_manifest.get(key), explicit.get(key)
            if ev is not None and mv is not None and ev != mv:
                problems.append(f"spec.resources.{key} is {ev!r} but the manifest says {mv!r}")
            resources[key] = ev if ev is not None else mv
        resources["online_serving"] = from_manifest.get("online_serving", False)
        if from_manifest.get("namespace"):
            resources["namespace"] = from_manifest["namespace"]
        return resources
    resources = {**explicit, "online_serving": True}
    for key in ("runner_service_account", "bigquery_dataset"):
        if key not in resources:
            problems.append(f"spec.resources.{key} is required when the environment has no manifest")
    return resources


def _pipelines(eff: dict, directory: Path, problems: list[str]) -> dict[str, PipelineRef]:
    refs = dict(eff.get("pipelines") or {})
    if "pipeline" in eff:
        if "training" in refs:
            problems.append("set either spec.pipeline or spec.pipelines.training, not both")
        refs["training"] = eff["pipeline"]
    out = {}
    for name, ref in refs.items():
        f = (directory / ref["file"]).resolve()
        if not f.is_file():
            problems.append(f"pipelines.{name}.file not found: {ref['file']}")
        out[name] = PipelineRef(name, f, ref["function"])
    return out


def _triggers(eff: dict, resources: dict, tenant: str, pipelines: dict, problems: list[str]) -> dict:
    triggers = {"landing": None, "schedules": [], "api": False, **eff.get("triggers", {})}
    if triggers["landing"]:
        triggers["landing"] = {"pipeline": "training", **triggers["landing"]}
        if not resources.get("landing_prefix"):
            problems.append(f"triggers.landing is set but tenant {tenant!r} has no landing prefix: "
                            "remove it or ask the platform team for one")
        if triggers["landing"]["pipeline"] not in pipelines:
            problems.append(f"triggers.landing.pipeline {triggers['landing']['pipeline']!r} is not in pipelines")
    names = [s["name"] for s in triggers["schedules"]]
    if len(names) != len(set(names)):
        problems.append("triggers.schedules: schedule names must be unique")
    for s in triggers["schedules"]:
        s.setdefault("pipeline", "training")
        if s["pipeline"] not in pipelines:
            problems.append(f"triggers.schedules[{s['name']}].pipeline {s['pipeline']!r} is not in pipelines")
        if not _CRON.match(s["cron"].strip()):
            problems.append(f"triggers.schedules[{s['name']}]: cron {s['cron']!r} must have 5 fields")
        try:
            ZoneInfo(s["timezone"])
        except (ZoneInfoNotFoundError, ValueError):
            problems.append(f"triggers.schedules[{s['name']}]: time zone {s['timezone']!r} is not valid "
                            "(e.g. Europe/London)")
    return triggers


def resolve_usecase(path: Path, env: Environment) -> ResolvedUseCase:
    assembled = load_usecase_document(Path(path))
    doc, path = assembled.doc, assembled.path
    validate(doc, "usecase", str(path))
    meta, spec = doc["metadata"], doc["spec"]
    directory = path.parent.resolve()
    problems: list[str] = []

    if meta["name"] != path.parent.name:
        problems.append(f"metadata.name is {meta['name']!r} but the folder is {path.parent.name!r}")

    # Per-environment overrides: deep merge, lists replace.
    overrides = spec.get("environments", {}).get(env.name, {})
    base = {k: v for k, v in spec.items() if k not in ("environments", "include")}
    eff = deep_merge(base, overrides)

    tenant = eff["tenant"]
    resources = _resources(tenant, eff.get("resources", {}), env, problems)
    pipelines = _pipelines(eff, directory, problems)
    bqml_spec = _bqml_pipelines(eff, pipelines, problems)
    synthetic_requirements = _synthetic_pipeline(eff.get("synthetic") or {}, pipelines, directory, problems)
    triggers = _triggers(eff, resources, tenant, pipelines, problems)

    # Model, serving, approvals, policies.
    model = {"promotion": "model", **eff["model"]}
    if bqml_spec:          # promoted by retraining, or (1.1) by its exported files for the types that allow it
        if eff["model"].get("promotion") == "model":
            from pipewright.bqml.promotion import EXPORTABLE

            if str(bqml_spec.get("model_type", "")).lower() not in EXPORTABLE:
                problems.append(f"model.promotion model: a bqml {bqml_spec.get('model_type')} is promoted by "
                                f"retraining (promotion by files supports {', '.join(EXPORTABLE)}, which export as "
                                "TensorFlow and score identically when imported); set promotion: retrain")
        else:
            model["promotion"] = "retrain"
    model["serving"] = deep_merge(_SERVING_DEFAULTS, eff["model"].get("serving", {}))
    serving = model["serving"]
    if serving["replicas"]["min"] > serving["replicas"]["max"]:
        problems.append("model.serving.replicas: min is greater than max")
    if serving["enabled"]:
        if serving["target"] == "kserve" and env.orchestrator != "kfp":
            problems.append(f"model.serving.target kserve is only available on private-gke (this is {env.blueprint})")
        if not resources.get("online_serving", False):
            problems.append(f"tenant {tenant!r} is not enabled for online serving in {env.name!r}")

    approvals = deep_merge(_APPROVAL_DEFAULTS, eff.get("approvals", {}))
    for key, minimum in env.policies.items():
        if key in _APPROVAL_DEFAULTS and minimum == "manual" and approvals.get(key) == "auto":
            problems.append(f"approvals.{key}: auto is not allowed in {env.name} (platform policy)")

    # Components. A bqml use case runs Pipewright's BigQuery ML steps.
    if bqml_spec:
        from pipewright import bqml

        eff = {**eff, "components": {"dir": str(bqml.STEPS_DIR), "requirements": str(bqml.REQUIREMENTS)}}
    components_dir = (directory / eff["components"]["dir"]).resolve()
    if not components_dir.is_dir():
        problems.append(f"components.dir not found: {eff['components']['dir']}")
    requirements = None
    if eff["components"].get("requirements"):
        requirements = (directory / eff["components"]["requirements"]).resolve()
        if not requirements.is_file():
            problems.append(f"components.requirements not found: {eff['components']['requirements']}")
    ray_requirements = None
    if eff["components"].get("ray_requirements"):
        ray_requirements = (directory / eff["components"]["ray_requirements"]).resolve()
        if not ray_requirements.is_file():
            problems.append(f"components.ray_requirements not found: {eff['components']['ray_requirements']}")

    # Tables.
    tables: dict[str, Table] = {}
    bq = eff.get("outputs", {}).get("bigquery", {})
    for tname, tdef in bq.get("tables", {}).items():
        tdef = {"schema": tdef} if isinstance(tdef, str) else tdef
        label = f"outputs.bigquery.tables.{tname}"
        schema_path = (directory / tdef["schema"]).resolve()
        fields = _load_table_schema(schema_path, problems, label)
        columns = {f["name"] for f in fields}
        partition = tdef.get("partition")
        if fields and partition and partition["field"] not in columns:
            problems.append(f"{label}: partition field {partition['field']!r} is not a column")
        for c in tdef.get("cluster", []):
            if fields and c not in columns:
                problems.append(f"{label}: cluster field {c!r} is not a column")
        tables[tname] = Table(tname, schema_path, fields, partition, tdef.get("cluster", []), tdef.get("description"))

    # ${...} references: parameters, schedule sources, ML sections.
    ctx = {
        "usecase": meta["name"], "tenant": tenant, "environment": env.name, "project": env.project,
        "region": env.region, "resources": {k: v for k, v in resources.items() if isinstance(v, str)},
        "model": {"name": model["name"]},
        "buckets": {"artifacts": env.artifacts_bucket, "landing": env.landing_bucket or ""},
    }
    parameters = {k: interpolate(v, ctx, f"parameters.{k}", problems) for k, v in eff.get("parameters", {}).items()}
    for s in triggers["schedules"]:
        if "source_uri" in s:
            s["source_uri"] = interpolate(s["source_uri"], ctx, f"triggers.schedules[{s['name']}].source_uri",
                                          problems)
            if "${" not in s["source_uri"] and sources.problem(s["source_uri"]):
                problems.append(f"triggers.schedules[{s['name']}].source_uri: {sources.problem(s['source_uri'])}")
    ml_spec = interpolate_tree({k: eff[k] for k in eff if k in _ml_keys()}, ctx, "spec", problems)
    ml = resolve_ml(ml_spec, meta["name"], "inference" in pipelines, set(tables), problems)
    retrain_source = ml["retraining"].get("source_uri")
    if retrain_source and "${" not in retrain_source and sources.problem(retrain_source):
        problems.append(f"retraining.source_uri: {sources.problem(retrain_source)}")
    if ml["data"].get("features_from"):
        # The run's source is then the entity table, read with a query: a gs:// file cannot be one.
        if triggers["landing"]:
            problems.append("triggers.landing delivers gs:// files, but with data.features_from a run's source is "
                            "the entity table (bq://); use a schedule or `pipewright run --source-uri bq://...`")
        named = [(f"triggers.schedules[{s['name']}].source_uri", s.get("source_uri")) for s in triggers["schedules"]]
        for where, uri in [*named, ("retraining.source_uri", retrain_source)]:
            if uri and uri.startswith("gs://"):
                problems.append(f"{where}: with data.features_from the source is the entity table, a bq:// table")
    if model["serving"].get("features") == "online":     # FR-51: only when set; 1.0 use cases resolve as before
        online = _online_features(ml, env, tenant, problems)
        if online:
            ml["online_features"] = online
    task = ((ml["data"].get("label") or {}).get("task")) or "binary"
    if task in ("clustering", "decomposition") and not bqml_spec:
        problems.append(f"data.label.task {task}: BigQuery ML only (bqml.model_type kmeans or pca)")
    if bqml_spec:
        _bqml_section(bqml_spec, ml, model, directory, env, tables, pipelines, triggers, steps_spec(eff),
                      f"{meta['name']}", tenant, problems)
    if "synthetic" in ml:
        from pipewright.synthetic import config as synthetic_config

        problems += synthetic_config.output_problems(ml["synthetic"], ml["data"]["contract"] or {"columns": []},
                                                     tables)
        src = ml["synthetic"].get("source_uri")
        if src and "${" not in src and sources.problem(src):
            problems.append(f"synthetic.source_uri: {sources.problem(src)}")
        for s in triggers["schedules"]:                  # a synthetic schedule learns from synthetic.source_uri
            if s["pipeline"] == "synthetic" and not s.get("source_uri") and src:
                s["source_uri"] = src
        synthetic_config.apply_policy(ml["synthetic"], env.policies.get("synthetic") or {}, env.name,
                                      env.approver_groups if env.manifest_source else None, problems)
    _feature_policy(ml, env, tenant, problems)
    _retraining_policy(ml["retraining"], approvals, env, problems)
    # Who makes a registered version the default: the pipeline (on_approval) or a person (`model approve`).
    registry = ml["registry"]
    if registry["set_default"] is None:
        registry["set_default"] = "on_approval" if approvals.get("promote_model") == "auto" else "manual"
    elif registry["set_default"] == "on_approval" and approvals.get("promote_model") != "auto":
        problems.append(f"registry.set_default on_approval lets the pipeline promote models, but "
                        f"approvals.promote_model is {approvals.get('promote_model')} in {env.name}: remove "
                        "registry.set_default (it then follows the approval) or set promote_model: auto")

    steps = steps_spec(eff)
    ray = _ray(steps, tenant, env, problems)

    # Where tables are written (section 7.4): BigQuery, Parquet on GCS, or both.
    outputs_cfg = eff.get("outputs", {})
    store = outputs_cfg.get("store", "bigquery")
    gcs_cfg = {"format": "parquet", "external_tables": True, **outputs_cfg.get("gcs", {})}
    gcs_uri = None
    if store in ("gcs", "both"):
        if not resources.get("outputs_uri"):
            problems.append(f"outputs.store {store} needs an outputs location: tenant {tenant!r} has no outputs_uri "
                            "in the manifest (ask the platform team)")
        else:
            gcs_uri = f"{resources['outputs_uri']}{meta['name']}/"
    sql_reads = [what for what, on in (("monitoring.performance", ml["monitoring"]["performance"]["enabled"]),
                                       ("retraining cooldown", ml["retraining"]["enabled"]),
                                       ("pipewright monitor status", bool(ml["monitoring"]["output_table"])))
                 if on]
    if store == "gcs" and not gcs_cfg["external_tables"] and sql_reads:
        problems.append(f"outputs.gcs.external_tables is false, but {', '.join(sql_reads)} read tables with SQL: "
                        "keep external tables on, or use store: both")
    outputs = {"store": store, "gcs": {**gcs_cfg, "uri": gcs_uri},
               "tables": {n: [{"name": f["name"], "type": f["type"].upper(),
                               "mode": (f.get("mode") or "NULLABLE").upper()} for f in t.fields]
                          for n, t in tables.items()}}

    if problems:
        raise ConfigError(problems, source=f"{path} ({env.name})")

    return ResolvedUseCase(
        name=meta["name"], environment=env.name, owner=meta["owner"], tenant=tenant, resources=resources,
        directory=directory, pipelines=pipelines, components_dir=components_dir, requirements=requirements,
        framework="bqml" if bqml_spec else eff.get("framework", "pytorch"), triggers=triggers,
        parameters=parameters, steps=steps,
        tables=tables, retention_days=bq.get("retention_days"), model=model, approvals=approvals,
        labels=meta.get("labels", {}), contacts=meta.get("contacts", []), source=str(path),
        description=meta.get("description", ""), objective=meta.get("objective", ""), ml=ml,
        tests_command=(eff.get("tests") or {}).get("command"), sources=assembled.sources,
        platform={"project": env.project, "region": env.region, "launcher_url": env.launcher.get("url"),
                  "bigquery_dataset": resources.get("bigquery_dataset"), "tenant": tenant,
                  "events_topic": env.observability.get("events_topic"),
                  "run_requests_topic": env.launcher.get("run_requests_topic"),
                  "kms_key": env.security.get("kms_key"),
                  "orchestrator": env.orchestrator,
                  **({"kubernetes_namespace": (env.kubernetes or {}).get("namespace") or "kubeflow"}
                     if env.orchestrator == "kfp" else {}),
                  "bigquery_kms_key": env.security.get("bigquery_kms_key"),
                  **({"ray": ray} if ray else {}),
                  # read_source refuses feature tables (and groups' tables) outside them
                  **({"feature_datasets": fd} if (fd := ((env.tenants.get(tenant) or {}).get("features") or {})
                                                  .get("datasets")) is not None else {})},
        outputs=outputs, ray_requirements=ray_requirements, synthetic_requirements=synthetic_requirements,
    )


def _online_features(ml: dict, env: Environment, tenant: str, problems: list[str]) -> dict | None:
    """model.serving.features online (FR-51): every model feature from a feature view of the tenant's online store,
    by one entity key. Carried in ml_config, so the training step packages it with the model (serving.json)."""
    where = "model.serving.features online"
    ff = (ml.get("data") or {}).get("features_from")
    if not ff:
        problems.append(f"{where}: needs data.features_from (the views serve the same features training read)")
        return None
    store = (((env.tenants.get(tenant) or {}).get("features") or {}).get("online_store"))
    if not store:
        problems.append(f"{where}: tenant {tenant!r} has no online store in {env.name} "
                        "(tenants.<tenant>.features.online_store, set by the platform team)")
    key = (ff.get("entity") or {}).get("columns") or []
    if len(key) != 1:
        problems.append(f"{where}: needs one entity column (data.features_from.entity.columns has {len(key)})")
    views, served = [], set()
    for i, src in enumerate(ff.get("sources") or []):
        view = src.get("online_view") or src.get("feature_group")
        if not view:
            problems.append(f"{where}: data.features_from.sources[{i}] reads a table: name its feature view "
                            "(online_view)")
            continue
        prefix = src.get("prefix", "")
        names = {prefix + f: f for f in src.get("features") or []}
        served |= set(names)
        views.append({"view": view, "features": names})
    include = ((ml.get("features") or {}).get("include")) or []
    missing = [f for f in include if f not in served]
    if missing:
        problems.append(f"{where}: model features no view serves: {', '.join(missing)} (with online features, "
                        "every feature comes from a feature view)")
    if not store or len(key) != 1:
        return None
    return {"store": store, "key": key[0], "views": views}


def steps_spec(eff: dict) -> dict:
    return deep_merge(eff.get("resources_per_step", {}), eff.get("steps", {}))


def _bqml_pipelines(eff: dict, pipelines: dict, problems: list[str]) -> dict | None:
    """A `bqml` section gets the training and scoring pipelines Pipewright provides, on its BigQuery ML steps:
    the team's own pipelines, components and framework are refused next to it."""
    spec = eff.get("bqml")
    if not spec:
        return None
    from pipewright import bqml

    for key in ("pipeline", "pipelines", "components", "framework"):
        if key in eff:
            problems.append(f"{key}: Pipewright provides the pipelines and steps of a bqml use case; remove {key}")
    pipelines.clear()
    pipelines["training"] = PipelineRef("training", bqml.PIPELINES_FILE, bqml.TRAINING_FUNCTION)
    pipelines["inference"] = PipelineRef("inference", bqml.PIPELINES_FILE, bqml.SCORING_FUNCTION)
    return spec


def _bqml_section(spec: dict, ml: dict, model: dict, directory: Path, env: Environment, tables: dict,
                  pipelines: dict, triggers: dict, steps: dict, name: str, tenant: str,
                  problems: list[str]) -> None:
    """ml["bqml"], and what a BigQuery ML use case cannot use: gs:// sources (the data is in BigQuery), Ray steps,
    KServe serving."""
    from pipewright.bqml import config as bqml_config

    ml["bqml"] = bqml_config.resolve(spec, ml, directory, env.policies.get("bqml") or {}, tables,
                                     "inference" in pipelines, env.name, problems)
    ml["bqml"]["registry_id"] = model["name"]
    # Each run's evaluation record, in the use case's pipeline root (publish.pipeline_root): `model approve`,
    # `deploy-model` and evidence find a BigQuery ML version's record from its pipewright-run label.
    ml["bqml"]["records"] = f"gs://{env.artifacts_bucket}/pipeline-root/{tenant}/{name}/bqml/"
    if triggers["landing"]:
        problems.append("triggers.landing delivers gs:// files; a bqml use case reads bq:// tables or views: use a "
                        "schedule or `pipewright run --source-uri bq://...`")
    named = [(f"triggers.schedules[{s['name']}].source_uri", s.get("source_uri")) for s in triggers["schedules"]]
    for where, uri in [*named, ("retraining.source_uri", ml["retraining"].get("source_uri"))]:
        if uri and uri.startswith("gs://"):
            problems.append(f"{where}: a bqml use case reads bq:// tables or views")
    if any(isinstance(cfg, dict) and "ray" in cfg for cfg in steps.values()):
        problems.append("steps: Ray steps are not part of a bqml use case (training runs in BigQuery)")
    if model["serving"]["enabled"] and model["serving"]["target"] != "vertex":
        problems.append("model.serving.target: a bqml model is served from Model Registry (target vertex)")


def _synthetic_pipeline(spec: dict, pipelines: dict, directory: Path, problems: list[str]) -> Path | None:
    """synthetic.enabled with the learned tier adds the pipeline Pipewright provides (the contract tier needs no
    pipeline: it runs anywhere, from the CLI). Returns synthetic.requirements, resolved."""
    from pipewright.synthetic import PIPELINE, PIPELINE_FILE, PIPELINE_FUNCTION

    if not spec.get("enabled") or spec.get("tier", "learned") != "learned":
        return None
    if PIPELINE in pipelines:
        problems.append("pipelines.synthetic: Pipewright provides the synthetic pipeline when synthetic.enabled is "
                        "set; remove pipelines.synthetic")
    else:
        pipelines[PIPELINE] = PipelineRef(PIPELINE, PIPELINE_FILE, PIPELINE_FUNCTION)
    if not spec.get("requirements"):
        return None
    path = (directory / spec["requirements"]).resolve()
    if not path.is_file():
        problems.append(f"synthetic.requirements not found: {spec['requirements']}")
    return path


def _ray(steps: dict, tenant: str, env: Environment, problems: list[str]) -> dict | None:
    """Ray steps against the platform's limits: {mode, ray_version, python_version, steps}, or None without any."""
    from pipewright.ray_clusters import settings as ray_settings

    if not any(isinstance(cfg, dict) and "ray" in cfg for cfg in steps.values()):
        return None
    settings = ray_settings.platform_settings(env.ray or None, (env.tenants.get(tenant) or {}).get("ray"),
                                              private=bool(env.security.get("network")))
    clusters, found = ray_settings.resolve_steps(steps, settings, orchestrator=env.orchestrator)
    problems += found
    out = {"mode": settings["mode"], "ray_version": settings["ray_version"],
           "python_version": settings["python_version"], "steps": clusters}
    if settings["mode"] == "standing":
        out["standing"] = {k: (settings.get("standing") or {}).get(k) for k in ("cluster", "address", "image")}
    return out


def _feature_policy(ml: dict, env: Environment, tenant: str, problems: list[str]) -> None:
    """Feature tables against the platform: a tenant's feature tables must be in the datasets the manifest lists for
    it (tenants.<t>.features.datasets), and with features.required every model feature comes from a feature table."""
    from pipewright.config.ml import feature_columns

    spec = ml["data"].get("features_from")
    allowed = ((env.tenants.get(tenant) or {}).get("features") or {}).get("datasets")
    if spec and allowed is not None:
        for i, src in enumerate(spec["sources"]):
            if "table" not in src or sources.problem(src["table"]):
                continue                     # a feature group's table: doctor and read_source check it
            dataset = sources.bq_table(src["table"], env.project).rsplit(".", 1)[0]
            if dataset not in allowed:
                problems.append(f"data.features_from.sources[{i}].table: {dataset} is not a feature dataset of tenant "
                                f"{tenant!r} in {env.name} ({', '.join(allowed) or 'none listed'}); ask the platform "
                                "team")
    if (env.features or {}).get("required"):
        if not spec:
            problems.append(f"{env.name} requires features from feature tables (features.required): add "
                            "data.features_from")
            return
        outside = [c for c in ml["features"]["include"] or [] if c not in feature_columns(spec)]
        if outside:
            problems.append(f"{env.name} requires every model feature from data.features_from (features.required); "
                            f"not from a feature table: {', '.join(outside)}")


def _retraining_policy(retraining: dict, approvals: dict, env: Environment, problems: list[str]) -> None:
    """Retrain decisions against the platform: allowed modes, required approver groups, the snooze cap."""
    policy = env.policies.get("retraining") or {}
    mode = retraining["mode"]
    allowed = policy.get("allowed_modes")
    if allowed and mode not in allowed:
        problems.append(f"retraining.mode {mode} is not allowed in {env.name} (platform policy: {', '.join(allowed)})")
    required = list(dict.fromkeys([*policy.get("required_approvers", []), *retraining["approvals"]["required"]]))
    retraining["approvals"]["required"] = required
    if mode == "off":
        return
    if env.manifest_source:
        for group in required:
            if group not in env.approver_groups:
                known = ", ".join(sorted(env.approver_groups)) or "none"
                problems.append(f"retraining.approvals.required: {group!r} is not an approver group in {env.name} "
                                f"(registered: {known})")
    cap = policy.get("max_snooze_days")
    if cap and retraining["snooze"]["max_days"] > cap:
        problems.append(f"retraining.snooze.max_days {retraining['snooze']['max_days']} is above the {env.name} "
                        f"limit of {cap} days")
    if mode == "candidate" and approvals.get("promote_model") != "manual":
        problems.append("retraining.mode candidate trains without a decision, so a person must promote: set "
                        "approvals.promote_model: manual")


def _ml_keys() -> tuple[str, ...]:
    from pipewright.config.ml import SECTIONS

    return SECTIONS
