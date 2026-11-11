"""pipewright command line."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from pipewright import __version__
from pipewright.config import discover_usecases, load_environment, resolve_usecase, run_in
from pipewright.errors import ConfigError


def _selected(root: Path, names: list[str] | None, env: str | None = None) -> dict[str, Path]:
    """The use cases a command works on: those named, or all of them. With env, all of them leaves out those whose
    spec.run_in names other environments (Pipewright 1.1), with a note; a named one is refused when resolved."""
    found = discover_usecases(root)
    if not names:
        if not found:
            raise ConfigError(f"no use cases found under {root / 'usecases'}")
        if env is None:
            return found
        out = {}
        for name, path in found.items():
            envs = run_in(path)
            if envs and env not in envs:
                print(f"SKIP  {name}: runs only in {', '.join(envs)} (spec.run_in)", file=sys.stderr)
            else:
                out[name] = path
        return out
    missing = [n for n in names if n not in found]
    if missing:
        raise ConfigError(f"use case(s) not found: {', '.join(missing)} (looked in {root / 'usecases'})")
    return {n: found[n] for n in names}


def _ignored_keys(env) -> str | None:
    if not env.manifest_ignored:
        return None
    from pipewright import __version__

    return (f"the manifest has keys this Pipewright ({__version__}) does not know, ignored: "
            f"{', '.join(env.manifest_ignored)}. Upgrade to use them")


def cmd_context(args) -> int:
    """`pipewright context build|check` (Pipewright 1.3.0): the context pack coding agents read on start."""
    from pipewright.ai import context, identity

    root = Path(args.root)
    env = load_environment(root, args.env)
    if not identity.hosted(env):             # a pack is names and rules: agents of a hosted server need it too
        identity.allowed(env)
    if args.context_action == "build":
        for p in context.build(root, env):
            print(f"wrote {p.relative_to(root).as_posix()}")
        return 0
    ok, message = context.check(root, env)
    print(("OK    " if ok else "FAIL  ") + message)
    return 0 if ok else 1


def cmd_mcp(args) -> int:
    """`pipewright mcp` (Pipewright 1.3.0): the local MCP tool server, read and verify only."""
    from pipewright.ai import identity, mcp

    if args.print_config:
        env = load_environment(Path(args.root), args.env)
        config = mcp.client_config(args.env, args.tenant, env, direct=args.direct, agent=args.agent)
        if "proxy" not in config:            # a hosted tool server serves even where local tools are forbidden
            identity.allowed(env)
        print(json.dumps(config, indent=2))
        return 0
    mcp.serve(Path(args.root), args.env, args.tenant, args.agent)
    return 0


def cmd_review(args) -> int:
    """`pipewright review` (Pipewright 1.3.0): the review bundle, Markdown for a pull request."""
    from pipewright.ai import review

    root = Path(args.root)
    text = review.bundle(root, load_environment(root, args.env), base=args.base)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
        print(f"wrote {args.output}")
    else:
        print(text, end="")
    return 0


def cmd_validate(args) -> int:
    root = Path(args.root)
    env = load_environment(root, args.env)
    if note := _ignored_keys(env):
        print(f"WARN  {note}\n")
    selected = _selected(root, args.usecase, args.env)
    failed = 0
    for name, path in selected.items():
        try:
            uc = resolve_usecase(path, env)
            from pipewright.checks import kfp_version, ray_version

            pin = kfp_version.check(uc)
            if not pin.ok:
                raise ConfigError(f"kfp version: {pin.message}", source=f"{path} ({env.name})")
            ray_pin = ray_version.check(uc)
            if not ray_pin.ok:
                raise ConfigError(f"ray version: {ray_pin.message}", source=f"{path} ({env.name})")
            from pipewright.build import allowlist

            if refused := allowlist.check(uc, env):                 # policies.images (FR-54)
                raise ConfigError(refused, source=f"{path} ({env.name})")
            print(f"OK    {name} ({env.name})")
            for warning in uc.warnings:
                print(f"WARN  {name}: {warning}")
        except ConfigError as e:
            failed += 1
            print(f"FAIL  {name} ({env.name})\n{e}\n")
    print(f"\n{len(selected) - failed} valid, {failed} with problems")
    return 1 if failed else 0


def cmd_resolve(args) -> int:
    root = Path(args.root)
    env = load_environment(root, args.env)
    for name, path in _selected(root, args.usecase, args.env).items():
        doc = resolve_usecase(path, env).to_dict()
        print(f"# resolved {name} for environment {env.name}")
        print(yaml.safe_dump(doc, sort_keys=False, default_flow_style=False).rstrip())
        print("---")
    return 0


def cmd_doctor(args) -> int:
    from pipewright.checks import doctor

    root = Path(args.root)
    env = load_environment(root, args.env)
    usecases = [resolve_usecase(path, env) for path in _selected(root, args.usecase, args.env).values()]
    report = doctor.run(env, usecases, compile_check=not args.no_compile)
    print(report.render())
    return 1 if report.failed else 0


def cmd_tables_sync(args) -> int:
    from pipewright.tables import sync as tables_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    failed = 0
    for path in _selected(root, args.usecase, args.env).values():
        uc = resolve_usecase(path, env)
        results = tables_sync.sync(uc, env, dry_run=args.dry_run)
        print(tables_sync.render(results, uc, args.dry_run))
        failed += sum(1 for r in results if r.error or r.plan.action == "blocked")
    return 1 if failed else 0


def cmd_schedules_sync(args) -> int:
    from pipewright.schedules import sync as schedules_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    for path in _selected(root, args.usecase, args.env).values():
        uc = resolve_usecase(path, env)
        print(schedules_sync.render(schedules_sync.sync(uc, env, dry_run=args.dry_run), uc, args.dry_run))
    return 0


def cmd_alerts_sync(args) -> int:
    from pipewright.alerts import sync as alerts_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    failed = 0
    selected = _selected(root, args.usecase, args.env)
    for path in selected.values():
        uc = resolve_usecase(path, env)
        plan = alerts_sync.sync(uc, env, dry_run=args.dry_run)
        print(alerts_sync.render(uc, plan, args.dry_run))
        failed += len(plan.errors)
        failed += _health_sync(uc, env, args.dry_run)
    if not args.usecase:        # a whole-repo sync: the policies of use cases that are gone (Pipewright 1.1)
        deleted, kept, errors = alerts_sync.prune(env, set(selected), dry_run=args.dry_run)
        if deleted or kept or errors:
            print(f"Alerts of use cases not in this repo ({env.name})")
        for name in deleted:
            print(f"  {'PLAN' if args.dry_run else 'DONE'}  {name}: {'would delete' if args.dry_run else 'deleted'} "
                  "(the use case is not in this repo and not released here)")
        for name in kept:
            print(f"  OK    {name}: kept (released in {env.name}, from another repo)")
        for e in errors:
            print(f"  FAIL  {e}")
        failed += len(errors)
    return 1 if failed else 0


def _health_sync(uc, env, dry_run: bool = False) -> int:
    """1.4.3 (FR-98): capture on the use case's endpoint (Agent Platform) and its health policies for what is
    deployed now. Nothing when neither is set. Returns the number of errors."""
    from pipewright.gcp import Gcp

    gcp, failed = Gcp(), 0
    if "capture" in (uc.model.get("serving") or {}) and uc.model["serving"].get("target") != "kserve":
        from pipewright.models.deploy import find_endpoint
        from pipewright.serving import capture

        endpoint = find_endpoint(uc, env, gcp)
        if endpoint:
            try:
                capture.sync_endpoint(uc, env, gcp, endpoint)
            except ConfigError as e:
                print(f"  capture: {e}")
                failed += 1
    if "health" not in (uc.ml.get("monitoring") or {}):
        return failed
    from pipewright.serving import health

    try:
        targets = health.deployed_targets(uc, env, gcp)
    except ConfigError as e:
        print(f"  health: deployed versions not readable: {e}")
        return 1
    plan = health.sync(uc, env, targets, gcp, dry_run=dry_run)
    print(health.render(plan))
    return failed + len(plan.errors)


def cmd_new_usecase(args) -> int:
    from pipewright.scaffold import new_usecase as scaffold

    example = args.from_example or ("minimal" if args.from_bq else "churn")
    data_yaml = raw_schema = None
    notes = []
    if args.from_bq:
        from pipewright.gcp import Gcp
        from pipewright.scaffold import from_bq

        if example != "minimal":
            raise ConfigError(f"--from-bq drafts the minimal example's data contract; the {example} example's "
                              "evaluation and scoring use its own columns")
        if not args.label:
            raise ConfigError("--from-bq needs --label: the column the model predicts")
        fields, values = from_bq.read_table(Gcp(), args.from_bq, args.label)
        data_yaml, raw_schema, notes = from_bq.draft(fields, args.label, values, args.from_bq)
    dest = scaffold.new_usecase(Path(args.root), args.name, args.tenant, args.owner, example, args.table_prefix,
                                data_yaml=data_yaml, raw_schema=raw_schema)
    print(f"created {dest} from example {example}" + (f", data contract drafted from {args.from_bq}"
                                                      if args.from_bq else ""))
    for note in notes:
        print(f"  {note}")
    print("next: edit usecase.yaml, then  pipewright validate / doctor / tables sync / release  --env <env>")
    if args.from_bq:
        project, dataset, table = from_bq.parse_ref(args.from_bq)
        print(f"then train on the table:  pipewright run --env <env> --usecase {args.name} "
              f"--source-uri bq://{project}.{dataset}.{table}")
    return 0


def cmd_schemas(args) -> int:
    from pipewright.config import reference

    root = Path(args.root)
    written = reference.write_editor_schemas(root)
    print(f"wrote {len(written)} schemas to {root / reference.SCHEMAS_DIR}")
    files = sorted(root.glob("environments/*.yaml")) + sorted((root / "usecases").rglob("*.yaml"))
    changed = [f for f in files if reference.kind_of(root, f) and reference.add_modeline(root, f)]
    print(f"schema line added or updated in {len(changed)} of {len(files)} YAML files")
    return 0


def cmd_events(args) -> int:
    """Every event Pipewright sends (1.4.3): by persona or family, with the Pub/Sub subscription filter that receives
    exactly those. No cloud access."""
    from pipewright.observability import catalogue

    try:
        chosen = catalogue.select(args.persona, args.family)
        if args.persona and not args.family:
            flt = catalogue.persona_filter(args.persona)
        else:
            flt = catalogue.subscription_filter(chosen) if chosen else ""
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if args.family and args.family not in catalogue.FAMILIES:
        print(f"error: unknown family {args.family!r} (known: {', '.join(catalogue.FAMILIES)})", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps({"filter": flt, "events": [
            {"type": e.type, "family": e.family, "source": e.source, "when": e.when, "personas": list(e.personas),
             "since": e.since} for e in chosen]}, indent=2))
        return 0
    if args.filter:
        print(flt)
        return 0
    if not args.persona and not args.family:
        print("personas (--persona):")
        for k, v in catalogue.PERSONAS.items():
            print(f"  {k:<11} {v}")
        print()
    width = max((len(e.type) for e in chosen), default=10)
    family = None
    for e in chosen:
        if e.family != family:
            family = e.family
            print(f"{family}: {catalogue.FAMILIES[family]}")
        print(f"  {e.type:<{width}}  {e.when}  [{e.source}; {', '.join(e.personas)}]")
    print()
    print(f"{len(chosen)} event types. Pub/Sub subscription filter: {flt}")
    return 0


def cmd_explain(args) -> int:
    from pipewright.config import reference

    if args.list or not args.key:
        print("\n".join(reference.all_keys()))
        return 0
    print(reference.render(reference.explain(args.key)))
    return 0


def cmd_deploy_model(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.models import deploy as deploy_model

    env = load_environment(Path(args.root), args.env)
    uc = resolve_usecase(_selected(Path(args.root), [args.usecase])[args.usecase], env)
    gcp = Gcp()
    if uc.model["serving"]["target"] == "kserve":
        from pipewright.models import kserve

        if args.undeploy:
            kserve.undeploy(uc, env, gcp)
            _remove_lookup_service(uc, env, gcp, args)
            _health_sync(uc, env)
            return 0
        if args.promote or args.undeploy_canary:
            raise ConfigError("model.serving.target kserve has no canary to promote or remove (KServe in Standard "
                              "mode cannot split traffic)")
        if not args.dry_run:
            deploy_model.check_approval(uc, gcp.identity(), args.approve)
        kserve.deploy(uc, env, gcp, canary=args.canary, dry_run=args.dry_run)
        if not args.dry_run:
            _health_sync(uc, env)          # 1.4.3 (FR-98): the deployed version's health policies
        return 0
    creds = gcp.session.credentials
    if args.undeploy:
        deploy_model.undeploy(uc, env, creds)
        _remove_lookup_service(uc, env, gcp, args)
        _health_sync(uc, env)
        return 0
    if args.promote or args.undeploy_canary:
        deploy_model.check_approval(uc, gcp.identity(), args.approve)
        (deploy_model.promote if args.promote else deploy_model.undeploy_canary)(uc, env, creds)
        _print_how_to_call(uc, env, gcp)
        _health_sync(uc, env)
        return 0
    p = deploy_model.plan(uc, env, creds)
    print(f"model {p.model} default version v{p.version} -> endpoint {p.endpoint} ({p.machine}, replicas "
          f"{p.replicas['min']}-{p.replicas['max']})"
          + ("  [already live]" if p.already_deployed else "")
          + (f"; replaces {', '.join(p.other_versions)}" if p.other_versions else ""))
    print("evaluation gates: " + ("pass" if not p.gate_problems else "FAIL\n  " + "\n  ".join(p.gate_problems)))
    if uc.ml.get("online_features"):
        print("online store: " + ("FAIL\n  " + p.store_problem if p.store_problem else "this environment's"))
    if args.dry_run:
        return 0
    deploy_model.check_approval(uc, gcp.identity(), args.approve)
    deploy_model.deploy(uc, env, creds, canary=args.canary)
    _print_how_to_call(uc, env, gcp)
    _health_sync(uc, env)                  # 1.4.3 (FR-98): the deployed version's health policies
    return 0


def _remove_lookup_service(uc, env, gcp, args) -> None:
    """1.4.2 (design item 8): undeploy also removes the lookup service, unless --keep-lookup-service."""
    if getattr(args, "keep_lookup_service", False):
        return
    from pipewright.online import launcher as online_launcher

    try:
        online_launcher.remove_from_cli(uc, env, gcp=gcp)
    except Exception as e:  # noqa: BLE001 - the models are undeployed; the service is the launcher's to remove
        print(f"warning: the lookup service was not removed ({e}); remove it with gcloud run services delete")


def _print_how_to_call(uc, env, gcp) -> bool:
    """Pipewright 1.2.0 (FR-60): the endpoint ID, predict URL, caller's role and a sample request."""
    from pipewright.models import deploy as deploy_model

    resource = deploy_model.find_endpoint(uc, env, gcp)
    if resource:
        print(deploy_model.how_to_call(uc, env, resource))
    return bool(resource)


def cmd_model_endpoint(args) -> int:
    from pipewright.gcp import Gcp

    uc, env = _one(args)
    if uc.model["serving"]["target"] == "kserve":
        from pipewright.models import kserve

        text = kserve.KServe(uc, env, Gcp()).how_to_call()
        if text is None:
            raise ConfigError(f"{uc.name} is not deployed on KServe in {env.name}: run `pipewright deploy-model`")
        print(text)
        return 0
    if not _print_how_to_call(uc, env, Gcp()):
        raise ConfigError(f"{uc.name} has no endpoint in {env.name}: run `pipewright deploy-model`")
    return 0


def _one(args):
    env = load_environment(Path(args.root), args.env)
    return resolve_usecase(_selected(Path(args.root), [args.usecase])[args.usecase], env), env


def cmd_pipelines_ui(args) -> int:
    from pipewright import pipelines_ui

    pipelines_ui.serve(load_environment(Path(args.root), args.env), port=args.port)
    return 0


def cmd_model_predict(args) -> int:
    import json

    from pipewright.gcp import Gcp

    uc, env = _one(args)
    text = Path(args.instances).read_text(encoding="utf-8") if Path(args.instances).is_file() else args.instances
    try:
        instances = json.loads(text)
    except ValueError as e:
        raise ConfigError(f"--instances is not JSON (a list of instances, or a file with one): {e}") from None
    instances = instances.get("instances", instances) if isinstance(instances, dict) else instances
    gcp = Gcp()
    if uc.model["serving"]["target"] == "kserve":
        from pipewright.models import kserve

        result = kserve.KServe(uc, env, gcp).predict(instances)
    else:
        from pipewright.gcp import error_text
        from pipewright.models.deploy import endpoint_name

        api = f"https://{env.region}-aiplatform.googleapis.com/v1/projects/{env.project}/locations/{env.region}"
        found = gcp.get(f"{api}/endpoints", filter=f'display_name="{endpoint_name(uc)}"')
        endpoints = (found.body or {}).get("endpoints") or [] if found.ok else []
        if not endpoints:
            raise ConfigError(f"{endpoint_name(uc)} does not exist: run `pipewright deploy-model`")
        from pipewright.serving import endpoints as tf_endpoints

        if tf_endpoints.is_private(env, uc.name):     # FR-92: its DNS name, inside the network only
            print(json.dumps(tf_endpoints.predict_private(
                gcp, tf_endpoints.predict_url(env, uc.name, endpoints[0]["name"]), instances, env, uc.name), indent=2))
            return 0
        r = gcp.post(f"https://{env.region}-aiplatform.googleapis.com/v1/{endpoints[0]['name']}:predict",
                     {"instances": instances})
        if not r.ok:
            raise ConfigError(f"prediction failed: {error_text(r)}")
        result = r.body
    print(json.dumps(result, indent=2))
    return 0


def cmd_serving_pools(args) -> int:
    """`pipewright serving pools plan | apply | list` (FR-105): only pools the platform declared, managed_by ci."""
    import json

    from pipewright.config import load_environment
    from pipewright.gcp import Gcp
    from pipewright.serving import pools, prices

    env = load_environment(Path(args.root), args.env)
    declared = pools.declared(env)
    if args.pool and args.pool not in declared:
        raise ConfigError(f"{env.name} has no pool {args.pool!r} (manifest serving.pools: "
                          f"{', '.join(sorted(declared)) or 'none'})")
    if not declared:
        print(f"{env.name} declares no serving pools (manifest serving.pools): every use case deploys on its own "
              "machines")
        return 0
    gcp = Gcp()
    if args.pools_action == "apply":
        steps = pools.apply(gcp, env, only=args.pool)
        if args.json:
            print(json.dumps(steps, indent=2))
        return 0
    rows = []
    for name, pool in sorted(declared.items()):
        if args.pool not in (None, name):
            continue
        problems, warnings = pools.pool_problems(env, name, pool)
        row = {"pool": name, "tenant": pool.get("tenant"), "machine": pool["machine"],
               "replicas": f"{pool['min_replicas']}-{pool['max_replicas']}", "network": pool["network"],
               "managed_by": pool["managed_by"], "cost": prices.pool_cost(pool), "problems": problems,
               "notes": [w for w in warnings if "a month" not in w]}
        if args.pools_action == "plan":
            step = next((s for s in pools.plan(gcp, env) if s["pool"] == name), {})
            row["action"], row["detail"] = step.get("action"), step.get("detail")
        else:
            row["models"] = pools.deployed_models(gcp, env, name)
        rows.append(row)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 1 if any(r["problems"] for r in rows) else 0
    for r in rows:
        head = f"{r['pool']} ({r['tenant']}): {r['machine']}, {r['replicas']} machines, {r['network']} endpoints, " \
               f"managed by {r['managed_by']}"
        print(head)
        print(f"  cost      {r['cost']}")
        if "action" in r:
            print(f"  apply     {r['action']}" + (f": {r['detail']}" if r.get("detail") else ""))
        if "models" in r:
            names = sorted({m.get('usecase') or m['endpoint'] for m in r["models"]})
            print(f"  models    {len(r['models'])}" + (f" ({', '.join(names)})" if names else ""))
        for n in r["notes"]:
            print(f"  note      {n}")
        for p in r["problems"]:
            print(f"  PROBLEM   {p}")
    return 1 if any(r["problems"] for r in rows) else 0


def cmd_model_list(args) -> int:
    import contextlib

    from pipewright.gcp import Gcp
    from pipewright.models import registry

    uc, env = _one(args)
    _quiet_agent_platform_sdk()              # its "Getting versions for ..." line (seen re-running the book on 1.4.2)
    with contextlib.redirect_stdout(sys.stderr) if args.json else contextlib.nullcontext():
        rows = registry.versions(uc, env, Gcp().session.credentials)
    if args.json:                            # Pipewright 1.2.0 (FR-86): stable field names
        print(json.dumps({"usecase": uc.name, "environment": env.name, "model": uc.model["name"],
                          "versions": [{"version": v.version_id, "default": v.default, "aliases": v.aliases,
                                        "description": v.description, "created": v.created, "labels": v.labels,
                                        "gates": v.gates or None,
                                        "approved": not registry.pending_approval(v.labels)} for v in rows]},
                         indent=2, default=str))
        return 0
    print(registry.render(uc, rows))
    return 0


def cmd_features_register(args) -> int:
    """Phase 6 (FR-75): the data team registers feature groups from a spec file."""
    from pipewright.data import feature_groups
    from pipewright.gcp import Gcp

    env = load_environment(Path(args.root), args.env)
    specs = feature_groups.load_spec(Path(args.file))
    done = feature_groups.register(Gcp(), env, specs, args.names or None, dry_run=args.dry_run)
    if args.json:
        print(json.dumps(done, indent=2))
    return 0


def _online_source(args):
    """(ml_config, environment, tenant, usecase, release) for the online sync: the live release's configuration
    (--manifest, as the scheduled job runs), or the checkout's (--root and --env)."""
    from pipewright.gcp import Gcp

    gcp = Gcp()
    if getattr(args, "request", None):           # the scheduled sync: what the launcher handed the tenant's job
        from pipewright.config.environment import environment_from_dict
        from pipewright.gcp.storage import gcs_read

        bucket, _, name = args.request[len("gs://"):].partition("/")
        text, _ = gcs_read(gcp, bucket, name)
        if not text:
            raise ConfigError(f"no sync request at {args.request}")
        req = json.loads(text)
        if req.get("usecase") != args.usecase:
            raise ConfigError(f"the sync request is for {req.get('usecase')}, not {args.usecase}")
        env = environment_from_dict(req["manifest"])
        return gcp, req["ml_config"] or {}, env, req["tenant"], req["usecase"], req.get("release")
    if args.manifest:
        from pipewright.config.environment import environment_from_manifest
        from pipewright.release import service as release

        env = environment_from_manifest(args.manifest)
        pointer, _ = release.read_pointer(gcp, env, args.usecase)
        if not pointer:
            raise ConfigError(f"{args.usecase} has no release in {env.name}")
        contract = release.read_contract(gcp, env, pointer.contract)
        ml = contract.pipeline("training").static_parameters.get("ml_config") or {}
        return gcp, ml, env, contract.tenant, args.usecase, contract.release_id
    if not args.env:
        raise ConfigError("give --env (with the MLOps checkout) or --manifest (the environment manifest)")
    env = load_environment(Path(args.root), args.env)
    path = _selected(Path(args.root), [args.usecase])[args.usecase]
    uc = resolve_usecase(path, env)
    return gcp, uc.ml_config(), env, uc.tenant, uc.name, None


def cmd_features_sync(args, load: bool = True) -> int:
    """Pipewright 1.4.2 (FR-104): the latest feature values per entity into the use case's online store (bigquery,
    cloudsql), or with `features export` only the Parquet generation and its manifest."""
    from pipewright.online import sync

    gcp, ml, env, tenant, usecase, release_id = _online_source(args)
    online = ml.get("online_features") or {}
    if load and not online:
        raise ConfigError(f"{usecase} has no online features to sync: model.serving.features is request (set it to "
                          "online, with an online_view on the feature source)")
    if load and online.get("store") and online.get("type", "bigtable") == "bigtable":
        # FR-110: the tenant's Bigtable store (1.1) syncs itself on its schedule; this starts one now and waits
        out = sync.sync_bigtable_views(online, gcp)
        if args.json:
            print(json.dumps(out, indent=2, default=str))
        return 0
    if not load and not online.get("export"):
        from pipewright.online import config as online_config

        online = {**online, "export": {"uri": online_config.export_prefix(env, tenant, usecase), "keep": 3,
                                       "readers": []}}
        ml = {**ml, "online_features": online}
    import io

    out = sync.run(ml, env, tenant, usecase, gcp, release=release_id, load=load,
                   event_stream=None if getattr(args, "request", None) else io.StringIO())
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(f"{usecase}: generation {out['generation']}, {out['rows']} entities as of {out['as_of']}"
              + (f", manifest {out['manifest']}" if out.get("manifest") else "")
              + f" ({out['timings']['total_s']} s)")
    return 0


def cmd_features_online_setup(args) -> int:
    """FR-104: the Cloud SQL online store's one-time setup, as the platform's admin identity."""
    from pipewright.config.environment import environment_from_manifest
    from pipewright.gcp import Gcp
    from pipewright.online import setup, sync

    env = environment_from_manifest(args.manifest)
    gcp = Gcp()
    store = ((env.features or {}).get("online_store") or {}).get("cloudsql") or {}

    def connect_admin(database):
        return sync.connector(env, gcp, {"instance": store.get("instance", "::"), "database": database})()
    setup.run(env, gcp, connect_admin, dry_run=args.dry_run)
    return 0


def cmd_features_export(args) -> int:
    return cmd_features_sync(args, load=False)


def cmd_features_contract(args) -> int:
    """Pipewright 1.4.1 (FR-103): write or check the use case's feature contract from its live feature sources.
    Exit codes with --check: 0 the sources match the contract, 1 they differ, 2 they cannot be checked."""
    import yaml as _yaml

    from pipewright.gcp import Gcp
    from pipewright.models import definitions, integrity

    env = load_environment(Path(args.root), args.env)
    path = _selected(Path(args.root), [args.usecase])[args.usecase]
    uc = resolve_usecase(path, env)
    folder = path if path.is_dir() else path.parent
    features_from = (uc.ml.get("data") or {}).get("features_from")
    existing = uc.ml.get("feature_contract")
    gcp = Gcp()
    pol = integrity.policy(env)["features"]
    kms = (env.security or {}).get("bigquery_kms_key")
    if args.check:
        if not existing:
            print(f"{uc.name}: no feature_contract yet (`pipewright features contract --env {env.name} "
                  f"--usecase {uc.name}` writes one)")
            return 1
        try:
            problems, notes, _ = definitions.check(gcp, existing.get("sources") or [], features_from, env.project,
                                                   env.region, args.built_tables or pol["built_tables"], kms)
        except ConfigError as e:
            print(f"cannot check: {e}")
            return 2
        for n in notes:
            print(f"note: {n}")
        for p in problems:
            print(f"DIFFERS {p}")
        print(f"{uc.name}: feature contract {existing.get('version')} "
              + ("does NOT match the live sources" if problems else f"matches the live sources in {env.name}"))
        unreadable = [p for p in problems if "cannot be read" in p]
        if problems and len(unreadable) == len(problems):
            return 2                         # cannot check, not shown to differ
        return 1 if problems else 0
    out = definitions.build(gcp, features_from, env.project, env.region, existing, folder)
    section = out["section"]
    for c in out["changes"]:
        print(f"  {c}")
    for s in section["sources"]:
        if s["kind"] == "table" and not (s.get("sql_file") or s.get("sql_files") or s.get("scheduled_query")):
            print(f"note: {s['name']} is a table built by a query: add its sql_file (sql_files when several jobs "
                  "write it) or scheduled_query to check its SQL; until then it is checked by schema only")
    if existing and out["level"] is None:
        print(f"{uc.name}: feature contract {existing.get('version')} matches the live sources; nothing to write")
        return 0
    what = "new" if not existing else f"{out['level']} change: {existing.get('version')} -> {section['version']}"
    target = None
    for f in sorted(folder.rglob("*.y*ml")):
        try:
            doc = _yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        except (OSError, _yaml.YAMLError):
            continue
        if isinstance(doc, dict) and "feature_contract" in doc:
            target = f
            break
    target = target or folder / "config" / "feature_contract.yaml"
    if args.dry_run:
        print(f"{uc.name}: feature contract {section['version']} ({what}); would write {target}")
        return 0
    header = ("# The feature contract (Pipewright 1.4.1, FR-103): the approved definitions of the feature sources\n"
              "# this use case reads. Written by `pipewright features contract`; commit it with the use case.\n"
              "# Descriptions, sql_file and scheduled_query are yours to edit; the fingerprints are not.\n")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(header + _yaml.safe_dump({"feature_contract": section}, sort_keys=False, width=110),
                      encoding="utf-8")
    print(f"{uc.name}: wrote feature contract {section['version']} ({what}) to {target}")
    return 0


def cmd_report_fleet(args) -> int:
    """Phase 6 (FR-86): every use case in every environment on one read-only HTML page."""
    from pipewright.gcp import Gcp
    from pipewright.report import fleet

    _quiet_agent_platform_sdk()
    data = fleet.collect(Path(args.root), Gcp())
    Path(args.out).write_text(fleet.render(data, args.mask), encoding="utf-8")
    print(f"wrote {args.out}: {len(data['usecases'])} use cases in {len(data['envs'])} environments")
    return 0


def _integrity_env(args):
    """--env (with the MLOps repo) or --manifest (the renewer job and CI without a repo); the model to use case map
    from the repo when there is one, else from the registry's labels."""
    from pipewright.config.environment import environment_from_manifest

    if getattr(args, "manifest", None):
        return environment_from_manifest(args.manifest), None
    if not args.env:
        raise ConfigError("give --env (with the MLOps repo) or --manifest gs://<artifacts>/pipewright/environment.json")
    env = load_environment(Path(args.root), args.env)
    models = {}
    try:
        for name, path in _selected(Path(args.root), None, args.env).items():
            try:
                models[resolve_usecase(path, env).model["name"]] = name
            except ConfigError:
                continue
    except ConfigError:
        models = {}
    return env, models or None


def cmd_integrity_check(args) -> int:
    """1.4.1 (FR-101 item 6): every default and serving version against its approval; exit 0 unchanged, 1 changed or
    unapproved, 2 something could not be checked (a CI gate before deploy, or a schedule without the launcher)."""
    import contextlib

    from pipewright.gcp import Gcp
    from pipewright.models import watch

    env, models = _integrity_env(args)
    _quiet_agent_platform_sdk()
    with contextlib.redirect_stdout(sys.stderr):
        uses, notes = watch.check(Gcp(), env, models, {args.usecase} if args.usecase else None)
    code = watch.exit_code(uses)
    if args.json:
        print(json.dumps({"environment": env.name, "exit": code, "uses": [u.record() for u in uses], "notes": notes},
                         indent=2))
        return code
    marks = {"unchanged": "ok  ", "changed": "FAIL", "unapproved": "FAIL", "cannot_check": "?   "}
    for u in uses:
        print(f"{marks[u.status]} {u.usecase} v{u.version} ({u.where}): {u.status}"
              + (f" [{u.severity}]" if u.severity else ""))
        for problem in u.problems[:5]:
            print(f"       {problem}")
    for n in notes:
        print(f"note: {n}")
    verdict = {0: "every approval unchanged", 1: "NOT what was approved", 2: "some could not be checked"}[code]
    print(f"{len(uses)} checked in {env.name}: {verdict}")
    return code


def cmd_integrity_renew(args) -> int:
    """1.4.1 (FR-101 item 8): the renewer's run (a dedicated identity, a Cloud Run job, daily): keep every artifact an
    active version needs protected lock.days ahead; never shorten. Exit 1 when protection failed."""
    from pipewright.gcp import Gcp
    from pipewright.models import protection

    env, models = _integrity_env(args)
    _quiet_agent_platform_sdk()
    out = protection.renew(Gcp(), env, models, dry_run=args.dry_run, log=lambda m: print(m, file=sys.stderr))
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    elif args.dry_run:
        for a in out.get("artifacts") or []:
            print(f"would protect  {a['type']}  gs://{a['bucket']}/{a['object']}#{a['generation']}  "
                  f"({', '.join(a['reasons'])})")
    elif out.get("status") == "off":
        print(out["reason"])
    return 1 if out.get("status") == "failed" else 0


def cmd_model_restore_files(args) -> int:
    """1.4.1 (FR-101 item 9): the response to a changed file: the approved generations written back as the live
    version (each copied from its approved generation inside Cloud Storage, then checked by SHA-256), recorded as
    model.restored with who did it. Without --confirm: says what would be restored."""
    import io

    from pipewright.gcp import Gcp
    from pipewright.models import integrity
    from pipewright.observability import events

    uc, env = _one(args)
    gcp = Gcp()
    approval = (integrity.approval_events(gcp, env, uc.name, args.version) or [None])[0]
    spec = integrity.approved_files(approval)
    if not spec:
        raise ConfigError(f"v{args.version} has no approval that records its files: nothing to restore from")
    restored = integrity.restore_files(gcp, spec, dry_run=not args.confirm)
    for r in restored:
        print(("RESTORED  " if args.confirm else "would restore  ") + f"{r['file']}: generation {r['from']} -> live"
              + (f" (now {r['to']})" if r.get("to") else ""))
    if not restored:
        print(f"every approved file of v{args.version} is already the live version: nothing to restore")
    elif args.confirm:
        events.emit(events.event("model.restored", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                                 version=args.version, by=gcp.identity(), files=restored),
                    env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)
    else:
        print("run again with --confirm to restore")
    return 0


def cmd_model_retire(args) -> int:
    """1.4.1 (FR-101): retiring a version stops its renewal (it is no longer in use) and, with --release-holds,
    releases the temporary holds on its approved files (buckets without object retention), as the caller, recorded
    as model.retired. Refused while the version is in use."""
    import io

    from pipewright.gcp import Gcp
    from pipewright.models import integrity, watch
    from pipewright.observability import events

    uc, env = _one(args)
    gcp = Gcp()
    uses, _ = watch.in_use(gcp, env, {uc.model["name"]: uc.name}, {uc.name})
    busy = [u.where for u in uses if str(u.version) == str(args.version)]
    if busy:
        raise ConfigError(f"v{args.version} is in use ({', '.join(busy)}): not retired")
    approval = (integrity.approval_events(gcp, env, uc.name, args.version) or [None])[0]
    spec = integrity.approved_files(approval)
    released = integrity.release_holds(gcp, spec, dry_run=not args.confirm) if (spec and args.release_holds) else []
    for name in released:
        print(("released hold  " if args.confirm else "would release hold  ") + name)
    if args.confirm:
        events.emit(events.event("model.retired", uc.name, env.name, uc.tenant, None, None, model=uc.model["name"],
                                 version=args.version, by=gcp.identity(), holds_released=len(released)),
                    env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)
        print(f"v{args.version} retired: its renewal has stopped; its files keep their last retention date")
    else:
        print("run again with --confirm to retire")
    return 0


def cmd_lineage(args) -> int:
    import contextlib

    from pipewright.gcp import Gcp
    from pipewright.models import lineage

    uc, env = _one(args)
    _quiet_agent_platform_sdk()
    with contextlib.redirect_stdout(sys.stderr):
        info = lineage.lineage(uc, env, Gcp(), args.version)
    print(json.dumps(info, indent=2, default=str) if args.json else lineage.render(info))
    return 0


def cmd_model_check(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.models import check

    uc, env = _one(args)
    result = check.check(uc, env, Gcp().session.credentials, args.version)
    print(check.render(uc, result))
    return 1 if result.problems else 0


def cmd_model_approve(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.models import registry

    uc, env = _one(args)
    _quiet_agent_platform_sdk()
    gcp = Gcp()
    registry.approve(uc, env, gcp.session.credentials, args.version, gcp.identity(), args.approve)
    return 0


def cmd_model_rollbacks(args) -> int:
    """1.4.3 (FR-98): the rollback proposals health opened for the use case."""
    from pipewright.gcp import Gcp
    from pipewright.serving import rollback

    uc, env = _one(args)
    found = rollback.cases(Gcp(), env.artifacts_bucket, uc.name)
    if not found:
        print(f"no rollback proposals for {uc.name} in {env.name}")
    for c in found:
        print(f"{c['case_id']}  {c['status']:<9} v{c.get('version')} -> v{c.get('previous_version') or '?'}  "
              f"{c.get('signal')} (threshold {c.get('threshold')}, observed {c.get('observed_value')}), "
              f"{c.get('hours_since_deploy')} h after deploy")
    return 0


def cmd_model_rollback(args) -> int:
    """1.4.3 (FR-98): decide a rollback proposal. Approving makes the previous version the default through
    `model approve` (its gates and the released rules checked again) and deploys it; never automatic."""
    from pipewright.gcp import Gcp
    from pipewright.models import deploy as deploy_model
    from pipewright.models import registry
    from pipewright.serving import rollback

    uc, env = _one(args)
    gcp = Gcp()
    case = rollback.decide(gcp, env.artifacts_bucket, uc.name, args.case, args.approve, gcp.identity(),
                           args.reason or "")
    from pipewright.observability import events

    events.emit(events.event("rollback.decided", uc.name, env.name, uc.tenant, None, None, case_id=args.case,
                             approved=args.approve, by=gcp.identity(), version=case.get("version"),
                             previous_version=case.get("previous_version"), reason=args.reason or ""),
                env.observability.get("events_topic"), gcp, region=env.region)
    if not args.approve:
        print(f"declined: {args.case}; v{case.get('version')} keeps serving")
        return 0
    previous = case.get("previous_version")
    if not previous:
        raise ConfigError(f"{args.case} names no previous version: roll back by hand (pipewright model approve, then "
                          "pipewright deploy-model)")
    _quiet_agent_platform_sdk()
    registry.approve(uc, env, gcp.session.credentials, previous, gcp.identity(), True)
    if uc.model["serving"]["target"] == "kserve":
        from pipewright.models import kserve

        kserve.deploy(uc, env, gcp)
    else:
        deploy_model.deploy(uc, env, gcp.session.credentials)
    _health_sync(uc, env)
    print(f"rolled back: v{previous} is the default and serving ({args.case})")
    return 0


def cmd_data_erase(args) -> int:
    from pipewright.data import erase

    uc, env = _one(args)
    keys = erase.read_keys(args.key, args.keys_file)
    results = erase.erase(uc, env, keys, args.confirm)
    print(erase.render(uc, results, len(keys), args.confirm))
    return 1 if any(r.error for r in results) else 0


def cmd_monitor_status(args) -> int:
    from pipewright.monitoring import status

    uc, env = _one(args)
    runs = status.recent(uc, env, args.limit)
    if args.json:                            # Pipewright 1.2.0 (FR-86): the monitoring rows as recorded
        cases = []
        if uc.ml["retraining"]["mode"] != "off":
            from pipewright.gcp import Gcp
            from pipewright.retraining import client

            cases = [c for c in client.store(env, Gcp()).list(uc.name) if c["state"] != "approved"][:5]
        print(json.dumps({"usecase": uc.name, "environment": env.name, "runs": runs, "open_cases": cases},
                         indent=2, default=str))
        return 0
    print(status.render(uc, runs))
    if uc.ml["retraining"]["mode"] != "off":
        from pipewright.gcp import Gcp
        from pipewright.retraining import client

        cases = [c for c in client.store(env, Gcp()).list(uc.name) if c["state"] != "approved"]
        print(client.render_list(uc.name, env, cases[:5]) if cases else "retrain cases: none open")
    return 0


def _retrain_env(args):
    from pipewright.config.environment import environment_from_manifest

    if args.manifest:
        return environment_from_manifest(args.manifest)
    if not args.env:
        raise ConfigError("give --env (with the MLOps repo) or --manifest gs://<artifacts>/pipewright/environment.json")
    return load_environment(Path(args.root), args.env)


def cmd_retrain_list(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.retraining import client

    env = _retrain_env(args)
    cases = client.store(env, Gcp()).list(args.usecase)[: args.limit]
    if args.json:                            # Pipewright 1.2.0 (FR-86): the cases as stored
        print(json.dumps({"usecase": args.usecase, "environment": env.name, "cases": cases}, indent=2, default=str))
        return 0
    print(client.render_list(args.usecase, env, cases))
    return 0


def cmd_retrain_show(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.retraining import client, summary

    env = _retrain_env(args)
    case = client.get_case(env, args.usecase, args.case, Gcp())
    print(json.dumps(case, indent=2) if args.json else summary.render(case))
    return 0


def _loop(args):
    """The loop of a production case: the production environment (--env) and the one its releases come from."""
    from pipewright.gcp import Gcp
    from pipewright.loop.backend import GcpBackend
    from pipewright.loop.driver import Loop

    root = Path(args.root)
    production = load_environment(root, args.env)
    if not production.promotion_from:
        raise ConfigError(f"{production.name} has no promotion.from: the closed retrain loop trains in the "
                          "environment its releases come from (environments/<env>.yaml promotion.from)")
    test = load_environment(root, production.promotion_from)
    path = _selected(root, [args.usecase])[args.usecase]
    gcp = Gcp()
    backend = GcpBackend(path, args.case, test, production, gcp)
    return Loop(args.usecase, args.case, test.name, production.name, backend, by=gcp.identity())


def _render_loop(loop: dict) -> str:
    lines = [f"state: {loop.get('state')}"]
    for key in ("source_uri", "data_from", "champion", "training", "candidate", "validation", "promotion",
                "verification", "canary", "rolled_back_to"):
        if loop.get(key) is not None:
            lines.append(f"  {key}: {json.dumps(loop[key], default=str)}")
    lines.append("history:")
    lines += [f"  {h['at']}  {h['state']:<20} {h.get('by') or ''}  {h.get('note') or ''}".rstrip()
              for h in loop.get("history") or []]
    return "\n".join(lines)


def _pause(args) -> int:
    """Phase 6 safeguards: `loop pause | resume`, recorded on the production case store and as an event."""
    import io
    from datetime import datetime, timezone

    from pipewright.gcp import Gcp
    from pipewright.observability import events
    from pipewright.retraining import client

    root = Path(args.root)
    production = load_environment(root, args.env)
    uc = resolve_usecase(_selected(root, [args.usecase])[args.usecase], production)
    gcp = Gcp()
    who = gcp.identity()
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    record = {"by": who, "at": now, "reason": args.reason}
    paused = record if args.loop_command == "pause" else None
    client.store(production, gcp).set_paused(uc.name, paused, None if paused else record)
    events.emit(events.event(f"loop.{'paused' if paused else 'resumed'}", uc.name, production.name, uc.tenant, None,
                             None, by=who, reason=args.reason),
                production.observability.get("events_topic"), gcp, stream=io.StringIO(), region=production.region)
    print(f"automatic retraining of {uc.name} in {production.name}: "
          + ("paused" if paused else "resumed") + f" by {who} ({args.reason})")
    return 0


def cmd_loop(args) -> int:
    from pipewright.loop import driver

    if args.loop_command in ("pause", "resume"):
        return _pause(args)
    loop = _loop(args)
    if args.loop_command == "start":
        record = loop.start(args.source_uri)
    elif args.loop_command == "advance":
        record = loop.record()
        for _ in range(args.steps):          # --steps: as far as the loop can go now (CI runs it on a schedule)
            before = record.get("state")
            record = loop.advance(approve=args.approve)
            if record.get("state") == before or driver.finished(record):
                break
    elif args.loop_command == "rollback":
        record = loop.rollback(args.reason)
    else:
        record = loop.record()
    for line in loop.log:
        print(line)
    print(_render_loop(record))
    return 1 if record.get("state") in ("failed", "validation-failed", "verification-failed") else 0


def cmd_ab(args) -> int:
    """FR-57: A/B tests of models."""
    from pipewright.abtest import service as ab
    from pipewright.gcp import Gcp

    if getattr(args, "request", None):            # the tenant's scheduled job, from the launcher's tick
        from pipewright.abtest import watch

        out = watch.run_request(Gcp(), args.request, args.usecase)
        print(json.dumps(out, indent=2, default=str))
        return 0
    if not args.env:
        raise ConfigError("give --env (or --request: the launcher's analysis request)")
    env = load_environment(Path(args.root), args.env)
    if args.ab_command not in ("start", "plan"):
        # The platform's rules govern starting a test (and were applied, and recorded, when it started). A running
        # test can always be read, paused, lowered, analysed, decided and stopped, also after the platform tightens
        # them or sets allowed: false (found in the QA battle test: `ab stop` was refused). feed_exposures stays: it
        # governs data leaving the tenant's dataset.
        import dataclasses

        rules = (env.policies or {}).get("ab_test") or {}
        kept = {k: v for k, v in rules.items() if k == "feed_exposures"}
        env = dataclasses.replace(env, policies={**{k: v for k, v in (env.policies or {}).items() if k != "ab_test"},
                                                 **({"ab_test": kept} if kept else {})})
    uc = resolve_usecase(_selected(Path(args.root), [args.usecase])[args.usecase], env)
    gcp = Gcp()
    tests = ab.ABTests(uc, env, gcp, online=_ab_online(uc, env, gcp))
    c = args.ab_command
    who = gcp.identity() if c in ("start", "set", "pause", "resume", "stop", "conclude") else None
    if c == "plan":
        out = tests.plan(args.baseline, args.sd, args.per_day)
        print(json.dumps(out, indent=2) if args.json else "\n".join(f"{k}: {v}" for k, v in out.items()))
        return 0
    if c == "list":
        for t in tests.store.list(uc.name):
            print(f"{t['name']:<24} {t['state']:<18} started {t.get('started_at')} control v"
                  f"{t['arms']['control']['version']} treatment v{t['arms']['treatment']['version']}"
                  + (f" winner {t['decision']['winner']}" if (t.get('decision') or {}).get('winner') else ""))
        return 0
    if c == "status":
        t = tests.get(args.name)
        print(json.dumps(t, indent=2, default=str) if args.json else ab.render_test(t))
        return 0
    if c == "start":
        tests.start(who, args.approve)
    elif c == "set":
        tests.set_allocation(args.allocation, args.reason, who)
    elif c in ("pause", "resume", "stop"):
        tests.set_state({"pause": "paused", "resume": "running", "stop": "stopped"}[c], args.reason, who)
    elif c == "analyse":
        out = tests.analyse(args.name)
        print(json.dumps(out["result"], indent=2, default=str) if args.json else
              ab.ab_analysis.render(out["result"], out["test"]))
        return 1 if out["result"]["blocked"] else 0
    elif c == "decide":
        print(tests.decide(args.winner, args.reason, args.group))
        return 0
    elif c == "conclude":
        tests.conclude(args.winner, args.reason, who, args.approve)
    print(ab.render_test(tests.get(None) if c not in ("stop", "conclude") else tests.store.list(uc.name)[0]))
    return 0


def _ab_online(uc, env, gcp):
    """Phase B: the serving arm when the use case serves online; None for batch-only use cases."""
    if not uc.model.get("serving", {}).get("enabled"):
        return None
    from pipewright.abtest import online

    return online.arms_for(uc, env, gcp)


def cmd_decisions(args) -> int:
    import contextlib

    from pipewright import decisions

    # The Agent Platform SDK logs "Getting versions for ..." to stdout: keep stdout for the list (the workflows
    # parse --json).
    _quiet_agent_platform_sdk()
    with contextlib.redirect_stdout(sys.stderr):
        items = decisions.pending(args.root, args.env, args.usecase or None)
    if args.json:
        print(json.dumps([i.to_dict() for i in items], indent=2))
        return 0
    if not items:
        print(f"{args.env}: nothing waits for a decision")
    for i in items:
        gate = f"gate {i.gate}" if i.gate else "no person needed"
        print(f"{i.kind:<16} {i.usecase:<20} {i.key:<24} {gate:<28} {i.summary}")
    return 0


def cmd_retrain_decide(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.retraining import client

    env = _retrain_env(args)
    gcp = Gcp()
    case = client.get_case(env, args.usecase, args.case, gcp)
    action = args.retrain_command
    body = client.message(env, case, action, args.reason, getattr(args, "because", None),
                          getattr(args, "days", None), getattr(args, "source_uri", None))
    group = client.choose_group(env, case, action, args.group, gcp)
    since = client.now_iso()
    queued = bool(group or client.run_requests_topic(env, group))      # sent over a topic: the launcher records
    how = client.send(env, case, body, group, gcp)
    print(f"{action} for case {case['case_id']} {how}")
    if not queued or args.no_wait:
        return 0
    print("waiting for the launcher to record it ...")
    answer = client.wait_for(env, case, since, gcp)
    if answer is None:
        older = "" if group else (" A launcher older than Pipewright 1.1 drops decisions sent over the run requests "
                                  "topic: ask the platform team which launcher runs, or decide as a required "
                                  "approver group.")
        print(f"not recorded yet: check later with `pipewright retrain show --case {case['case_id']}`.{older}")
        return 0
    entry, latest = answer["entry"], answer["case"]
    if entry["action"] == "refused":
        print(f"REFUSED  {entry.get('reason')}")
        return 1
    print(f"recorded: case {latest['case_id']} is {latest['state']}" + (
        f" until {latest['until']}" if latest.get("until") and latest["state"] in ("snoozed", "declined") else ""))
    if latest["state"] == "approved" and latest.get("loop"):
        print(f"handed to the closed retrain loop: `pipewright loop start --env {args.env} --usecase "
              f"{latest['usecase']} --case {latest['case_id']}` (or the pipewright-decisions workflow)")
    elif latest["state"] == "approved" and latest.get("mode") != "candidate":
        print("training is submitted by the launcher; `pipewright retrain show` shows the job")
    return 0


def cmd_run(args) -> int:
    from pipewright.runs import launch

    env = load_environment(Path(args.root), args.env)
    if args.print_launcher_url:
        print(launch.launcher_url(env))
        return 0
    found = _selected(Path(args.root), [args.usecase])
    source_uri = args.source_uri
    replay = getattr(args, "replay", None)
    if replay:
        from pipewright.runs import sources

        if source_uri or args.schedule:
            raise ConfigError("--replay reads an earlier run's snapshot: give it without --source-uri or --schedule")
        if not (resolve_usecase(found[args.usecase], env).ml.get("data") or {}).get("snapshot_table"):
            raise ConfigError(f"{args.usecase} sets no data.snapshot_table in {env.name}: there is no snapshot to "
                              "replay")
        try:
            source_uri = sources.snapshot_uri(replay)
        except ValueError as e:
            raise ConfigError(str(e)) from None
    if args.pipeline == "synthetic" and not source_uri and not args.schedule:
        synthetic = resolve_usecase(found[args.usecase], env).synthetic or {}
        source_uri = synthetic.get("source_uri")          # the data the engine learns from, as configured
        if not source_uri:
            raise ConfigError(f"{args.usecase} has no synthetic pipeline in {env.name} (synthetic.enabled)")
    payload = launch.body(args.usecase, args.pipeline, source_uri, args.schedule, args.run_key,
                          replay=bool(replay), champion_version=args.champion_version,
                          model_version=args.model_version)
    release = launch.live_release(env, args.usecase)
    result = launch.run(env, {**payload, **({"release": release} if release else {})})
    # the schedule's own pipeline when a schedule is run (the launcher picks it), not "training" (seen writing Ch. 23)
    pipeline = result.get("pipeline") or (f"schedule {args.schedule}" if args.schedule and not args.pipeline
                                          else "training")
    if result.get("status") == "requested":
        print(f"requested: {result['usecase']} {pipeline} over Pub/Sub (run key {result['run_key']}, "
              f"message {result['message_id']}). The launcher submits it; the run.submitted event has the job.")
        return 0
    print(f"{result.get('status')}: {result.get('usecase')} {pipeline} job {result.get('job_id')}"
          + (f" (release {result['release']})" if result.get("release") else ""))
    return 0


def cmd_synthetic_generate(args) -> int:
    """Contract-only rows: from the data contract and synthetic.columns, no real data, no ML (development and CI)."""
    import csv

    from pipewright.synthetic.runtime import rules

    uc, env = _one(args)
    ml = uc.ml
    contract = ml["data"]["contract"]
    if not contract:
        raise ConfigError(f"{uc.name} has no data.contract to generate from")
    syn = uc.synthetic or {}
    rows = args.rows or syn.get("rows") or contract.get("min_rows") or 1000
    seed = args.seed if args.seed is not None else (syn.get("seed") or ml["training"]["seed"])
    data = rules.contract_rows(contract, ml["data"].get("label"), syn.get("columns") or {}, rows, seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    names = [c["name"] for c in contract["columns"]]
    if out.suffix == ".parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ConfigError("writing Parquet needs pyarrow (the gcp extra); or write .csv") from None
        pq.write_table(pa.Table.from_pylist(data), out)
    else:
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=names)
            writer.writeheader()
            writer.writerows(data)
    print(f"wrote {rows} contract-only rows to {out} (seed {seed}; no real data was read)")
    return 0


def cmd_synthetic_gates(args) -> int:
    """The gates a synthetic release must pass in this environment: the purpose's defaults with the overrides."""
    from pipewright.synthetic.runtime import gates

    uc, env = _one(args)
    syn = uc.synthetic
    if not syn:
        print(f"{uc.name}: synthetic data is off in {env.name} (synthetic.enabled)")
        return 0
    purpose_defaults = gates.defaults(syn["purpose"])
    print(f"{uc.name} in {env.name}: purpose {syn['purpose']}, tier {syn['tier']}")
    for metric, gate in (syn.get("gates") or purpose_defaults).items():
        (direction, value), = gate.items()
        note = "" if gate == purpose_defaults.get(metric) else f"  (default {purpose_defaults[metric][direction]})"
        print(f"  {metric:15} {direction} {value}{note}")
    return 0


def cmd_ci_init(args) -> int:
    from pipewright.scaffold import ci

    labels = args.runs_on.split(",") if args.runs_on else None
    environments = [e.strip() for e in args.environments.split(",") if e.strip()]
    cron = args.decisions_cron or ci.DECISIONS_CRON
    for path in ci.init(Path(args.root), environments, args.force, labels, cron):
        print(f"wrote {path}")
    for env, source in ci.promotions(Path(args.root), environments).items():
        print(f"{env}: no release job (promotion.from {source}); pipewright-promote brings {source}'s releases")
    print(f"decisions: pipewright-decisions runs on cron {cron!r} (UTC)")
    print("next: in GitHub, create an environment per name with PIPEWRIGHT_PIP_SPEC (and GCP_WIF_PROVIDER + "
          "PIPEWRIGHT_SERVICE_ACCOUNT unless the runners use their own identity); add required reviewers to production")
    # Found in 1.2.0 QA: without these the decisions job has no variables, and nothing said to create them.
    print("      and <name>-decisions for each ("
          + ", ".join(f"{e}-decisions" for e in environments)
          + "), with the same variables and no reviewers: pipewright-decisions runs there. A gate refuses until "
            "its environment has required reviewers (GitHub Team or Enterprise, or a public repository)")
    return 0


def cmd_test(args) -> int:
    from pipewright.testing import runner

    root = Path(args.root)
    env = load_environment(root, args.env)
    codes = [runner.run(resolve_usecase(path, env)) for path in _selected(root, args.usecase, args.env).values()]
    return 1 if any(c for c in codes if c) else 0


def cmd_release(args) -> int:
    from pipewright.release import service as release

    root = Path(args.root)
    env = load_environment(root, args.env)
    for path in _selected(root, args.usecase, args.env).values():
        uc = resolve_usecase(path, env)
        release.release(uc, env, component_image=args.component_image, skip_tables=args.skip_tables)
    return 0


def cmd_rollback(args) -> int:
    from pipewright.release import service as release

    env = load_environment(Path(args.root), args.env)
    release.rollback(args.usecase, env, to=args.to)
    return 0


def cmd_promote(args) -> int:
    from pipewright.release import promote

    root = Path(args.root)
    path = _selected(root, [args.usecase])[args.usecase]
    src, dst = load_environment(root, args.source), load_environment(root, args.to)
    if args.print_source_commit:
        print(promote.source_commit(src, args.usecase, args.release))
        return 0
    result = promote.promote(path, src, dst, release_id=args.release, model_version=args.model_version,
                             accept_template_change=args.accept_template_change, approve=args.approve)
    if result.unchanged:
        return 0
    for c in result.checks:
        print(f"  verified: {c}")
    if result.model_version and result.model_default:
        print(f"model v{result.model_version} is the default in {dst.name}: its first version there"
              + (", approved with this promotion" if args.approve else ""))
    elif result.model_version:
        approve = dst.policies.get("promote_model", "manual")
        print(f"model v{result.model_version} is a candidate in {dst.name}: "
              + ("make it the default with `pipewright model approve --env " + dst.name + " --usecase " + args.usecase
                 + f" --version {result.model_version}`" if approve != "auto" else "approve it to serve"))
    return 0


def cmd_status(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.release import service as release

    root = Path(args.root)
    env = load_environment(root, args.env)
    gcp = Gcp()
    found = []
    for name in _selected(root, args.usecase, args.env):
        pointer, _ = release.read_pointer(gcp, env, name)
        if args.json:                        # Pipewright 1.2.0 (FR-86): the pointer, and the live contract's summary
            contract = release.read_contract(gcp, env, pointer.contract) if pointer else None
            found.append({"usecase": name, "environment": env.name, "released": pointer is not None,
                          **({"release": pointer.release_id, "since": pointer.updated_at, "by": pointer.updated_by,
                              "approval": pointer.approval, "contract_version": contract.contract_version,
                              "pipelines": list(contract.pipeline_names),
                              "history": pointer.history} if pointer else {})})
            continue
        if pointer is None:
            print(f"{name} ({env.name}): never released")
            continue
        how = pointer.approval or {}
        print(f"{name} ({env.name}): live {pointer.release_id} since {pointer.updated_at}"
              + (f" by {pointer.updated_by}" if pointer.updated_by else "")
              + (f" (GitHub {how.get('actor')}, {how.get('run_url')})" if how.get("via") == "github" else ""))
        contract = release.read_contract(gcp, env, pointer.contract)
        print(f"    contract v{contract.contract_version}, pipelines: {', '.join(contract.pipeline_names)}")
        for h in pointer.history[:5]:
            print(f"    previous {h['release_id']}  ({h['updated_at']})")
    if args.json:
        print(json.dumps(found, indent=2, default=str))
    return 0


def cmd_images_prune(args) -> int:
    from pipewright.build import prune
    from pipewright.gcp import Gcp

    root = Path(args.root)
    env = load_environment(root, args.env)
    if args.keep < 1:
        raise ConfigError("--keep: at least 1")
    sharing = []
    for path in sorted((root / "environments").glob("*.yaml")):
        name = path.stem
        if name == env.name or "." in name:
            continue
        try:
            other = load_environment(root, name)
        except ConfigError:
            continue
        if other.artifact_registry.get("docker") == env.artifact_registry.get("docker"):
            sharing.append(other)
    gcp = Gcp()
    p = prune.plan(gcp, env, args.keep, sharing)
    if args.apply:
        prune.apply(gcp, env, p)
    print(prune.render(p, args.apply))
    return 1 if p.errors else 0


def cmd_plugins(args) -> int:
    from pipewright import plugins

    found = plugins.discover()
    if not found:
        print(f"no plugins installed (entry point group {plugins.GROUP}, API v{plugins.API_VERSION})")
        return 0
    status = getattr(args, "_plugins", {})
    for ep in found:
        print(f"{ep.name:<16} {ep.value:<48} {status.get(ep.name, 'ok')}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipewright", description="Metadata-driven MLOps on Google Cloud (ML persona).")
    p.add_argument("--version", action="version", version=f"pipewright {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
        sp.add_argument("--env", required=True, help="environment name, e.g. dev or prod")
        sp.add_argument("--usecase", action="append", help="use case name (repeatable); default: all")

    sp = sub.add_parser("validate", help="validate environment and use-case config")
    common(sp)
    sp.set_defaults(func=cmd_validate)

    sp = sub.add_parser("resolve", help="print the effective use-case config for an environment")
    common(sp)
    sp.set_defaults(func=cmd_resolve)

    sp = sub.add_parser("doctor", help="read-only checks against the live environment (needs the gcp extra)")
    common(sp)
    sp.add_argument("--no-compile", action="store_true", help="skip compiling the pipeline")
    sp.set_defaults(func=cmd_doctor)

    sp = sub.add_parser("tables", help="BigQuery tables owned by use cases")
    tsub = sp.add_subparsers(dest="tables_command", required=True)
    sp = tsub.add_parser("sync", help="create tables and apply additive changes; refuse breaking ones")
    common(sp)
    sp.add_argument("--dry-run", action="store_true", help="show the plan without changing anything")
    sp.set_defaults(func=cmd_tables_sync)

    sp = sub.add_parser("schedules", help="Cloud Scheduler jobs owned by use cases")
    ssub = sp.add_subparsers(dest="schedules_command", required=True)
    sp = ssub.add_parser("sync", help="create, update and delete a use case's schedules")
    common(sp)
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_schedules_sync)

    sp = sub.add_parser("alerts", help="email alerts for a use case's events (monitoring.alerts)")
    asub = sp.add_subparsers(dest="alerts_command", required=True)
    sp = asub.add_parser("sync", help="create, update and delete alert policies and email channels")
    common(sp)
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_alerts_sync)

    sp = sub.add_parser("release", help="tables sync, build, publish, verify, then make the release live")
    common(sp)
    sp.add_argument("--component-image", help="reuse an already pushed image (repo/name@sha256:...)")
    sp.add_argument("--skip-tables", action="store_true", help="do not run tables sync first")
    sp.set_defaults(func=cmd_release)

    sp = sub.add_parser("rollback", help="move a use case's pointer back to an earlier release")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--usecase", required=True)
    sp.add_argument("--to", help="release id (default: the previous one)")
    sp.set_defaults(func=cmd_rollback)

    sp = sub.add_parser("promote", help="promote a tested release (and its approved model) to the next environment")
    sp.add_argument("--root", default=".")
    sp.add_argument("--from", dest="source", required=True, help="the environment it was tested in, e.g. test")
    sp.add_argument("--to", required=True, help="the environment to promote to, e.g. prod (its promotion.from)")
    sp.add_argument("--usecase", required=True)
    sp.add_argument("--release", help="the source release id (default: the one live in the source)")
    sp.add_argument("--model-version", help="the source model version (default: its default, approved version)")
    sp.add_argument("--accept-template-change", action="store_true",
                    help="promote even if the destination compiles the pipelines differently (recorded)")
    sp.add_argument("--approve", action="store_true",
                    help="approve the model as part of the promotion (needed for a model's first version under a "
                         "manual approval policy; recorded)")
    sp.add_argument("--print-source-commit", action="store_true",
                    help="print the commit the source release was built from and exit (CI checks it out first)")
    sp.set_defaults(func=cmd_promote)

    sp = sub.add_parser("new-usecase", help="start a use case from a Pipewright example")
    sp.add_argument("--root", default=".")
    sp.add_argument("--name", required=True, help="use case name, e.g. churn-nimbus")
    sp.add_argument("--tenant", required=True, help="tenant the platform team gave you")
    sp.add_argument("--owner", required=True, help="owning team")
    sp.add_argument("--from", dest="from_example",
                    help="example to copy: churn (every option), minimal (only what is required), ray (tuning on a "
                         "Ray cluster) or features (features from feature tables); default churn, or minimal "
                         "with --from-bq")
    sp.add_argument("--table-prefix", help="prefix for BigQuery tables (default: <name>_)")
    sp.add_argument("--from-bq", metavar="PROJECT.DATASET.TABLE",
                    help="draft the data contract from this BigQuery table's schema (needs the gcp extra)")
    sp.add_argument("--label", help="with --from-bq: the column the model predicts (0/1 or true/false)")
    sp.set_defaults(func=cmd_new_usecase)

    sp = sub.add_parser("schemas", help="write editor schemas and point each YAML file at its schema")
    sp.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
    sp.set_defaults(func=cmd_schemas)

    sp = sub.add_parser("context", help="context packs for coding agents: build or check APP_CONTEXT.md (1.3.0)")
    csub = sp.add_subparsers(dest="context_command", required=True)
    for action, text in (("build", "write APP_CONTEXT.md, the playbooks and the pointer files' Pipewright block"),
                         ("check", "fail when APP_CONTEXT.md is missing or stale (for CI)")):
        cp = csub.add_parser(action, help=text)
        cp.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
        cp.add_argument("--env", required=True, help="environment the pack describes")
        cp.set_defaults(func=cmd_context, context_action=action)

    sp = sub.add_parser("mcp", help="the MCP tool server for coding agents, over stdio (1.3.0; needs the mcp extra)")
    sp.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
    sp.add_argument("--env", required=True, help="environment the tools work against")
    sp.add_argument("--tenant", help="the team (default: the only tenant in the manifest)")
    sp.add_argument("--agent", help="the agent's name, recorded with every call (e.g. claude-code)")
    sp.add_argument("--print-config", action="store_true", help="print the MCP client settings to add, and exit")
    sp.add_argument("--direct", action="store_true", help="with --print-config and a hosted server: straight to the "
                                                         "service with your ID token, no local proxy")
    sp.set_defaults(func=cmd_mcp)

    sp = sub.add_parser("review", help="the review bundle for a change: a pull request description (1.3.0)")
    sp.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
    sp.add_argument("--env", required=True, help="environment to check the change against")
    sp.add_argument("--base", default="main", help="the revision to compare with (default: main)")
    sp.add_argument("--output", help="write the Markdown here instead of printing it")
    sp.set_defaults(func=cmd_review)

    sp = sub.add_parser("events", help="every event Pipewright sends, by persona or family, with its Pub/Sub filter")
    sp.add_argument("--persona", help="ml, sre, governance, product, data or finops")
    sp.add_argument("--family", help="run, release, model, training, data, monitoring, retrain, loop, ab_test, ...")
    sp.add_argument("--filter", action="store_true", help="print only the Pub/Sub subscription filter")
    sp.add_argument("--json", action="store_true", help="machine-readable")
    sp.set_defaults(func=cmd_events)

    sp = sub.add_parser("explain", help="what a use case key means: description, type, allowed values, default")
    sp.add_argument("key", nargs="?", help="dotted key under spec, e.g. data.split.strategy")
    sp.add_argument("--list", action="store_true", help="list every key")
    sp.set_defaults(func=cmd_explain)

    sp = sub.add_parser("deploy-model", help="serve the registry's default model version on an endpoint")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--usecase", required=True)
    sp.add_argument("--approve", action="store_true", help="confirm a manual deploy approval")
    sp.add_argument("--dry-run", action="store_true", help="show what would be deployed")
    sp.add_argument("--undeploy", action="store_true", help="remove all models from the endpoint (stops cost)")
    sp.add_argument("--keep-lookup-service", action="store_true",
                    help="with --undeploy: keep the use case's lookup service (by default the launcher removes it)")
    rollout = sp.add_mutually_exclusive_group()
    rollout.add_argument("--canary", action="store_true",
                         help="give the new version model.serving.canary_percent of traffic, keep the current one")
    rollout.add_argument("--promote", action="store_true", help="move all traffic to the canary, remove the rest")
    rollout.add_argument("--undeploy-canary", action="store_true", help="abandon the canary")
    sp.set_defaults(func=cmd_deploy_model)

    def one(sp):
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", required=True)
        sp.add_argument("--usecase", required=True)

    sp = sub.add_parser("pipelines-ui", help="the Kubeflow Pipelines UI of a private-gke environment, on localhost "
                                            "(read only, through the cluster's API as you)")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--port", type=int, default=8080)
    sp.set_defaults(func=cmd_pipelines_ui)

    sp = sub.add_parser("features", help="feature groups: the data team registers feature tables (FR-75)")
    fsub = sp.add_subparsers(dest="features_command", required=True)
    sp = fsub.add_parser("register", help="create the feature groups and features a spec file lists; existing ones "
                                          "are left as they are")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--file", required=True, help="YAML: per group, table, entity, description, features")
    sp.add_argument("names", nargs="*", help="only these groups (default: every group in the file)")
    sp.add_argument("--dry-run", action="store_true", help="say what would be created; create nothing")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_features_register)
    sp = fsub.add_parser("contract", help="write (or --check) the use case's feature contract from its live feature "
                                          "sources (FR-103)")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--usecase", required=True)
    sp.add_argument("--check", action="store_true", help="compare the contract with the live sources: exit 0 same, "
                                                         "1 different, 2 cannot check (for CI)")
    sp.add_argument("--built-tables", action="store_true", help="with --check: also the SQL of the job that last "
                                                                "wrote each table built by a query")
    sp.add_argument("--dry-run", action="store_true", help="say what would change; write nothing")
    sp.set_defaults(func=cmd_features_contract)
    for name, func, text in (
            ("sync", cmd_features_sync, "the latest feature values per entity into the use case's online store "
                                        "(bigquery or cloudsql, FR-104); for the Bigtable store, starts each "
                                        "view's sync and waits (FR-110)"),
            ("export", cmd_features_export, "the latest feature values per entity as Parquet with a manifest, for a "
                                            "store the team builds (FR-104)")):
        sp = fsub.add_parser(name, help=text)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", help="with the MLOps checkout")
        sp.add_argument("--manifest", help="the environment manifest (gs:// or a file): read the live release")
        sp.add_argument("--request", help="gs:// sync request the launcher wrote (the scheduled sync's job)")
        sp.add_argument("--usecase", required=True)
        sp.add_argument("--json", action="store_true")
        sp.set_defaults(func=func)
    sp = fsub.add_parser("online-setup", help="the Cloud SQL online store's one-time setup: the database and each "
                                              "tenant's grants, as the platform's admin identity (FR-104)")
    sp.add_argument("--manifest", required=True)
    sp.add_argument("--dry-run", action="store_true", help="print the statements; run nothing")
    sp.set_defaults(func=cmd_features_online_setup)

    sp = sub.add_parser("serving", help="shared serving pools for model co-hosting (FR-105, 1.4.2)")
    ssub = sp.add_subparsers(dest="serving_command", required=True)
    sp = ssub.add_parser("pools", help="the environment's pools (manifest serving.pools)")
    psub = sp.add_subparsers(dest="pools_command", required=True)
    for name, text in (("plan", "what apply would do, with each pool's cost; changes nothing"),
                       ("apply", "create missing pools and resize changed ones in place (CI/CD); never deletes"),
                       ("list", "each pool and the use cases on it")):
        sp = psub.add_parser(name, help=text)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", required=True)
        sp.add_argument("--pool", help="only this pool")
        sp.add_argument("--json", action="store_true")
        sp.set_defaults(func=cmd_serving_pools, pools_action=name)

    sp = sub.add_parser("report", help="read-only reports from the records Pipewright keeps")
    rsub = sp.add_subparsers(dest="report_command", required=True)
    sp = rsub.add_parser("fleet", help="every use case in every environment on one HTML page (FR-86)")
    sp.add_argument("--root", default=".")
    sp.add_argument("--out", default="fleet.html")
    sp.add_argument("--mask", action="store_true", help="replace email addresses with ...")
    sp.set_defaults(func=cmd_report_fleet)

    sp = sub.add_parser("integrity", help="what runs is what was approved (1.4.1): check, renew")
    isub = sp.add_subparsers(dest="integrity_command", required=True)
    for cmd, help_, fn in (("check", "every default and serving version against its approval; exit 0 unchanged, 1 "
                                     "changed or unapproved, 2 could not check", cmd_integrity_check),
                           ("renew", "keep the approved files of every version in use protected (the renewer's "
                                     "daily run)", cmd_integrity_renew)):
        sp = isub.add_parser(cmd, help=help_)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env")
        sp.add_argument("--manifest", help="gs://<artifacts>/pipewright/environment.json (no MLOps repo needed)")
        if cmd == "check":
            sp.add_argument("--usecase")
        else:
            sp.add_argument("--dry-run", action="store_true", help="list what would be protected; change nothing")
        sp.add_argument("--json", action="store_true")
        sp.set_defaults(func=fn)

    sp = sub.add_parser("lineage", help="a model version back to its run, release, rows and sources (FR-55)")
    one(sp)
    sp.add_argument("--version", help="version id (default: the default version)")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_lineage)

    sp = sub.add_parser("model", help="Model Registry versions of a use case's model")
    msub = sp.add_subparsers(dest="model_command", required=True)
    sp = msub.add_parser("list", help="versions, newest first; DEFAULT is the champion")
    one(sp)
    sp.add_argument("--json", action="store_true", help="machine-readable output (stable field names)")
    sp.set_defaults(func=cmd_model_list)
    sp = msub.add_parser("predict", help="send instances to the use case's endpoint (Agent Platform or KServe)")
    one(sp)
    sp.add_argument("--instances", required=True,
                    help='JSON list of instances, or a file with one (or {"instances": ...})')
    sp.set_defaults(func=cmd_model_predict)
    sp = msub.add_parser("endpoint", help="how to call the use case's endpoint: ID, predict URL, role, a sample "
                                          "request (read only)")
    one(sp)
    sp.set_defaults(func=cmd_model_endpoint)
    sp = msub.add_parser("check", help="a version against today's gates, slices and fairness (read only; exit 1 "
                                       "when it fails)")
    one(sp)
    sp.add_argument("--version", required=True, help="version id, e.g. 4")
    sp.set_defaults(func=cmd_model_check)
    sp = msub.add_parser("approve", help="make a version the default (approvals.promote_model)")
    one(sp)
    sp.add_argument("--version", required=True, help="version id, e.g. 4")
    sp.add_argument("--approve", action="store_true", help="confirm a manual promotion")
    sp.set_defaults(func=cmd_model_approve)

    sp = msub.add_parser("restore-files", help="write a version's approved file generations back as the live "
                                               "files (1.4.1; dry run without --confirm)")
    one(sp)
    sp.add_argument("--version", required=True)
    sp.add_argument("--confirm", action="store_true")
    sp.set_defaults(func=cmd_model_restore_files)
    sp = msub.add_parser("rollbacks", help="rollback proposals opened by serving health (1.4.3, read only)")
    one(sp)
    sp.set_defaults(func=cmd_model_rollbacks)
    sp = msub.add_parser("rollback", help="decide a rollback proposal: --approve makes the previous version the "
                                          "default (model approve) and deploys it; --decline keeps the current one")
    one(sp)
    sp.add_argument("--case", required=True, help="the proposal's case ID (pipewright model rollbacks)")
    decision = sp.add_mutually_exclusive_group(required=True)
    decision.add_argument("--approve", action="store_true", dest="approve")
    decision.add_argument("--decline", action="store_false", dest="approve")
    sp.add_argument("--reason", help="recorded with the decision")
    sp.set_defaults(func=cmd_model_rollback)
    sp = msub.add_parser("retire", help="retire a version no longer in use: its renewal stops; --release-holds "
                                        "releases its temporary holds (1.4.1; dry run without --confirm)")
    one(sp)
    sp.add_argument("--version", required=True)
    sp.add_argument("--release-holds", action="store_true")
    sp.add_argument("--confirm", action="store_true")
    sp.set_defaults(func=cmd_model_retire)

    sp = sub.add_parser("data", help="data protection")
    dsub = sp.add_subparsers(dest="data_command", required=True)
    sp = dsub.add_parser("erase", help="delete a data subject's rows from the use case's tables (dry run first)")
    one(sp)
    sp.add_argument("--key", action="append", help="value of data_protection.erasure.key_column (repeatable)")
    sp.add_argument("--keys-file", help="file with one key per line")
    sp.add_argument("--confirm", action="store_true", help="delete (without it: count only)")
    sp.set_defaults(func=cmd_data_erase)

    sp = sub.add_parser("monitor", help="what inference runs found: drift, performance, retrain requests")
    msub2 = sp.add_subparsers(dest="monitor_command", required=True)
    sp = msub2.add_parser("status", help="recent monitoring rows and the latest PSI per feature")
    one(sp)
    sp.add_argument("--limit", type=int, default=10, help="how many recent runs (default 10)")
    sp.add_argument("--json", action="store_true", help="machine-readable output (stable field names)")
    sp.set_defaults(func=cmd_monitor_status)

    def retrain_target(sp, case_required: bool = False):
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", help="environment (with the MLOps repo)")
        sp.add_argument("--manifest", help="or the environment manifest, gs://... (approvers without the repo)")
        sp.add_argument("--usecase", required=True)
        sp.add_argument("--case", required=case_required, help="case ID (default: the open case)")

    sp = sub.add_parser("decisions", help="what waits for a person: model approvals, retrain cases, loops")
    dsub = sp.add_subparsers(dest="decisions_command", required=True)
    sp = dsub.add_parser("pending", help="pending decisions, with the GitHub environment that gates each (FR-56)")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--usecase", action="append", help="only these use cases (repeatable)")
    sp.add_argument("--json", action="store_true", help="for the generated workflows")
    sp.set_defaults(func=cmd_decisions)

    sp = sub.add_parser("ab", help="A/B tests of models (FR-57): the champion against a candidate, units assigned "
                                   "stickily, a business metric, a person decides")
    absub = sp.add_subparsers(dest="ab_command", required=True)
    for action, text in (
            ("plan", "units needed per arm, how long that takes and the online arm's cost; changes nothing"),
            ("start", "approve the treatment for the test and start it (a deploy: approvals.deploy_model)"),
            ("status", "the test and its newest analysis"), ("list", "this use case's tests, newest first"),
            ("set", "lower the treatment's allocation (it may only go down while a test runs)"),
            ("pause", "send every unit to the control, keep the test"), ("resume", "resume a paused test"),
            ("stop", "end the test, keep the champion, remove the treatment's serving arm"),
            ("analyse", "analyse now: share checks, the metric per arm, guardrails, progress"),
            ("decide", "an approver group's decision on the winner (sent over the group's topic)"),
            ("conclude", "end the test with its winner; the treatment winning becomes the default")):
        sp = absub.add_parser(action, help=text)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", required=action != "analyse")
        sp.add_argument("--usecase", required=True)
        if action in ("status", "analyse"):
            sp.add_argument("--name", help="a past test (default: the running one)")
        if action == "analyse":
            sp.add_argument("--request", help="gs:// analysis request the launcher wrote (the tenant's scheduled "
                                              "job; no checkout needed)")
        if action in ("plan", "status", "analyse"):
            sp.add_argument("--json", action="store_true", help="machine-readable output")
        if action == "plan":
            sp.add_argument("--baseline", type=float, help="the control's current value of the metric")
            sp.add_argument("--sd", type=float, help="mean or ratio: the unit-level standard deviation")
            sp.add_argument("--per-day", type=float, help="units exposed a day (scored or calling)")
        if action in ("start", "conclude"):
            sp.add_argument("--approve", action="store_true", help="confirm a manual approval")
        if action == "set":
            sp.add_argument("--allocation", type=int, required=True, help="the treatment's share in basis points")
        if action in ("set", "pause", "resume", "stop", "decide", "conclude"):
            sp.add_argument("--reason", required=True, help="recorded with your name")
        if action in ("decide", "conclude"):
            sp.add_argument("--winner", choices=("treatment", "control"), required=action == "decide")
        if action == "decide":
            sp.add_argument("--group", help="the approver group you act for (default: the first you belong to)")
        sp.set_defaults(func=cmd_ab)

    sp = sub.add_parser("loop", help="the closed retrain loop: a production case trained in test, validated, "
                                     "promoted, approved and verified in production")
    lsub = sp.add_subparsers(dest="loop_command", required=True)
    for action, text in (("start", "train in test the approved production case (production never trains)"),
                         ("advance", "take the next step when the current one is done"),
                         ("show", "the loop as one record"),
                         ("rollback", "make production's earlier default the default again")):
        sp = lsub.add_parser(action, help=text)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", required=True, help="the production environment (the case's)")
        sp.add_argument("--usecase", required=True)
        sp.add_argument("--case", required=True, help="the production case ID")
        if action == "start":
            sp.add_argument("--source-uri", help="the data to train on (default: the case's)")
        if action == "advance":
            sp.add_argument("--approve", action="store_true",
                            help="the production approval: promote the validated candidate and make it the default")
            sp.add_argument("--steps", type=int, default=1, help="steps to take at most (default 1)")
        if action == "rollback":
            sp.add_argument("--reason", required=True, help="recorded with your name")
        sp.set_defaults(func=cmd_loop)
    for action, text in (("pause", "pause automatic retraining of a use case (safeguards; recorded)"),
                         ("resume", "resume automatic retraining (after a pause, or the safeguards' circuit breaker)")):
        sp = lsub.add_parser(action, help=text)
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", required=True, help="the production environment")
        sp.add_argument("--usecase", required=True)
        sp.add_argument("--reason", required=True, help="recorded with your name")
        sp.set_defaults(func=cmd_loop)

    sp = sub.add_parser("retrain", help="retrain decisions: drift opens a case, a person decides")
    rsub = sp.add_subparsers(dest="retrain_command", required=True)
    sp = rsub.add_parser("list", help="cases, newest first")
    retrain_target(sp)
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--json", action="store_true", help="machine-readable output (stable field names)")
    sp.set_defaults(func=cmd_retrain_list)
    sp = rsub.add_parser("show", help="the decision pack: is it real, is it the data, what would retraining use")
    retrain_target(sp)
    sp.add_argument("--json", action="store_true", help="the case as stored")
    sp.set_defaults(func=cmd_retrain_show)
    for action, text in (("approve", "approve retraining (one approval per required approver group)"),
                         ("decline", "decline: no retrain now; the case re-opens when the decline expires or "
                                     "things get worse"),
                         ("snooze", "put off the decision for --days; re-opens early if things get worse"),
                         ("retry", "resubmit an approved case's failed training, under its approval")):
        sp = rsub.add_parser(action, help=text)
        retrain_target(sp)
        sp.add_argument("--reason", required=True, help="recorded with your name")
        sp.add_argument("--group", help="the approver group you act for (default: the first you belong to)")
        sp.add_argument("--no-wait", action="store_true", help="do not wait for the launcher to record it")
        if action in ("approve", "retry"):
            sp.add_argument("--source-uri", help="training data to use (default: retraining.source_uri; a retry: "
                                                 "the failed attempt's)")
        else:
            sp.add_argument("--because", required=True, choices=["data-problem", "seasonal", "no-harm", "other"])
            sp.add_argument("--days", type=int, required=action == "snooze",
                            help="until it re-opens" + (" (default: snooze.max_days)" if action == "decline" else ""))
        sp.set_defaults(func=cmd_retrain_decide)

    sp = sub.add_parser("run", help="start a pipeline through the launcher (file-free: give a gs:// source)")
    one(sp)
    sp.add_argument("--pipeline", help="training (default), inference or synthetic")
    sp.add_argument("--source-uri", help="gs:// path of the data")
    sp.add_argument("--schedule", help="run one of the use case's schedules now")
    sp.add_argument("--replay", metavar="RUN_ID",
                    help="train on exactly the rows an earlier run read (its snapshot in data.snapshot_table)")
    sp.add_argument("--run-key", help="idempotency key: the same key never starts a second run")
    sp.add_argument("--champion-version", help="training: compare with this model version instead of the default "
                                                "(the pipeline must declare champion_version)")
    sp.add_argument("--model-version", help="inference: score with this model version instead of the default "
                                             "(the pipeline must declare model_version)")
    sp.add_argument("--print-launcher-url", action="store_true", help="print the launcher URL and exit")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("synthetic", help="synthetic data (synthetic.*): contract-only rows, the gates in force")
    ysub = sp.add_subparsers(dest="synthetic_command", required=True)
    sp = ysub.add_parser("generate", help="rows from the data contract and rules alone (no real data, no ML)")
    one(sp)
    sp.add_argument("--out", required=True, help="a .csv or .parquet file")
    sp.add_argument("--rows", type=int, help="default: synthetic.rows, else data.contract.min_rows")
    sp.add_argument("--seed", type=int, help="default: synthetic.seed, else training.seed")
    sp.set_defaults(func=cmd_synthetic_generate)
    sp = ysub.add_parser("gates", help="the gates a synthetic release must pass, with their defaults")
    one(sp)
    sp.set_defaults(func=cmd_synthetic_gates)

    sp = sub.add_parser("ci", help="CI workflows for the MLOps repo")
    csub = sp.add_subparsers(dest="ci_command", required=True)
    sp = csub.add_parser("init", help="write GitHub Actions workflows that run Pipewright end to end")
    sp.add_argument("--root", default=".")
    sp.add_argument("--environments", required=True, help="promotion order, comma separated, e.g. dev,prod")
    sp.add_argument("--force", action="store_true", help="replace existing workflow files")
    sp.add_argument("--runs-on", help="self-hosted runner labels, comma separated (default: ubuntu-latest)")
    sp.add_argument("--decisions-cron", default=None,
                    help="when pipewright-decisions runs, five cron fields in UTC (default: hourly, 07:17 to 18:17, "
                         "Monday to Friday; '*/15 * * * *' was 1.1's)")
    sp.set_defaults(func=cmd_ci_init)

    sp = sub.add_parser("test", help="run each use case's tests.command")
    common(sp)
    sp.set_defaults(func=cmd_test)

    sp = sub.add_parser("status", help="which release is live per use case")
    common(sp)
    sp.add_argument("--json", action="store_true", help="machine-readable output (stable field names)")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("images", help="the environment's container images")
    isub = sp.add_subparsers(dest="images_command", required=True)
    sp = isub.add_parser("prune", help="delete images no release needs (dry run unless --apply)")
    sp.add_argument("--root", default=".")
    sp.add_argument("--env", required=True)
    sp.add_argument("--keep", type=int, default=3, help="also keep the newest N images of each package (default 3)")
    sp.add_argument("--apply", action="store_true", help="delete; without it, only list what would go")
    sp.set_defaults(func=cmd_images_prune)

    sp = sub.add_parser("plugins", help="list installed command plugins")
    sp.set_defaults(func=cmd_plugins)

    from pipewright import plugins

    p.plugins = plugins.register_all(sub, common)
    return p


def _quiet_agent_platform_sdk() -> None:
    """The Agent Platform SDK logs "Getting versions for ..." per model at INFO, and sets its logger's level when it is
    imported: import it first, then quiet it (setting the level before the import did nothing)."""
    import logging

    try:
        import google.cloud.aiplatform  # noqa: F401
    except ImportError:
        return
    for name in ("google.cloud.aiplatform", "google.cloud.aiplatform.models"):
        logging.getLogger(name).setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args._plugins = parser.plugins
    try:
        return args.func(args)
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    finally:
        from pipewright.observability import events

        if lost := events.lost_summary():          # Pipewright 1.2.0 (FR-80): a lost event is said, not only logged
            print(lost)
            events.LOST.clear()


if __name__ == "__main__":
    sys.exit(main())
