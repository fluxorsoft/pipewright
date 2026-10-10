"""pipewright command line."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from pipewright import __version__
from pipewright.config import discover_usecases, load_environment, resolve_usecase
from pipewright.errors import ConfigError


def _selected(root: Path, names: list[str] | None) -> dict[str, Path]:
    found = discover_usecases(root)
    if not names:
        if not found:
            raise ConfigError(f"no use cases found under {root / 'usecases'}")
        return found
    missing = [n for n in names if n not in found]
    if missing:
        raise ConfigError(f"use case(s) not found: {', '.join(missing)} (looked in {root / 'usecases'})")
    return {n: found[n] for n in names}


def cmd_validate(args) -> int:
    root = Path(args.root)
    env = load_environment(root, args.env)
    selected = _selected(root, args.usecase)
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
            print(f"OK    {name} ({env.name})")
        except ConfigError as e:
            failed += 1
            print(f"FAIL  {name} ({env.name})\n{e}\n")
    print(f"\n{len(selected) - failed} valid, {failed} with problems")
    return 1 if failed else 0


def cmd_resolve(args) -> int:
    root = Path(args.root)
    env = load_environment(root, args.env)
    for name, path in _selected(root, args.usecase).items():
        doc = resolve_usecase(path, env).to_dict()
        print(f"# resolved {name} for environment {env.name}")
        print(yaml.safe_dump(doc, sort_keys=False, default_flow_style=False).rstrip())
        print("---")
    return 0


def cmd_doctor(args) -> int:
    from pipewright.checks import doctor

    root = Path(args.root)
    env = load_environment(root, args.env)
    usecases = [resolve_usecase(path, env) for path in _selected(root, args.usecase).values()]
    report = doctor.run(env, usecases, compile_check=not args.no_compile)
    print(report.render())
    return 1 if report.failed else 0


def cmd_tables_sync(args) -> int:
    from pipewright.tables import sync as tables_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    failed = 0
    for path in _selected(root, args.usecase).values():
        uc = resolve_usecase(path, env)
        results = tables_sync.sync(uc, env, dry_run=args.dry_run)
        print(tables_sync.render(results, uc, args.dry_run))
        failed += sum(1 for r in results if r.error or r.plan.action == "blocked")
    return 1 if failed else 0


def cmd_schedules_sync(args) -> int:
    from pipewright.schedules import sync as schedules_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    for path in _selected(root, args.usecase).values():
        uc = resolve_usecase(path, env)
        print(schedules_sync.render(schedules_sync.sync(uc, env, dry_run=args.dry_run), uc, args.dry_run))
    return 0


def cmd_alerts_sync(args) -> int:
    from pipewright.alerts import sync as alerts_sync

    root = Path(args.root)
    env = load_environment(root, args.env)
    failed = 0
    for path in _selected(root, args.usecase).values():
        uc = resolve_usecase(path, env)
        plan = alerts_sync.sync(uc, env, dry_run=args.dry_run)
        print(alerts_sync.render(uc, plan, args.dry_run))
        failed += len(plan.errors)
    return 1 if failed else 0


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
    creds = gcp.session.credentials
    if args.undeploy:
        deploy_model.undeploy(uc, env, creds)
        return 0
    if args.promote or args.undeploy_canary:
        deploy_model.check_approval(uc, gcp.identity(), args.approve)
        (deploy_model.promote if args.promote else deploy_model.undeploy_canary)(uc, env, creds)
        return 0
    p = deploy_model.plan(uc, env, creds)
    print(f"model {p.model} default version v{p.version} -> endpoint {p.endpoint} ({p.machine}, replicas "
          f"{p.replicas['min']}-{p.replicas['max']})"
          + ("  [already live]" if p.already_deployed else "")
          + (f"; replaces {', '.join(p.other_versions)}" if p.other_versions else ""))
    print("evaluation gates: " + ("pass" if not p.gate_problems else "FAIL\n  " + "\n  ".join(p.gate_problems)))
    if args.dry_run:
        return 0
    deploy_model.check_approval(uc, gcp.identity(), args.approve)
    deploy_model.deploy(uc, env, creds, canary=args.canary)
    return 0


def _one(args):
    env = load_environment(Path(args.root), args.env)
    return resolve_usecase(_selected(Path(args.root), [args.usecase])[args.usecase], env), env


def cmd_model_list(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.models import registry

    uc, env = _one(args)
    print(registry.render(uc, registry.versions(uc, env, Gcp().session.credentials)))
    return 0


def cmd_model_approve(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.models import registry

    uc, env = _one(args)
    gcp = Gcp()
    registry.approve(uc, env, gcp.session.credentials, args.version, gcp.identity(), args.approve)
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
    print(status.render(uc, status.recent(uc, env, args.limit)))
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
    print(client.render_list(args.usecase, env, client.store(env, Gcp()).list(args.usecase)[: args.limit]))
    return 0


def cmd_retrain_show(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.retraining import client, summary

    env = _retrain_env(args)
    case = client.get_case(env, args.usecase, args.case, Gcp())
    print(json.dumps(case, indent=2) if args.json else summary.render(case))
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
    how = client.send(env, case, body, group, gcp)
    print(f"{action} for case {case['case_id']} {how}")
    if group is None or args.no_wait:
        return 0
    print("waiting for the launcher to record it ...")
    answer = client.wait_for(env, case, since, gcp)
    if answer is None:
        print(f"not recorded yet: check later with `pipewright retrain show --case {case['case_id']}`")
        return 0
    entry, latest = answer["entry"], answer["case"]
    if entry["action"] == "refused":
        print(f"REFUSED  {entry.get('reason')}")
        return 1
    print(f"recorded: case {latest['case_id']} is {latest['state']}" + (
        f" until {latest['until']}" if latest.get("until") and latest["state"] in ("snoozed", "declined") else ""))
    if latest["state"] == "approved" and latest.get("mode") != "candidate":
        print("training is submitted by the launcher; `pipewright retrain show` shows the job")
    return 0


def cmd_run(args) -> int:
    from pipewright.runs import launch

    env = load_environment(Path(args.root), args.env)
    if args.print_launcher_url:
        print(launch.launcher_url(env))
        return 0
    _selected(Path(args.root), [args.usecase])
    result = launch.run(env, launch.body(args.usecase, args.pipeline, args.source_uri, args.schedule, args.run_key))
    if result.get("status") == "requested":
        print(f"requested: {result['usecase']} {result['pipeline']} over Pub/Sub (run key {result['run_key']}, "
              f"message {result['message_id']}). The launcher submits it; the run.submitted event has the job.")
        return 0
    print(f"{result.get('status')}: {result.get('usecase')} {result.get('pipeline', '')} job {result.get('job_id')}"
          + (f" (release {result['release']})" if result.get("release") else ""))
    return 0


def cmd_ci_init(args) -> int:
    from pipewright.scaffold import ci

    labels = args.runs_on.split(",") if args.runs_on else None
    for path in ci.init(Path(args.root), [e.strip() for e in args.environments.split(",") if e.strip()], args.force,
                        labels):
        print(f"wrote {path}")
    print("next: in GitHub, create an environment per name with PIPEWRIGHT_PIP_SPEC (and GCP_WIF_PROVIDER + "
          "PIPEWRIGHT_SERVICE_ACCOUNT unless the runners use their own identity); add required reviewers to production")
    return 0


def cmd_test(args) -> int:
    from pipewright.testing import runner

    root = Path(args.root)
    env = load_environment(root, args.env)
    codes = [runner.run(resolve_usecase(path, env)) for path in _selected(root, args.usecase).values()]
    return 1 if any(c for c in codes if c) else 0


def cmd_release(args) -> int:
    from pipewright.release import service as release

    root = Path(args.root)
    env = load_environment(root, args.env)
    for path in _selected(root, args.usecase).values():
        uc = resolve_usecase(path, env)
        release.release(uc, env, component_image=args.component_image, skip_tables=args.skip_tables)
    return 0


def cmd_rollback(args) -> int:
    from pipewright.release import service as release

    env = load_environment(Path(args.root), args.env)
    release.rollback(args.usecase, env, to=args.to)
    return 0


def cmd_status(args) -> int:
    from pipewright.gcp import Gcp
    from pipewright.release import service as release

    root = Path(args.root)
    env = load_environment(root, args.env)
    gcp = Gcp()
    for name in _selected(root, args.usecase):
        pointer, _ = release.read_pointer(gcp, env, name)
        if pointer is None:
            print(f"{name} ({env.name}): never released")
            continue
        print(f"{name} ({env.name}): live {pointer.release_id} since {pointer.updated_at}"
              + (f" by {pointer.updated_by}" if pointer.updated_by else ""))
        contract = release.read_contract(gcp, env, pointer.contract)
        print(f"    contract v{contract.contract_version}, pipelines: {', '.join(contract.pipeline_names)}")
        for h in pointer.history[:5]:
            print(f"    previous {h['release_id']}  ({h['updated_at']})")
    return 0


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

    sp = sub.add_parser("model", help="Model Registry versions of a use case's model")
    msub = sp.add_subparsers(dest="model_command", required=True)
    sp = msub.add_parser("list", help="versions, newest first; DEFAULT is the champion")
    one(sp)
    sp.set_defaults(func=cmd_model_list)
    sp = msub.add_parser("approve", help="make a version the default (approvals.promote_model)")
    one(sp)
    sp.add_argument("--version", required=True, help="version id, e.g. 4")
    sp.add_argument("--approve", action="store_true", help="confirm a manual promotion")
    sp.set_defaults(func=cmd_model_approve)

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
    sp.set_defaults(func=cmd_monitor_status)

    def retrain_target(sp, case_required: bool = False):
        sp.add_argument("--root", default=".")
        sp.add_argument("--env", help="environment (with the MLOps repo)")
        sp.add_argument("--manifest", help="or the environment manifest, gs://... (approvers without the repo)")
        sp.add_argument("--usecase", required=True)
        sp.add_argument("--case", required=case_required, help="case ID (default: the open case)")

    sp = sub.add_parser("retrain", help="retrain decisions: drift opens a case, a person decides")
    rsub = sp.add_subparsers(dest="retrain_command", required=True)
    sp = rsub.add_parser("list", help="cases, newest first")
    retrain_target(sp)
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_retrain_list)
    sp = rsub.add_parser("show", help="the decision pack: is it real, is it the data, what would retraining use")
    retrain_target(sp)
    sp.add_argument("--json", action="store_true", help="the case as stored")
    sp.set_defaults(func=cmd_retrain_show)
    for action, text in (("approve", "approve retraining (one approval per required approver group)"),
                         ("decline", "decline: no retrain now; the case re-opens when the decline expires or "
                                     "things get worse"),
                         ("snooze", "put off the decision for --days; re-opens early if things get worse")):
        sp = rsub.add_parser(action, help=text)
        retrain_target(sp)
        sp.add_argument("--reason", required=True, help="recorded with your name")
        sp.add_argument("--group", help="the approver group you act for (default: the first you belong to)")
        sp.add_argument("--no-wait", action="store_true", help="do not wait for the launcher to record it")
        if action == "approve":
            sp.add_argument("--source-uri", help="training data to use (default: retraining.source_uri)")
        else:
            sp.add_argument("--because", required=True, choices=["data-problem", "seasonal", "no-harm", "other"])
            sp.add_argument("--days", type=int, required=action == "snooze",
                            help="until it re-opens" + (" (default: snooze.max_days)" if action == "decline" else ""))
        sp.set_defaults(func=cmd_retrain_decide)

    sp = sub.add_parser("run", help="start a pipeline through the launcher (file-free: give a gs:// source)")
    one(sp)
    sp.add_argument("--pipeline", help="training (default) or inference")
    sp.add_argument("--source-uri", help="gs:// path of the data")
    sp.add_argument("--schedule", help="run one of the use case's schedules now")
    sp.add_argument("--run-key", help="idempotency key: the same key never starts a second run")
    sp.add_argument("--print-launcher-url", action="store_true", help="print the launcher URL and exit")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("ci", help="CI workflows for the MLOps repo")
    csub = sp.add_subparsers(dest="ci_command", required=True)
    sp = csub.add_parser("init", help="write GitHub Actions workflows that run Pipewright end to end")
    sp.add_argument("--root", default=".")
    sp.add_argument("--environments", required=True, help="promotion order, comma separated, e.g. dev,prod")
    sp.add_argument("--force", action="store_true", help="replace existing workflow files")
    sp.add_argument("--runs-on", help="self-hosted runner labels, comma separated (default: ubuntu-latest)")
    sp.set_defaults(func=cmd_ci_init)

    sp = sub.add_parser("test", help="run each use case's tests.command")
    common(sp)
    sp.set_defaults(func=cmd_test)

    sp = sub.add_parser("status", help="which release is live per use case")
    common(sp)
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("plugins", help="list installed command plugins")
    sp.set_defaults(func=cmd_plugins)

    from pipewright import plugins

    p.plugins = plugins.register_all(sub, common)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args._plugins = parser.plugins
    try:
        return args.func(args)
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
