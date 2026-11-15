"""`pipewright assist`: explain, ask, test."""

from __future__ import annotations

import json
from pathlib import Path

from pipewright.errors import ConfigError


def _usecase(root: Path, env, name: str):
    from pipewright.config import resolve_usecase
    from pipewright.config.document import discover_usecases

    found = discover_usecases(root)
    if name not in found:
        raise ConfigError(f"no use case {name} in this repository (found: {', '.join(sorted(found)) or 'none'})")
    return resolve_usecase(found[name], env)


def _team_usecases(root: Path, env, tenant: str) -> dict:
    """The team's use cases in this repository, by name, with why Assist is off for each (None: on)."""
    from pipewright.assist import settings
    from pipewright.config import resolve_usecase
    from pipewright.config.document import discover_usecases

    out = {}
    for name, path in sorted(discover_usecases(root).items()):
        try:
            uc = resolve_usecase(path, env)
        except ConfigError:
            continue
        if uc.tenant == tenant:
            out[name] = settings.team_reason(uc)
    return out


def assemble(root: Path, env_name: str, usecase: str | None = None, tenant: str | None = None,
             platform: bool = False, brain=None, source=None):
    """Everything one question needs, or a ConfigError saying why Assist does not start. Returns
    (engine, settings, env, team)."""
    from pipewright.ai import mcp
    from pipewright.assist import brain as brain_module
    from pipewright.assist import settings as assist_settings
    from pipewright.assist.engine import Engine
    from pipewright.assist.screen import Screen
    from pipewright.assist.tools import LiveSource, Tools
    from pipewright.config import load_environment

    env = load_environment(root, env_name)
    s = assist_settings.resolve(env)
    if s.platform_reason:
        raise ConfigError(f"Pipewright Assist is off: {s.platform_reason}")
    allowed: set[str] | None
    if usecase:
        uc = _usecase(root, env, usecase)
        off = assist_settings.team_reason(uc)
        if off:
            raise ConfigError(f"Pipewright Assist is off: {off}")
        tenant, allowed = uc.tenant, {usecase}
    elif platform:
        off = assist_settings.why_off(env, None)
        if off:
            raise ConfigError(f"Pipewright Assist is off: {off}")
        tenant, allowed = None, None
    else:
        tenants = list((env.tenants or {}).keys())
        tenant = tenant or (tenants[0] if len(tenants) == 1 else None)
        if not tenant:
            raise ConfigError("name the use case (--usecase), your team (--tenant), or ask as the platform team "
                              "(--platform)")
        states = _team_usecases(root, env, tenant)
        allowed = {n for n, off in states.items() if off is None}
        if not allowed:
            why = next(iter(states.values()), f"team {tenant} has no use case in this repository")
            raise ConfigError(f"Pipewright Assist is off: {why}")
    if source is None:
        server = mcp.Server(root, env_name, tenant, agent="pipewright-assist")
        source = LiveSource(server)             # open source: the person's own access (no agent identity)
    creds = brain_module.model_credentials(env, s) if (brain is None or s.screening) else None
    screen = None
    if s.screening:
        from pipewright.gcp.session import Gcp

        screen = Screen(s.screening_template, Gcp(creds))   # Model Armor is called as whoever calls the model
    tools = Tools(source, tenant, allowed, s.max_tool_calls, screen, platform=platform)
    if brain is None:
        brain = brain_module.AdkBrain(env.project, s, creds)
    return Engine(brain, tools, env_name, screen, usecase), s, env, tenant


def cmd_explain(args) -> int:
    from pipewright.assist import explain
    from pipewright.assist.record import Record

    root = Path(args.root)
    if not args.env:
        print(explain.page())
        return 0
    from pipewright.config import load_environment

    env = load_environment(root, args.env)
    uc = _usecase(root, env, args.usecase) if args.usecase else None
    asked = Record(root).asked_this_month(env.name, uc.tenant if uc is not None else None) if uc is not None else None
    print(explain.page(env, uc, asked))
    return 0


def _team(root: Path, env, usecase: str | None, tenant: str | None) -> tuple[str, str]:
    """The team a question is asked for and the explain version it confirmed, or a ConfigError saying why Assist is
    off for it. The team's switch lives in its repository, so it is checked here, wherever the answer is made."""
    from pipewright.assist import settings as assist_settings

    if usecase:
        uc = _usecase(root, env, usecase)
        off = assist_settings.team_reason(uc)
        if off:
            raise ConfigError(f"Pipewright Assist is off: {off}")
        return uc.tenant, uc.assist["understood"]
    tenants = list((env.tenants or {}).keys())
    tenant = tenant or (tenants[0] if len(tenants) == 1 else None)
    if not tenant:
        raise ConfigError("name the use case (--usecase), your team (--tenant), or ask as the platform team "
                          "(--platform)")
    states = _team_usecases(root, env, tenant)
    if not any(off is None for off in states.values()):
        why = next(iter(states.values()), f"team {tenant} has no use case in this repository")
        raise ConfigError(f"Pipewright Assist is off: {why}")
    return tenant, assist_settings.EXPLAIN_VERSION


def _ask_hosted(args, root: Path, env) -> int | None:
    """Pipewright Enterprise: where the team has a hosted tool server, the question is answered there (the server
    enforces the platform's switch and the budget, and records the answer). None: answer here instead."""
    from pipewright.assist import hosted
    from pipewright.assist import settings as assist_settings

    if args.platform or args.local:
        return None
    s = assist_settings.resolve(env)
    if s.platform_reason:
        raise ConfigError(f"Pipewright Assist is off: {s.platform_reason}")
    tenant, understood = _team(root, env, args.usecase, args.tenant)
    if not (hosted.server_of(env, tenant) or args.server):
        return None
    out = hosted.ask(env, tenant, args.question, args.usecase, understood, args.server)
    if args.json:
        print(json.dumps({"environment": env.name, "team": tenant, **out}, indent=2))
    elif out.get("ok"):
        print(out["answer"])
        if out.get("budget") is not None:
            print(f"(Team {tenant}: {out.get('asked_this_month')} of {out['budget']} questions this month. "
                  "Recorded in the environment's evidence.)")
    else:
        print(out.get("refused") or out.get("error") or "the tool server gave no answer")
    return 0 if out.get("ok") else 1


def cmd_ask(args) -> int:
    from pipewright.assist.record import Record
    from pipewright.config import load_environment

    root = Path(args.root)
    answered = _ask_hosted(args, root, load_environment(root, args.env))
    if answered is not None:
        return answered
    engine, s, env, team = assemble(root, args.env, args.usecase, args.tenant, args.platform)
    answer = engine.ask(args.question)
    try:
        from pipewright.gcp.session import Gcp

        who = Gcp().identity()
    except Exception:  # noqa: BLE001 - the record is kept with or without the person's address
        who = None
    record = Record(root)
    record.write({"environment": env.name, "team": team, "asked_by": who, "question": args.question,
                  "model": s.model, "location": s.location, **answer.to_record()})
    if args.json:
        print(json.dumps({"environment": env.name, "team": team, **answer.to_record()}, indent=2))
    else:
        print(answer.text)
    return 0


def cmd_test(args) -> int:
    from pipewright.assist import replay

    return replay.main(Path(args.root), args.env, runs=args.runs, only=args.case, output=args.output,
                       scripted=args.scripted)


def add_parser(sub) -> None:
    sp = sub.add_parser("assist", help="Pipewright Assist (preview, 1.4.4, off by default): incident questions "
                                       "answered from what Pipewright recorded")
    actions = sp.add_subparsers(dest="assist_action", required=True)

    p = actions.add_parser("explain", help="what Assist is, what it reads and never reads, the model, who pays, "
                                           "and the settings in play: read it before switching Assist on")
    p.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
    p.add_argument("--env", help="show the settings in play for this environment")
    p.add_argument("--usecase", help="show whether this use case has switched Assist on")
    p.set_defaults(func=cmd_explain)

    p = actions.add_parser("ask", help="ask an incident question, e.g. \"why did last night's training fail?\"")
    p.add_argument("question", help="the question, in quotes")
    p.add_argument("--root", default=".", help="MLOps repo root (contains environments/ and usecases/)")
    p.add_argument("--env", required=True, help="the environment the question is about")
    p.add_argument("--usecase", help="the use case the question is about")
    p.add_argument("--tenant", help="your team, for a question about all its use cases")
    p.add_argument("--platform", action="store_true",
                   help="ask as the platform team, about the whole environment (what is blocked, platform causes)")
    p.add_argument("--json", action="store_true", help="the answer and what it was based on, as JSON")
    p.add_argument("--server", help="Pipewright Enterprise: the team's hosted tool server, when reached through a "
                                    "local proxy (http://localhost:8765); default: the manifest's address")
    p.add_argument("--local", action="store_true",
                   help="answer on this machine with your own access even where a hosted tool server exists "
                        "(not counted or recorded by the platform)")
    p.set_defaults(func=cmd_ask)

    p = actions.add_parser("test", help="replay the frozen incident cases against the model and score them against "
                                        "the pass mark (the release gate)")
    p.add_argument("--root", default=".", help="MLOps repo root (contains environments/)")
    p.add_argument("--env", required=True, help="the environment whose project and settings call the model")
    p.add_argument("--runs", type=int, default=3, help="times each case is run (default 3)")
    p.add_argument("--case", action="append", help="run only this case (repeatable)")
    p.add_argument("--output", help="write the full report (JSON) here")
    p.add_argument("--scripted", action="store_true",
                   help="no model: a scripted stand-in, to check the harness itself (costs nothing)")
    p.set_defaults(func=cmd_test)
