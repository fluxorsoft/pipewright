"""pipewright decisions pending: what waits for a person in an environment, for the generated workflows (FR-56).

Each item names the GitHub environment that gates it (D-120: the environment itself, or <environment>-<group> for an
approver group) and the pipewright command a gate job runs once that environment's reviewers approve:

    model-approve   a registered version newer than the default that passes the gates
    retrain-approve an open retrain case, once per required approver group not yet approved (or once with none)
    loop-approve    a closed retrain loop validated in test, waiting for the production approval
    loop-advance    a running loop: no person needed, the scheduled workflow advances it (`--steps`)
    deploy-approve  approvals.deploy_model ci (Pipewright 1.2): the default version passes its gates and is not served
    deploy-auto     approvals.deploy_model on_register (1.2): the same, deployed by the scheduled workflow, no person
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

ACTIVE_LOOP = ("approved", "training", "candidate", "validating", "verifying")


@dataclass(frozen=True)
class Item:
    kind: str
    usecase: str
    environment: str          # the Pipewright environment
    gate: str | None          # the GitHub environment whose reviewers decide (None: no person needed)
    key: str                  # the version, case or group it is about
    command: list[str]
    summary: str
    launchers: list[str] = field(default_factory=list)   # the launcher URLs the command calls (ID tokens for each)

    @property
    def id(self) -> str:      # stable: the workflow dispatches one gate run per id
        return f"{self.environment}/{self.usecase}/{self.kind}/{self.key}"

    def to_dict(self) -> dict:
        return {"id": self.id, **asdict(self)}


def pending_for(usecase: str, env: str, versions: list, cases: list[dict],
                launchers: dict[str, str] | None = None, deploy: dict | None = None) -> list[Item]:
    """The pending decisions of one use case, from its registry versions (models.registry.Version) and its cases.
    `launchers`: {"production": url, "test": url} where known; a loop calls both, a retrain decision production's.
    `deploy` (Pipewright 1.2, FR-63): {"mode": ci | on_register, "served": {version ids on the endpoint},
    "canary": bool} for a use case that deploys through the workflows."""
    launchers = launchers or {}
    own = [u for u in (launchers.get("production"),) if u]
    both = [u for u in (launchers.get("production"), launchers.get("test")) if u]
    out: list[Item] = []
    default = next((v for v in versions if v.default), None)
    number = int(default.version_id) if default and str(default.version_id).isdigit() else 0
    # Only the newest version that passes the gates: approving an older one once a newer exists is never the ask.
    newer = [v for v in versions if not v.default and str(v.version_id).isdigit() and int(v.version_id) > number
             and v.gates == "pass"]
    for v in sorted(newer, key=lambda v: int(v.version_id))[-1:]:
        out.append(Item("model-approve", usecase, env, env, f"v{v.version_id}",
                        ["model", "approve", "--env", env, "--usecase", usecase, "--version", str(v.version_id),
                         "--approve"], f"v{v.version_id} passes the gates; the default is "
                                       f"v{default.version_id if default else 'none'}"))
    unserved = deploy and default and str(default.version_id) not in deploy.get("served", set())
    if unserved and default.gates == "pass" and (default.labels or {}).get("approval") != "pending":
        ci = deploy.get("mode") == "ci"
        out.append(Item("deploy-approve" if ci else "deploy-auto", usecase, env, env if ci else None,
                        f"v{default.version_id}",
                        ["deploy-model", "--env", env, "--usecase", usecase, "--approve",
                         *(["--canary"] if deploy.get("canary") and deploy.get("served") else [])],
                        f"v{default.version_id} is the default and passes its gates; the endpoint serves "
                        + (", ".join(f"v{s}" for s in sorted(deploy.get("served") or [])) or "nothing")))
    for c in cases:
        loop = c.get("loop") or {}
        state = loop.get("state")
        if state == "validated":
            out.append(Item("loop-approve", usecase, env, env, c["case_id"],
                            ["loop", "advance", "--env", env, "--usecase", usecase, "--case", c["case_id"],
                             "--approve"], f"case {c['case_id']}: candidate validated in test, ready to promote", both))
        elif state in ACTIVE_LOOP:
            out.append(Item("loop-advance", usecase, env, None, c["case_id"],
                            ["loop", "advance", "--env", env, "--usecase", usecase, "--case", c["case_id"],
                             "--steps", "5"], f"case {c['case_id']}: loop {state}", both))
        if c.get("state") != "open":
            continue
        approved = {a.get("group") for a in c.get("approvals") or []}
        groups = [g for g in c.get("required") or [] if g not in approved] or ([None] if not c.get("required") else [])
        for g in groups:
            out.append(Item("retrain-approve", usecase, env, f"{env}-{g}" if g else env,
                            f"{c['case_id']}" + (f"-{g}" if g else ""),
                            ["retrain", "approve", "--env", env, "--usecase", usecase, "--case", c["case_id"],
                             *(["--group", g] if g else []), "--reason", "approved in GitHub (see the run)"],
                            f"case {c['case_id']} ({', '.join(c.get('triggers') or [])})"
                            + (f": approval of {g}" if g else ""), own))
    return out


def pending(root, env_name: str, usecases: list[str] | None = None) -> list[Item]:
    """Every pending decision of the environment's released use cases."""
    from pathlib import Path

    from pipewright.config import discover_usecases, load_environment, resolve_usecase, run_in
    from pipewright.gcp import Gcp
    from pipewright.models import registry
    from pipewright.release import service as release
    from pipewright.retraining import client

    env = load_environment(Path(root), env_name)
    gcp = Gcp()
    store = client.store(env, gcp)
    out: list[Item] = []
    for name, path in sorted(discover_usecases(Path(root)).items()):
        envs = run_in(path)                            # 1.1: a use case for other environments only
        if usecases and name not in usecases or envs and env.name not in envs:
            continue
        pointer, _ = release.read_pointer(gcp, env, name)
        if pointer is None:
            continue                                   # never released here
        uc = resolve_usecase(path, env)
        try:
            versions = registry.versions(uc, env, gcp.session.credentials)
        except Exception:  # noqa: BLE001 - no model yet: nothing to approve
            versions = []
        out += pending_for(name, env.name, versions, store.list(name), _launchers(root, env), _deploy(uc, env, gcp))
    return out


def _deploy(uc, env, gcp) -> dict | None:
    """approvals.deploy_model ci or on_register (Pipewright 1.2, FR-63): the mode, the versions the endpoint serves and
    whether new versions start as a canary."""
    mode = (uc.approvals or {}).get("deploy_model")
    serving = (uc.model or {}).get("serving") or {}
    if mode not in ("ci", "on_register") or serving.get("target", "vertex") != "vertex":
        return None
    from pipewright.models.deploy import find_endpoint

    served: set[str] = set()
    resource = find_endpoint(uc, env, gcp)
    if resource:
        r = gcp.get(f"https://{env.region}-aiplatform.googleapis.com/v1/{resource}")
        served = {str(d.get("modelVersionId")) for d in ((r.body or {}).get("deployedModels") or [])} if r.ok else set()
    return {"mode": mode, "served": served, "canary": bool(serving.get("canary_percent"))}


def _launchers(root, env) -> dict[str, str]:
    from pathlib import Path

    from pipewright.config import load_environment

    out = {"production": (env.launcher or {}).get("url")}
    source = env.promotion_from
    if source:
        try:
            out["test"] = (load_environment(Path(root), source).launcher or {}).get("url")
        except Exception:  # noqa: BLE001 - no such environment here: the loop cannot run either
            pass
    return {k: v.rstrip("/") for k, v in out.items() if v}
