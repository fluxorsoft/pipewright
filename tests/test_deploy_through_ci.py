"""FR-63 (Pipewright 1.2.0): approvals.deploy_model ci (a GitHub-approved gate per new default version) and on_register
(deployed by the decisions workflow). manual stays the default; the platform may require ci."""
import pytest
import yaml

from pipewright import decisions
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.models import deploy
from pipewright.models.registry import Version
from pipewright.scaffold import ci


def versions(default="3", gates="pass", approval=None):
    return [Version(default, True, [], "", "2026-10-31", {"approval": approval} if approval else {}, gates),
            Version("2", False, [], "", "2026-10-30", {}, "pass")]


def test_ci_asks_a_gate_for_a_default_version_the_endpoint_does_not_serve():
    (item,) = decisions.pending_for("churn", "prod", versions(), [], deploy={"mode": "ci", "served": {"2"},
                                                                             "canary": True})
    assert (item.kind, item.gate, item.key) == ("deploy-approve", "prod", "v3")
    assert item.command == ["deploy-model", "--env", "prod", "--usecase", "churn", "--approve", "--canary"]
    assert "the endpoint serves v2" in item.summary


def test_on_register_needs_no_person_and_nothing_is_asked_twice():
    (item,) = decisions.pending_for("churn", "test", versions(), [], deploy={"mode": "on_register", "served": set()})
    assert item.kind == "deploy-auto" and item.gate is None and "--canary" not in item.command
    served = {"mode": "ci", "served": {"3"}}
    assert decisions.pending_for("churn", "prod", versions(), [], deploy=served) == []
    assert decisions.pending_for("churn", "prod", versions(gates="FAIL: auc"), [], deploy={"mode": "ci"}) == []
    assert decisions.pending_for("churn", "prod", versions(approval="pending"), [], deploy={"mode": "ci"}) == []
    assert decisions.pending_for("churn", "prod", versions(), []) == []                   # manual: as in 1.1


def resolved(repo, mode, policy=None):
    repo.usecase["spec"]["approvals"]["deploy_model"] = mode
    repo.usecase["spec"]["environments"]["dev"].pop("approvals", None)     # the fixture's dev override: auto
    if policy:
        repo.manifests["dev"]["policies"]["deploy_model"] = policy
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_the_platform_policy(repo):
    with pytest.raises(ConfigError, match="on_register is not allowed in dev"):
        resolved(repo, "on_register", "manual")
    assert resolved(repo, "ci", "manual").approvals["deploy_model"] == "ci"
    with pytest.raises(ConfigError, match="only through a GitHub-approved gate"):
        resolved(repo, "manual", "ci")


def test_ci_needs_the_gates_approval_on_the_command_line(repo):
    uc = resolved(repo, "ci")
    with pytest.raises(ConfigError, match="approvals.deploy_model is ci in dev: re-run with --approve .from the "
                                          "pipewright-gate workflow"):
        deploy.check_approval(uc, "ci@p", False)
    deploy.check_approval(uc, "ci@p", True)
    deploy.check_approval(resolved(repo, "on_register"), "ci@p", False)


def test_the_decisions_workflow_deploys_on_register_items():
    files = ci.render(["dev"])
    wf = yaml.safe_load(files[".github/workflows/pipewright-decisions.yml"])
    step = next(s for s in wf["jobs"][next(iter(wf["jobs"]))]["steps"] if s.get("name") == "Deploy registered models")
    assert 'select(.kind == "deploy-auto")' in step["run"]
