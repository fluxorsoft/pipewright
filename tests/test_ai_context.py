"""Context packs (Pipewright 1.3.0): generated, stamped, checked; the team's own text in pointer files is kept."""

from pipewright.ai import context
from pipewright.cli.main import main
from pipewright.config import load_environment
from tests.test_config_ml import ml_repo


def built(repo):
    ml_repo(repo)
    repo.write()
    env = load_environment(repo.root, "dev")
    return env, context.build(repo.root, env)


def test_build_writes_the_pack_the_playbooks_and_the_pointers(repo):
    env, written = built(repo)
    names = {p.name for p in written}
    assert {"APP_CONTEXT.md", "AGENTS.md", "CLAUDE.md", "GEMINI.md"} <= names
    pack = (repo.root / "APP_CONTEXT.md").read_text(encoding="utf-8")
    assert "pipewright-context-stamp:" in pack and "## Teams (tenants)" in pack and "**churn**" in pack
    assert "Features come from **feature tables or Feature Store feature groups**" in pack
    assert "from its own prepared table" in pack            # the fixture use case has no features_from
    assert (repo.root / ".pipewright" / "playbooks" / "onboard-a-use-case.md").exists()
    assert ".pipewright/playbooks/onboard-a-use-case.md" in pack


def test_the_teams_own_text_survives_a_rebuild(repo):
    (repo.root / "CLAUDE.md").write_text("# Our conventions\n\nUse British spelling.\n", encoding="utf-8")
    env, _ = built(repo)
    first = (repo.root / "CLAUDE.md").read_text(encoding="utf-8")
    assert first.startswith("# Our conventions") and context.BEGIN in first and first.count(context.BEGIN) == 1
    context.build(repo.root, env)
    again = (repo.root / "CLAUDE.md").read_text(encoding="utf-8")
    assert again == first


def test_check_passes_when_current_and_fails_when_stale(repo):
    env, _ = built(repo)
    assert context.check(repo.root, env)[0]
    repo.usecase["spec"]["data"]["split"]["test_fraction"] = 0.25
    repo.write()
    ok, msg = context.check(repo.root, load_environment(repo.root, "dev"))
    assert not ok and "stale" in msg


def test_check_fails_when_missing(repo):
    ml_repo(repo)
    repo.write()
    ok, msg = context.check(repo.root, load_environment(repo.root, "dev"))
    assert not ok and "missing" in msg


def test_policies_and_agent_identity_reach_the_pack(repo):
    repo.manifests["dev"]["policies"] = {"promote_model": "manual", "evaluation": {"min_gates": {"auc": 0.7}}}
    repo.manifests["dev"]["tenants"]["churn"]["agent_service_account"] = "pw-agent-churn@acme.iam"
    built(repo)
    pack = (repo.root / "APP_CONTEXT.md").read_text(encoding="utf-8")
    assert "approvals.promote_model: at least `manual`" in pack
    assert "A gate on `auc`" in pack and "agent identity `pw-agent-churn@acme.iam`" in pack


def test_the_cli_builds_checks_and_respects_a_forbidden_environment(repo, capsys):
    ml_repo(repo)
    repo.write()
    assert main(["context", "build", "--root", str(repo.root), "--env", "dev"]) == 0
    assert main(["context", "check", "--root", str(repo.root), "--env", "dev"]) == 0
    repo.manifests["dev"]["ai"] = {"local_tools": "forbidden"}
    repo.write()
    assert main(["context", "build", "--root", str(repo.root), "--env", "dev"]) != 0
    assert "ai.local_tools: forbidden" in capsys.readouterr().err
