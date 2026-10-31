"""FR-80 (Pipewright 1.2): an environment with promotion.from gets its releases through `pipewright promote`, not a
release built there. ci init writes a promote workflow and an operate action instead of a release job, the
decisions workflow's cadence is an option, and `release` warns (or with promotion.release: refuse, refuses)."""
import shutil
import subprocess
from types import SimpleNamespace

import pytest
import yaml

from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError
from pipewright.release import service
from pipewright.scaffold import ci


def test_a_promoted_environment_gets_the_promote_workflow_instead_of_a_release_job():
    files = ci.render(["dev", "test", "prod"], promotions={"prod": "test"})
    release = yaml.safe_load(files[".github/workflows/pipewright-release.yml"])
    assert list(release["jobs"]) == ["changes", "release-dev", "release-test"]
    promote = yaml.safe_load(files[ci.PROMOTE_FILE])
    assert promote[True]["workflow_dispatch"]["inputs"]["environment"]["options"] == ["prod"]
    job = promote["jobs"]["promote"]
    assert job["environment"] == "${{ inputs.environment }}" and job["steps"][0]["with"]["fetch-depth"] == 0
    run = job["steps"][2]["run"]
    assert "prod) SOURCE=test ;;" in run and 'pipewright promote --from "$SOURCE" --to "$ENV"' in run
    operate = yaml.safe_load(files[".github/workflows/pipewright-operate.yml"])
    assert "promote" in operate[True]["workflow_dispatch"]["inputs"]["action"]["options"]
    script = operate["jobs"]["operate"]["steps"][-1]["run"]
    assert 'promote)         case "$ENV" in prod) SOURCE=test ;;' in script
    for text in files.values():
        assert "@@" not in text


@pytest.mark.skipif(not shutil.which("bash"), reason="needs bash")
def test_the_scripts_parse_as_bash():
    files = ci.render(["dev", "test", "prod"], promotions={"prod": "test"})
    for path in (ci.PROMOTE_FILE, ".github/workflows/pipewright-operate.yml"):
        doc = yaml.safe_load(files[path])
        for job in doc["jobs"].values():
            for step in job["steps"]:
                if "run" in step:
                    script = step["run"].replace("${{", "").replace("}}", "")
                    r = subprocess.run(["bash", "-n"], input=script.encode(), capture_output=True)   # bytes: no CRLF
                    assert r.returncode == 0, (path, r.stderr)


def test_without_promotions_nothing_changes_but_the_cadence():
    files = ci.render(["dev", "prod"])
    assert ci.PROMOTE_FILE not in files
    operate = yaml.safe_load(files[".github/workflows/pipewright-operate.yml"])
    assert "promote" not in operate[True]["workflow_dispatch"]["inputs"]["action"]["options"]
    decisions = yaml.safe_load(files[".github/workflows/pipewright-decisions.yml"])
    assert decisions[True]["schedule"] == [{"cron": "17 7-18 * * 1-5"}]
    fast = ci.render(["dev"], decisions_cron="*/15 * * * *")[".github/workflows/pipewright-decisions.yml"]
    fast = yaml.safe_load(fast)
    assert fast[True]["schedule"] == [{"cron": "*/15 * * * *"}]
    with pytest.raises(ConfigError, match="five cron fields"):
        ci.render(["dev"], decisions_cron="hourly")
    with pytest.raises(ConfigError, match="cannot have promotion.from"):
        ci.render(["prod", "dev"], promotions={"prod": "dev"})


def test_init_reads_promotion_from_the_environment_files(repo):
    repo.add_env("prod")
    repo.environments["prod"]["promotion"] = {"from": "dev"}
    repo.write()
    assert ci.promotions(repo.root, ["dev", "prod"]) == {"prod": "dev"}
    written = {p.relative_to(repo.root).as_posix() for p in ci.init(repo.root, ["dev", "prod"])}
    assert ci.PROMOTE_FILE in written


def releasing(repo, monkeypatch, promotion):
    repo.add_env("prod")
    repo.environments["prod"]["promotion"] = promotion
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "prod"))
    env = load_environment(repo.root, "prod")
    monkeypatch.setattr(service, "release_id_for", lambda d: ("abc123", "abc123full"))
    monkeypatch.setattr(service, "read_pointer", lambda gcp, env, name: (_ for _ in ()).throw(RuntimeError("built")))
    gcp = SimpleNamespace(identity=lambda: "ci@acme")
    logs = []
    return lambda: service.release(uc, env, gcp, log=logs.append), logs


def test_release_warns_by_default(repo, monkeypatch):
    run, logs = releasing(repo, monkeypatch, {"from": "dev"})
    with pytest.raises(RuntimeError, match="built"):
        run()
    assert logs[0].startswith("WARN  prod's releases come from dev (promotion.from)")
    assert "`pipewright promote --from dev --to prod --usecase churn`" in logs[0]


def test_release_refuses_when_asked(repo, monkeypatch):
    run, logs = releasing(repo, monkeypatch, {"from": "dev", "release": "refuse"})
    with pytest.raises(ConfigError, match="promotion.release: refuse stops a release built here"):
        run()


def test_a_lost_event_ends_the_command_with_a_warning(monkeypatch, capsys):
    import importlib

    main = importlib.import_module("pipewright.cli.main")
    from pipewright.observability import events

    class Refused:
        def post(self, url, body):
            return SimpleNamespace(ok=False, status=403)

    def command(args):
        events.emit(events.event("release.live", "churn", "prod"), "projects/p/topics/pw-events", Refused())
        return 0

    parser = main.build_parser()
    monkeypatch.setattr(main, "build_parser", lambda: parser)
    monkeypatch.setattr(parser, "parse_args", lambda argv: SimpleNamespace(func=command))
    parser.plugins = []
    assert main.main([]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == ("WARN  1 event(s) did not reach the event log: release.live (HTTP 403); the identity needs "
                       "roles/pubsub.publisher on the events topic")
    assert events.LOST == []
