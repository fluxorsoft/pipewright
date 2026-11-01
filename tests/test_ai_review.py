"""The review bundle (Pipewright 1.3.0): what changes in Google Cloud, settings that moved, checks, judgement calls."""

import subprocess

from pipewright.ai import review
from pipewright.cli.main import main
from pipewright.config import load_environment
from tests.test_config_ml import ml_repo


def git(root, *args):
    subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)


def committed(repo):
    ml_repo(repo, evaluation={"primary_metric": "auc", "gates": [{"metric": "auc", "min": 0.7}]})
    repo.write()
    git(repo.root, "init", "-q", "-b", "main")
    git(repo.root, "-c", "user.email=t@example.com", "-c", "user.name=t", "add", "-A")
    git(repo.root, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-q", "-m", "base")
    return load_environment(repo.root, "dev")


def test_a_change_shows_the_settings_that_moved(repo):
    env = committed(repo)
    repo.usecase["spec"]["evaluation"]["gates"][0]["min"] = 0.75
    repo.usecase["spec"]["data"]["split"] = {"strategy": "random"}
    repo.write()
    text = review.bundle(repo.root, env)
    assert "## Review bundle: churn (dev)" in text
    assert "`evaluation.gates`: `[{'metric': 'auc', 'min': 0.7}]` -> `[{'metric': 'auc', 'min': 0.75}]`" in text
    assert "`data.split.strategy`: `time` -> `random`" in text
    assert "(judgement check)" in text                             # the new warning is listed under checks
    assert "### Judgement calls" in text and "no IAM, network or project changes" in text
    assert "from the use case's own prepared table" in text       # the feature pattern is stated


def test_a_new_use_case_lists_its_tables(repo):
    committed(repo)
    import shutil

    shutil.copytree(repo.root / "usecases" / "churn", repo.root / "usecases" / "lapse")
    import yaml

    path = repo.root / "usecases" / "lapse" / "usecase.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["metadata"]["name"] = "lapse"
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    text = review.bundle(repo.root, load_environment(repo.root, "dev"))
    assert "### lapse (new)" in text and "+ table `" in text


def test_a_change_that_does_not_validate_says_so(repo):
    env = committed(repo)
    repo.usecase["spec"]["data"]["split"] = {"strategy": "time"}      # no time_column
    repo.write()
    text = review.bundle(repo.root, env)
    assert "**Does not validate:**" in text and "fix them before review" in text


def test_the_cli_writes_the_bundle(repo, tmp_path):
    committed(repo)
    out = tmp_path / "PR.md"
    assert main(["review", "--root", str(repo.root), "--env", "dev", "--output", str(out)]) == 0
    assert out.read_text(encoding="utf-8").startswith("## Review bundle")
