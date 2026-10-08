import pytest

from pipewright.checks.kfp_version import check
from pipewright.config import load_environment, resolve_usecase


def uc_with(repo, requirements: str):
    path = repo.write()
    (path.parent / "components" / "requirements.txt").write_text(requirements, encoding="utf-8")
    return resolve_usecase(path, load_environment(repo.root, "dev"))


@pytest.mark.parametrize("line", [
    "kfp==2.17.0", "kfp==2.17.*", "kfp~=2.17.0", "kfp>=2.17,<2.18", "kfp == 2.17.3", "KFP==2.17.0",
    "kfp[kubernetes]==2.17.0",
])
def test_compatible_pins(repo, line):
    result = check(uc_with(repo, f"--extra-index-url https://example/whl\n# comment\n{line}\ntorch\n"), "2.17.0")
    assert result.ok, result.message


@pytest.mark.parametrize("line, fragment", [
    ("kfp", "allows kfp versions other than 2.17.x"),
    ("kfp>=2.0", "allows kfp versions other than 2.17.x"),
    ("kfp~=2.17", "allows kfp versions other than 2.17.x"),
    ("kfp==2.7.0", "does not match Pipewright's compiler (kfp 2.17.0)"),
    ("kfp<2", "does not match"),
])
def test_incompatible_pins(repo, line, fragment):
    result = check(uc_with(repo, f"{line}\n"), "2.17.0")
    assert not result.ok and fragment in result.message and "pin `kfp==2.17.*`" in result.message


def test_missing_kfp(repo):
    result = check(uc_with(repo, "torch\npandas\n"), "2.17.0")
    assert not result.ok and "has no kfp requirement: add `kfp==2.17.*`" in result.message


def test_follows_includes(repo):
    uc = uc_with(repo, "-r base.txt\ntorch\n")
    (uc.components_dir / "base.txt").write_text("kfp==2.17.1\n", encoding="utf-8")
    assert check(uc, "2.17.0").ok


def test_validate_reports_mismatch(repo, capsys):
    from pipewright.cli import main

    uc_with(repo, "kfp==2.7.0\n")
    assert main(["validate", "--root", str(repo.root), "--env", "dev"]) == 1
    assert "kfp version" in capsys.readouterr().out
