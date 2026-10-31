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


def test_a_newer_compiler_points_at_the_tested_kfp(repo):
    # FR-81 (Pipewright 1.2), the ebook's Chapter 16: a fresh install resolved kfp 2.18 against a 2.17 pin.
    result = check(uc_with(repo, "kfp==2.17.0\n"), "2.18.0")
    assert not result.ok and 'pin `kfp==2.18.*`, or install the kfp Pipewright was tested with: pip install ' \
        '"kfp==2.17.*"' in result.message


def test_the_examples_pin_the_tested_kfp():
    from pathlib import Path

    from pipewright.checks.kfp_version import TESTED_KFP, constraints

    examples = Path(__file__).parent.parent / "examples"
    pins = {p.parent.parent.name: [line for line in p.read_text().splitlines() if line.startswith("kfp")]
            for p in examples.glob("*/components/requirements.txt")}
    assert pins and all(lines == [f"kfp=={TESTED_KFP}.0"] for lines in pins.values()), pins
    assert f"kfp=={TESTED_KFP}.*" in constraints()


def test_the_package_allows_kfp_only_up_to_the_tested_version():
    """Found releasing 1.2.0: the package allowed any kfp 2.x, a fresh install took a newer kfp than the examples
    pin, and validate refused every new use case. The upper bound is the next minor after TESTED_KFP."""
    import re
    from pathlib import Path

    from pipewright.checks.kfp_version import TESTED_KFP

    text = (Path(__file__).parent.parent / "pyproject.toml").read_text(encoding="utf-8")
    upper = re.search(r'"kfp>=[\d.]+,<([\d.]+)"', text).group(1)
    major, minor = TESTED_KFP.split(".")
    assert upper == f"{major}.{int(minor) + 1}"
