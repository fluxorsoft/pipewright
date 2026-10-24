"""FR-54: the approved image allow-list (manifest policies.images)."""
from dataclasses import replace

from pipewright.build import allowlist, images
from pipewright.config import load_environment, resolve_usecase
from pipewright.release.contract import ReleaseContract

MIRROR = "https://europe-west2-python.pkg.dev/acme-ml/pypi-mirror/simple/"
POLICY = {"base_images": ["europe-west2-docker.pkg.dev/acme-ml/mirror/*"], "package_indexes": [MIRROR],
          "default_base_image": "europe-west2-docker.pkg.dev/acme-ml/mirror/python:3.11-slim"}


def setup(repo, policy, requirements=None):
    repo.manifests["dev"].setdefault("policies", {})["images"] = policy
    path = repo.write()
    if requirements is not None:
        (path.parent / "components" / "requirements.txt").write_text(requirements, encoding="utf-8")
    env = load_environment(repo.root, "dev")
    return resolve_usecase(path, env), env


def test_requirements_name_their_indexes_and_direct_references(tmp_path):
    (tmp_path / "more.txt").write_text("--extra-index-url https://other.example/simple\nrequests\n", encoding="utf-8")
    text = ("--index-url=https://a.example/simple  # the mirror\n-r more.txt\ntorch==2.4.1\n"
            "mylib @ https://files.example/mylib.whl\ngit+https://github.com/x/y.git\n-e ./local\n# git+ignored\n")
    indexes, direct = allowlist.sources(text, tmp_path)
    assert indexes == ["https://a.example/simple", "https://other.example/simple"]
    assert direct == ["mylib @ https://files.example/mylib.whl", "git+https://github.com/x/y.git", "-e ./local"]


def test_no_policy_changes_nothing(repo):
    uc, env = setup(repo, {})
    assert allowlist.check(uc, env) == []
    assert allowlist.with_index("torch\n", {}) == "torch\n"


def test_bases_and_indexes_outside_the_policy_are_refused(repo):
    uc, env = setup(repo, POLICY, "--index-url https://pypi.org/simple\ntorch\nx @ https://e.example/x.whl\n")
    found = allowlist.check(uc, env)
    assert any("names the package index https://pypi.org/simple" in p for p in found)
    assert any("direct reference (x @ https://e.example/x.whl)" in p for p in found)
    uc, env = setup(repo, {**POLICY, "default_base_image": "python:3.11-slim"}, "torch\n")
    assert allowlist.check(uc, env) == ["policies.images: the component image is built from python:3.11-slim, "
                                        "which is not an approved base image "
                                        "(europe-west2-docker.pkg.dev/acme-ml/mirror/*)"]
    uc, env = setup(repo, POLICY, f"--index-url {MIRROR.rstrip('/')}\ntorch\n")
    assert allowlist.check(uc, env) == []                       # the approved index, with or without the slash


def test_the_first_approved_index_is_used_when_a_file_names_none():
    assert allowlist.with_index("torch\n", POLICY) == f"--index-url {MIRROR}\ntorch\n"
    named = "--index-url https://x.example/simple\ntorch\n"
    assert allowlist.with_index(named, POLICY) == named          # validate refuses it if it is not approved


def test_builds_use_the_platform_base_and_index(repo, tmp_path, monkeypatch):
    uc, env = setup(repo, POLICY)
    (uc.components_dir / "train.py").write_text("from kfp import dsl\n@dsl.component\ndef train(): ...\n",
                                                encoding="utf-8")
    calls = []

    def docker(*args, input_text=None, quiet=True):
        calls.append(args)
        if args[0] == "inspect":
            return "r.example/p/churn-components@sha256:" + "a" * 64
        return ""

    monkeypatch.setattr(images, "_docker", docker)
    monkeypatch.setattr(images, "docker_login", lambda host, token: None)
    seen = {}
    real = images._build_and_push

    def spy(ctx, *a, **k):
        out = real(ctx, *a, **k)
        seen["requirements"] = (ctx / "requirements.txt").read_text(encoding="utf-8")
        return out

    monkeypatch.setattr(images, "_build_and_push", spy)
    images.build_and_push(uc, "r.example/p", "rel1", "token", images_policy=POLICY)
    build = next(a for a in calls if a[0] == "build")
    assert f"BASE_IMAGE={POLICY['default_base_image']}" in build
    assert seen["requirements"].startswith(f"--index-url {MIRROR}\n")


def test_the_contract_records_bases_only_with_a_policy():
    from tests.test_release_parts import CONTRACT

    assert '"images"' not in CONTRACT.to_json()                   # as in 1.0
    recorded = replace(CONTRACT, images={"base_images": {"component image": "m/python@sha256:1"},
                                         "package_indexes": [MIRROR]})
    again = ReleaseContract.from_json(recorded.to_json())
    assert again.images["base_images"]["component image"] == "m/python@sha256:1"


def test_validate_refuses_an_unapproved_index(repo, capsys):
    from pipewright.checks.kfp_version import compiler_version
    from pipewright.cli.main import main

    setup(repo, POLICY, f"--index-url https://pypi.org/simple\nkfp=={compiler_version() or '2.17.0'}\ntorch\n")
    assert main(["validate", "--root", str(repo.root), "--env", "dev"]) == 1
    assert "not approved" in capsys.readouterr().out
