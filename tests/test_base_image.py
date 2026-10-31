"""FR-59 (Pipewright 1.2.0): components.base_image, a base per use case within the approved base images."""
from pipewright.build import allowlist
from pipewright.config import load_environment, resolve_usecase

CUDA = "nvcr.io/nvidia/pytorch@sha256:" + "a" * 64


def resolved(repo, base=None):
    if base:
        repo.usecase["spec"]["components"]["base_image"] = base
    return resolve_usecase(repo.write(), load_environment(repo.root, "dev"))


def test_a_use_case_names_its_base(repo):
    uc = resolved(repo, CUDA)
    assert uc.base_image == CUDA and uc.to_dict()["components"]["base_image"] == CUDA
    env = load_environment(repo.root, "dev")
    assert allowlist.usecase_bases(uc, env, {})["component image"] == CUDA


def test_the_allow_list_refuses_a_base_it_does_not_name(repo):
    repo.manifests["dev"]["policies"]["images"] = {"base_images": ["python:3.11-slim*"]}
    uc = resolved(repo, CUDA)
    refused = allowlist.check(uc, load_environment(repo.root, "dev"))
    assert refused and any("nvcr.io/nvidia/pytorch" in r for r in refused)
    repo.manifests["dev"]["policies"]["images"] = {"base_images": ["python:3.11-slim*", "nvcr.io/nvidia/*"]}
    assert not allowlist.check(resolved(repo, CUDA), load_environment(repo.root, "dev"))


def test_a_1_x_use_case_keeps_the_environments_base(repo):
    uc = resolved(repo)
    assert uc.base_image is None and "base_image" not in uc.to_dict()["components"]
    assert allowlist.usecase_bases(uc, load_environment(repo.root, "dev"), {})["component image"] == \
        allowlist.DEFAULT_BASE
