"""FR-87 (Pipewright 1.2.0): `pipewright images prune` deletes old images, never one a release may still need, and
never one in a package no release uses. Dry run unless --apply."""
import json
from types import SimpleNamespace

from pipewright.build import prune

REPO = "europe-west2-docker.pkg.dev/acme-ml-dev/ml-images"


def digest(n: int) -> str:
    return "sha256:" + f"{n:064x}"


def contract(release, *images):
    return json.dumps({"release_id": release, "component_image": f"{REPO}/churn-components@{images[0]}",
                       **({"synthetic": {"image": f"{REPO}/churn-synthetic@{images[1]}",
                                         "evaluation_image": f"{REPO}/churn-synthetic-eval@{images[1]}"}}
                          if len(images) > 1 else {})})


class FakeGcp:
    """An artifacts bucket with one use case's pointer, and a docker repository."""

    def __init__(self, objects, docker):
        self.objects, self.docker, self.deleted = objects, docker, []

    def get(self, url, **params):
        if url.endswith("/dockerImages"):
            return SimpleNamespace(ok=True, status=200, body={"dockerImages": self.docker})
        if "/o/" in url:                                       # gcs_read: metadata
            name = __import__("urllib.parse").parse.unquote(url.rsplit("/o/", 1)[1])
            if name not in self.objects:
                return SimpleNamespace(ok=False, status=404, body={})
            return SimpleNamespace(ok=True, status=200, body={"generation": "1", "name": name})
        return SimpleNamespace(ok=True, status=200, body={"items": [{"name": n} for n in self.objects
                                                                     if n.startswith(params.get("prefix", ""))]})

    def delete(self, url, **params):
        self.deleted.append((url, params))
        return SimpleNamespace(ok=True, status=200, body={})


def docker_image(package, n, day):
    return {"uri": f"{REPO}/{package}@{digest(n)}", "tags": [f"r{n}-abc"], "uploadTime": f"2026-10-{day:02d}T00:00:00Z",
            "imageSizeBytes": str(900_000_000)}


def env(name="dev", bucket="acme-ml-dev-artifacts"):
    return SimpleNamespace(name=name, artifacts_bucket=bucket, artifact_registry={"docker": REPO})


def fake(monkeypatch, objects, docker):
    gcp = FakeGcp(objects, docker)
    monkeypatch.setattr(prune, "gcs_read", lambda g, bucket, name: (objects.get(name), 1 if name in objects else 0))
    monkeypatch.setattr(prune, "gcs_list", lambda g, bucket, prefix: [n for n in objects if n.startswith(prefix)])
    return gcp


def pointer(contract_name, history):
    return json.dumps({"usecase": "churn", "release_id": "r9", "contract": contract_name, "updated_at": "x",
                       "history": [{"release_id": r, "contract": c, "updated_at": "x"} for r, c in history]})


def test_prune_keeps_what_releases_pin_and_the_newest(monkeypatch):
    objects = {"contracts/churn/current.json": pointer("contracts/churn/r9.json", [("r5", "contracts/churn/r5.json")]),
               "contracts/churn/r9.json": contract("r9", digest(9), digest(90)),
               "contracts/churn/r5.json": contract("r5", digest(5)),
               "contracts/churn/r2.json": contract("r2", digest(2))}          # not in the pointer: may go
    docker = [docker_image("churn-components", n, n) for n in range(1, 10)]
    docker += [docker_image("churn-synthetic", 90, 9), docker_image("team-own-tool", 50, 1)]
    gcp = fake(monkeypatch, objects, docker)
    p = prune.plan(gcp, env(), keep=2)
    kept = {i.digest: p.reasons[i.digest] for i in p.keep}
    assert kept[digest(9)] == "release churn r9" and kept[digest(5)] == "release churn r5"
    assert kept[digest(90)] == "release churn r9"
    assert kept[digest(8)] == "one of the newest 2"                      # r9 counts among the newest two
    assert kept[digest(50)] == "not a package releases use"
    assert sorted(i.digest for i in p.delete) == sorted(digest(n) for n in (1, 2, 3, 4, 6, 7))
    text = prune.render(p, applied=False)
    assert "would free up to 5.4 GB (less where layers are shared); run again with --apply" in text
    assert gcp.deleted == []

    prune.apply(gcp, env(), p)
    assert len(gcp.deleted) == 6 and all(params == {"force": "true"} for _, params in gcp.deleted)
    assert any(url.endswith(f"/repositories/ml-images/packages/churn-components/versions/{digest(1)}")
               for url, _ in gcp.deleted)
    assert "freed up to 5.4 GB" in prune.render(p, applied=True)


def test_an_environment_sharing_the_repository_keeps_its_images(monkeypatch):
    objects = {"contracts/churn/current.json": pointer("contracts/churn/r9.json", []),
               "contracts/churn/r9.json": contract("r9", digest(9))}
    docker = [docker_image("churn-components", n, n) for n in range(1, 10)]
    gcp = fake(monkeypatch, objects, docker)
    others = {"contracts/churn/current.json": pointer("contracts/churn/r3.json", []),
              "contracts/churn/r3.json": contract("r3", digest(3))}
    real = prune.referenced

    def referenced(g, e):
        if e.name == "test":
            monkeypatch.setattr(prune, "gcs_read", lambda g, b, n: (others.get(n), 1))
            monkeypatch.setattr(prune, "gcs_list", lambda g, b, prefix: list(others))
            try:
                return real(g, e)
            finally:
                fake(monkeypatch, objects, docker)
        return real(g, e)

    monkeypatch.setattr(prune, "referenced", referenced)
    p = prune.plan(gcp, env(), keep=1, sharing=[env("test", "acme-ml-test-artifacts")])
    assert {i.digest for i in p.keep} == {digest(9), digest(3)}
