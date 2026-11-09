"""FR-109: a version that no longer takes traffic keeps serving for model.serving.drain_seconds before it is
undeployed."""
from types import SimpleNamespace

from pipewright.models import deploy


def uc(serving):
    return SimpleNamespace(model={"serving": serving})


def old():
    return [SimpleNamespace(id="1", model_version_id="9")]


def test_drains_240s_by_default():
    waited, said = [], []
    deploy._drain(uc({}), old(), said.append, sleep=waited.append)
    assert waited == [240]
    assert "v9" in said[0]


def test_drain_seconds_is_honoured_and_zero_undeploys_at_once():
    waited = []
    deploy._drain(uc({"drain_seconds": 30}), old(), lambda *_: None, sleep=waited.append)
    deploy._drain(uc({"drain_seconds": 0}), old(), lambda *_: None, sleep=waited.append)
    deploy._drain(uc({}), [], lambda *_: None, sleep=waited.append)
    assert waited == [30]
