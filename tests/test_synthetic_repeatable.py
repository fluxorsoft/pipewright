"""Repeatable synthetic runs (FR-107, 1.4.3): seed and max_epochs reach the engine, the evidence says whether a run
repeats, validate warns on a seed without epochs, and nothing changes when neither is set."""

import socket
import sys
import types

import pytest

from pipewright.synthetic.runtime import engines
from tests.test_synthetic import problems, resolve, synthetic_repo


class _Gen:
    def __init__(self, config):
        self.config = config


class _Data:
    def data(self):
        import pandas as pd

        return pd.DataFrame({"x": [1, 2, 3]})


class _FakeMostly:
    """Records what the engine passes to the MOSTLY AI SDK."""
    calls: list = []

    def __init__(self, **kw):
        _FakeMostly.calls.append(("init", kw))

    def train(self, config, progress_bar=False):
        _FakeMostly.calls.append(("train", config))
        return _Gen(config)

    def generate(self, g, size=None, config=None, progress_bar=False):
        _FakeMostly.calls.append(("generate", {"size": size, "config": config}))
        return _Data()


@pytest.fixture
def mostly(monkeypatch):
    _FakeMostly.calls = []
    sdk = types.ModuleType("mostlyai.sdk")
    sdk.MostlyAI = _FakeMostly
    monkeypatch.setitem(sys.modules, "mostlyai", types.ModuleType("mostlyai"))
    monkeypatch.setitem(sys.modules, "mostlyai.sdk", sdk)
    return _FakeMostly.calls


def _call(calls, kind):
    return next(c[1] for c in calls if c[0] == kind)


def _train():
    import pandas as pd

    return pd.DataFrame({"x": [1, 2, 3]})


def test_unset_is_as_before(mostly, tmp_path):
    out, facts = engines.generate(_train(), 3, engines.Settings.from_dict({"name": "default"}), str(tmp_path))
    train = _call(mostly, "train")
    assert "random_state" not in train
    assert "max_epochs" not in train["tables"][0]["tabular_model_configuration"]
    assert _call(mostly, "generate")["size"] == 3                     # the plain call, as before
    assert facts["repeatability"]["repeatable"] is False
    assert "no seed" in facts["repeatability"]["why"]


def test_seed_and_epochs_reach_training_and_generation(mostly, tmp_path):
    s = engines.Settings.from_dict({"name": "default", "seed": 42, "max_epochs": 3, "max_training_minutes": 10})
    _, facts = engines.generate(_train(), 5, s, str(tmp_path))
    train, gen = _call(mostly, "train"), _call(mostly, "generate")["config"]
    assert train["random_state"] == 42 and gen["random_state"] == 42
    cfg = train["tables"][0]["tabular_model_configuration"]
    assert cfg["max_epochs"] == 3 and cfg["max_training_time"] == 10
    assert gen["tables"][0]["configuration"]["sample_size"] == 5
    r = facts["repeatability"]
    assert r["repeatable"] is True and r["seed"] == 42 and r["max_epochs"] == 3 and r["ceiling_reached"] is False


def test_a_free_local_port_every_run(mostly, tmp_path):
    engines.generate(_train(), 3, engines.Settings.from_dict({"name": "default"}), str(tmp_path))
    port = _call(mostly, "init")["local_port"]
    assert isinstance(port, int) and port != 8080
    with socket.socket() as sock:                                   # nothing listens there: it can be bound
        sock.bind(("127.0.0.1", port))


def test_relational_is_seeded_too(mostly, tmp_path):
    import pandas as pd

    s = engines.Settings.from_dict({"name": "default", "seed": 7, "max_epochs": 2})
    parent = pd.DataFrame({"id": [1, 2]})
    child = pd.DataFrame({"id": [1, 1, 2], "v": [1, 2, 3]})
    try:
        engines.generate_tables(parent, "id", {"events": (child, "id")}, 2, s, str(tmp_path))
    except KeyError:
        pass                                                         # the fake returns one table; only calls matter
    assert _call(mostly, "train")["random_state"] == 7
    assert _call(mostly, "generate")["config"]["random_state"] == 7


@pytest.mark.parametrize("settings,seconds,repeatable,why", [
    ({"seed": 1, "max_epochs": 3, "max_training_minutes": 10}, 120, True, "seed 1 and 3 epochs"),
    ({"seed": 1, "max_training_minutes": 10}, 120, False, "same machine only"),
    ({"seed": 1, "max_epochs": 3, "max_training_minutes": 1}, 59, False, "time ceiling"),
    ({"max_epochs": 3}, 10, False, "no seed"),
    ({"seed": 1, "engine": "sdv"}, None, False, "SDV"),
])
def test_repeatability_says_why(settings, seconds, repeatable, why):
    r = engines.repeatability(engines.Settings.from_dict(settings), seconds)
    assert r["repeatable"] is repeatable and why in r["why"]


def test_config_defaults_and_checks(repo):
    uc = resolve(synthetic_repo(repo))
    assert all(c["seed"] is None and c["max_epochs"] is None for c in uc.synthetic["candidates"])
    uc = resolve(synthetic_repo(repo, engine={"type": "mostlyai", "seed": 42, "max_epochs": 3},
                                candidates=[{"name": "a"}, {"name": "b", "seed": 7}]))
    by = {c["name"]: c for c in uc.synthetic["candidates"]}
    assert (by["a"]["seed"], by["a"]["max_epochs"]) == (42, 3) and by["b"]["seed"] == 7
    assert not [w for w in uc.warnings if "FR-107" in w]
    found = problems(synthetic_repo(repo, engine={"type": "mostlyai", "max_epochs": 0}))
    assert "max_epochs" in found


def test_seed_without_epochs_warns(repo):
    uc = resolve(synthetic_repo(repo, engine={"type": "mostlyai", "seed": 42}))
    warned = [w for w in uc.warnings if "FR-107" in w]
    assert warned and "same machine only" in warned[0]


def test_a_numeric_label_is_the_positive_class_after_prepare():
    """1.4.3 battle test: prepare() makes 1 into 1.0; utility compared "1.0" with "1" and was never measured."""
    import pandas as pd

    from pipewright.synthetic.runtime import evaluation

    labels = evaluation.prepare(pd.DataFrame({"churned": [0, 1, 1, 0]}))["churned"]
    assert list(evaluation.positive_class(labels, 1)) == [0, 1, 1, 0]
    assert list(evaluation.positive_class(pd.Series(["yes", "no"]), "yes")) == [1, 0]
    assert list(evaluation.positive_class(pd.Series([True, False]).astype(str), "True")) == [1, 0]
