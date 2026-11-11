"""Engines behind one interface: learn from a table, generate a table of the same shape.

Standalone (no Pipewright imports). Runs in the synthetic image only; engine packages are imported when used, so
an engine a use case does not name is never needed.

    mostlyai   MOSTLY AI Synthetic Data SDK, local mode (Apache-2.0): the default, in the synthetic image
    sdv        SDV (Business Source Licence 1.1): only when the team adds it to synthetic.requirements, accepting
               its licence directly; Pipewright never installs it
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

ENGINES = ("mostlyai", "sdv")
SDV_SYNTHESIZERS = ("gaussian_copula", "ctgan", "tvae", "copulagan")
MOSTLY_MODELS = ("MOSTLY_AI/Small", "MOSTLY_AI/Medium", "MOSTLY_AI/Large")


@dataclass
class Settings:
    """One candidate's settings (synthetic.engine, or one entry of synthetic.candidates over it)."""
    name: str = "default"
    engine: str = "mostlyai"
    model: str | None = None                  # mostlyai: MOSTLY_AI/Small|Medium|Large; sdv: a synthesizer
    max_training_minutes: float = 10.0
    epsilon: float | None = None              # differential privacy budget (mostlyai)
    sampling_temperature: float = 1.0         # mostlyai: above 1 spreads the samples further from training rows
    rare_categories: str = "constant"         # mostlyai: constant (a _RARE_ token) or sample (common categories)
    epochs: int = 300                         # sdv deep synthesizers
    max_epochs: float | None = None           # 1.4.3 (FR-107), mostlyai: a fixed amount of training; time = ceiling
    seed: int | None = None                   # 1.4.3 (FR-107): one integer for training and generation

    @classmethod
    def from_dict(cls, d: dict) -> Settings:
        dp = d.get("differential_privacy") or {}
        max_epochs, seed = d.get("max_epochs"), d.get("seed")
        return cls(name=d.get("name", "default"), engine=d.get("engine", "mostlyai"), model=d.get("model"),
                   max_training_minutes=float(d.get("max_training_minutes", 10.0)), epsilon=dp.get("epsilon"),
                   sampling_temperature=float(d.get("sampling_temperature", 1.0)),
                   rare_categories=d.get("rare_categories", "constant"), epochs=int(d.get("epochs", 300)),
                   max_epochs=float(max_epochs) if max_epochs is not None else None,
                   seed=int(seed) if seed is not None else None)


# Training that used this share of the time ceiling or more may have been cut short by it (FR-107).
CEILING_SHARE = 0.95


def free_port() -> int:
    """A local port nothing listens on. The MOSTLY AI SDK in local mode talks to a server it starts on a port; with
    the default (8080) taken by another service, it talked to that service instead (found in the FR-107 spike)."""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def repeatability(s: Settings, train_seconds: float | None) -> dict:
    """What the evidence says about repeating this candidate (FR-107): a seed alone repeats on the same machine
    only; a seed with a fixed number of epochs repeats anywhere, unless the time ceiling stopped training first."""
    out = {"seed": s.seed, "max_epochs": s.max_epochs, "max_training_minutes": s.max_training_minutes,
           "training_seconds": round(train_seconds, 1) if train_seconds is not None else None}
    ceiling = bool(train_seconds is not None and train_seconds >= CEILING_SHARE * s.max_training_minutes * 60)
    out["ceiling_reached"] = ceiling
    if s.seed is None:
        out.update(repeatable=False, why="no seed: each run starts from a different random state")
    elif s.engine == "sdv":
        out.update(repeatable=False, why="SDV: seeded, but not verified to repeat exactly")
    elif s.max_epochs is None:
        out.update(repeatable=False, why="a seed without max_epochs: training is time-bound, so it repeats on the "
                                         "same machine only")
    elif ceiling:
        out.update(repeatable=False, why=f"the time ceiling ({s.max_training_minutes:g} minutes) may have stopped "
                                         f"training before {s.max_epochs:g} epochs: raise max_training_minutes")
    else:
        out.update(repeatable=True, why=f"seed {s.seed} and {s.max_epochs:g} epochs")
    return out


def offline() -> None:
    """No model downloads or telemetry from Hugging Face libraries, whatever else is configured."""
    for k in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HUB_DISABLE_TELEMETRY"):
        os.environ[k] = "1"


def generate(train, rows: int, settings: Settings, workdir: str, table: str = "data"):
    """Learns from `train` (a pandas DataFrame, personal-data columns already removed) and returns `rows` new rows,
    plus a dict of engine facts for the evaluation record (versions, settings actually used)."""
    if settings.engine == "mostlyai":
        return _mostlyai(train, rows, settings, workdir, table)
    if settings.engine == "sdv":
        return _sdv(train, rows, settings)
    raise ValueError(f"unknown engine {settings.engine!r} (known: {', '.join(ENGINES)})")


def generate_tables(parent, key: str, children: dict, entities: int, settings: Settings, workdir: str,
                    table: str = "entity"):
    """Relational and sequential: learns the parent table and its child tables together (each child a sequence
    per entity, in the order given) and generates `entities` new parents with their children. children maps
    name -> (DataFrame, key column). Returns ({"parent": df, name: df, ...}, facts)."""
    if settings.engine == "mostlyai":
        return _mostlyai_tables(parent, key, children, entities, settings, workdir, table)
    if settings.engine == "sdv":
        return _sdv_tables(parent, key, children, entities, settings)
    raise ValueError(f"unknown engine {settings.engine!r} (known: {', '.join(ENGINES)})")


def _mostlyai_config(s: Settings) -> dict:
    cfg = {"max_training_time": s.max_training_minutes, "value_protection": True,
           "rare_category_replacement_method": s.rare_categories.upper()}
    if s.max_epochs is not None:
        cfg["max_epochs"] = s.max_epochs
    if s.model:
        cfg["model"] = s.model
    if s.epsilon:
        cfg["differential_privacy"] = {"max_epsilon": s.epsilon}
    return cfg


def _seeded(config: dict, s: Settings) -> dict:
    """MOSTLY AI takes random_state on the generator (training) and on the synthetic dataset (generation)."""
    return {**config, "random_state": s.seed} if s.seed is not None else config


def _seed_globals(seed: int | None) -> None:
    """SDV has no public seed: it draws from Python's, NumPy's and (deep synthesizers) PyTorch's global generators."""
    if seed is None:
        return
    import random

    import numpy as np

    random.seed(seed)
    np.random.seed(seed % 2**32)
    try:
        import torch

        torch.manual_seed(seed)
    except ImportError:
        pass


def _mostlyai_tables(parent, key: str, children: dict, entities: int, s: Settings, workdir: str, table: str):
    offline()
    from mostlyai.sdk import MostlyAI

    cfg = _mostlyai_config(s)
    tables = [{"name": table, "data": parent, "primary_key": key, "tabular_model_configuration": cfg}]
    for name, (child, ck) in children.items():
        tables.append({"name": name, "data": child, "tabular_model_configuration": cfg,
                       "foreign_keys": [{"column": ck, "referenced_table": table, "is_context": True}]})
    mostly = MostlyAI(local=True, local_dir=workdir, local_port=free_port(), quiet=True)
    t0 = time.time()
    g = mostly.train(config=_seeded({"name": f"pipewright-{table}-relational", "tables": tables}, s),
                     progress_bar=False)
    trained = time.time() - t0
    gen_tables = [{"name": table, "configuration": {"sample_size": entities,
                                                    "sampling_temperature": s.sampling_temperature}}]
    out = mostly.generate(g, config=_seeded({"tables": gen_tables}, s), progress_bar=False).data()
    result = {"parent": out[table], **{name: out[name] for name in children}}
    return result, {"engine": "mostlyai", "mode": "relational",
                    "versions": {p: _version(p) for p in ("mostlyai", "mostlyai-engine", "torch")},
                    "configuration": cfg, "sampling_temperature": s.sampling_temperature,
                    "tables": {n: len(d) for n, d in result.items()}, "repeatability": repeatability(s, trained)}


def _sdv_tables(parent, key: str, children: dict, entities: int, s: Settings):
    try:
        from sdv.metadata import Metadata
        from sdv.multi_table import HMASynthesizer
    except ImportError:
        raise RuntimeError("engine sdv: SDV is not installed in the synthetic image (synthetic.requirements; SDV is "
                           "under the Business Source Licence 1.1, which your team accepts directly)") from None
    data = {"parent": parent, **{n: c for n, (c, _) in children.items()}}
    meta = Metadata.detect_from_dataframes(data=data)
    synth = HMASynthesizer(meta)
    _seed_globals(s.seed)
    synth.fit(data)
    scale = entities / max(len(parent), 1)
    _seed_globals(s.seed)
    out = synth.sample(scale=scale)
    return out, {"engine": "sdv", "mode": "relational", "versions": {"sdv": _version("sdv")},
                 "synthesizer": "hma", "licence": "BUSL-1.1 (installed and accepted by the team)",
                 "tables": {n: len(d) for n, d in out.items()}, "repeatability": repeatability(s, None)}


def _version(package: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(package)
    except PackageNotFoundError:
        return None


def _mostlyai(train, rows: int, s: Settings, workdir: str, table: str):
    offline()
    from mostlyai.sdk import MostlyAI

    cfg = _mostlyai_config(s)
    mostly = MostlyAI(local=True, local_dir=workdir, local_port=free_port(), quiet=True)
    t0 = time.time()
    g = mostly.train(config=_seeded({"name": f"pipewright-{table}", "tables": [
        {"name": table, "data": train, "tabular_model_configuration": cfg}]}, s), progress_bar=False)
    trained = time.time() - t0
    if s.sampling_temperature == 1.0 and s.seed is None:
        out = mostly.generate(g, size=rows, progress_bar=False).data()
    else:
        out = mostly.generate(g, config=_seeded({"tables": [{"name": table, "configuration": {
            "sample_size": rows, "sampling_temperature": s.sampling_temperature}}]}, s), progress_bar=False).data()
    return out, {"engine": "mostlyai", "versions": {p: _version(p) for p in ("mostlyai", "mostlyai-engine", "torch")},
                 "configuration": cfg, "sampling_temperature": s.sampling_temperature,
                 "repeatability": repeatability(s, trained)}


def _sdv(train, rows: int, s: Settings):
    try:
        from sdv.metadata import Metadata
    except ImportError:
        raise RuntimeError("engine sdv: SDV is not installed in the synthetic image. Add it to the file named by "
                           "synthetic.requirements (SDV is under the Business Source Licence 1.1, which your team "
                           "accepts directly; Pipewright never installs it)") from None
    from sdv import single_table

    kind = s.model or "gaussian_copula"
    classes = {"gaussian_copula": "GaussianCopulaSynthesizer", "ctgan": "CTGANSynthesizer",
               "tvae": "TVAESynthesizer", "copulagan": "CopulaGANSynthesizer"}
    meta = Metadata.detect_from_dataframes(data={"data": train}, infer_keys=None)
    cls = getattr(single_table, classes[kind])
    synth = cls(meta, epochs=s.epochs) if kind != "gaussian_copula" else cls(meta)
    sdv2 = int((_version("sdv") or "0").split(".")[0]) >= 2      # SDV 2 takes and returns tables by name
    _seed_globals(s.seed)
    synth.fit({"data": train} if sdv2 else train)
    _seed_globals(s.seed)
    out = synth.sample(num_rows=rows)
    out = out["data"] if isinstance(out, dict) else out
    return out, {"engine": "sdv", "versions": {"sdv": _version("sdv")}, "synthesizer": kind,
                 "licence": "BUSL-1.1 (installed and accepted by the team)", "repeatability": repeatability(s, None)}
