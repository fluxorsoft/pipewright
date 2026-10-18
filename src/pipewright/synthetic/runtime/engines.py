"""Engines behind one interface: learn from a table, generate a table of the same shape.

Standalone (no Pipewright imports). Runs in the synthetic image only; engine packages are imported when used, so
an engine a use case does not name is never needed.

    mostlyai   MOSTLY AI Synthetic Data SDK, local mode (Apache-2.0): the default, in the synthetic image
    sdv        SDV (Business Source Licence 1.1): only when the team adds it to synthetic.requirements, accepting
               its licence directly; Pipewright never installs it
"""

from __future__ import annotations

import os
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

    @classmethod
    def from_dict(cls, d: dict) -> Settings:
        dp = d.get("differential_privacy") or {}
        return cls(name=d.get("name", "default"), engine=d.get("engine", "mostlyai"), model=d.get("model"),
                   max_training_minutes=float(d.get("max_training_minutes", 10.0)), epsilon=dp.get("epsilon"),
                   sampling_temperature=float(d.get("sampling_temperature", 1.0)),
                   rare_categories=d.get("rare_categories", "constant"), epochs=int(d.get("epochs", 300)))


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


def _version(package: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(package)
    except PackageNotFoundError:
        return None


def _mostlyai(train, rows: int, s: Settings, workdir: str, table: str):
    offline()
    from mostlyai.sdk import MostlyAI

    cfg = {"max_training_time": s.max_training_minutes, "value_protection": True,
           "rare_category_replacement_method": s.rare_categories.upper()}
    if s.model:
        cfg["model"] = s.model
    if s.epsilon:
        cfg["differential_privacy"] = {"max_epsilon": s.epsilon}
    mostly = MostlyAI(local=True, local_dir=workdir, quiet=True)
    g = mostly.train(config={"name": f"pipewright-{table}", "tables": [
        {"name": table, "data": train, "tabular_model_configuration": cfg}]}, progress_bar=False)
    if s.sampling_temperature == 1.0:
        out = mostly.generate(g, size=rows, progress_bar=False).data()
    else:
        out = mostly.generate(g, config={"tables": [{"name": table, "configuration": {
            "sample_size": rows, "sampling_temperature": s.sampling_temperature}}]}, progress_bar=False).data()
    return out, {"engine": "mostlyai", "versions": {p: _version(p) for p in ("mostlyai", "mostlyai-engine", "torch")},
                 "configuration": cfg, "sampling_temperature": s.sampling_temperature}


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
    synth.fit({"data": train} if sdv2 else train)
    out = synth.sample(num_rows=rows)
    out = out["data"] if isinstance(out, dict) else out
    return out, {"engine": "sdv", "versions": {"sdv": _version("sdv")}, "synthesizer": kind,
                 "licence": "BUSL-1.1 (installed and accepted by the team)"}
