"""Pure routing logic for the launcher: which use cases a trigger starts, and with which run ID.

Kept free of Flask and Google clients so it can be unit-tested directly.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from pipewright.errors import ConfigError
from pipewright.release.contract import LAUNCHER_PARAMETERS, RUN_OVERRIDES, PipelineTarget, ReleaseContract
from pipewright.runs import sources


class Ignored(Exception):
    """The message is valid but not for us (wrong event type, no matching use case)."""


class BadRequest(Exception):
    """The message can never succeed: acknowledge it so it does not loop."""


@dataclass(frozen=True)
class FileEvent:
    bucket: str
    name: str
    generation: str
    message_id: str | None = None

    @property
    def uri(self) -> str:
        return f"gs://{self.bucket}/{self.name}"


def parse_pubsub(envelope: dict | None) -> FileEvent:
    message = (envelope or {}).get("message")
    if not message:
        raise BadRequest("not a Pub/Sub push envelope")
    attrs = message.get("attributes", {})
    if attrs.get("eventType") != "OBJECT_FINALIZE":
        raise Ignored(f"event type {attrs.get('eventType')!r}")
    try:
        data = json.loads(base64.b64decode(message.get("data", "")) or b"{}")
    except (ValueError, TypeError) as e:
        raise BadRequest(f"undecodable message data: {e}") from e
    bucket = attrs.get("bucketId") or data.get("bucket")
    name = attrs.get("objectId") or data.get("name")
    if not bucket or not name:
        raise BadRequest("event is missing bucket/object")
    return FileEvent(bucket, name, str(attrs.get("objectGeneration") or data.get("generation", "0")),
                     message.get("messageId"))


def parse_run_request(envelope: dict | None) -> dict:
    """A run request (the /run body) delivered by Pub/Sub push."""
    message = (envelope or {}).get("message")
    if not message:
        raise BadRequest("not a Pub/Sub push envelope")
    try:
        body = json.loads(base64.b64decode(message.get("data", "")) or b"{}")
    except (ValueError, TypeError) as e:
        raise BadRequest(f"undecodable message data: {e}") from e
    if not isinstance(body, dict):
        raise BadRequest("run request must be a JSON object")
    return body


def route_file(contracts: list[ReleaseContract], event: FileEvent) -> list[ReleaseContract]:
    """Use cases whose live contract claims this object. The longest matching prefix wins, so a tenant
    prefix like incoming/nimbus/ takes precedence over a broader incoming/."""
    candidates = []
    for c in contracts:
        prefix = c.triggers.get("landing_prefix")
        if prefix is None or not event.name.startswith(prefix):
            continue
        suffixes = c.triggers.get("landing_suffixes") or []
        if suffixes and not any(event.name.lower().endswith(s.lower()) for s in suffixes):
            continue
        candidates.append(c)
    if not candidates:
        raise Ignored(f"no live use case claims {event.name}")
    longest = max(len(c.triggers["landing_prefix"]) for c in candidates)
    return [c for c in candidates if len(c.triggers["landing_prefix"]) == longest]


def run_id(*parts: str) -> str:
    """Deterministic and short: the same trigger always maps to the same job (duplicates are rejected)."""
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:12]
    return f"r{digest}"


def job_id(usecase: str, rid: str, hint: str = "") -> str:
    slug = re.sub(r"[^a-z0-9-]", "-", hint.lower()).strip("-")
    base = f"{usecase}-{rid}" + (f"-{slug}" if slug else "")
    return base[:128].rstrip("-")


def minute_key(now: datetime | None = None) -> str:
    """Scheduled calls retried within the same minute map to one run."""
    return (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M")


# RUN_OVERRIDES (release.contract): champion_version (what production serves, for a training run to compare with)
# and model_version (a candidate a scoring run validates). A request for a pipeline that does not declare one is
# refused, so an older template never ignores it silently.
_VERSION = re.compile(r"^[0-9]{1,9}$")
_LOOP = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


def overrides(target: PipelineTarget, body: dict) -> dict:
    out = {}
    for key in RUN_OVERRIDES:
        value = body.get(key)
        if value in (None, ""):
            continue
        if not _VERSION.match(str(value)):
            raise BadRequest(f"{key} {value!r} is not a model version number")
        if key not in target.parameters:
            raise BadRequest(f"the {target.name} pipeline of this release takes no {key} parameter (Pipewright's "
                             "BigQuery ML pipelines and the 1.1 example declare it; add it to the pipeline)")
        out[key] = str(value)
    return out


def loop_case(body: dict) -> str | None:
    """The production case a run belongs to, when it is a step of a closed retrain loop."""
    value = body.get("loop")
    if value in (None, ""):
        return None
    if not _LOOP.match(str(value)):
        raise BadRequest(f"loop {value!r} is not a case ID")
    return str(value)


def parameters_for(contract: ReleaseContract, target: PipelineTarget, source_uri: str, rid: str,
                   extra: dict | None = None) -> dict:
    params = dict(target.static_parameters)
    params.update({"project": contract.project, "region": contract.region, "source_uri": source_uri, "run_id": rid})
    params.update(extra or {})
    assert set(LAUNCHER_PARAMETERS) <= set(params)
    return params


def labels_for(contract: ReleaseContract, trigger: str, rid: str, pipeline: str = "training",
               loop: str | None = None) -> dict:
    def clean(v: str) -> str:
        return re.sub(r"[^a-z0-9_-]", "-", v.lower())[:63]

    return {"pipewright-usecase": clean(contract.usecase), "pipewright-release": clean(contract.release_id),
            "pipewright-tenant": clean(contract.tenant), "pipewright-pipeline": clean(pipeline),
            "trigger": clean(trigger), "run-id": clean(rid), **({"pipewright-loop": clean(loop)} if loop else {})}


def target_for(contract: ReleaseContract, pipeline: str) -> PipelineTarget:
    try:
        return contract.pipeline(pipeline)
    except ConfigError as e:
        raise BadRequest(str(e)) from None


def landing_pipeline(contract: ReleaseContract) -> str:
    return contract.triggers.get("landing_pipeline") or "training"


@dataclass(frozen=True)
class Run:
    pipeline: str
    source_uri: str
    run_id: str
    trigger: str
    overrides: tuple = ()             # (name, value) pairs from RUN_OVERRIDES
    loop: str | None = None


def resolve_run(contract: ReleaseContract, body: dict, now: datetime | None = None) -> Run:
    """For /run: a schedule (its own pipeline) or an API call (body "pipeline", default training)."""
    schedule = body.get("schedule")
    if schedule:
        sched = contract.triggers.get("schedules", {}).get(schedule)
        if sched is None:
            raise BadRequest(f"{contract.usecase} has no schedule {schedule!r}")
        source = body.get("source_uri") or sched.get("source_uri")
        if not source:
            raise BadRequest(f"schedule {schedule!r} of {contract.usecase} has no source_uri")
        pipeline = sched.get("pipeline", "training")
        if body.get("pipeline") and body["pipeline"] != pipeline:
            raise BadRequest(f"schedule {schedule!r} runs the {pipeline} pipeline, not {body['pipeline']}")
        if sources.problem(source):
            raise BadRequest(sources.problem(source))
        key = body.get("run_key") or minute_key(now)
        extra = tuple(overrides(target_for(contract, pipeline), body).items())
        return Run(pipeline, source, run_id(contract.usecase, "schedule", schedule, key), "schedule", extra,
                   loop_case(body))
    if not contract.triggers.get("api"):
        raise BadRequest(f"{contract.usecase} does not accept API runs (triggers.api is false)")
    source = body.get("source_uri")
    if sources.problem(source):
        raise BadRequest(sources.problem(source))
    pipeline = body.get("pipeline") or "training"
    extra = tuple(overrides(target_for(contract, pipeline), body).items())
    key = body.get("run_key") or minute_key(now)
    parts = ["api", source, key] if pipeline == "training" else ["api", pipeline, source, key]
    loop = loop_case(body)
    return Run(pipeline, source, run_id(contract.usecase, *parts), "loop" if loop else "api", extra, loop)
