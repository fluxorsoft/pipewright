"""The cause of a failed run (Pipewright 1.4.4, off by default): what `run.failed` says about why, and whose it is.

The launcher's check for failed runs (launcher.watch) calls cause() for a use case with failure records on
(platform.failure_records in its release contract: the environment's observability.failure_records, or the use
case's monitoring.failure_records). The cause comes from, in order:

1. the failure record the failed step dropped in the tenant's pipeline root (pipewright_failure_hook), cleaned here
   again: a team's step is never trusted directly;
2. Google's own error line, where it names the cause (out of memory, a missing image, quota, a machine shape);
3. on Kubeflow, the failed pod's status (OOMKilled, image pull errors), never its logs;
4. otherwise "unclear", with where to look.

The owner (team, platform or unclear) comes from a fixed, ordered rules table: no model, the same answer every time,
one test per rule. It holds no data values: names, IDs, error types, Pipewright's own messages and check counts.
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone

from pipewright.errors import value_free

log = logging.getLogger(__name__)

TEAM, PLATFORM, UNCLEAR = "team", "platform", "unclear"
MAX_TEXT = 300
MAX_RECORD_BYTES = 32_000
MAX_LISTED = 500                     # failure records looked at per failed run
_NAME = re.compile(r"^[A-Za-z0-9_.@:/ -]{1,200}$")
# "403 GET https://...: " in a Google error; BigQuery's job errors carry the request without the status (GA QA)
_URL = re.compile(r"(?:\b\d{3} )?\b(?:GET|POST|PUT|PATCH|DELETE) https?://\S+ ")

# The rules, in order: the first that matches names the owner. `on` says what a rule reads: "line" is Google's error
# line or a pod's status, "record" the step's failure record. Patterns confirmed live in the 1.4.4 spikes.
_TAIL = re.compile(r": \('\.\.\.', \d{3}, .*\)\s*$")                        # the storage client's trailing tuple

RULES = (
    # Google's own line or the pod's status names the cause
    ("out_of_memory", TEAM, "line",
     r"low on memory|out of memory|OOMKilled|exit(?:ed)? (?:with )?(?:code |status )?137",
     "the step ran out of memory: give it more (steps.<step>.memory in usecase.yaml)"),
    ("machine_shape", TEAM, "line",
     r"Unable to find available machine|does not support accelerator|"
     r"machine type .* (?:is )?not (?:supported|available)",
     "the region cannot provide the machine the step asks for: change steps.<step> (cpu, memory, gpu) in usecase.yaml"),
    ("quota", PLATFORM, "line", r"[Qq]uota|RESOURCE_EXHAUSTED",
     "a Google Cloud quota was reached: the platform raises it or frees capacity"),
    ("missing_image", PLATFORM, "line",
     r"ImagePullBackOff|ErrImagePull|Failed to pull|manifest unknown|[Ii]mage .*(?:does not exist|not found)",
     "the step's image is missing from the registry: release the use case again, or restore the image"),
    ("perimeter", PLATFORM, "line", r"VPC Service Controls|vpcServiceControls|SECURITY_POLICY_VIOLATED",
     "a service perimeter refused the call: the platform reviews the perimeter's rules"),
    # the step's own record. A perimeter's refusal and a quota's both arrive as a 403 (Forbidden, PermissionDenied),
    # so they are read before the permission rule
    ("perimeter", PLATFORM, "record", r"VPC Service Controls|vpcServiceControls|SECURITY_POLICY_VIOLATED",
     "a service perimeter refused the call: the platform reviews the perimeter's rules"),
    # "quotas are exceeded": Agent Platform's wording for a Ray cluster it cannot place (1.4.4 GA QA, live)
    ("quota", PLATFORM, "record", r"[Qq]uotas? (?:is |are )?exceeded|RESOURCE_EXHAUSTED|TooManyRequests",
     "a Google Cloud quota was reached: the platform raises it or frees capacity"),
    ("permission", PLATFORM, "record",
     r"[Ff]orbidden|PermissionDenied|Permission \S+ denied|IAM_PERMISSION_DENIED|Unauthorized|\b403\b",
     "the step's identity was refused a permission: the platform grants it, or the use case asks for less"),
)
RULE_NAMES = ("out_of_memory", "machine_shape", "quota", "missing_image", "perimeter", "permission", "checks",
              "data_source", "use_case_settings", "platform_settings", "team_code", "cancelled", "unclear")

# 1.4.5 (founder, GA QA): a BigQuery table the run reads does not exist (BigQuery's NotFound, naming project, dataset
# and table): the team named it, or has not built it yet, or named another location. A missing DATASET stays unclear
# (more often the environment changed), and so does a table Pipewright makes itself (<use case>_..., from tables sync).
_MISSING_TABLE = re.compile(r"\bNot found\b.*?\b([a-z][a-z0-9-]{4,29})[:.]([A-Za-z_]\w*)\.([\w$]+)")
DATA_SOURCE_HINT = ("a table the run reads does not exist: check its name and location in the use case, or run the "
                    "job that builds it, then run again")


def missing_table(record: dict | None, usecase: str | None = None) -> str | None:
    """project.dataset.table of a missing source table the record names, or None (see _MISSING_TABLE)."""
    if not record or record.get("error_type") not in ("NotFound", None):
        return None
    m = _MISSING_TABLE.search(str(record.get("message") or ""))
    if not m:
        return None
    table = m.group(3)
    if usecase and table.lower().startswith(usecase.lower().replace("-", "_") + "_"):
        return None                                   # Pipewright's own table: tables sync, not the team's data
    return f"{m.group(1)}.{m.group(2)}.{table}"


def _text(value, limit: int = MAX_TEXT) -> str | None:
    if value in (None, ""):
        return None
    # Google's storage errors arrive HTML-escaped and end with the client's own tuple ("...: ('...', 403, ...)"):
    # found live under an enforced perimeter (1.4.4 QA)
    text = _TAIL.sub("", html.unescape(value_free(str(value)).replace("\n", " ")))
    return text[:limit]


def _line(value, limit: int) -> str | None:
    """A line of the team's own source code, kept as written (its column names are what make it useful: found in the
    1.4.4 live test, where hiding quoted text left `df["..."] / df["..."]`): one line, cut short. Source code is not
    data; the step's hook has already dropped the comment."""
    if not isinstance(value, str) or not value.strip():
        return None
    return " ".join(value.split())[:limit]


def _name(value) -> str | None:
    """A name or an ID from a record: kept only if it looks like one."""
    return str(value) if isinstance(value, (str, int)) and _NAME.match(str(value)) else None


def clean(record: dict) -> dict:
    """What the launcher keeps of a step's failure record. A step wrote it, so nothing is taken on trust: known
    fields only, names must look like names, text passes the value-free rule again and is cut short, and a message
    is kept only when the record says Pipewright wrote it."""
    code = record.get("code") if isinstance(record.get("code"), dict) else {}
    ours = record.get("kind") == "pipewright_error"
    out = {
        "step": _name(record.get("step")), "function": _name(record.get("function")),
        "identity": _name(record.get("identity")),
        "owner": (record.get("owner") if ours and record.get("owner") in (TEAM, PLATFORM) else None),
        "kind": "pipewright_error" if ours else "exception",
        "error_type": _name(record.get("error_type")),
        # without the request line ("403 GET https://...: "): it repeats what follows and used up the length
        "message": _text(_URL.sub("", str(record.get("message") or ""))) if ours else None,
        "code": ({"file": _name(code.get("file")),
                  "line": code.get("line") if isinstance(code.get("line"), int) else None,
                  "function": _name(code.get("function")), "code": _line(code.get("code"), 200)}
                 if code.get("file") else None),
        "checks": [c for c in (_text(c, 240) for c in (record.get("checks") or [])[:20]
                               if isinstance(c, (str, int, float))) if c],
    }
    return {k: v for k, v in out.items() if v not in (None, [], "")}


def owner(record: dict | None, line: str = "") -> tuple[str, str, str]:
    """(rule, owner, what to do) from the fixed table. record: a cleaned failure record; line: Google's error line
    or a pod's status."""
    about = " ".join(str((record or {}).get(k) or "") for k in ("error_type", "message"))
    for rule, who, on, pattern, hint in RULES:
        if re.search(pattern, line if on == "line" else about):
            return rule, who, hint
    if record and record.get("checks"):
        return "checks", TEAM, "the step's own checks failed (a data contract, a gate): see the checks"
    # Pipewright's own refusal, which says who changes something (1.4.4, founder's decision 16 November 2026)
    if record and record.get("kind") == "pipewright_error" and record.get("message"):
        if record.get("owner") == TEAM:
            return ("use_case_settings", TEAM, "Pipewright refused the step: the message says what to change in the "
                    "use case's settings or data")
        if record.get("owner") == PLATFORM:
            return ("platform_settings", PLATFORM, "Pipewright refused the step: the message says what the "
                    "environment is missing")
    if record and record.get("kind") == "exception" and record.get("code"):
        return "team_code", TEAM, "an error in the use case's own code: see the file and line"
    return "unclear", UNCLEAR, "the recorded facts do not name the cause: read the step's logs"


def _matches(record: dict, run_id: str | None, job_id: str, extra: tuple = ()) -> bool:
    """Is this record from that run? By the run ID the step was given, or by a name of the run in the step's output
    paths or pod name: Agent Platform puts the job's ID in every output path; on Kubeflow the run's ID is in the
    paths and the workflow's name starts the pod's."""
    def clean_id(v):
        return re.sub(r"[^a-z0-9_-]", "-", str(v).lower())[:63]

    if run_id and record.get("run_id") and clean_id(record["run_id"]) == clean_id(run_id):
        return True
    where = [str(w) for w in (record.get("where") or []) if isinstance(w, str)]
    for key in (job_id, *extra):
        if key and any(f"/{key}/" in w or w == key or w.startswith(f"{key}-") for w in where):
            return True
    return False


def folder(contract) -> str | None:
    """Where the use case's failed steps drop their records, or None when failure records are off for it."""
    value = _platform(contract).get("failure_records")
    return value if isinstance(value, str) and value.startswith("gs://") else None


def _platform(contract) -> dict:
    """The platform block of the ml_config a release compiled into its pipelines: the first pipeline that has one."""
    found = [getattr(contract, "static_parameters", None) or {}]
    found += [(p or {}).get("static_parameters") or {} for p in (getattr(contract, "pipelines", None) or {}).values()]
    for static in found:
        ml = static.get("ml_config")
        if isinstance(ml, str):
            try:
                ml = json.loads(ml)
            except ValueError:
                continue
        if isinstance(ml, dict) and isinstance(ml.get("platform"), dict):
            return ml["platform"]
    return {}


def records(gcp, where: str, usecase: str, run_id: str | None, job_id: str, since: datetime,
            extra: tuple = (), now: datetime | None = None) -> list[dict]:
    """The cleaned failure records of one run: looked up in the folder the release contract names, among the days
    the run can have failed on."""
    from pipewright.gcp import gcs_list, gcs_read

    bucket, _, prefix = where[5:].partition("/")
    today = (now or datetime.now(timezone.utc)).date()
    days = sorted({(since.date() + timedelta(days=d)).isoformat() for d in range((today - since.date()).days + 1)}
                  | {today.isoformat()})
    out = []
    for day in days:
        for name in gcs_list(gcp, bucket, f"{prefix.rstrip('/')}/{day}/")[:MAX_LISTED]:
            try:
                text, _ = gcs_read(gcp, bucket, name)
                if not text or len(text.encode("utf-8")) > MAX_RECORD_BYTES:
                    continue
                raw = json.loads(text)
            except Exception:  # noqa: BLE001 - a record that cannot be read is no record
                continue
            if (isinstance(raw, dict) and raw.get("usecase") in (None, usecase)
                    and _matches(raw, run_id, job_id, extra)):
                out.append(clean(raw))
    return out


def _what(record: dict) -> str:
    """What a cleaned failure record says, in one line."""
    if record.get("message"):
        return _URL.sub("", record["message"])              # the request line says nothing the rest does not
    if record.get("checks"):
        return "; ".join(record["checks"][:3])
    if record.get("code"):
        c = record["code"]
        return f"{record.get('error_type') or 'error'} in {c.get('file')}, line {c.get('line')}"
    return record.get("error_type") or "error"


def _one_line(record: dict) -> str:
    return f"{record.get('step')}: {_what(record)}"[:MAX_TEXT]


def cause(detail: dict, found: list[dict], pod: dict | None = None) -> dict:
    """The cause run.failed carries. detail: launcher.watch.failure() of the job; found: the run's cleaned failure
    records; pod: on Kubeflow, the failed pod's status ({step, reason, exit_code})."""
    if detail.get("state") == "cancelled":
        return {"rule": "cancelled", "owner": UNCLEAR, "summary": "the run was cancelled", "source": "state"}
    steps = detail.get("failed_steps") or []
    record = next((r for r in found if r.get("step") in steps), None) or (found[0] if found else None)
    line = str(detail.get("error") or "")
    if pod and pod.get("reason"):
        line = f"{line} {pod['reason']} exit {pod.get('exit_code')}".strip()
    rule, who, hint = owner(record, line)
    if rule == "unclear" and missing_table(record, detail.get("usecase")):
        rule, who, hint = "data_source", TEAM, DATA_SOURCE_HINT
    step = (record or {}).get("step") or (pod or {}).get("step") or (steps[0] if steps else None)
    if record and rule not in ("out_of_memory", "machine_shape", "missing_image"):
        source = "failure_record"
        what = _what(record)
    elif rule != "unclear":
        source = "pod_status" if pod and pod.get("reason") and re.search(
            next(p for r, _, _, p, _ in RULES if r == rule), f"{pod['reason']} exit {pod.get('exit_code')}") \
            else "platform_error"
        what = hint.split(":")[0]
    else:
        source, what = "none", "the cause is not recorded"
    out = {"rule": rule, "owner": who, "step": step, "source": source,
           "summary": f"{step + ': ' if step else ''}{what}"[:MAX_TEXT], "next": hint}
    if record:
        out["record"] = record
    # More than one step failed (found live: a contract check and an unrelated step in the same run): the other
    # steps' causes are listed too, each from its own record, so the summary of one does not hide the others.
    also = []
    for other in found:
        if other is record or not other.get("step") or other.get("step") == step:
            continue
        o_rule, o_who, _ = owner(other, "")
        also.append({"step": other["step"], "rule": o_rule, "owner": o_who, "summary": _one_line(other)})
    if also:
        out["also"] = also[:5]
    if pod and pod.get("reason"):
        out["pod"] = {k: pod[k] for k in ("reason", "exit_code") if pod.get(k) is not None}
    return {k: v for k, v in out.items() if v is not None}


# ------------------------------------------------------------------------------------ run.submit_failed --
# Refusals that cannot get better by trying again: the request itself is wrong, or the caller may not make it.
# Anything else (a timeout, an unavailable service, a quota that refills) keeps the trigger's retry.
PERMANENT = ("InvalidArgument", "FailedPrecondition", "PermissionDenied", "Forbidden", "NotFound", "BadRequest",
             "Unauthenticated", "OutOfRange", "Unimplemented")
_HTTP_PERMANENT = re.compile(r"\b(?:failed|HTTP):? (400|401|403|404|412|422)\b")


def submit_failed_on(manifest: dict, contract) -> bool:
    """Is run.submit_failed on: for the environment (the manifest), or for this use case (its release contract)?"""
    if ((manifest or {}).get("observability") or {}).get("submit_failed"):
        return True
    return bool(_platform(contract).get("submit_failed"))


_KUBE_DENIED = re.compile(r'is forbidden: User "')
_KUBE_NAME = re.compile(r'"([A-Za-z0-9_.@:/*-]{0,120})"')


def _refusal_text(text: str) -> str:
    """The orchestrator's reason, value-free. Kubernetes' own access refusal quotes names only (the resource, the
    user, the API group, the namespace, the verb), and they are what the platform team needs, so they are kept:
    found live on Kubeflow, where hiding them left `User "..." cannot get resource "..."`."""
    if not _KUBE_DENIED.search(text):
        return value_free(text)
    kept: list[str] = []

    def keep(m):
        kept.append(m.group(1))
        return f"PWNAME{len(kept) - 1}X"

    out = value_free(_KUBE_NAME.sub(keep, text))
    for i, name in enumerate(kept):
        out = out.replace(f"PWNAME{i}X", f'"{name}"')
    return out


def submit_refusal(error: BaseException) -> dict | None:
    """What run.submit_failed says about a refused submission, or None for an error that may pass on a retry.
    Google's reason passes the value-free rule; the owner comes from the same rules table."""
    kind = type(error).__name__
    text = str(error)
    if kind not in PERMANENT and not (isinstance(error, RuntimeError) and _HTTP_PERMANENT.search(text)):
        return None
    line = " ".join(_refusal_text(text).split())[:MAX_TEXT]
    rule, who, hint = owner({"error_type": kind, "message": line}, line)
    if rule == "unclear" and kind in ("InvalidArgument", "FailedPrecondition", "BadRequest", "OutOfRange"):
        rule, who, hint = "bad_request", TEAM, "the orchestrator refused the run as asked: check the use case's " \
                                               "pipeline and step settings"
    if rule == "permission":          # at submission no step has run: it is the launcher or the run's identity
        hint = "the orchestrator refused the launcher or the run's identity: the platform grants the permission"
    return {"error_type": kind, "error": line, "rule": rule, "owner": who, "next": hint}


NOT_SUBMITTED = "not_submitted"
NOT_SUBMITTED_PREFIX = "runs/failed/not-submitted"


def submit_marker_name(usecase: str, job_id: str, now: datetime | None = None) -> str:
    """Under runs/failed/ (what an agent identity may read), by use case and time, so the newest are found by name."""
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{NOT_SUBMITTED_PREFIX}/{usecase}/{stamp}-{job_id}"


def submit_marker(contract, pipeline: str, trigger: str, job_id: str, run_id: str | None, refusal: dict,
                  now: datetime | None = None) -> dict:
    """What the launcher keeps in runs/failed/<job> for a refused submission, in the shape of a failed run's marker,
    so the `runs` tool (and Pipewright Assist) can show a run the orchestrator never listed."""
    summary = f"not submitted: {refusal['error']}"[:MAX_TEXT]
    return {"job_id": job_id, "usecase": contract.usecase, "pipeline": pipeline, "state": NOT_SUBMITTED,
            "trigger": trigger, "run_id": run_id, "release": contract.release_id, "failed_steps": [],
            "error": refusal["error"], "ended": (now or datetime.now(timezone.utc)).isoformat(),
            "owner": refusal["owner"], "why": summary, "next": refusal["next"],
            "cause": {"rule": refusal["rule"], "owner": refusal["owner"], "source": "submission",
                      "summary": summary, "next": refusal["next"]}}
