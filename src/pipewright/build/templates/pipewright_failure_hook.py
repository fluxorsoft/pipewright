"""The failure record (Pipewright 1.4.4, off by default): when a pipeline step's Python process ends with an error,
this writes one small JSON file saying why, with no data values, then lets the step fail exactly as before.

Copied into every component image by `pipewright release`, next to pipewright_runtime.py, and loaded at interpreter
start by a .pth file the image build writes. It does nothing unless the step's ml_config carries
platform.failure_records (the environment's observability.failure_records, or the use case's
monitoring.failure_records): that value is the folder the record is dropped in, inside the tenant's own pipeline root.
The launcher's hourly check reads the record, cleans it again and keeps the cause in run.failed; nothing else reads
this folder.

What a record holds: names and IDs (use case, release, step, function, run ID, the identity the step ran as), the
error's type, the message when Pipewright wrote it (its messages are already value-free), the place in the team's
code (file, line, function, and the code line without comments), and the check results a step attached with
pipewright_runtime.failure_detail(). A message Pipewright did not write is never kept: it may quote data.

Rules: it never changes the step's outcome or its error; if it cannot write the record it prints one line and gives
up; it is limited in size and in time. Owned by the platform: do not edit in the use case.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
import tokenize
import uuid
from datetime import datetime, timezone

SCHEMA = 1
MAX_TEXT = 500            # the message, the code line
MAX_CHECKS = 20           # check results a step attached
MAX_CHECK = 240
MAX_BYTES = 16_000        # the whole record
WRITE_SECONDS = 15
IDENTITY_SECONDS = 2
COMPONENTS = "/app/components"
OWN = ("pipewright_runtime.py", "pipewright_failure_hook.py")

_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_AFTER_VALUE = re.compile(r"(?i)\b(values?|got|found|literal)(\s*[:=]\s*)(?![`'\"])[^\s,;)\]]+")
# Python and pandas type names are not data: "unsupported operand type(s) for /: 'float' and 'str'" keeps them.
# The same list as pipewright.errors.TYPE_NAMES and pipewright_runtime.TYPE_NAMES (kept in step by a test).
TYPE_NAMES = frozenset((
    "int", "float", "str", "bool", "bytes", "complex", "NoneType", "list", "dict", "tuple", "set", "frozenset",
    "object", "function", "module", "type", "datetime", "date", "time", "timedelta", "Decimal", "Timestamp",
    "Timedelta", "NaTType", "Series", "DataFrame", "Index", "ndarray", "int8", "int16", "int32", "int64", "uint8",
    "uint16", "uint32", "uint64", "float16", "float32", "float64", "bool_", "str_", "object_", "datetime64",
    "timedelta64", "category", "Int64", "Float64", "boolean", "string"))


def value_free(text: str) -> str:
    """The message with data values taken out (the same rules as pipewright_runtime.value_free)."""
    def quoted(m):
        q = m.group(0)
        return q if q[1:-1] in TYPE_NAMES else q[0] + "..." + q[0]

    return _AFTER_VALUE.sub(lambda m: m.group(1) + m.group(2) + "...", _QUOTED.sub(quoted, str(text)))


def _argument(argv: list, name: str) -> str | None:
    for i, a in enumerate(argv):
        if a == name and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith(name + "="):
            return a.split("=", 1)[1]
    return None


def _without_comments(line: str) -> str:
    """The code line without its comment: a comment can say more than the code (found in the spike: one handed the
    answer over)."""
    line = line.strip()
    try:
        for token in tokenize.generate_tokens(io.StringIO(line + "\n").readline):
            if token.type == tokenize.COMMENT:
                return line[:token.start[1]].rstrip()
        return line
    except (tokenize.TokenError, IndentationError, SyntaxError):       # part of a statement that spans lines
        return line.split("#", 1)[0].strip()


def _code(tb, components: str = COMPONENTS) -> dict | None:
    """The innermost frame in the team's own components: file, line, function and the code line."""
    import linecache

    found = None
    root = os.path.normpath(components).replace("\\", "/")
    while tb is not None:
        path = os.path.normpath(tb.tb_frame.f_code.co_filename).replace("\\", "/")   # /app/components/./x.py
        if path.startswith(root.replace("\\", "/") + "/") and os.path.basename(path) not in OWN:
            found = (path, tb.tb_lineno, tb.tb_frame.f_code.co_name)
        tb = tb.tb_next
    if not found:
        return None
    path, line, function = found
    text = _without_comments(linecache.getline(path, line))[:MAX_TEXT]
    return {"file": path[len(root) + 1:], "line": line, "function": function, "code": text}


def _step(executor_input: dict, function: str | None) -> str:
    """The step's name: on Agent Platform the task's folder (<task>_<number>), otherwise the function's name as the
    compiler names its task."""
    out = str(((executor_input.get("outputs") or {}).get("outputFile")) or "")
    m = re.search(r"/([^/]+)_-?\d+/executor_output\.json$", out)
    return m.group(1) if m else (function or "unknown").replace("_", "-")


def _identity() -> str | None:
    try:
        import urllib.request

        req = urllib.request.Request("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/"
                                     "default/email", headers={"Metadata-Flavor": "Google"})
        with urllib.request.urlopen(req, timeout=IDENTITY_SECONDS) as r:  # noqa: S310 - the metadata server
            return r.read(300).decode("utf-8", "replace").strip() or None
    except Exception:  # noqa: BLE001
        return None


def _where(executor_input: dict) -> list:
    """Names that tie this step to its run, for the launcher to match the record: output paths and pod names hold
    the run's and the task's IDs. Names and IDs only."""
    outputs = executor_input.get("outputs") or {}
    out = [str(outputs.get("outputFile") or "")]
    for group in (outputs.get("artifacts") or {}).values():
        out += [str(a.get("uri") or "") for a in (group.get("artifacts") or [])[:2]]
    out += [os.environ.get(k, "") for k in ("KFP_POD_NAME", "CLOUD_ML_JOB_ID", "HOSTNAME")]
    return [v[:400] for v in out if v][:8]


def build(exc_type, exc, tb, argv: list, components: str = COMPONENTS) -> tuple[str, dict] | None:
    """(folder, record) for a failed step, or None when this is not a step or records are off."""
    raw = _argument(argv, "--executor_input")
    if not raw:
        return None
    executor_input = json.loads(raw)
    values = ((executor_input.get("inputs") or {}).get("parameterValues")) or {}
    ml_config = values.get("ml_config")
    if isinstance(ml_config, str):                     # a dict parameter can arrive as its JSON text
        ml_config = json.loads(ml_config)
    if not isinstance(ml_config, dict):
        return None
    platform = ml_config.get("platform") or {}
    folder = platform.get("failure_records")
    if not folder or not isinstance(folder, str):
        return None
    function = _argument(argv, "--function_to_execute")
    run_id = values.get("run_id")
    runtime = sys.modules.get("pipewright_runtime")
    checks = [value_free(str(c))[:MAX_CHECK] for c in (getattr(runtime, "_FAILURE_CHECKS", None) or [])][:MAX_CHECKS]
    ours = bool(getattr(exc, "pipewright_value_free", False))
    record = {
        "schema": SCHEMA, "time": datetime.now(timezone.utc).isoformat(),
        "usecase": ml_config.get("usecase"), "environment": ml_config.get("environment"),
        "tenant": platform.get("tenant"), "release": os.environ.get("PIPEWRIGHT_RELEASE"),
        "run_id": run_id if isinstance(run_id, str) else None,
        "step": _step(executor_input, function), "function": function, "identity": _identity(),
        "kind": "pipewright_error" if ours else "exception",
        # A Google client error Pipewright wrapped keeps its own type (Forbidden, NotFound), not RuntimeError.
        "error_type": str(getattr(exc, "pipewright_error_type", None)
                          or getattr(exc_type, "__name__", exc_type))[:120],
        # Only Pipewright's own messages are kept: they are value-free already (and pass the rule once more).
        "message": value_free(str(exc))[:MAX_TEXT] if ours else None,
        "code": _code(tb, components), "checks": checks, "where": _where(executor_input),
    }
    return folder, record


def write(folder: str, record: dict) -> str:
    """Drops the record in the folder: gs://bucket/prefix (the tenant's pipeline root) or, for tests, a local folder."""
    text = json.dumps(record, default=str)
    if len(text.encode("utf-8")) > MAX_BYTES:
        record = {**record, "checks": [], "where": record.get("where", [])[:2]}
        text = json.dumps(record, default=str)
    name = f"{datetime.now(timezone.utc):%Y-%m-%d}/{record.get('step') or 'unknown'}-{uuid.uuid4().hex[:10]}.json"
    if folder.startswith("gs://"):
        from google.cloud import storage

        bucket, _, prefix = folder[5:].partition("/")
        blob = storage.Client().bucket(bucket).blob(f"{prefix.rstrip('/')}/{name}")
        blob.upload_from_string(text, content_type="application/json", timeout=WRITE_SECONDS, if_generation_match=0)
        return f"gs://{bucket}/{blob.name}"
    path = os.path.join(folder, *name.split("/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def record_failure(exc_type, exc, tb, argv: list | None = None) -> str | None:
    """Never raises: whatever goes wrong here, the step's own error is what the run reports."""
    try:
        built = build(exc_type, exc, tb, sys.argv if argv is None else argv)
        if built is None:
            return None
        where = write(*built)
        print(json.dumps({"severity": "INFO", "message": f"pipewright: failure record written to {where}"}),
              flush=True)
        return where
    except BaseException as e:  # noqa: BLE001 - never a second error on top of the step's own
        try:
            print(json.dumps({"severity": "WARNING",
                              "message": f"pipewright: failure record not written: {type(e).__name__}"}), flush=True)
        except BaseException:  # noqa: BLE001
            pass
        return None


def install() -> None:
    """Wraps sys.excepthook once: the record is written first, then the error is reported exactly as before."""
    previous = sys.excepthook
    if getattr(previous, "pipewright_failure_hook", False):
        return

    def hook(exc_type, exc, tb):
        if not issubclass(exc_type, (KeyboardInterrupt, SystemExit)):
            record_failure(exc_type, exc, tb)
        previous(exc_type, exc, tb)

    hook.pipewright_failure_hook = True
    sys.excepthook = hook


install()
