"""What a schedule's CronJob runs (schedules.backend: kubernetes): publish the schedule's run request.

    python -m pipewright.schedules.fire        (PIPEWRIGHT_TOPIC, PIPEWRIGHT_REGION, PIPEWRIGHT_USECASE,
                                                PIPEWRIGHT_SCHEDULE from the CronJob)

The message is the one `pipewright run --schedule` publishes and a Cloud Scheduler job sends: {"usecase", "schedule"}.
The launcher resolves the pipeline, source and run ID from the live release; a tick retried within the same minute
maps to the same run ID, so it never starts a second run. Exits non-zero when the request is not published, so the
Job retries it.
"""

from __future__ import annotations

import base64
import json
import os
import sys


def message(usecase: str, schedule: str) -> dict:
    data = json.dumps({"usecase": usecase, "schedule": schedule}).encode()
    return {"data": base64.b64encode(data).decode(), "attributes": {"usecase": usecase}}


def main() -> int:
    from pipewright.gcp import Gcp, error_text
    from pipewright.observability.events import publish_url

    try:
        topic, usecase, schedule = (os.environ["PIPEWRIGHT_TOPIC"], os.environ["PIPEWRIGHT_USECASE"],
                                    os.environ["PIPEWRIGHT_SCHEDULE"])
    except KeyError as e:
        print(f"pipewright schedule: {e.args[0]} is not set", file=sys.stderr)
        return 2
    resp = Gcp().post(publish_url(topic, os.environ.get("PIPEWRIGHT_REGION")),
                      {"messages": [message(usecase, schedule)]})
    if not resp.ok:
        print(f"pipewright schedule: {usecase} {schedule} not published to {topic}: {error_text(resp)}",
              file=sys.stderr)
        return 1
    ids = (resp.body or {}).get("messageIds") or []
    print(json.dumps({"usecase": usecase, "schedule": schedule, "topic": topic, "message": ids[0] if ids else None}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
