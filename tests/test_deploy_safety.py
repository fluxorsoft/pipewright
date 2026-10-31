"""FR-74: deploy-model is safe to repeat, and one change to an endpoint at a time."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.models import deploy

ENV = SimpleNamespace(region="europe-west2")
ENDPOINT = SimpleNamespace(resource_name="projects/p/locations/europe-west2/endpoints/42")


def gcp_with(operations, ok=True):
    seen = {}

    def get(url, **params):
        seen.update(url=url, params=params)
        return SimpleNamespace(ok=ok, body={"operations": operations})
    return SimpleNamespace(get=get), seen


def test_an_unfinished_deploy_is_reported_with_when_it_started():
    gcp, seen = gcp_with([{"name": "op1", "metadata": {
        "@type": "type.googleapis.com/google.cloud.aiplatform.v1.DeployModelOperationMetadata",
        "genericMetadata": {"createTime": "2026-10-30T20:36:40.123Z"}}}])
    assert deploy.operations_in_progress(gcp, ENV, ENDPOINT.resource_name) == [
        "DeployModel started 2026-10-30 20:36:40 UTC"]
    assert seen["url"].endswith("/endpoints/42/operations") and seen["params"] == {"filter": "done=false"}


def test_a_busy_endpoint_refuses_a_second_change():
    # The ebook's Chapter 13: a second deploy of v8 started while the first was still running.
    gcp, _ = gcp_with([{"metadata": {"@type": "x.DeployModelOperationMetadata"}}])
    with pytest.raises(ConfigError, match="lapse-risk-endpoint is busy: DeployModel"):
        deploy.refuse_if_busy(gcp, ENV, ENDPOINT, "lapse-risk-endpoint")


def test_finished_operations_and_unreadable_lists_do_not_block():
    gcp, _ = gcp_with([{"done": True, "metadata": {"@type": "x.DeployModelOperationMetadata"}}])
    deploy.refuse_if_busy(gcp, ENV, ENDPOINT, "e")
    gcp, _ = gcp_with([], ok=False)
    deploy.refuse_if_busy(gcp, ENV, ENDPOINT, "e")
