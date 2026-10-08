import pytest

from pipewright.build.templates import pipewright_runtime as rt
from pipewright.runs import sources

GOOD = ["gs://acme-landing/incoming/nimbus/jan.csv", "gs://b/x.parquet", "bq://acme-ml-dev.retention.customers",
        "bq://retention.customers_v2", "bq://my-project-123.ds_1.view$20261001"]
BAD = ["", "s3://b/x.csv", "gs://bucket", "gs://", "bq://customers", "bq://a.b.c.d", "bq://Bad_Project.ds.t",
       "https://storage.googleapis.com/b/x.csv"]


@pytest.mark.parametrize("uri", GOOD)
def test_accepted(uri):
    assert sources.problem(uri) is None


@pytest.mark.parametrize("uri", BAD)
def test_rejected(uri):
    assert sources.problem(uri)


def test_runtime_copy_agrees_with_core():
    for uri in GOOD + BAD:
        if not uri.startswith("bq://"):
            continue
        try:
            rt_table = rt.bq_table(uri, "proj-default")
        except ValueError:
            rt_table = None
        core = sources.bq_table(uri, "proj-default") if sources.problem(uri) is None else None
        assert rt_table == core, uri
    assert sources.bq_table("bq://retention.customers", "acme-ml-dev") == "acme-ml-dev.retention.customers"


def test_schedule_and_retrain_sources_are_checked(repo):
    from pipewright.config import load_environment, resolve_usecase
    from pipewright.errors import ConfigError

    spec = repo.usecase["spec"]
    spec["environments"]["dev"]["triggers"] = {"schedules": [
        {"name": "nightly", "cron": "0 2 * * *", "timezone": "Europe/London", "source_uri": "s3://x/y.csv"}]}
    with pytest.raises(ConfigError, match="source_uri must start with gs:// .* or bq://"):
        resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    spec["environments"]["dev"]["triggers"]["schedules"][0]["source_uri"] = "bq://${resources.bigquery_dataset}.raw"
    uc = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    assert uc.triggers["schedules"][0]["source_uri"] == "bq://churn.raw"
