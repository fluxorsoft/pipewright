"""FR-48 phase 6 (user, 2026-11-06): retraining.loop.data production. A loop's training run in test reads
production's feature tables and label window, sent by `pipewright loop start`. The test launcher accepts them only
for a loop's training run, only in the production project its manifest names, and only with the release's own
features and label columns: where the data is read from and the label window may differ, nothing else."""
from dataclasses import replace

import pytest

from pipewright.launcher import routing
from pipewright.launcher.routing import BadRequest
from tests.test_release_parts import INFERENCE, TRAINING, V3

DATA = {"features_from": {"entity": {"columns": ["customer_id"], "time_column": "snapshot_date"}, "sources": [
            {"feature_group": "customer_profile", "features": ["tenure_months", "num_products"]}]},
        "label_source": {"table": "bq://events.renewals", "key": ["customer_id"], "time_column": "renewed_at",
                         "cutoff_column": "snapshot_date", "window_days": 30}}
ML = {"usecase": "churn", "data": DATA, "platform": {"feature_datasets": ["acme-ml-test.features"]}}
RELEASE = replace(V3, pipelines={"training": {**TRAINING, "static_parameters": {"ml_config": ML}},
                                 "inference": INFERENCE})
MANIFEST = {"tenants": {"churn": {"loop_production_project": "acme-ml-prod"}}}
PROD = {"features_from": {"sources": [{
            "table": "bq://acme-ml-prod.features.customer_profile", "entity_columns": ["customer_id"],
            "time_column": "feature_timestamp", "features": ["tenure_months", "num_products"],
            "columns": {"tenure_months": "tenure_months", "num_products": "num_products"},
            "feature_group": "projects/9/locations/r/featureGroups/customer_profile"}]},
        "label_source": {**DATA["label_source"], "table": "bq://acme-ml-prod.events.renewals", "window_days": 45}}
LOOP = {"source_uri": "bq://acme-ml-prod.churn.entities", "loop": "rc-20261106-ab12cd", "run_key": "rc-20261106-ab12cd"}


def run(body, manifest=MANIFEST):
    return routing.resolve_run(RELEASE, {**LOOP, **body}, manifest=manifest)


def test_production_tables_and_window_for_the_loops_training_run():
    r = run({"loop_data": PROD})
    data = r.ml_config["data"]
    (src,) = data["features_from"]["sources"]
    assert src["table"] == "bq://acme-ml-prod.features.customer_profile" and src["features"] == ["tenure_months",
                                                                                              "num_products"]
    assert data["features_from"]["entity"] == DATA["features_from"]["entity"]      # the release's entity
    assert "feature_group" not in src             # a table source: never looked up again in test's registry
    assert data["label_source"]["table"] == "bq://acme-ml-prod.events.renewals"
    assert data["label_source"]["window_days"] == 45                                # production's window
    assert r.ml_config["platform"]["feature_datasets"] == ["acme-ml-prod.features", "acme-ml-test.features"]
    assert dict(r.recorded) == {"data_from": "production", "data_tables": [
        "acme-ml-prod.events.renewals", "acme-ml-prod.features.customer_profile"]}
    assert ML["data"]["label_source"]["window_days"] == 30                          # the release is untouched
    params = routing.parameters_for(RELEASE, RELEASE.pipeline("training"), r.source_uri, r.run_id,
                                    {"ml_config": r.ml_config})
    assert params["ml_config"]["data"]["label_source"]["table"] == "bq://acme-ml-prod.events.renewals"
    assert run({}).ml_config is None                                                # off: test's own, as before


def test_only_a_loops_training_run_in_the_named_production_project():
    with pytest.raises(BadRequest, match="closed retrain loop's training run only"):
        routing.resolve_run(RELEASE, {"source_uri": "bq://d.t", "loop_data": PROD}, manifest=MANIFEST)
    with pytest.raises(BadRequest, match="names no loop_production_project"):
        run({"loop_data": PROD}, manifest={"tenants": {"churn": {}}})
    elsewhere = {**PROD, "label_source": {**PROD["label_source"], "table": "attacker-proj.x.y"}}
    with pytest.raises(BadRequest, match="not the loop's production project acme-ml-prod"):
        run({"loop_data": elsewhere})
    with pytest.raises(BadRequest, match="is not project.dataset.table"):
        run({"loop_data": {"label_source": {**PROD["label_source"], "table": "renewals; DROP TABLE x"}}})


def test_nothing_but_the_location_and_the_window_may_differ():
    src = PROD["features_from"]["sources"][0]
    more = {"features_from": {"sources": [{**src, "features": ["tenure_months", "num_products", "income"]}]}}
    with pytest.raises(BadRequest, match="the release's features and prefix"):
        run({"loop_data": more})
    with pytest.raises(BadRequest, match="map each of its features to a column"):
        run({"loop_data": {"features_from": {"sources": [{**src, "columns": {"tenure_months": "x"}}]}}})
    with pytest.raises(BadRequest, match="2 feature sources, the release has 1"):
        run({"loop_data": {"features_from": {"sources": [src, src]}}})
    with pytest.raises(BadRequest, match="keep the release's key, time and cutoff columns"):
        run({"loop_data": {"label_source": {**PROD["label_source"], "cutoff_column": "other"}}})
    with pytest.raises(BadRequest, match="1 to 3650 days"):
        run({"loop_data": {"label_source": {**PROD["label_source"], "window_days": 0}}})


def test_a_schedule_never_takes_production_data():
    release = replace(RELEASE, triggers={**RELEASE.triggers, "schedules": {"w": {"pipeline": "training",
                                                                                 "source_uri": "bq://d.t"}}})
    with pytest.raises(BadRequest, match="not a schedule"):
        routing.resolve_run(release, {"schedule": "w", "loop_data": PROD}, manifest=MANIFEST)


def test_loop_start_sends_production_data_when_production_asks():
    from pipewright.loop.driver import Loop
    from tests.test_loop import FakeBackend

    backend = FakeBackend()
    backend.production_data = lambda: PROD
    loop = Loop("churn", "rc-1", "test", "prod", backend)
    loop.start()
    body = backend.submitted[-1][1]
    assert body["loop_data"] == PROD and backend.case()["loop"]["data_from"] == "production"


def test_loop_start_resolves_productions_feature_groups_as_the_runtime_would():
    from types import SimpleNamespace

    from pipewright.loop.backend import GcpBackend

    def get(url, **params):
        if url.endswith("/features"):
            return SimpleNamespace(ok=True, status=200, body={"features": [
                {"name": "x/features/tenure_months"}, {"name": "x/features/num_products", "versionColumnName": "np"}]})
        return SimpleNamespace(ok=True, status=200, body={"name": "projects/9/locations/r/featureGroups/cp",
                                                          "bigQuery": {"bigQuerySource": {
                                                              "inputUri": "bq://acme-ml-prod.features.customer_profile"},
                                                              "entityIdColumns": ["customer_id"]}})

    b = GcpBackend.__new__(GcpBackend)
    prod = SimpleNamespace(name="prod", project="acme-ml-prod", region="europe-west2")
    b.production, b.gcp = prod, SimpleNamespace(get=get)
    uc = SimpleNamespace(ml={"data": DATA, "retraining": {"loop": {"enabled": True, "data": "production"}}})
    b.ucs = {"prod": uc}
    got = b.production_data()
    (src,) = got["features_from"]["sources"]
    assert src["table"] == "bq://acme-ml-prod.features.customer_profile" and src["columns"]["num_products"] == "np"
    assert got["label_source"]["table"] == "bq://acme-ml-prod.events.renewals"
    assert routing.resolve_run(RELEASE, {**LOOP, "loop_data": got}, manifest=MANIFEST).ml_config   # accepted as sent
    uc.ml["retraining"]["loop"]["data"] = "test"
    assert b.production_data() is None
