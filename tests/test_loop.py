"""The closed retrain loop (FR-48, D-104 to D-109): run requests that name versions, and the loop's own steps."""
from dataclasses import replace

import pytest

from pipewright.launcher import routing
from pipewright.launcher.routing import BadRequest
from tests.test_release_parts import INFERENCE, TRAINING, V3

# A release whose pipelines declare the loop's parameters (Pipewright's BigQuery ML pipelines and the 1.1 example).
LOOPING = replace(V3, pipelines={
    "training": {**TRAINING, "parameters": [*TRAINING["parameters"], "champion_version"]},
    "inference": {**INFERENCE, "parameters": [*INFERENCE["parameters"], "model_version"]}})


def test_run_overrides_are_the_same_everywhere():
    from pipewright.build import compile as build
    from pipewright.release import contract

    assert build.RUN_OVERRIDES == contract.RUN_OVERRIDES and build.LAUNCHER_PARAMETERS == contract.LAUNCHER_PARAMETERS


def test_a_run_names_the_champion_or_the_model_version():
    run = routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "champion_version": "4", "loop": "rc-20261024-ab12cd",
                                        "run_key": "rc-20261024-ab12cd"})
    assert dict(run.overrides) == {"champion_version": "4"} and run.loop == "rc-20261024-ab12cd"
    assert run.trigger == "loop"
    target = LOOPING.pipeline("training")
    params = routing.parameters_for(LOOPING, target, run.source_uri, run.run_id, dict(run.overrides))
    assert params["champion_version"] == "4"
    labels = routing.labels_for(LOOPING, run.trigger, run.run_id, "training", run.loop)
    assert labels["pipewright-loop"] == "rc-20261024-ab12cd" and labels["trigger"] == "loop"
    run = routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "pipeline": "inference", "model_version": "5"})
    assert dict(run.overrides) == {"model_version": "5"} and run.loop is None and run.trigger == "api"


def test_versions_are_refused_where_the_pipeline_does_not_take_them():
    with pytest.raises(BadRequest, match="takes no champion_version parameter"):
        routing.resolve_run(V3, {"source_uri": "bq://d.t", "champion_version": "4"})      # a 1.0 template
    with pytest.raises(BadRequest, match="not a model version number"):
        routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "champion_version": "4; drop"})
    with pytest.raises(BadRequest, match="not a case ID"):
        routing.resolve_run(LOOPING, {"source_uri": "bq://d.t", "loop": "Bad Case"})
    plain = routing.resolve_run(V3, {"source_uri": "bq://d.t"})                          # 1.0 requests: unchanged
    assert plain.overrides == () and plain.loop is None
    assert "pipewright-loop" not in routing.labels_for(V3, plain.trigger, plain.run_id)


# ------------------------------------------------------------------ the loop's steps --

class FakeBackend:
    """Test and production in memory: jobs end when told, versions appear when a run registers one."""

    def __init__(self, production_default=("3", {"promoted-from": "test", "source-version": "7"})):
        version, labels = production_default
        self.versions_of = {"test": [{"version": "7", "default": True, "labels": {}}],
                            "prod": [{"version": version, "default": True, "labels": labels}]}
        self.jobs, self.rows, self.submitted, self.events = {}, {}, [], []
        self.the_case = {"case_id": "rc-1", "loop": {"state": "approved", "source_uri": "bq://prod.d.customers",
                                                     "history": []}}

    def run_ids(self, pipeline, source_uri, run_key):
        rid = f"r-{pipeline}-{run_key}"
        return rid, f"churn-{rid}"

    def submit(self, env, body):
        self.submitted.append((env, body))
        rid, job = self.run_ids(body["pipeline"], body["source_uri"], body["run_key"])
        self.jobs[(env, job)] = "RUNNING"
        return {"status": "requested"}

    def finish(self, env, pipeline, key, state="SUCCEEDED", register=None, alert=None):
        rid, job = self.run_ids(pipeline, "bq://prod.d.customers", key)
        self.jobs[(env, job)] = state
        if register:
            self.versions_of[env].append({"version": register, "default": False, "labels": {"run_id": f"run-{rid}"}})
        if alert is not None:
            self.rows[(env, rid)] = {"alert": alert, "triggers": "drift" if alert else None}

    def job_state(self, env, job_id):
        return self.jobs.get((env, job_id))

    def versions(self, env):
        return self.versions_of[env]

    def monitoring(self, env, run_id):
        return self.rows.get((env, run_id))

    def promote(self, version):
        new = str(len(self.versions_of["prod"]) + 3)
        self.versions_of["prod"].append({"version": new, "default": False,
                                         "labels": {"promoted-from": "test", "source-version": version}})
        return {"version": new, "release_id": "rel9"}

    def approve(self, env, version):
        for v in self.versions_of[env]:
            v["default"] = v["version"] == version

    def case(self):
        return self.the_case

    def save(self, change):
        self.the_case = change(self.the_case)
        return self.the_case

    def emit(self, event_type, data):
        self.events.append(event_type)


def loop(backend, **kw):
    from pipewright.loop.driver import Loop

    return Loop("churn", "rc-1", "test", "prod", backend, by="lead@x", **kw)


def test_a_loop_end_to_end_production_never_trains():
    b = FakeBackend()
    lp = loop(b)
    assert lp.start()["state"] == "training"
    env, body = b.submitted[-1]
    assert env == "test" and body["champion_version"] == "7" and body["loop"] == "rc-1"     # what prod serves
    assert lp.advance()["state"] == "training"                                              # still running
    b.finish("test", "training", "rc-1", register="8")
    assert lp.advance()["candidate"] == {"test_version": "8"}
    assert lp.advance()["state"] == "validating"
    env, body = b.submitted[-1]
    assert env == "test" and body["pipeline"] == "inference" and body["model_version"] == "8"
    b.finish("test", "inference", "rc-1-validate", alert=False)
    assert lp.advance()["state"] == "validated"
    assert lp.advance()["state"] == "validated"                     # waits for the production approval
    out = lp.advance(approve=True)
    assert out["state"] == "verifying" and out["promotion"]["production_version"] == "4"
    assert out["promotion"]["previous_version"] == "3"
    assert next(v for v in b.versions_of["prod"] if v["default"])["version"] == "4"
    assert all(e != "prod" or body["pipeline"] == "inference" for e, body in b.submitted)  # nothing trains in prod
    b.finish("prod", "inference", "rc-1-verify", alert=False)
    assert lp.advance()["state"] == "closed"
    assert [h["state"] for h in b.the_case["loop"]["history"]] == [
        "training", "candidate", "validating", "validated", "verifying", "closed"]
    assert b.events[-1] == "loop.closed"
    assert lp.advance()["state"] == "closed"                        # repeating a finished loop does nothing
    assert lp.rollback("complaints")["rolled_back_to"] == "3"
    assert next(v for v in b.versions_of["prod"] if v["default"])["version"] == "3"


def test_rejection_and_failures_leave_production_untouched():
    b = FakeBackend()
    lp = loop(b)
    lp.start()
    b.finish("test", "training", "rc-1")                            # succeeded, registered nothing: gates said no
    assert lp.advance()["state"] == "rejected"
    assert all(env == "test" for env, _ in b.submitted)
    assert next(v for v in b.versions_of["prod"] if v["default"])["version"] == "3"

    b = FakeBackend()
    lp = loop(b)
    lp.start()
    b.finish("test", "training", "rc-1", register="8")
    lp.advance(), lp.advance()
    b.finish("test", "inference", "rc-1-validate", alert=True)       # the candidate drifts on current data
    assert lp.advance()["state"] == "validation-failed"
    assert lp.advance(approve=True)["state"] == "validation-failed"  # an approval cannot push it on


def test_the_loop_compares_with_what_production_serves():
    b = FakeBackend(production_default=("3", {}))                    # trained in prod, not promoted from test
    with pytest.raises(Exception, match="was not promoted from test"):
        loop(b).start()
    b = FakeBackend()
    b.versions_of["prod"] = []                                       # production has no model yet
    lp = loop(b)
    lp.start()
    assert "champion_version" not in b.submitted[-1][1]


def test_the_launcher_accepts_a_replay_for_training_only():
    """Found live: the CLI allowed snapshot:// for --replay, the launcher dropped it (D-110)."""
    run = routing.resolve_run(V3, {"source_uri": "snapshot://r54da1c8b2b67", "run_key": "k"})
    assert run.pipeline == "training" and run.source_uri == "snapshot://r54da1c8b2b67"
    with pytest.raises(BadRequest, match="must start with gs://"):
        routing.resolve_run(V3, {"source_uri": "snapshot://r54da1c8b2b67", "pipeline": "inference"})
    with pytest.raises(BadRequest):
        routing.resolve_run(V3, {"source_uri": "snapshot://bad id"})
