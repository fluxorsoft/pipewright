"""doctor's BigQuery source checks (1.4.3 GA QA, live): a source in another project that the checking identity may not
read is a warning (the runner reads it there, e.g. the closed loop's production data), not a failure."""
from types import SimpleNamespace

from pipewright.checks import doctor


class Resp(SimpleNamespace):
    pass


class FakeGcp:
    def __init__(self, status):
        self.status = status

    def get(self, url):
        ok = self.status == 200
        return Resp(ok=ok, status=self.status, body={"numRows": "5", "type": "TABLE"} if ok else
                    {"error": {"message": "Access Denied"}}, text="Access Denied")


class FakeReport:
    def __init__(self):
        self.results = []

    def add(self, section, result):
        self.results.append(result)


def run(uri, status):
    uc = SimpleNamespace(triggers={"schedules": []}, resources={"runner_service_account": "run@test.iam"})
    env = SimpleNamespace(project="test-proj")
    report = FakeReport()
    doctor._check_sources(uc, env, FakeGcp(status), report, "Use case", {"source_uri": uri})
    (result,) = report.results
    return result


def test_a_denied_source_in_another_project_is_a_warning_naming_the_runner():
    r = run("bq://prod-proj.ds.entities", 403)
    assert r.status == doctor.WARN and "run@test.iam" in r.detail


def test_a_denied_source_in_the_environment_project_still_fails():
    assert run("bq://test-proj.ds.entities", 403).status == doctor.FAIL
    assert run("bq://prod-proj.ds.entities", 404).status == doctor.FAIL


def test_a_readable_source_is_ok():
    assert run("bq://prod-proj.ds.entities", 200).status == doctor.OK
