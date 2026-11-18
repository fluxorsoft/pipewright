"""FR-131: pull request checks with a read-only identity: `doctor --checks`, the PR workflow uses it, and `ci init`
names the GitHub environments the platform binds the CI identity to."""
from pipewright.config import load_environment
from pipewright.gcp.session import Response
from pipewright.scaffold import ci


def test_doctor_in_checks_wants_a_read_only_identity_and_never_fails_on_it(repo):
    from pipewright.checks import doctor
    from pipewright.config import resolve_usecase
    from tests.test_config_ml import ml_repo

    ml_repo(repo)
    path = repo.write()
    env = load_environment(repo.root, "dev")
    uc = resolve_usecase(path, env)

    class G:
        def __init__(self, held):
            self.held = held

        def post(self, url, body, **params):
            return Response(200, {"permissions": [p for p in body["permissions"] if p in self.held]})

        def get(self, url, **params):
            return Response(200, {"permissions": [p for p in params.get("permissions", []) if p in self.held]})

    def results(held):
        report = doctor.Report()
        doctor._check_permissions(uc, env, G(held), report, as_checks=True)
        return [(r.status, r.check) for rs in report.sections.values() for r in rs]

    assert results(set()) == [(doctor.OK, "checks identity")]
    # an identity that can release is a warning, never a failure: checks still run where no read-only identity exists
    assert results({"aiplatform.pipelineJobs.create"}) == [
        (doctor.WARN, "the checks run as an identity that can change things")]
    assert results({"pubsub.topics.publish"})[0][0] == doctor.WARN
    assert results({"storage.objects.create"})[0][0] == doctor.WARN


def test_the_pull_request_workflow_runs_doctor_in_checks_mode():
    files = ci.render(["test", "prod"], checks_environment="test-checks")
    pr = next(v for k, v in files.items() if k.endswith("pipewright-pr.yml"))
    assert 'pipewright doctor --env "$ENV" --usecase "$UC" --checks' in pr


def test_ci_init_names_the_environments_to_bind_the_ci_identity_to():
    assert ci.github_environments(["test", "prod"], "test-checks") == [
        "test", "test-decisions", "prod", "prod-decisions"]
    assert "test-checks" not in ci.github_environments(["test", "prod"], "test-checks")
