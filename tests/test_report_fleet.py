"""Phase 6 (FR-86): pipewright report fleet, one read-only page for every use case in every environment."""

from datetime import datetime, timezone

from pipewright.report import fleet


def data(masked_email="lead@acme.example"):
    at = datetime(2026, 11, 5, 9, 0, tzinfo=timezone.utc)
    training = [{"evaluated_at": at, "primary_metric": "auc", "primary_value": v, "decision": d, "run_id": f"r{i}"}
                for i, (v, d) in enumerate([(0.80, "approve"), (0.78, "refuse"), (0.81, "approve")])]
    scoring = [{"monitored_at": at, "model_version_id": "4", "max_feature_psi": p, "alert": p > 0.2,
                "source_uri": "gs://landing/incoming/customers_postcode_SW1A.csv"} for p in (0.05, 0.12, 0.31)]
    champion = {"version": "4", "labels": {"promoted-from": "test"},
                "lineage": {"evaluation": {"primary_metric": "auc", "value": 0.81}},
                "integrity": {"result": "verified", "problems": 0}}
    env = {"name": "prod", "project": "p", "evidence": "pw_evidence", "pending": [{"usecase": "churn"}],
           "compute": {"churn": {"hours": 2.5, "runs": 3}},
           "usecases": {"churn": {"pointer": {"release_id": "rel1", "updated_at": "2026-11-05T08:00:00+00:00",
                                              "updated_by": masked_email},
                                  "champion": champion, "training": training, "scoring": scoring,
                                  "drift_threshold": 0.2}},
           "events": [{"publish_time": "2026-11-05T08:30:00+00:00", "event_type": "model.approved",
                       "usecase": "churn", "release": None,
                       "detail": '{"version": "4", "by": "lead@acme.example", "approval": {"via": "outside-ci"}}'}]}
    return {"usecases": ["churn"], "envs": [env], "at": at}


def test_the_page_shows_the_fleet_with_each_champion_s_integrity():
    page = fleet.render(data())
    assert "<title>Fleet report</title>" in page and "release <code>rel1</code>" in page
    assert "v4 auc 0.81" in page and "from test" in page and "integrity verified" in page
    assert "Training runs: churn in prod" in page and "2 became a model" in page
    assert "Drift: churn in prod" in page and "1 of 3 runs alerted" in page
    assert "v4 champion" in page and "2.5 h" in page and "1 pending" in page


def test_the_page_never_shows_source_file_names_and_masks_emails_on_request():
    page = fleet.render(data())
    assert "SW1A" not in page and "customers_postcode" not in page        # file names can carry business names
    assert "lead@acme.example" in page
    masked = fleet.render(data(), masked=True)
    assert "lead@acme.example" not in masked and "..." in masked


def test_api_times_with_nanoseconds_are_read():
    assert fleet._api_time("2026-11-05T08:00:00.123456789Z") == datetime(2026, 11, 5, 8, 0, 0, 123456,
                                                                         tzinfo=timezone.utc)
    assert fleet._api_time("2026-11-05T08:00:00Z").hour == 8


def test_the_cli_command_is_there():
    from pipewright.cli.main import build_parser

    args = build_parser().parse_args(["report", "fleet", "--out", "f.html", "--mask"])
    assert args.report_command == "fleet" and args.mask and args.out == "f.html"
