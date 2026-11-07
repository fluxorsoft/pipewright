from pipewright.online import sizing


def test_lookup_defaults_pass():
    assert sizing.lookup_problems({}) == []


def test_lookup_cpu_and_memory_follow_cloud_run():
    assert "fractional CPU" in sizing.lookup_problems({"cpu": 0.5})[0]
    assert "1, 2, 4, 6 or 8" in sizing.lookup_problems({"cpu": 3})[0]
    assert "2Gi to 16Gi" in sizing.lookup_problems({"cpu": 4, "memory": "1Gi"})[0]
    assert "512Mi to 4Gi" in sizing.lookup_problems({"cpu": 1, "memory": "8Gi"})[0]
    assert sizing.lookup_problems({"cpu": 8, "memory": "32Gi"}) == []
    assert "Mi or Gi" in sizing.lookup_problems({"memory": "1GB"})[0]


def test_lookup_limits_and_floor():
    limits = {"max_cpu": 2, "max_instances": 4, "min_min_instances": 1, "max_min_instances": 2}
    got = sizing.lookup_problems({"cpu": 4, "memory": "4Gi", "max_instances": 5, "min_instances": 0}, limits)
    text = " | ".join(got)
    assert "at most 2" in text and "at most 4" in text and "keeps at least 1 warm" in text


def test_lookup_connections_fit():
    got = sizing.lookup_problems({"pool_size": 10, "max_instances": 5}, connection_limit=40)
    assert "50 connections" in got[0]


def test_standing_cost():
    assert sizing.lookup_standing_cost({}) == 0
    warm = sizing.lookup_standing_cost({"min_instances": 1})
    assert 13 < warm < 15                                     # about $14 (design table)
    assert 26 < sizing.lookup_standing_cost({"min_instances": 2}) < 29
    assert 58 < sizing.lookup_standing_cost({"min_instances": 1, "billing": "instance"}) < 62


def test_instance_tiers():
    assert sizing.instance_problems({}) == []
    assert sizing.instance_problems({"tier": "db-custom-2-7680"}) == []
    assert "even number" in sizing.instance_problems({"tier": "db-custom-3-7680"})[0]
    assert "multiple of 256" in sizing.instance_problems({"tier": "db-custom-1-3000"})[0]
    assert "set edition enterprise_plus" in sizing.instance_problems({"tier": "db-perf-optimized-N-2"})[0]
    assert sizing.instance_problems({"tier": "db-perf-optimized-N-2", "edition": "enterprise_plus"}) == []
    assert "one of" in sizing.instance_problems({"tier": "db-perf-optimized-N-6", "edition": "enterprise_plus"})[0]
    assert "not a Cloud SQL" in sizing.instance_problems({"tier": "db-n1-standard-1"})[0]
    assert "N4, C4 and C4A" in sizing.instance_problems({"disk": {"type": "hyperdisk"}})[0]
    assert sizing.instance_problems({"tier": "db-custom-N4-2-8192", "disk": {"type": "hyperdisk"}}) == []
    assert "Enterprise edition only" in " ".join(sizing.instance_problems(
        {"tier": "db-c4a-highmem-2", "edition": "enterprise_plus", "disk": {"type": "hdd"}}))
    assert "Enterprise Plus only" in sizing.instance_problems({"data_cache": True})[0]


def test_instance_costs_match_the_design():
    assert 59 < sizing.instance_monthly_cost({}) < 63                                   # about $61
    assert 118 < sizing.instance_monthly_cost({"availability": "regional"}) < 126      # about $122
    assert 9 < sizing.instance_monthly_cost({"tier": "db-f1-micro"}) < 13              # about $11
    assert 215 < sizing.instance_monthly_cost({"tier": "db-perf-optimized-N-2", "edition": "enterprise_plus"}) < 230


def test_shared_core_warned_in_production_only():
    assert sizing.instance_warnings({"tier": "db-g1-small"}, production=True)
    assert not sizing.instance_warnings({"tier": "db-g1-small"}, production=False)


def test_default_connections():
    assert sizing.default_connections("db-custom-1-3840") == 100       # measured in spike (d)
    assert sizing.default_connections("db-f1-micro") == 25
    assert sizing.default_connections("db-custom-2-7680") == 400
