import copy

import pytest

from pipewright.alerts import sync as alerts
from pipewright.config import load_environment, resolve_usecase
from pipewright.errors import ConfigError


class R:
    def __init__(self, status, body=None):
        self.status, self.body = status, body or {}

    @property
    def ok(self):
        return 200 <= self.status < 300


class FakeMonitoring:
    """Notification channels and alert policies in memory, enough of the v3 REST API for sync()."""

    def __init__(self):
        self.channels, self.policies, self.calls, self.n = {}, {}, [], 0

    def _id(self, kind):
        self.n += 1
        return f"projects/acme-ml-dev/{kind}/{self.n}"

    def get(self, url, **params):
        if url.endswith("/notificationChannels"):
            return R(200, {"notificationChannels": list(self.channels.values())})
        flt = params.get("filter", "")
        found = [p for p in self.policies.values()
                 if not flt or (f'"{p["userLabels"]["pipewright-usecase"]}"' in flt and
                                f'"{p["userLabels"]["pipewright-environment"]}"' in flt)]
        return R(200, {"alertPolicies": copy.deepcopy(found)})

    def post(self, url, body):
        self.calls.append(("POST", url.rsplit("/", 1)[-1]))
        kind = url.rsplit("/", 1)[-1]
        obj = {**copy.deepcopy(body), "name": self._id(kind)}
        (self.channels if kind == "notificationChannels" else self.policies)[obj["name"]] = obj
        return R(200, obj)

    def patch(self, url, body):
        self.calls.append(("PATCH", url))
        name = url.split("/v3/", 1)[1]
        self.policies[name] = {**copy.deepcopy(body), "name": name}
        return R(200, self.policies[name])

    def delete(self, url):
        self.calls.append(("DELETE", url))
        self.policies.pop(url.split("/v3/", 1)[1], None)
        self.channels.pop(url.split("/v3/", 1)[1], None)
        return R(200)


def uc_for(repo, env="dev", **alerts_cfg):
    repo.usecase["spec"]["monitoring"] = {"alerts": alerts_cfg}
    return resolve_usecase(repo.write(), load_environment(repo.root, env))


def test_sync_creates_then_is_idempotent_then_deletes(repo):
    mon = FakeMonitoring()
    uc = uc_for(repo, notify=["team@acme.example", "oncall-list@acme.example"],
                events=["monitoring.alert", "training.rejected"])
    env = load_environment(repo.root, "dev")
    plan = alerts.sync(uc, env, mon)
    assert sorted(plan.channels_create) == ["oncall-list@acme.example", "team@acme.example"]
    assert plan.create == ["monitoring.alert", "training.rejected"] and not plan.errors
    policy = next(p for p in mon.policies.values() if "rejected" in p["displayName"])
    cond = policy["conditions"][0]["conditionMatchedLog"]
    assert 'labels."pipewright-usecase"="churn"' in cond["filter"]
    assert 'jsonPayload.pipewright_event.data.decision="reject"' in cond["filter"]
    assert cond["labelExtractors"]["champion_value"] == "EXTRACT(jsonPayload.pipewright_event.data.champion_value)"
    assert len(policy["notificationChannels"]) == 2 and policy["documentation"]["subject"].startswith("[dev] churn")
    assert "${log.extracted_label.primary_value}" in policy["documentation"]["content"]

    again = alerts.sync(uc, env, mon)
    assert again.unchanged == ["monitoring.alert", "training.rejected"] and not again.create + again.update

    fewer = uc_for(repo, notify=["team@acme.example"], events=["monitoring.alert"])
    plan = alerts.sync(fewer, env, mon)
    assert plan.update == ["monitoring.alert"] and len(plan.delete) == 1
    assert [p["displayName"] for p in mon.policies.values()] == ["pipewright churn dev: drift or performance alert"]


def test_another_channel_for_the_same_address_changes_nothing(repo):
    # A platform's own email channel for the same address (the GKE logging agent alert) must not repoint the use
    # case's policies: found 2026-10-17, when every release's alerts sync tried to update all its policies.
    mon = FakeMonitoring()
    uc = uc_for(repo, notify=["team@acme.example"], events=["monitoring.alert"])
    env = load_environment(repo.root, "dev")
    alerts.sync(uc, env, mon)
    mine = next(iter(mon.channels))
    other = "projects/acme-ml-dev/notificationChannels/zzz-platform"
    mon.channels[other] = {"name": other, "type": "email", "labels": {"email_address": "team@acme.example"}}
    again = alerts.sync(uc, env, mon)
    assert again.unchanged == ["monitoring.alert"] and not again.update and not again.channels_create
    assert next(iter(mon.policies.values()))["notificationChannels"] == [mine]


def test_no_recipients_removes_policies_and_dry_run_changes_nothing(repo):
    mon = FakeMonitoring()
    uc = uc_for(repo, notify=["team@acme.example"])
    env = load_environment(repo.root, "dev")
    alerts.sync(uc, env, mon)
    calls = len(mon.calls)
    silent = uc_for(repo, notify=[])
    dry = alerts.sync(silent, env, mon, dry_run=True)
    assert len(dry.delete) == 1 and len(mon.calls) == calls
    assert "no alert emails" in alerts.render(silent, dry, True)
    alerts.sync(silent, env, mon)
    assert mon.policies == {}


def test_recipients_vary_per_environment(repo):
    spec = repo.usecase["spec"]
    spec["monitoring"] = {"alerts": {"notify": ["oncall@acme.example"]}}
    spec["environments"]["dev"]["monitoring"] = {"alerts": {"notify": []}}       # dev: nobody
    repo.add_env("prod")
    dev = resolve_usecase(repo.write(), load_environment(repo.root, "dev"))
    prod = resolve_usecase(repo.write(), load_environment(repo.root, "prod"))
    assert dev.ml["monitoring"]["alerts"]["notify"] == []
    assert prod.ml["monitoring"]["alerts"] == {"notify": ["oncall@acme.example"], "events": ["monitoring.alert"],
                                               "severity": "WARNING"}


def test_bad_addresses_and_events_rejected(repo):
    with pytest.raises(ConfigError, match="does not match"):
        uc_for(repo, notify=["not-an-email"])
    with pytest.raises(ConfigError, match="is not one of"):
        uc_for(repo, notify=["a@b.co"], events=["drift"])


def test_an_address_that_left_notify_loses_its_channel(repo):
    # FR-77 (Pipewright 1.2), the ebook's Chapter 14: a test address swapped for the team's left its channel behind.
    mon = FakeMonitoring()
    first = uc_for(repo, notify=["tester@example.com"], events=["monitoring.alert"])
    env = load_environment(repo.root, "dev")
    alerts.sync(first, env, mon)
    other = "projects/acme-ml-dev/notificationChannels/zzz-platform"     # not Pipewright's: never touched
    mon.channels[other] = {"name": other, "type": "email", "labels": {"email_address": "ops@example.com"}}
    swapped = uc_for(repo, notify=["ml-team@example.com"], events=["monitoring.alert"])
    dry = alerts.sync(swapped, env, mon, dry_run=True)
    assert list(dry.channels_delete) == ["tester@example.com"] and len(mon.channels) == 2
    assert "email channel tester@example.com: would delete (no policy uses it)" in alerts.render(swapped, dry, True)
    plan = alerts.sync(swapped, env, mon)
    assert list(plan.channels_delete) == ["tester@example.com"] and not plan.errors
    left = sorted(c["labels"]["email_address"] for c in mon.channels.values())
    assert left == ["ml-team@example.com", "ops@example.com"]
    assert not alerts.sync(swapped, env, mon).channels_delete
