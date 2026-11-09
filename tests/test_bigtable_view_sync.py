"""FR-110: `pipewright features sync` starts a sync of each Bigtable feature view and waits for it."""
from types import SimpleNamespace

import pytest

from pipewright.errors import ConfigError
from pipewright.online import sync

STORE = "projects/p/locations/europe-west2/featureOnlineStores/pw_retail_online"
ONLINE = {"store": STORE, "views": [{"view": "customer_activity", "features": ["a"]}]}


class Gcp:
    def __init__(self, polls):
        self.polls, self.posted = list(polls), []

    def post(self, url, body):
        self.posted.append(url)
        sync = f"{STORE}/featureViews/customer_activity/featureViewSyncs/1"
        return SimpleNamespace(ok=True, body={"featureViewSync": sync})

    def get(self, url, **_):
        return SimpleNamespace(ok=True, body=self.polls.pop(0))


def test_starts_and_waits_for_each_view():
    gcp = Gcp([{"runTime": {"startTime": "t"}},
               {"runTime": {"startTime": "t", "endTime": "u"}, "finalStatus": {},
                "syncSummary": {"rowSynced": "4298"}}])
    said = []
    out = sync.sync_bigtable_views(ONLINE, gcp, said.append, sleep=lambda _: None)
    assert gcp.posted == [f"https://europe-west2-aiplatform.googleapis.com/v1/{STORE}/featureViews/customer_activity:sync"]
    assert out == [{"view": "customer_activity", "sync": f"{STORE}/featureViews/customer_activity/featureViewSyncs/1",
                    "rows": 4298}]
    assert "synced 4298 rows" in said[-1]


def test_a_failed_sync_says_so_and_why():
    gcp = Gcp([{"runTime": {"endTime": "u"}, "finalStatus": {"code": 7, "message": "permission denied"}}])
    with pytest.raises(ConfigError, match="the sync failed: permission denied"):
        sync.sync_bigtable_views(ONLINE, gcp, lambda *_: None, sleep=lambda _: None)
