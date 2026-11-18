"""FR-149: the manifest carries policies.feature_registry, which the starter and Enterprise write (runners_read)."""
import copy

import pytest

from pipewright.config.schema import validate
from pipewright.errors import ConfigError
from tests.conftest import MANIFEST


@pytest.mark.parametrize("value", ["runners_write", "runners_read"])
def test_the_manifest_takes_the_feature_registry_policy(value):
    manifest = copy.deepcopy(MANIFEST)
    manifest.setdefault("policies", {})["feature_registry"] = value
    validate(manifest, "manifest", "m.json")


def test_an_unknown_feature_registry_policy_is_refused():
    manifest = copy.deepcopy(MANIFEST)
    manifest.setdefault("policies", {})["feature_registry"] = "nobody"
    with pytest.raises(ConfigError):
        validate(manifest, "manifest", "m.json")
