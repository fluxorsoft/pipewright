"""Value-free errors (NFR-18, D-65): data-path messages name the column and the rule, never the value."""
import pytest

from pipewright.build.templates import pipewright_runtime as rt
from pipewright.errors import value_free

MESSAGES = {
    "Bad int64 value: abc": "Bad int64 value: ...",
    "Invalid date: '2026-13-01'": "Invalid date: '...'",
    'Could not cast literal "x7" to type INT64 at [1:8]': 'Could not cast literal "..." to type INT64 at [1:8]',
    "Column `churned` has value=7, expected 0 or 1": "Column `churned` has value=..., expected 0 or 1",
    "row 12 got: 3.5e9; column tenure_months": "row 12 got: ...; column tenure_months",
    "Table `p.d.t` was not found in location us-central1": "Table `p.d.t` was not found in location us-central1",
}


@pytest.mark.parametrize("message", MESSAGES)
def test_values_are_taken_out_identifiers_kept(message):
    assert value_free(message) == MESSAGES[message]


def test_the_runtime_copy_is_in_step():
    for message in MESSAGES:
        assert rt.value_free(message) == value_free(message)
    assert rt._QUOTED.pattern == __import__("pipewright.errors", fromlist=["_QUOTED"])._QUOTED.pattern


def test_google_errors_leave_the_data_paths_value_free():
    class BadRequest(Exception):
        pass

    BadRequest.__module__ = "google.api_core.exceptions"

    @rt._value_free_errors
    def read():
        raise BadRequest("Bad int64 value: 4111-1111-1111-1111 in column card")

    with pytest.raises(RuntimeError) as e:
        read()
    assert "4111" not in str(e.value) and "column card" in str(e.value) and e.value.__cause__ is None

    @rt._value_free_errors
    def own():
        raise ValueError("data.label_source.key: 'customer_id' is not a column")   # Pipewright's own: unchanged

    with pytest.raises(ValueError, match="'customer_id'"):
        own()

