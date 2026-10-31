"""FR-70: bqml.contract_on: source checks the data contract on the rows as read, before bqml.prepare, as the
container path does (Pipewright 1.2). The default stays the prepared rows, as in 1.x."""
import sys

from pipewright import bqml
from tests.test_bqml_config import bqml_repo, resolve

sys.path.insert(0, str(bqml.STEPS_DIR))
import bqml_runtime as rt  # noqa: E402

PREPARE = {"sql/prepare.sql": "SELECT * REPLACE (LN(1 + tenure_months) AS tenure_months) FROM {rows}\n"}


def test_a_1_x_use_case_resolves_without_the_key(repo):
    section = resolve(bqml_repo(repo, prepare="sql/prepare.sql"), sql=PREPARE).ml["bqml"]
    assert "contract_on" not in section
    assert rt.contract_table({"bqml": section}, "p.d.churn_bqml_rows_r1") == "p.d.churn_bqml_rows_r1"


def test_source_checks_the_rows_as_read(repo):
    section = resolve(bqml_repo(repo, prepare="sql/prepare.sql", contract_on="source"), sql=PREPARE).ml["bqml"]
    assert section["contract_on"] == "source"
    assert rt.contract_table({"bqml": section}, "p.d.churn_bqml_rows_r1") == "p.d.churn_bqml_rows_r1_source"


def test_without_a_prepare_there_is_nothing_else_to_check(repo):
    section = resolve(bqml_repo(repo, contract_on="source")).ml["bqml"]
    assert rt.contract_table({"bqml": section}, "p.d.t") == "p.d.t"
