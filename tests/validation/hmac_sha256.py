"""
Validate HMAC-SHA256 implementation against the NIST test vectors.

The vectors are located in tests/validation/data/hmac_sha256_validation.json.
"""

import json
import pytest
from bigquery_etl.parse_udf import read_udf_dirs, RawUdf, udf_tests_sql
from google.cloud import bigquery

validation_data_file = "tests/validation/data/hmac_sha256_validation.json"


def udfs():
    """Get all udfs and assertions"""
    return read_udf_dirs("tests/assert", "udf", "udf_js")


def load_data():
    """Load test data"""
    with open(validation_data_file, "r") as f:
        return json.load(f)["data"]


def generate_raw_udf(test_cases):
    """Generates a SQL test for each validation instance in hmac_sha256_validation.json."""
    test_sql_fixture = (
        "SELECT assert_equals("
        "'{Mac}',"
        "TO_HEX(SUBSTR("
        "udf.hmac_sha256("
        "FROM_HEX('{Key}'),"
        "FROM_HEX('{Msg}')),"
        "1,"
        "{Tlen})));"
    )
    test_sql_stmnts = [test_sql_fixture.format(**test_case) for test_case in test_cases]

    return RawUdf.from_text(
        "\n".join(test_sql_stmnts), "udf", "hmac_sha256", is_defined=False
    )


def generate_sql():
    """Generate SQL statements to test"""
    return udf_tests_sql(generate_raw_udf(load_data()), udfs())


@pytest.mark.parametrize("sql", generate_sql())
def test_validate_hmac_sha256(sql):
    """Validate hmac_sha256"""
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    job = bigquery.Client().query(sql, job_config=job_config)
    job.result()
