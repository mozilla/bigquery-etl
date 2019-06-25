import os
import sys

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
import pytest

# sys.path needs to be modified to enable package imports from parent
# and sibling directories. Also see:
# https://stackoverflow.com/questions/6323860/sibling-package-imports/23542795#23542795
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bigquery_etl import parse_udf  # noqa: E402


TEST_UDFS = """
CREATE TEMP FUNCTION
  assert_null(actual ANY TYPE) AS (
    IF(actual IS NULL, TRUE, ERROR(CONCAT(
      'Expected null, but got ',
      TO_JSON_STRING(actual)))));

CREATE TEMP FUNCTION
  assert_true(actual ANY TYPE) AS (
    IF(actual, TRUE, ERROR(CONCAT(
      'Expected true, but got ',
      TO_JSON_STRING(actual)))));

CREATE TEMP FUNCTION
  assert_false(actual ANY TYPE) AS (
    IF(actual, ERROR(CONCAT(
      'Expected false, but got ',
      TO_JSON_STRING(actual))),
      TRUE));

CREATE TEMP FUNCTION
  assert_equals(expected ANY TYPE, actual ANY TYPE) AS (
    IF(expected = actual, TRUE, ERROR(CONCAT(
      'Expected ',
      TO_JSON_STRING(expected),
      ' but got ',
      TO_JSON_STRING(actual)))));

CREATE TEMP FUNCTION
  assert_array_equals(expected ANY TYPE, actual ANY TYPE) AS (
    IF(
      EXISTS(
        (SELECT * FROM UNNEST(expected) WITH OFFSET
         EXCEPT DISTINCT
         SELECT * FROM UNNEST(actual) WITH OFFSET)
        UNION ALL
        (SELECT * FROM UNNEST(actual) WITH OFFSET
         EXCEPT DISTINCT
         SELECT * FROM UNNEST(expected) WITH OFFSET)
      ),
    ERROR(CONCAT(
      'Expected ',
      TO_JSON_STRING(expected),
      ' but got ',
      TO_JSON_STRING(actual))),
    TRUE));
"""


@pytest.fixture(scope="session")
def bq():
    return bigquery.Client()


@pytest.mark.parametrize(
    "test_num,test_query,udf",
    [
        (i + 1, test, udf)
        for udf in parse_udf.parse_udf_dir("udf")
        for i, test in enumerate(udf.tests)
    ],
)
def test_udfs(bq, test_num, test_query, udf):
    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    # run query
    sql = "\n".join([TEST_UDFS, udf.full_sql, test_query])
    job = bq.query(sql, job_config=job_config)
    try:
        job.result()
    except BadRequest as e:
        raise Exception(f"Failed test #{test_num} for {udf.name}: {e}")
