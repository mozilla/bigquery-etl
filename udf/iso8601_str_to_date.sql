/*

Take a ISO 8601 date or date and time string and return a DATE.  Return null if parse fails.

Possible formats: 2019-11-04, 2019-11-04T21:15:00+00:00, 2019-11-04T21:15:00Z, 20191104T211500Z

 */

CREATE TEMP FUNCTION udf_iso8601_str_to_date(date_str STRING) RETURNS DATE AS (
  COALESCE(
    SAFE.PARSE_DATE('%F', SAFE.SUBSTR(date_str, 0, 10)),
    SAFE.PARSE_DATE('%Y%m%d', SAFE.SUBSTR(date_str, 0, 8))
  )
);

-- Test

SELECT
  assert_equals(DATE '2019-11-04', udf_iso8601_str_to_date('2019-11-04')),
  assert_equals(DATE '2019-11-04', udf_iso8601_str_to_date('2019-11-04T21:15:00+00:00')),
  assert_equals(DATE '2019-11-04', udf_iso8601_str_to_date('20191104T211500Z')),
  assert_equals(DATE '0100-01-01', udf_iso8601_str_to_date('100-1-1')),
  assert_null(udf_iso8601_str_to_date('2000/01/01'))
