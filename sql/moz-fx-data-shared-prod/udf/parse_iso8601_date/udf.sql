/*

Take a ISO 8601 date or date and time string and return a DATE.  Return null if parse fails.

Possible formats: 2019-11-04, 2019-11-04T21:15:00+00:00, 2019-11-04T21:15:00Z, 20191104T211500Z

 */
CREATE OR REPLACE FUNCTION udf.parse_iso8601_date(date_str STRING)
RETURNS DATE AS (
  COALESCE(
    SAFE.PARSE_DATE('%F', SAFE.SUBSTR(date_str, 0, 10)),
    SAFE.PARSE_DATE('%Y%m%d', SAFE.SUBSTR(date_str, 0, 8))
  )
);

-- Test
SELECT
  mozfun.assert.equals(DATE '2019-11-04', udf.parse_iso8601_date('2019-11-04')),
  mozfun.assert.equals(DATE '2019-11-04', udf.parse_iso8601_date('2019-11-04T21:15:00+00:00')),
  mozfun.assert.equals(DATE '2019-11-04', udf.parse_iso8601_date('20191104T211500Z')),
  mozfun.assert.equals(DATE '0100-01-01', udf.parse_iso8601_date('100-1-1')),
  mozfun.assert.null(udf.parse_iso8601_date('2000/01/01'))
