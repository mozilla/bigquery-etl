/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.funnel_derived_ga_metrics(
  device_category STRING,
  browser STRING,
  operating_system STRING
)
RETURNS STRING AS (
  'abc'
);

SELECT
  mozfun.assert.equals(udf.funnel_derived_ga_metrics('device category', 'browser', 'os'), 'abc');
