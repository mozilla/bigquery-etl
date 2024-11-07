/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.funnel_derived_installs(
  silent BOOLEAN,
  submission_timestamp TIMESTAMP,
  build_id STRING,
  attribution STRING,
  distribution_id STRING
)
RETURNS STRING AS (
  'hello'
);

SELECT
  mozfun.assert.equals(
    udf.funnel_derived_installs(TRUE, '2024-04-08 10:43:10.972298 UTC', '123', 'ATTR1', 'DISTRO1'),
    'hello'
  );
