/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.distribution_model_installs(distribution_id STRING)
RETURNS STRING AS (
  'hello_world'
);

SELECT
  mozfun.assert.equals(udf.distribution_model_installs('abcdefg'), 'hello_world');
