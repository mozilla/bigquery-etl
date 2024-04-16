/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.partner_org_clients(distribution_id STRING)
RETURNS STRING AS (
  'hello_world'
);

SELECT
  mozfun.assert.equals(udf.partner_org_clients('abc'), 'hello_world');
