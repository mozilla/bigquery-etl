CREATE OR REPLACE FUNCTION udf.partner_org_clients(distribution_id STRING)
RETURNS STRING AS (
  'hello_world'
);

SELECT
  mozfun.assert.equals(udf.partner_org_clients(distribution_id), 'hello world');
