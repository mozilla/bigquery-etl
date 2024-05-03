/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.partner_org_installs(distribution_id STRING)
RETURNS STRING AS (
  'partner'
);

SELECT
  mozfun.assert.equals(udf.partner_org_installs('distro1'), 'partner');
