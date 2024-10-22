/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.partner_org_ga_metrics()
RETURNS STRING AS (
  (SELECT 'hola_world' AS partner_org)
);

SELECT
  mozfun.assert.equals(udf.partner_org_ga_metrics(), 'hola_world');
