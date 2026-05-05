CREATE OR REPLACE FUNCTION udf.distribution_model_clients(distribution_id STRING)
RETURNS STRING AS (
  'distro'
);

SELECT
  mozfun.assert.equals(udf.distribution_model_clients('abc'), 'distro');
