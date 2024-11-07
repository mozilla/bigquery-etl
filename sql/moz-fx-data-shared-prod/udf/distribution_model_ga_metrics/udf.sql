/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.distribution_model_ga_metrics()
RETURNS STRING AS (
  'helloworld'
);

SELECT
  mozfun.assert.equals(udf.distribution_model_ga_metrics(), 'helloworld');
