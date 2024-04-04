CREATE OR REPLACE FUNCTION norm.distribution_model_ga_metrics()
RETURNS STRING AS (
  (SELECT 'non-distribution' AS distribution_model)
);

SELECT
  mozfun.assert.equals(mozfun.norm.distribution_model_ga_metrics(), 'non-distribution');
