/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_cost_center_to_steering_committee(cost_center STRING)
RETURNS STRING AS (
  'cost_center_group'
);

SELECT
  mozfun.assert.equals(udf.hr_cost_center_to_steering_committee(), 'cost_center_group');
