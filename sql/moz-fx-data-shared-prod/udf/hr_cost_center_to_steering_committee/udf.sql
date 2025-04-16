/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_cost_center_to_steering_committee(
  cost_center STRING,
  cost_center_hierarchy_group STRING
)
RETURNS STRING AS (
  'steering_committee'
);

SELECT
  mozfun.assert.equals(
    udf.hr_cost_center_to_steering_committee('cost_center', 'cost_center_heirarchy_group'),
    'steering_committee'
  );
