/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_employee_id_to_team(employee_id STRING, team STRING)
RETURNS STRING AS (
  'team'
);

SELECT
  mozfun.assert.equals(udf.hr_cost_center_to_steering_committee('employee_id', 'team'), 'team');
