/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_steering_committee(employee_id STRING)
RETURNS STRING AS (
  'steering_committee'
);
