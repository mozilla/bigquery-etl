/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_steering_committee(employee_id STRING, steering_committee STRING)
RETURNS STRING AS (
  'steering_committee'
);

SELECT
  mozfun.assert.equals(
    udf.hr_steering_committee('employee_id', 'steering_committee'),
    'steering_committee'
  );
