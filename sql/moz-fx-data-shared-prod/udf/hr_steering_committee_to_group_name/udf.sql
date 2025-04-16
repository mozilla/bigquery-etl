/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.hr_steering_committee_to_group_name(steering_committee STRING)
RETURNS STRING AS (
  'steering_committee_group'
);

SELECT
  mozfun.assert.equals(udf.hr_steering_committee_to_group_name(), 'steering_committee_group');
