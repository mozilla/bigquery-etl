/*
This is a stub implementation for use with tests in this repo
Real implementation is in private-bigquery-etl
*/
CREATE OR REPLACE FUNCTION udf.funnel_derived_clients(
  os STRING,
  first_seen_date DATE,
  build_id STRING,
  attribution_source STRING,
  attribution_ua STRING,
  startup_profile_selection_reason STRING,
  distribution_id STRING
)
RETURNS STRING AS (
  'hello_universe'
);

SELECT
  mozfun.assert.equals(
    udf.funnel_derived_clients(
      'os',
      '2024-04-01',
      'build_id',
      'attribution_source',
      'attribution_ua',
      'startup_profile_selection_reason',
      'distribution_id'
    ),
    'hello_universe'
  );
