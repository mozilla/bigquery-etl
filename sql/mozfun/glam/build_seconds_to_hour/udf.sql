CREATE OR REPLACE FUNCTION glam.build_seconds_to_hour(build_hour STRING)
RETURNS STRING AS (
  SUBSTRING(build_hour, 0, 10)
);

SELECT
  assert.equals("2018010100", glam.build_seconds_to_hour("20180101000103")),
  assert.equals("2020060514", glam.build_seconds_to_hour("20200605141414")),
  assert.equals("2020081304", glam.build_seconds_to_hour("20200813040404"))
