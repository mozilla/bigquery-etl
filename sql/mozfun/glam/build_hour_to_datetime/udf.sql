CREATE OR REPLACE FUNCTION glam.build_hour_to_datetime(build_hour STRING)
RETURNS DATETIME AS (
  PARSE_DATETIME("%Y%m%d%H", build_hour)
);

SELECT
  assert.equals(DATETIME '2018-01-01 00:00:00', glam.build_hour_to_datetime("2018010100")),
  assert.equals(DATETIME '2020-06-05 14:00:00', glam.build_hour_to_datetime("2020060514")),
  assert.equals(DATETIME '2020-08-13 04:00:00', glam.build_hour_to_datetime("2020081304"))
