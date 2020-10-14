CREATE OR REPLACE FUNCTION glam.fenix_build_to_build_hour(app_build_id STRING)
RETURNS STRING AS (
  FORMAT_DATETIME("%Y%m%d%H", mozfun.norm.fenix_build_to_datetime(app_build_id))
);

--Tests
SELECT
    -- old format, truncates on the hour
  assert.equals("2018010100", glam.fenix_build_to_build_hour("00010000")),
  assert.equals("2020060514", glam.fenix_build_to_build_hour("21571434")),
    -- new format
  assert.equals("2020081304", glam.fenix_build_to_build_hour("2015757667"))
