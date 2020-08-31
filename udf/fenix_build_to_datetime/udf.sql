/*
Convert the Fenix client_info.app_build-format string to a DATETIME.
May return NULL on failure.

The Fenix app_build format is documented here:
https://github.com/mozilla-mobile/fenix/blob/c72834479eb3e13ee91f82b529e59aa08392a92d/automation/gradle/versionCode.gradle#L13

In short it is yDDDHHmm
 * y is years since 2018
 * DDD is day of year, 0-padded, 001-366
 * HH is hour of day, 00-23
 * mm is minute of hour, 00-59

After using this you may wish to DATETIME_TRUNC(result, DAY) for grouping
by build date.

*/
CREATE OR REPLACE FUNCTION udf.fenix_build_to_datetime(app_build STRING) AS (
  DATETIME_ADD(
    DATETIME_ADD(
      DATETIME_ADD(
        DATETIME_ADD(
          DATETIME '2018-01-01 00:00:00',
          INTERVAL SAFE_CAST(SUBSTR(app_build, 1, 1) AS INT64) YEAR
        ),
        INTERVAL SAFE_CAST(SUBSTR(app_build, 2, 3) AS INT64) - 1 DAY
      ),
      INTERVAL SAFE_CAST(SUBSTR(app_build, 5, 2) AS INT64) HOUR
    ),
    INTERVAL SAFE_CAST(SUBSTR(app_build, 7, 2) AS INT64) MINUTE
  )
);

-- Tests
SELECT
  assert_equals(DATETIME '2020-06-05 14:34:00', udf.fenix_build_to_datetime("21571434"))
