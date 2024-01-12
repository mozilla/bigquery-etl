CREATE OR REPLACE FUNCTION norm.fenix_build_to_datetime(app_build STRING) AS (
  CASE
    WHEN LENGTH(app_build) = 8
      AND SUBSTR(app_build, 5, 2) < "24"
      AND SUBSTR(app_build, 7, 2) < "60"
      -- Ideally, we would use PARSE_DATETIME, but that doesn't support
      -- day of year (%j) or the custom single-character year used here.
      THEN DATETIME_ADD(
          DATETIME(
            2018 + SAFE_CAST(SUBSTR(app_build, 1, 1) AS INT64), -- year
            1, -- placeholder month
            1, -- placeholder year
            SAFE_CAST(SUBSTR(app_build, 5, 2) AS INT64),
            SAFE_CAST(SUBSTR(app_build, 7, 2) AS INT64),
            0  -- seconds is always zero
          ),
          INTERVAL SAFE_CAST(SUBSTR(app_build, 2, 3) AS INT64) - 1 DAY
        )
    WHEN LENGTH(app_build) = 10
      THEN DATETIME_ADD(
          DATETIME '2014-12-28 00:00:00',
          INTERVAL(
            SAFE_CAST(app_build AS INT64)
            -- We shift left and then right again to erase all but the 20 rightmost bits
            << (64 - 20) >> (64 - 20)
            -- We then shift right to erase the last 3 bits, leaving just the 17 representing time
            >> 3
          ) HOUR
        )
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals(DATETIME '2020-06-05 14:34:00', norm.fenix_build_to_datetime("21571434")),
  assert.equals(DATETIME '2018-01-01 00:00:00', norm.fenix_build_to_datetime("00010000")),
  assert.equals(DATETIME '2027-12-31 23:59:00', norm.fenix_build_to_datetime("93652359")),
  assert.equals(DATETIME '2020-08-13 04:00:00', norm.fenix_build_to_datetime("2015757667")),
  assert.equals(DATETIME '2014-12-28 00:00:00', norm.fenix_build_to_datetime("0000000000")),
  assert.equals(DATETIME '2014-12-28 00:00:00', norm.fenix_build_to_datetime("0000000001")),
  assert.equals(
    DATETIME '2014-12-28 00:00:00',
    norm.fenix_build_to_datetime(CAST(1 << 31 AS STRING))
  ),
  assert.equals(DATETIME '2014-12-28 01:00:00', norm.fenix_build_to_datetime("0000000009")),
  assert.null(norm.fenix_build_to_datetime("7777777")),
  assert.null(norm.fenix_build_to_datetime("999999999")),
  assert.null(norm.fenix_build_to_datetime("3")),
  assert.null(norm.fenix_build_to_datetime("hi")),
  -- 8 digits, minutes place is 60
  assert.null(norm.fenix_build_to_datetime("11831860")),
  -- 8 digits, hours place is 24
  assert.null(norm.fenix_build_to_datetime("11832459"))
