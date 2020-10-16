CREATE OR REPLACE FUNCTION udf.fenix_build_to_datetime(app_build STRING) AS (
  mozfun.norm.fenix_build_to_datetime(app_build)
);

-- Tests
SELECT
  assert.equals(DATETIME '2020-06-05 14:34:00', udf.fenix_build_to_datetime("21571434")),
  assert.equals(DATETIME '2018-01-01 00:00:00', udf.fenix_build_to_datetime("00010000")),
  assert.equals(DATETIME '2027-12-31 23:59:00', udf.fenix_build_to_datetime("93652359")),
  assert.equals(DATETIME '2020-08-13 04:00:00', udf.fenix_build_to_datetime("2015757667")),
  assert.equals(DATETIME '2014-12-28 00:00:00', udf.fenix_build_to_datetime("0000000000")),
  assert.equals(DATETIME '2014-12-28 00:00:00', udf.fenix_build_to_datetime("0000000001")),
  assert.equals(
    DATETIME '2014-12-28 00:00:00',
    udf.fenix_build_to_datetime(CAST(1 << 31 AS STRING))
  ),
  assert.equals(DATETIME '2014-12-28 01:00:00', udf.fenix_build_to_datetime("0000000009")),
  assert.null(udf.fenix_build_to_datetime("7777777")),
  assert.null(udf.fenix_build_to_datetime("999999999")),
  assert.null(udf.fenix_build_to_datetime("3")),
  assert.null(udf.fenix_build_to_datetime("hi")),
  -- 8 digits, minutes place is 60
  assert.null(udf.fenix_build_to_datetime("11831860")),
  -- 8 digits, hours place is 24
  assert.null(udf.fenix_build_to_datetime("11832459"))
