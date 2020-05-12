/*

Normalize an operating system string to one of the three major desktop platforms,
one of the two major mobile platforms, or "Other".

Reimplementation of logic used in the data pipeline:
https://github.com/mozilla/gcp-ingestion/blob/a6928fb089f1652856147c4605df715f327edfcd/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L52-L74

*/
CREATE OR REPLACE FUNCTION udf.normalize_os(os STRING) AS (
  CASE
  WHEN
    os LIKE 'Windows%'
    OR os LIKE 'WINNT%'
  THEN
    'Windows'
  WHEN
    os LIKE 'Darwin%'
  THEN
    'Mac'
  WHEN
    os LIKE '%Linux%'
    OR os LIKE '%BSD%'
    OR os LIKE '%SunOS%'
    OR os LIKE '%Solaris%'
  THEN
    'Linux'
  WHEN
    os LIKE 'iOS%'
    OR os LIKE '%iPhone%'
  THEN
    'iOS'
  WHEN
    os LIKE 'Android%'
  THEN
    'Android'
  ELSE
    'Other'
  END
);

-- Tests
SELECT
  -- Desktop OS.
  assert_equals("Windows", udf.normalize_os("Windows")),
  assert_equals("Windows", udf.normalize_os("WINNT")),
  assert_equals("Windows", udf.normalize_os("Windows_NT")),
  assert_equals("Windows", udf.normalize_os("WindowsNT")),
  assert_equals("Mac", udf.normalize_os("Darwin")),
  assert_equals("Linux", udf.normalize_os("Linux")),
  assert_equals("Linux", udf.normalize_os("GNU/Linux")),
  assert_equals("Linux", udf.normalize_os("SunOS")),
  assert_equals("Linux", udf.normalize_os("Solaris")),
  assert_equals("Linux", udf.normalize_os("FreeBSD")),
  assert_equals("Linux", udf.normalize_os("GNU/kFreeBSD")),
  assert_equals("Other", udf.normalize_os("AIX")),
  -- Mobile OS.
  assert_equals("iOS", udf.normalize_os("iOS")),
  assert_equals("iOS", udf.normalize_os("iOS?")),
  assert_equals("iOS", udf.normalize_os("iPhone")),
  assert_equals("iOS", udf.normalize_os("All the iPhones")),
  assert_equals("Other", udf.normalize_os("All the iOSes")),
  assert_equals("Other", udf.normalize_os("IOS")),
  assert_equals("Android", udf.normalize_os("Android")),
  assert_equals("Android", udf.normalize_os("Android?")),
  assert_equals("Other", udf.normalize_os("All the Androids")),
  -- Other.
  assert_equals("Other", udf.normalize_os("asdf"));
