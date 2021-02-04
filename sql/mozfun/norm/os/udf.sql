/*

Normalize an operating system string to one of the three major desktop platforms,
one of the two major mobile platforms, or "Other".

Reimplementation of logic used in the data pipeline:
https://github.com/mozilla/gcp-ingestion/blob/a6928fb089f1652856147c4605df715f327edfcd/ingestion-beam/src/main/java/com/mozilla/telemetry/transforms/NormalizeAttributes.java#L52-L74

*/
CREATE OR REPLACE FUNCTION norm.os(os STRING) AS (
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
  assert.equals("Windows", norm.os("Windows")),
  assert.equals("Windows", norm.os("WINNT")),
  assert.equals("Windows", norm.os("Windows_NT")),
  assert.equals("Windows", norm.os("WindowsNT")),
  assert.equals("Mac", norm.os("Darwin")),
  assert.equals("Linux", norm.os("Linux")),
  assert.equals("Linux", norm.os("GNU/Linux")),
  assert.equals("Linux", norm.os("SunOS")),
  assert.equals("Linux", norm.os("Solaris")),
  assert.equals("Linux", norm.os("FreeBSD")),
  assert.equals("Linux", norm.os("GNU/kFreeBSD")),
  assert.equals("Other", norm.os("AIX")),
  -- Mobile OS.
  assert.equals("iOS", norm.os("iOS")),
  assert.equals("iOS", norm.os("iOS?")),
  assert.equals("iOS", norm.os("iPhone")),
  assert.equals("iOS", norm.os("All the iPhones")),
  assert.equals("Other", norm.os("All the iOSes")),
  assert.equals("Other", norm.os("IOS")),
  assert.equals("Android", norm.os("Android")),
  assert.equals("Android", norm.os("Android?")),
  assert.equals("Other", norm.os("All the Androids")),
  -- Other.
  assert.equals("Other", norm.os("asdf"));
