CREATE OR REPLACE FUNCTION norm.browser_version_info(version_string STRING)
RETURNS STRUCT<
  version STRING,
  major_version NUMERIC,
  minor_version NUMERIC,
  patch_revision NUMERIC,
  is_major_release BOOLEAN
> AS (
  STRUCT(
    version_string AS version,
    norm.truncate_version(version_string, 'major') AS major_version,
    norm.extract_version(version_string, 'minor') AS minor_version,
    norm.extract_version(version_string, 'patch') AS patch_revision,
    (
      ARRAY_LENGTH(SPLIT(version_string, '.')) = 2
      AND ENDS_WITH(version_string, '.0')
    ) AS is_major_release
  )
);

-- Tests
WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('45.9.0')
)
SELECT
  assert.equals('45.9.0', browser_info.version),
  assert.equals(45, browser_info.major_version),
  assert.equals(9, browser_info.minor_version),
  assert.equals(0, browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('96.0')
)
SELECT
  assert.equals('96.0', browser_info.version),
  assert.equals(96, browser_info.major_version),
  assert.equals(0, browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.true(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('73.0.1')
)
SELECT
  assert.equals('73.0.1', browser_info.version),
  assert.equals(73, browser_info.major_version),
  assert.equals(0, browser_info.minor_version),
  assert.equals(1, browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('foo-bar')
)
SELECT
  assert.equals('foo-bar', browser_info.version),
  assert.null(browser_info.major_version),
  assert.null(browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info(NULL)
)
SELECT
  assert.null(browser_info.version),
  assert.null(browser_info.major_version),
  assert.null(browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.null(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('101.0a1')
)
SELECT
  assert.equals('101.0a1', browser_info.version),
  assert.equals(101, browser_info.major_version),
  assert.equals(0, browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('101.10')
)
SELECT
  assert.equals('101.10', browser_info.version),
  assert.equals(101, browser_info.major_version),
  assert.equals(10, browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;

WITH browser_info AS (
  SELECT AS VALUE
    norm.browser_version_info('101.01')
)
SELECT
  assert.equals('101.01', browser_info.version),
  assert.equals(101, browser_info.major_version),
  assert.equals(1, browser_info.minor_version),
  assert.null(browser_info.patch_revision),
  assert.false(browser_info.is_major_release)
FROM
  browser_info;
