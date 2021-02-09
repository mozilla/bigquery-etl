CREATE OR REPLACE FUNCTION norm.truncate_version(os_version STRING, truncation_level STRING)
RETURNS NUMERIC AS (
  CASE
  WHEN
    truncation_level = "minor"
  THEN
    CAST(REGEXP_EXTRACT(os_version, r"^([0-9]+[.]?[0-9]+).*") AS NUMERIC)
  WHEN
    truncation_level = "major"
  THEN
    CAST(REGEXP_EXTRACT(os_version, r"^([0-9]+).*") AS NUMERIC)
  ELSE
    NULL
  END
);

-- Tests
SELECT
  assert.equals(16.1, norm.truncate_version("16.1.1", "minor")),
  assert.equals(16.03, norm.truncate_version("16.03.1", "minor")),
  assert.equals(16, norm.truncate_version("16.1.1", "major")),
  assert.equals(10, norm.truncate_version("10", "minor")),
  assert.equals(5.1, norm.truncate_version("5.1.5-ubuntu-foobar", "minor")),
  assert.equals(CAST(NULL AS NUMERIC), norm.truncate_version("5.1.5-ubuntu-foobar", "patch")),
  assert.equals(CAST(NULL AS NUMERIC), norm.truncate_version("foo-bar", "minor"))
