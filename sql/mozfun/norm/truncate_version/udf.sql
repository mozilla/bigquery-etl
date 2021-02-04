CREATE OR REPLACE FUNCTION norm.truncate_version(os_version STRING, truncation_level STRING)
RETURNS STRING AS (
  CASE
  WHEN
    truncation_level = "minor"
  THEN
    REGEXP_EXTRACT(os_version, r"^([0-9]+[.]?[0-9]+).*")
  WHEN
    truncation_level = "major"
  THEN
    REGEXP_EXTRACT(os_version, r"^([0-9]+).*")
  ELSE
    NULL
  END
);

-- Tests
SELECT
  assert.equals("16.1", norm.truncate_version("16.1.1", "minor")),
  assert.equals("16", norm.truncate_version("16.1.1", "major")),
  assert.equals("10", norm.truncate_version("10", "minor")),
  assert.equals("5.1", norm.truncate_version("5.1.5-ubuntu-foobar", "minor")),
  assert.equals(CAST(NULL AS STRING), norm.truncate_version("5.1.5-ubuntu-foobar", "patch")),
  assert.equals(CAST(NULL AS STRING), norm.truncate_version("foo-bar", "minor"))
