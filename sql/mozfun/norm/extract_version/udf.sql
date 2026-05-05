CREATE OR REPLACE FUNCTION norm.extract_version(version_string STRING, extraction_level STRING)
RETURNS NUMERIC AS (
  CASE
    WHEN extraction_level = "patch"
      THEN CAST(REGEXP_EXTRACT(version_string, r"^[0-9]+[.][0-9]+[.]([0-9]+).*") AS NUMERIC)
    WHEN extraction_level = "minor"
      THEN CAST(REGEXP_EXTRACT(version_string, r"^[0-9]+[.]([0-9]+).*") AS NUMERIC)
    WHEN extraction_level = "major"
      THEN CAST(REGEXP_EXTRACT(version_string, r"^([0-9]+).*") AS NUMERIC)
    WHEN extraction_level = "beta"
      THEN CAST(REGEXP_EXTRACT(version_string, r"^[0-9\.]+[rc|b]+([0-9]+)$") AS NUMERIC)
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals(1, norm.extract_version("16.1.1", "minor")),
  assert.equals(3, norm.extract_version("16.03.1", "minor")),
  assert.equals(2, norm.extract_version("16.05.2", "patch")),
  assert.equals(16, norm.extract_version("16.1.1", "major")),
  assert.null(norm.extract_version("10", "minor")),
  assert.equals(1, norm.extract_version("5.1.5-ubuntu-foobar", "minor")),
  assert.equals(100, norm.extract_version("100.01.1", "major")),
  assert.equals(4, norm.extract_version("100.04.1", "minor")),
  assert.equals(5, norm.extract_version("5.1.5-ubuntu-foobar", "patch")),
  assert.null(norm.extract_version("foo-bar", "minor")),
  assert.equals(1, norm.extract_version("100.04b1", "beta")),
  assert.equals(4, norm.extract_version("100.04rc4", "beta")),
  assert.null(norm.extract_version("100.04esr4", "beta")),
  assert.null(norm.extract_version("100.04.1", "beta")),
  assert.null(norm.extract_version("5.1.5-ubuntu-foobar", "beta"))
