CREATE OR REPLACE FUNCTION ga.nullify_string(s STRING)
RETURNS STRING AS (
  IF(s = "" OR s = "(not set)", NULL, s)
);

SELECT
  assert.equals(ga.nullify_string("abc"), "abc"),
  assert.equals(ga.nullify_string(""), CAST(NULL AS STRING)),
  assert.equals(ga.nullify_string("(not set)"), CAST(NULL AS STRING)),
  assert.equals(ga.nullify_string("(not-set)"), "(not-set)"),
