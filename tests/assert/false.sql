CREATE TEMP FUNCTION assert_false(actual ANY TYPE) AS (
  IF(actual IS FALSE, TRUE, ERROR(CONCAT('Expected false, but got ', TO_JSON_STRING(actual))))
);
-- Tests
SELECT
  assert_false(FALSE);
#xfail
SELECT
  assert_false(CAST(NULL AS BOOL));
#xfail
SELECT
  assert_false(TRUE);
