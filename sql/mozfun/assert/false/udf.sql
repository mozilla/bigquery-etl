CREATE OR REPLACE FUNCTION assert.false(actual ANY TYPE) AS (
  IF(actual IS FALSE, TRUE, ERROR(CONCAT('Expected false, but got ', TO_JSON_STRING(actual))))
);

-- Tests
SELECT
  assert.false(FALSE);

#xfail
SELECT
  assert.false(CAST(NULL AS BOOL));

#xfail
SELECT
  assert.false(TRUE);
