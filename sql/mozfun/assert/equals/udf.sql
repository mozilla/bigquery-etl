CREATE OR REPLACE FUNCTION assert.equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    expected = actual
    OR expected IS NULL
    AND actual IS NULL,
    TRUE,
    ERROR(CONCAT('Expected ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual)))
  )
);
