CREATE OR REPLACE FUNCTION assert.array_empty(actual ANY TYPE) AS (
  IF(
    ARRAY_LENGTH(actual) = 0,
    TRUE,
    ERROR(CONCAT('Expected empty array', ' but got ', TO_JSON_STRING(actual)))
  )
);
