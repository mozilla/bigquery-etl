CREATE OR REPLACE FUNCTION assert.json_equals(expected ANY TYPE, actual ANY TYPE) AS (
  IF(
    TO_JSON_STRING(expected) = TO_JSON_STRING(actual),
    TRUE,
    ERROR(CONCAT('Expected ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual)))
  )
);
