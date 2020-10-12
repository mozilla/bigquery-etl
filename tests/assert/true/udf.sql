CREATE OR REPLACE FUNCTION assert.true(actual ANY TYPE) AS (
  IF(actual, TRUE, ERROR(CONCAT('Expected true, but got ', TO_JSON_STRING(actual))))
);
