CREATE OR REPLACE FUNCTION assert.null(actual ANY TYPE) AS (
  IF(actual IS NULL, TRUE, ERROR(CONCAT('Expected null, but got ', TO_JSON_STRING(actual))))
);
