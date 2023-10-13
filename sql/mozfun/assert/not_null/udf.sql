CREATE OR REPLACE FUNCTION assert.not_null(actual ANY TYPE) AS (
  IF(actual IS NOT NULL, TRUE, ERROR(CONCAT('Expected not null, but got ', TO_JSON_STRING(actual))))
);
