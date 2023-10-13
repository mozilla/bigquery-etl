CREATE OR REPLACE FUNCTION assert.error(name STRING, expected ANY TYPE, actual ANY TYPE)
RETURNS BOOLEAN AS (
  ERROR(
    CONCAT('Expected ', name, ' ', TO_JSON_STRING(expected), ' but got ', TO_JSON_STRING(actual))
  )
);
