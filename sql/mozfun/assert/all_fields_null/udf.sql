CREATE OR REPLACE FUNCTION assert.all_fields_null(actual ANY TYPE) AS (
  IF(
    TO_JSON_STRING((SELECT AS STRUCT [actual][SAFE_OFFSET(-1)].*)) = TO_JSON_STRING(actual),
    TRUE,
    ERROR(CONCAT('Expected all fields to be null but got ', TO_JSON_STRING(actual)))
  )
);
