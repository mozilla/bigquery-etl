CREATE TEMP FUNCTION
  assert_false(actual ANY TYPE) AS (
    IF(actual, ERROR(CONCAT(
      'Expected false, but got ',
      TO_JSON_STRING(actual))),
      TRUE));
