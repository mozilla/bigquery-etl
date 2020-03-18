CREATE TEMP FUNCTION assert_approx_equals(
  expected ANY TYPE,
  actual ANY TYPE,
  tolerance FLOAT64
) AS (
  IF(
    ABS(expected - actual) <= tolerance,
    TRUE,
    ERROR(
      CONCAT(
        'Expected ',
        TO_JSON_STRING(expected),
        '+/-',
        TO_JSON_STRING(tolerance),
        ' but got ',
        TO_JSON_STRING(actual)
      )
    )
  )
);
