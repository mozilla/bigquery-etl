CREATE OR REPLACE FUNCTION assert.approx_equals(
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
        TO_JSON_STRING(ROUND(expected, 2)),
        ' ± ',
        TO_JSON_STRING(tolerance),
        ' but got ',
        TO_JSON_STRING(ROUND(actual, 2))
      )
    )
  )
);

-- Tests
SELECT
  assert.approx_equals(1, 1, 0),
  assert.approx_equals(1, 2, 1);

#xfail
SELECT
  assert.approx_equals(1, 2, 0.9);
