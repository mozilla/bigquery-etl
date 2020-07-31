-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_days_since_seen(bits INT64) AS (
  mozfun.bits28.days_since_seen(bits)
);

-- Tests
SELECT
  assert_null(udf.bits28_days_since_seen(0)),
  assert_equals(0, udf.bits28_days_since_seen(1)),
  assert_equals(3, udf.bits28_days_since_seen(8)),
  assert_equals(0, udf.bits28_days_since_seen(8 + 1))
