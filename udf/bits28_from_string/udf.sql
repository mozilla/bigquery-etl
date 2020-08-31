-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_from_string(s STRING) AS (
  mozfun.bits28.from_string(s)
);

-- Tests
SELECT
  assert_equals(1, udf.bits28_from_string('1')),
  assert_equals(1, udf.bits28_from_string('01')),
  assert_equals(1, udf.bits28_from_string('0000000000000000000000000001')),
  assert_equals(2, udf.bits28_from_string('10')),
  assert_equals(5, udf.bits28_from_string('101'));
