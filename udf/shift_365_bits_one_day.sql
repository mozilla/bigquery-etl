/*

Shift input bits one day left and drop any bits beyond 365 days.

*/

CREATE TEMP FUNCTION
  udf_shift_365_bits_one_day(x BYTES) AS (COALESCE(x << 1 & udf_bitmask_365(), udf_zero_as_365_bits()));

-- Tests

SELECT
  assert_equals(udf_one_as_365_bits() << 1, udf_shift_365_bits_one_day(udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 8, udf_shift_365_bits_one_day(udf_one_as_365_bits() << 7)),
  assert_equals(udf_one_as_365_bits() << 364, udf_shift_365_bits_one_day(udf_one_as_365_bits() << 363)),
  assert_equals(udf_zero_as_365_bits(), udf_shift_365_bits_one_day(udf_one_as_365_bits() << 364)),
  assert_equals(udf_zero_as_365_bits(), udf_shift_365_bits_one_day(NULL));
