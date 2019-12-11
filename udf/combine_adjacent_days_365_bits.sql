CREATE TEMP FUNCTION
  udf_combine_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
    udf_shift_365_bits_one_day(prev) | COALESCE(curr, udf_zero_as_365_bits()));

SELECT
  assert_equals(udf_one_as_365_bits(), udf_combine_adjacent_days_365_bits(udf_zero_as_365_bits(), udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 364 | udf_one_as_365_bits(), udf_combine_adjacent_days_365_bits(udf_one_as_365_bits() << 363, udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits(), udf_combine_adjacent_days_365_bits(udf_one_as_365_bits() << 364, udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 1, udf_combine_adjacent_days_365_bits(udf_one_as_365_bits(), udf_zero_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 1, udf_combine_adjacent_days_365_bits(udf_one_as_365_bits(), NULL)),
  assert_equals(udf_one_as_365_bits(), udf_combine_adjacent_days_365_bits(NULL, udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 1 | udf_one_as_365_bits(), udf_combine_adjacent_days_365_bits(udf_one_as_365_bits(), udf_one_as_365_bits()));
