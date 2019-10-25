/*

Shift input bits one day left and drop any bits beyond 365 days.

*/

CREATE TEMP FUNCTION
  udf_shift_bytes_one_day(x BYTES) AS (COALESCE(x << 1 & udf_bytesmask_365_days(), udf_zeroed_365_days_bytes()));

-- Tests

SELECT
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 1, udf_shift_bytes_one_day(udf_zeroed_364_days_active_1_bytes())),
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 8, udf_shift_bytes_one_day(udf_zeroed_364_days_active_1_bytes() << 7)),
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 364, udf_shift_bytes_one_day(udf_zeroed_364_days_active_1_bytes() << 363)),
  assert_equals(udf_zeroed_365_days_bytes(), udf_shift_bytes_one_day(udf_zeroed_364_days_active_1_bytes() << 364)),
  assert_equals(udf_zeroed_365_days_bytes(), udf_shift_bytes_one_day(NULL));
