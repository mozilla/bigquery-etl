/*
Coalesce previous data's PCD with the new data's PCD.

We generally want to believe only the first reasonable profile creation
date that we receive from a client.
Given bytes representing usage from the previous day and the current day,
this function shifts the first argument by one day and returns either that
value if non-zero and non-null, the current day value if non-zero and non-null,
or else 0.
*/



CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_bytes(prev BYTES, curr BYTES) AS (
    COALESCE(
        NULLIF(udf_shift_bytes_one_day(prev), udf_zeroed_365_days_bytes()),
        curr,
        udf_zeroed_365_days_bytes()
    )); 


SELECT
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 1, udf_coalesce_adjacent_days_bytes(udf_zeroed_364_days_active_1_bytes(), udf_zeroed_364_days_active_1_bytes() << 10)),
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 10, udf_coalesce_adjacent_days_bytes(udf_zeroed_364_days_active_1_bytes() << 9, udf_zeroed_364_days_active_1_bytes())),
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 9, udf_coalesce_adjacent_days_bytes(udf_zeroed_365_days_bytes(), udf_zeroed_364_days_active_1_bytes() << 9)),
  assert_equals(udf_zeroed_364_days_active_1_bytes() << 9, udf_coalesce_adjacent_days_bytes(NULL, udf_zeroed_364_days_active_1_bytes() << 9)),
  assert_equals(udf_zeroed_365_days_bytes(), udf_coalesce_adjacent_days_bytes(NULL, NULL));
