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
  udf_coalesce_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
    COALESCE(
        NULLIF(udf_shift_365_bits_one_day(prev), udf_zero_as_365_bits()),
        curr,
        udf_zero_as_365_bits()
    )); 


SELECT
  assert_equals(udf_one_as_365_bits() << 1, udf_coalesce_adjacent_days_365_bits(udf_one_as_365_bits(), udf_one_as_365_bits() << 10)),
  assert_equals(udf_one_as_365_bits() << 10, udf_coalesce_adjacent_days_365_bits(udf_one_as_365_bits() << 9, udf_one_as_365_bits())),
  assert_equals(udf_one_as_365_bits() << 9, udf_coalesce_adjacent_days_365_bits(udf_zero_as_365_bits(), udf_one_as_365_bits() << 9)),
  assert_equals(udf_one_as_365_bits() << 9, udf_coalesce_adjacent_days_365_bits(NULL, udf_one_as_365_bits() << 9)),
  assert_equals(udf_zero_as_365_bits(), udf_coalesce_adjacent_days_365_bits(NULL, NULL));
