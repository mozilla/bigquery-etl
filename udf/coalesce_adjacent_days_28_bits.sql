CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_28_bits(prev INT64,
    curr INT64) AS ( COALESCE( NULLIF(udf_shift_28_bits_one_day(prev),
        0),
      curr,
      0));

/*

We generally want to believe only the first reasonable profile creation
date that we receive from a client.
Given bits representing usage from the previous day and the current day,
this function shifts the first argument by one day and returns either that
value if non-zero and non-null, the current day value if non-zero and non-null,
or else 0.

*/

-- Test

SELECT
  assert_equals(2, udf_coalesce_adjacent_days_28_bits(1, 64)),
  assert_equals(128, udf_coalesce_adjacent_days_28_bits(64, 1)),
  assert_equals(64, udf_coalesce_adjacent_days_28_bits(0, 64)),
  assert_equals(64, udf_coalesce_adjacent_days_28_bits(NULL, 64)),
  assert_equals(0, udf_coalesce_adjacent_days_28_bits(NULL, NULL));
