/*

Takes in a difference between submission date and profile creation date
and returns a bit pattern representing the profile creation date IFF
the profile date is the same as the submission date or no more than
6 days earlier.

Analysis has shown that client-reported profile creation dates are much
less reliable outside of this range and cannot be used as reliable indicators
of new profile creation.

*/

CREATE TEMP FUNCTION
  udf_days_since_created_profile_as_28_bits(days_since_created_profile INT64) AS (
  IF
    (days_since_created_profile BETWEEN 0
      AND 6,
      1 << days_since_created_profile,
      0));

-- Tests

SELECT
  assert_equals(2, udf_days_since_created_profile_as_28_bits(1)),
  assert_equals(4, udf_days_since_created_profile_as_28_bits(2)),
  assert_equals(64, udf_days_since_created_profile_as_28_bits(6)),
  assert_equals(0, udf_days_since_created_profile_as_28_bits(7)),
  assert_equals(0, udf_days_since_created_profile_as_28_bits(-1)),
  assert_equals(0, udf_days_since_created_profile_as_28_bits(NULL));
