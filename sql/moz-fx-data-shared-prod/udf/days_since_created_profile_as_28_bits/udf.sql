/*

Takes in a difference between submission date and profile creation date
and returns a bit pattern representing the profile creation date IFF
the profile date is the same as the submission date or no more than
6 days earlier.

Analysis has shown that client-reported profile creation dates are much
less reliable outside of this range and cannot be used as reliable indicators
of new profile creation.

*/
CREATE OR REPLACE FUNCTION udf.days_since_created_profile_as_28_bits(
  days_since_created_profile INT64
) AS (
  IF(days_since_created_profile BETWEEN 0 AND 6, 1 << days_since_created_profile, 0)
);

-- Tests
SELECT
  assert.equals(2, udf.days_since_created_profile_as_28_bits(1)),
  assert.equals(4, udf.days_since_created_profile_as_28_bits(2)),
  assert.equals(64, udf.days_since_created_profile_as_28_bits(6)),
  assert.equals(0, udf.days_since_created_profile_as_28_bits(7)),
  assert.equals(0, udf.days_since_created_profile_as_28_bits(-1)),
  assert.equals(0, udf.days_since_created_profile_as_28_bits(NULL));
