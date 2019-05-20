CREATE TEMP FUNCTION
  udf_bits_from_days_since_created_profile(days_since_created_profile INT64) AS (
  IF
    (days_since_created_profile BETWEEN 0
      AND 6,
      1 << days_since_created_profile,
      0));

/*

Takes in a difference between submission date and profile creation date
and returns a bit pattern representing the profile creation date IFF
the profile date is the same as the submission date or no more than
6 days earlier.

Analysis has shown that client-reported profile creation dates are much
less reliable outside of this range and cannot be used as reliable indicators
of new profile creation.

Example:

SELECT
  udf_bits_from_days_since_created_profile(0),
  udf_bits_from_days_since_created_profile(1),
  udf_bits_from_days_since_created_profile(6)
  udf_bits_from_days_since_created_profile(-1),
  udf_bits_from_days_since_created_profile(NULL),
  udf_bits_from_days_since_created_profile(7);
1, 2, 64, 0, 0, 0

*/
