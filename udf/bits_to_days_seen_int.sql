/*
Given a BYTE, get the number of days the user was seen.

NULL input returns NULL output.
*/

CREATE TEMP FUNCTION
  udf_bits_to_days_seen_int(b BYTES) AS (
    BIT_COUNT(b));
