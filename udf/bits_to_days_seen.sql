/*
Given a BYTE, get the number of days the user was seen.

NULL input returns NULL output.
*/

CREATE OR REPLACE FUNCTION
  udf.bits_to_days_seen(b BYTES) AS (
    BIT_COUNT(b));
