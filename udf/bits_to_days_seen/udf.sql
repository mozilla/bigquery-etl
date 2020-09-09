/*
Given a BYTE, get the number of days the user was seen.

NULL input returns NULL output.
*/
CREATE OR REPLACE FUNCTION udf.bits_to_days_seen(b BYTES) AS (
  BIT_COUNT(b)
);

-- Tests
SELECT
  assert_equals(0, udf.bits_to_days_seen(b'\x00')),
  assert_equals(2, udf.bits_to_days_seen(b'\x03')),
  assert_equals(16, udf.bits_to_days_seen(b'\xff\xff'))
