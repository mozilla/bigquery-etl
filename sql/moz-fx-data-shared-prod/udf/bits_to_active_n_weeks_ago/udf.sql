/*
Given a BYTE and an INT64, return whether the user
was active that many weeks ago.

NULL input returns NULL output.
*/
CREATE OR REPLACE FUNCTION udf.bits_to_active_n_weeks_ago(b BYTES, n INT64) AS (
  BIT_COUNT((b >> (n * 7)) & (CONCAT(REPEAT(b'\x00', BYTE_LENGTH(b) - 1), b'\x7F'))) > 0
);

-- Tests
SELECT
  assert.equals(TRUE, udf.bits_to_active_n_weeks_ago(b'\x00\x00\x00\x01', 0)),
  assert.equals(FALSE, udf.bits_to_active_n_weeks_ago(b'\x00\x00\x00\x01', 1)),
  assert.equals(TRUE, udf.bits_to_active_n_weeks_ago(b'\xF0\x00\x00\x01', 4)),
  assert.equals(TRUE, udf.bits_to_active_n_weeks_ago(b'\xF0\x00\x00\x70', 0)),
  assert.equals(CAST(NULL AS BOOL), udf.bits_to_active_n_weeks_ago(NULL, 1)),
  assert.equals(
    FALSE,
    udf.bits_to_active_n_weeks_ago(b'\x0F\x00\x00\x00\x00\x00\x00\x00\x00\x03', 5)
  ),
  assert.equals(
    TRUE,
    udf.bits_to_active_n_weeks_ago(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x03', 0)
  ),
  assert.equals(TRUE, udf.bits_to_active_n_weeks_ago(udf.one_as_365_bits() << 22, 3));
