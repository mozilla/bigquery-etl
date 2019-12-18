/*
Given a BYTE, return the number of days since the client was
last seen.

If no bits are set, returns NULL, indicating we don't know.
Otherwise the results are 0-indexed, meaning \x01 will return 0.

Tests showed this being 5-10x faster than the simpler alternative:
CREATE TEMP FUNCTION
  udf_bits_to_last_seen_int(b BYTES) AS ((
    SELECT MIN(n)
    FROM UNNEST(GENERATE_ARRAY(0, 364)) AS n
    WHERE BIT_COUNT(SUBSTR(b >> n, -1) & b'\x01') > 0));

See also: bits_to_last_seen_int.sql
*/

CREATE TEMP FUNCTION
  udf_bits_to_last_seen_int(b BYTES) AS ((
    WITH trailing AS (
      -- Extract the first set byte with the trailing zeroes
      -- LTRIM forces NULL for bytes with no set bits
      SELECT REGEXP_EXTRACT(LTRIM(b, b'\x00'), CAST('(.\x00*$)' AS BYTES)) AS tail
    )

    SELECT 
      -- Sum all trailing zeroes
      (8 * (BYTE_LENGTH(tail) - 1))
      -- Add the loc of the last set bit
          + udf_pos_of_trailing_set_bit(TO_CODE_POINTS(SUBSTR(tail, 1, 1))[OFFSET(0)])
    FROM trailing
  ));

-- Tests
SELECT
  assert_equals(0, udf_bits_to_last_seen_int(b'\x00\x01')),
  assert_equals(0, udf_bits_to_last_seen_int(b'\x00\x00\x00\x01')),
  assert_equals(8, udf_bits_to_last_seen_int(b'\x01\x00')),
  assert_equals(NULL, udf_bits_to_last_seen_int(b'\x00\x00')),
  assert_equals(0, udf_bits_to_last_seen_int(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03')),
  assert_equals(76, udf_bits_to_last_seen_int(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x00'));
