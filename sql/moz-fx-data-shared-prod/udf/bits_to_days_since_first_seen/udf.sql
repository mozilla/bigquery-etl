/*
Given a BYTES, return the number of days since the
client was first seen.

If no bits are set, returns NULL, indicating we don't know.
Otherwise the result is 0-indexed, meaning that for \x01,
it will return 0.

Results showed this being between 5-10x faster than the simpler alternative:
CREATE OR REPLACE FUNCTION
  udf.bits_to_days_since_first_seen(b BYTES) AS ((
    SELECT MAX(n)
    FROM UNNEST(GENERATE_ARRAY(0, 8 * BYTE_LENGTH(b))) AS n
    WHERE BIT_COUNT(SUBSTR(b >> n, -1) & b'\x01') > 0));

See also: bits_to_days_since_seen.sql
*/
CREATE OR REPLACE FUNCTION udf.bits_to_days_since_first_seen(b BYTES) AS (
   -- Number of trailing bytes (from first set byte)
  (8 * (BYTE_LENGTH(LTRIM(b, b'\x00')) - 1))
  -- Plus the index of the first set bit in the first set byte
  + udf.pos_of_leading_set_bit(TO_CODE_POINTS(LTRIM(b, b'\x00'))[SAFE_ORDINAL(1)])
);

-- Tests
SELECT
  mozfun.assert.equals(0, udf.bits_to_days_since_first_seen(b'\x00\x01')),
  mozfun.assert.equals(0, udf.bits_to_days_since_first_seen(b'\x00\x00\x00\x01')),
  mozfun.assert.equals(8, udf.bits_to_days_since_first_seen(b'\x01\x00')),
  mozfun.assert.equals(NULL, udf.bits_to_days_since_first_seen(b'\x00\x00')),
  mozfun.assert.equals(
    1,
    udf.bits_to_days_since_first_seen(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03')
  ),
  mozfun.assert.equals(
    79,
    udf.bits_to_days_since_first_seen(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x00')
  ),
  mozfun.assert.equals(
    79,
    udf.bits_to_days_since_first_seen(b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
  ),
  mozfun.assert.equals(
    71,
    udf.bits_to_days_since_first_seen(b'\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF')
  ),
  mozfun.assert.equals(
    3,
    udf.bits_to_days_since_first_seen(
      FROM_HEX(
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a"
      )
    )
  ),
  mozfun.assert.equals(
    11,
    udf.bits_to_days_since_first_seen(
      FROM_HEX(
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a00"
      )
    )
  ),
  mozfun.assert.equals(
    19,
    udf.bits_to_days_since_first_seen(
      FROM_HEX(
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a0000"
      )
    )
  ),
  mozfun.assert.equals(
    254,
    udf.bits_to_days_since_first_seen(
      FROM_HEX(
        "000000000000000000000000000045100a0000000000000000000000000000000000000000000000000000000000"
      )
    )
  ),
