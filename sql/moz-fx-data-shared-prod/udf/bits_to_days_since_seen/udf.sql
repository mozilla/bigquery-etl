/*
Given a BYTES, return the number of days since the client was
last seen.

If no bits are set, returns NULL, indicating we don't know.
Otherwise the results are 0-indexed, meaning \x01 will return 0.

Tests showed this being 100-300x faster than the simpler alternative:
CREATE OR REPLACE FUNCTION
  udf.bits_to_days_since_seen(b BYTES) AS ((
    SELECT MIN(n)
    FROM UNNEST(GENERATE_ARRAY(0, 364)) AS n
    WHERE BIT_COUNT(SUBSTR(b >> n, -1) & b'\x01') > 0));

See also: bits_to_days_since_first_seen.sql
*/
CREATE OR REPLACE FUNCTION udf.bits_to_days_since_seen(b BYTES) AS (
   -- Number of trailing zero bytes
  (8 * (BYTE_LENGTH(b) - BYTE_LENGTH(RTRIM(b, b'\x00'))))
  -- Plus the index of the last set bit in the last set byte
  + udf.pos_of_trailing_set_bit(TO_CODE_POINTS(b)[SAFE_ORDINAL(BYTE_LENGTH(RTRIM(b, b'\x00')))])
);

-- Tests
SELECT
  mozfun.assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x01')),
  mozfun.assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x00\x00\x01')),
  mozfun.assert.equals(8, udf.bits_to_days_since_seen(b'\x01\x00')),
  mozfun.assert.equals(NULL, udf.bits_to_days_since_seen(b'\x00\x00')),
  mozfun.assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03')),
  mozfun.assert.equals(
    76,
    udf.bits_to_days_since_seen(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x00')
  ),
  mozfun.assert.equals(
    1,
    udf.bits_to_days_since_seen(
      FROM_HEX(
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a"
      )
    )
  ),
  mozfun.assert.equals(
    9,
    udf.bits_to_days_since_seen(
      FROM_HEX(
        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a00"
      )
    )
  ),
  mozfun.assert.equals(
    17,
    udf.bits_to_days_since_seen(
      FROM_HEX(
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a0000"
      )
    )
  ),
  mozfun.assert.equals(
    233,
    udf.bits_to_days_since_seen(
      FROM_HEX(
        "000000000000000000000000000045100a0000000000000000000000000000000000000000000000000000000000"
      )
    )
  ),
