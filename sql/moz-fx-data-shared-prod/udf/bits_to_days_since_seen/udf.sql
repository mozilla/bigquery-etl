/*
Given a BYTES, return the number of days since the client was
last seen.

If no bits are set, returns NULL, indicating we don't know.
Otherwise the results are 0-indexed, meaning \x01 will return 0.

Tests showed this being 5-10x faster than the simpler alternative:
CREATE OR REPLACE FUNCTION
  udf.bits_to_days_since_seen(b BYTES) AS ((
    SELECT MIN(n)
    FROM UNNEST(GENERATE_ARRAY(0, 364)) AS n
    WHERE BIT_COUNT(SUBSTR(b >> n, -1) & b'\x01') > 0));

See also: bits_to_days_since_first_seen.sql
*/
CREATE OR REPLACE FUNCTION udf.bits_to_days_since_seen(b BYTES) AS (
  (
    WITH trailing AS (
      -- Extract the first set byte with the trailing zeroes
      -- LTRIM forces NULL for bytes with no set bits
      SELECT
        REGEXP_EXTRACT(LTRIM(b, b'\x00'), CAST('(.\x00*$)' AS BYTES)) AS tail
    )
    SELECT
      -- Sum all trailing zeroes
      (8 * (BYTE_LENGTH(tail) - 1))
      -- Add the loc of the last set bit
      + udf.pos_of_trailing_set_bit(TO_CODE_POINTS(SUBSTR(tail, 1, 1))[OFFSET(0)])
    FROM
      trailing
  )
);

-- Tests
SELECT
  assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x01')),
  assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x00\x00\x01')),
  assert.equals(8, udf.bits_to_days_since_seen(b'\x01\x00')),
  assert.equals(NULL, udf.bits_to_days_since_seen(b'\x00\x00')),
  assert.equals(0, udf.bits_to_days_since_seen(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03')),
  assert.equals(76, udf.bits_to_days_since_seen(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x00'));
