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
  (
    WITH leading AS (
      -- Extract the leading 0 bytes and first set byte.
      -- Trimming forces NULL for bytes with no set bits.
      SELECT
        REGEXP_EXTRACT(RTRIM(b, b'\x00'), CAST('(^\x00*.)' AS BYTES)) AS head
    )
    SELECT
      -- The remaining bytes in b, after head, are all days after first seen
      (8 * (BYTE_LENGTH(b) - BYTE_LENGTH(head)))
      -- Add the loc of the first set bit in the final byte of tail, for additional days
      + udf.pos_of_leading_set_bit(TO_CODE_POINTS(SUBSTR(head, -1, 1))[OFFSET(0)])
    FROM
      leading
  )
);

-- Tests
SELECT
  assert.equals(0, udf.bits_to_days_since_first_seen(b'\x00\x01')),
  assert.equals(0, udf.bits_to_days_since_first_seen(b'\x00\x00\x00\x01')),
  assert.equals(8, udf.bits_to_days_since_first_seen(b'\x01\x00')),
  assert.equals(NULL, udf.bits_to_days_since_first_seen(b'\x00\x00')),
  assert.equals(1, udf.bits_to_days_since_first_seen(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03')),
  assert.equals(79, udf.bits_to_days_since_first_seen(b'\xF0\x00\x00\x00\x00\x00\x00\x00\x00\x00'));
