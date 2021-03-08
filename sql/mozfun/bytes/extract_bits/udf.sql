CREATE OR REPLACE FUNCTION bytes.extract_bits(b BYTES, `begin` INT64, length INT64)
RETURNS BYTES AS (
  SUBSTR(
    mozfun.bytes.zero_right(
      b << IF(`begin` >= 0, GREATEST(`begin` - 1, 0), LENGTH(b) * 8 + `begin`),
      GREATEST(8 * LENGTH(b) - length, 0)
    ),
    1,
    mozfun.bytes.bit_pos_to_byte_pos(length)
  )
);

-- Tests
SELECT
  assert.equals(b'\xFF', bytes.extract_bits(b'\x01\xFE', 8, 8)),
  assert.equals(b'\xF0', bytes.extract_bits(b'\xFF', 5, 4)),
  assert.equals(b'\xFF', bytes.extract_bits(REPEAT(b'\xFF', 100), 50, 8)),
  assert.equals(b'\xFF', bytes.extract_bits(b'\x0F\xF0', -12, 8)),
  assert.equals(b'\x0F', bytes.extract_bits(b'\x0F\x77', 0, 8)),
  assert.equals(b'\xFC', bytes.extract_bits(b'\x0F\xF0', -10, 8)),
  assert.equals(b'\xFF', bytes.extract_bits(b'\x0F\xF0', 5, 8)),
  assert.equals(b'\xCC', bytes.extract_bits(b'\x0C\xC0', -12, 8)),
  assert.equals(b'\x80', bytes.extract_bits(b'\xFF', -4, 1)),
  assert.equals(b'\xC0', bytes.extract_bits(b'\xFF\xFF', 2, 2)),
  assert.equals(b'\x80', bytes.extract_bits(b'\xFF\xFF', 6, 1)),
  assert.equals(b'\x80', bytes.extract_bits(b'\xFF\xFF', 1, 1)),
  assert.equals(b'\xFF', bytes.extract_bits(b'\xFF', 1, 20)),
