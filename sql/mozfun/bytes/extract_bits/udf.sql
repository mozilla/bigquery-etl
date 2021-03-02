CREATE OR REPLACE FUNCTION bytes.extract_bits(b BYTES, `begin` INT64, length INT64)
RETURNS BYTES AS (
  SUBSTR(
    mozfun.bytes.zero_right(
      b << MOD(IF(`begin` > 0, `begin` - 1, LENGTH(b) * 8 + `begin`), 8),
      (mozfun.bytes.bit_pos_to_byte_pos(length) * 8 - length)
    ),
    mozfun.bytes.bit_pos_to_byte_pos(`begin`),
    mozfun.bytes.bit_pos_to_byte_pos(length)
  )
);

-- Tests
SELECT
  bytes.extract_bits(b'\x01\xFE', 8, 8) = b'\xFF',
  bytes.extract_bits(b'\xFF', 5, 4) = b'\xF0',
  bytes.extract_bits(REPEAT(b'\xFF', 100), 50, 8) = b'\xFF',
  bytes.extract_bits(b'\x0F\xF0', -12, 8) = b'\xFF',
  bytes.extract_bits(b'\x0F\x77', 0, 8) = b'\x0F',
  bytes.extract_bits(b'\x0F\xF0', -10, 8) = b'\xFC',
  bytes.extract_bits(b'\x0F\xF0', 5, 8) = b'\xFF',
  bytes.extract_bits(b'\x0C\xC0', -12, 8) = b'\xCC',
  bytes.extract_bits(b'\xFF', -4, 1) = b'\x80'
