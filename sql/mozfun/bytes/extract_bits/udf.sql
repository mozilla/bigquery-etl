CREATE OR REPLACE FUNCTION bytes.extract_bits(b BYTES, BEGIN
  INT64,
  length INT64
)
RETURNS BYTES AS (
  SUBSTR(mozfun.bytes.right_left_shift(b << MOD(IF(BEGIN
  >0,
BEGIN
  -1,
  LENGTH(b) * 8 +
BEGIN
),
8
),
(mozfun.bytes.bit_pos_to_byte_pos(length) * 8 - length)
),
mozfun.bytes.bit_pos_to_byte_pos(BEGIN
),
mozfun.bytes.bit_pos_to_byte_pos(length - 1)
)
);

-- Tests
SELECT
  extract_bits(b'\x01\xFE', 8, 8) = b'\xFF',
  extract_bits(b'\xFF', 5, 4) = b'\xF0',
  extract_bits(REPEAT(b'\xFF', 100), 50, 8) = b'\xFF',
  extract_bits(b'\x0F\xF0', -12, 8) = b'\xFF',
  extract_bits(b'\x0F\x77', 0, 8) = b'\x0F',
  extract_bits(b'\x0F\xF0', -10, 8) = b'\xFC',
