CREATE OR REPLACE FUNCTION bytes.bit_pos_to_byte_pos(bit_pos INT64)
RETURNS INT64 AS (
  CAST(CEIL(ABS(bit_pos) / 8) AS INT64) * SIGN(bit_pos)
);

-- Tests
SELECT
  assert.equals(bytes.bit_pos_to_byte_pos(0), 0),
  assert.equals(bytes.bit_pos_to_byte_pos(1), 1),
  assert.equals(bytes.bit_pos_to_byte_pos(-1), -1),
  assert.equals(bytes.bit_pos_to_byte_pos(8), 1),
  assert.equals(bytes.bit_pos_to_byte_pos(-8), -1),
  assert.equals(bytes.bit_pos_to_byte_pos(9), 2),
  assert.equals(bytes.bit_pos_to_byte_pos(-9), -2),
