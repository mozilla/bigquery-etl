CREATE OR REPLACE FUNCTION udf.int_to_365_bits(value INT64) AS (
  CONCAT(REPEAT(b'\x00', 37), FROM_HEX(udf.int_to_hex_string(value)))
);

-- Test
SELECT
  assert_equals(b'\x0F', ltrim(udf.int_to_365_bits(15), b'\x00')),
  assert_equals(udf.one_as_365_bits() << 63, udf.int_to_365_bits(1 << 63)),
  assert_equals(udf.one_as_365_bits() << 22, udf.int_to_365_bits(1 << 22)),
  assert_equals(b'\x01\x00', ltrim(udf.int_to_365_bits(1 << 8), b'\x00')),
  assert_equals(udf.one_as_365_bits() << 5, udf.int_to_365_bits(32)),
  assert_equals(CAST(NULL AS BYTES), udf.int_to_365_bits(NULL));
