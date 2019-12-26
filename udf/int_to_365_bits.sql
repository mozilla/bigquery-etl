CREATE TEMP FUNCTION
  udf_int_to_365_bits(value INT64) AS (
   CONCAT(REPEAT(b'\x00', 37), FROM_HEX(udf_int_to_hex_string(value))));

-- Test

SELECT
  assert_equals(b'\x0F', ltrim(udf_int_to_365_bits(15), b'\x00')),
  assert_equals(udf_one_as_365_bits() << 63, udf_int_to_365_bits(1 << 63)),
  assert_equals(udf_one_as_365_bits() << 22, udf_int_to_365_bits(1 << 22)),
  assert_equals(b'\x01\x00', ltrim(udf_int_to_365_bits(1 << 8), b'\x00')),
  assert_equals(udf_one_as_365_bits() << 5, udf_int_to_365_bits(32)),
  assert_equals(CAST(NULL AS BYTES), udf_int_to_365_bits(NULL));
