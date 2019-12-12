/* Convert a boolean to 365 bit byte array */

CREATE TEMP FUNCTION
  udf_bool_to_365_bits(val BOOLEAN) AS (
    IF(val, udf_one_as_365_bits(), udf_zero_as_365_bits()));

-- Test

SELECT
  assert_equals(b'\x01', LTRIM(udf_bool_to_365_bits(TRUE), b'\x00')),
  assert_equals(b'', LTRIM(udf_bool_to_365_bits(FALSE), b'\x00')),
  assert_equals(b'', LTRIM(udf_bool_to_365_bits(NULL), b'\x00'));
