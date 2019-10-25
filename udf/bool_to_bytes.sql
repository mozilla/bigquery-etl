/* Convert a boolean to byte array */

CREATE TEMP FUNCTION
  udf_bool_to_bytes(val BOOLEAN) AS (
    IF(COALESCE(val, FALSE), udf_zeroed_364_days_active_1_bytes(), udf_zeroed_365_days_bytes()));

-- Test

SELECT
  assert_equals(b'\x01', LTRIM(udf_bool_to_bytes(TRUE), b'\x00')),
  assert_equals(b'', LTRIM(udf_bool_to_bytes(FALSE), b'\x00')),
  assert_equals(b'', LTRIM(udf_bool_to_bytes(NULL), b'\x00'));
