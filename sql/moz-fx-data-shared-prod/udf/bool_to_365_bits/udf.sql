/* Convert a boolean to 365 bit byte array */
CREATE OR REPLACE FUNCTION udf.bool_to_365_bits(val BOOLEAN) AS (
  IF(val, udf.one_as_365_bits(), udf.zero_as_365_bits())
);

-- Test
SELECT
  mozfun.assert.equals(b'\x01', LTRIM(udf.bool_to_365_bits(TRUE), b'\x00')),
  mozfun.assert.equals(b'', LTRIM(udf.bool_to_365_bits(FALSE), b'\x00')),
  mozfun.assert.equals(b'', LTRIM(udf.bool_to_365_bits(NULL), b'\x00'));
