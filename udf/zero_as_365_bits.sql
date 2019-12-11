CREATE TEMP FUNCTION
  udf_zero_as_365_bits() AS (
    REPEAT(b'\x00', 46));
