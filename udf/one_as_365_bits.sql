CREATE TEMP FUNCTION
  udf_one_as_365_bits() AS (
    CONCAT(
        REPEAT(b'\x00', 45),
        b'\x01'));
