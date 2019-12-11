CREATE TEMP FUNCTION
  udf_bitmask_365() AS (
    CONCAT(
        b'\x1F',
        REPEAT(b'\xFF', 45)));
