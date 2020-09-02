/*
A bitmask for 365 bits
*/
CREATE OR REPLACE FUNCTION udf.bitmask_365() AS (
  CONCAT(b'\x1F', REPEAT(b'\xFF', 45))
);
