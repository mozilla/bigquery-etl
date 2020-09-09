/*
One represented as a byte array of 365 bits
*/
CREATE OR REPLACE FUNCTION udf.one_as_365_bits() AS (
  CONCAT(REPEAT(b'\x00', 45), b'\x01')
);
