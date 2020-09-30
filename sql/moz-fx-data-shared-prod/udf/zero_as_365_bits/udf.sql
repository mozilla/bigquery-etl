/*
Zero represented as a 365-bit byte array
*/
CREATE OR REPLACE FUNCTION udf.zero_as_365_bits() AS (
  REPEAT(b'\x00', 46)
);
