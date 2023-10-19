/*
This function returns "dividend mod divisor" where the dividend and the result
is encoded in bytes, and divisor is an integer.
*/
CREATE OR REPLACE FUNCTION udf.mod_uint128(dividend BYTES, divisor INT64) AS (
  -- Throw error if the divisor could result in overflow of INT64
  IF(
    -- max safe value of MOD(upper 8 bytes)
    SAFE.DIV(0x7FFFFFFFFFFFFFFF, MOD(0x100000000, divisor))
    -- max actual value
    < COALESCE(SAFE.ABS(divisor) - 1, ABS(divisor + 1)),
    ERROR("error: divisor could result in overflow"),
    0
  ) +
  -- Calculate MOD using a modified form of the solution here:
  -- https://stackoverflow.com/questions/10440326#answers
  MOD(
    -- Calculate MOD of the upper 8 bytes * 2^64
    MOD(
      -- Calculate MOD of the upper 8 bytes
      MOD(udf.decode_int64(SUBSTR(dividend, 1, 8)), divisor)
      -- Shift by MOD of 2^64
      * MOD(MOD(0x100000000, divisor) * MOD(0x100000000, divisor), divisor),
      -- MOD after shift to avoid overflow
      divisor
    )
    -- Add MOD of the lower 8 bytes
    + MOD(udf.decode_int64(SUBSTR(dividend, 9, 8)), divisor)
    -- Shift result to positive value
    + ABS(divisor) * 2,
    -- MOD after add to get final result
    divisor
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    0,
    udf.mod_uint128(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', 2)
  ),
  mozfun.assert.equals(
    1,
    udf.mod_uint128(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01', 2)
  ),
  mozfun.assert.equals(
    464,
    udf.mod_uint128(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x2c\x58\x1c\x36', 2342)
  ),
  mozfun.assert.equals(
    4065,
    udf.mod_uint128(b'\x00\x00\x00\x00\x00\x00\x2c\x2c\x2c\x2c\x2c\x2c\x2c\x58\x1c\x36', 10009)
  )
