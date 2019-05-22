CREATE TEMP FUNCTION
  udf_mod_uint128(dividend BYTES,
    divisor INT64) AS (
    -- Throw error if the divisor could result in overflow of INT64
    IF(
      -- max safe value of MOD(upper 8 bytes)
      SAFE.DIV(0x7FFFFFFFFFFFFFFF, MOD(0x100000000, divisor))
      -- max actual value
      < COALESCE(SAFE.ABS(divisor) - 1,
        ABS(divisor+1)),
      ERROR("error: divisor could result in overflow"),
      0 ) +
    -- Calculate MOD using a modified form of the solution here:
    -- https://stackoverflow.com/questions/10440326#answers
    MOD(
      -- Calculate MOD of the upper 8 bytes * 2^64
      MOD(
        -- Calculate MOD of the upper 8 bytes
        MOD(udf_decode_int64(SUBSTR(dividend, 1, 8)), divisor)
        -- Shift by MOD of 2^64
        * MOD(MOD(0x100000000, divisor) * MOD(0x100000000, divisor), divisor),
        -- MOD after shift to avoid overflow
        divisor)
      -- Add MOD of the lower 8 bytes
      + MOD(udf_decode_int64(SUBSTR(dividend, 9, 8)), divisor)
      -- Shift result to positive value
      + ABS(divisor) * 2,
      -- MOD after add to get final result
      divisor) );
