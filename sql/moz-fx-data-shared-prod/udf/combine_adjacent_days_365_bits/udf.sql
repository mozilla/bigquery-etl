CREATE OR REPLACE FUNCTION udf.combine_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
  udf.shift_365_bits_one_day(prev) | COALESCE(curr, udf.zero_as_365_bits())
);

-- Tests
SELECT
  mozfun.assert.equals(
    udf.one_as_365_bits(),
    udf.combine_adjacent_days_365_bits(udf.zero_as_365_bits(), udf.one_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 364 | udf.one_as_365_bits(),
    udf.combine_adjacent_days_365_bits(udf.one_as_365_bits() << 363, udf.one_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits(),
    udf.combine_adjacent_days_365_bits(udf.one_as_365_bits() << 364, udf.one_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 1,
    udf.combine_adjacent_days_365_bits(udf.one_as_365_bits(), udf.zero_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 1,
    udf.combine_adjacent_days_365_bits(udf.one_as_365_bits(), NULL)
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits(),
    udf.combine_adjacent_days_365_bits(NULL, udf.one_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 1 | udf.one_as_365_bits(),
    udf.combine_adjacent_days_365_bits(udf.one_as_365_bits(), udf.one_as_365_bits())
  );
