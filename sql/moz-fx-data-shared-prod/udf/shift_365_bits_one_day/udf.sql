/*

Shift input bits one day left and drop any bits beyond 365 days.

*/
CREATE OR REPLACE FUNCTION udf.shift_365_bits_one_day(x BYTES) AS (
  COALESCE(x << 1 & udf.bitmask_365(), udf.zero_as_365_bits())
);

-- Tests
SELECT
  assert.equals(udf.one_as_365_bits() << 1, udf.shift_365_bits_one_day(udf.one_as_365_bits())),
  assert.equals(udf.one_as_365_bits() << 8, udf.shift_365_bits_one_day(udf.one_as_365_bits() << 7)),
  assert.equals(
    udf.one_as_365_bits() << 364,
    udf.shift_365_bits_one_day(udf.one_as_365_bits() << 363)
  ),
  assert.equals(udf.zero_as_365_bits(), udf.shift_365_bits_one_day(udf.one_as_365_bits() << 364)),
  assert.equals(udf.zero_as_365_bits(), udf.shift_365_bits_one_day(NULL));
