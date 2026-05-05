/*
Coalesce previous data's PCD with the new data's PCD.

We generally want to believe only the first reasonable profile creation
date that we receive from a client.
Given bytes representing usage from the previous day and the current day,
this function shifts the first argument by one day and returns either that
value if non-zero and non-null, the current day value if non-zero and non-null,
or else 0.
*/
CREATE OR REPLACE FUNCTION udf.coalesce_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
  COALESCE(
    NULLIF(udf.shift_365_bits_one_day(prev), udf.zero_as_365_bits()),
    curr,
    udf.zero_as_365_bits()
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    udf.one_as_365_bits() << 1,
    udf.coalesce_adjacent_days_365_bits(udf.one_as_365_bits(), udf.one_as_365_bits() << 10)
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 10,
    udf.coalesce_adjacent_days_365_bits(udf.one_as_365_bits() << 9, udf.one_as_365_bits())
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 9,
    udf.coalesce_adjacent_days_365_bits(udf.zero_as_365_bits(), udf.one_as_365_bits() << 9)
  ),
  mozfun.assert.equals(
    udf.one_as_365_bits() << 9,
    udf.coalesce_adjacent_days_365_bits(NULL, udf.one_as_365_bits() << 9)
  ),
  mozfun.assert.equals(udf.zero_as_365_bits(), udf.coalesce_adjacent_days_365_bits(NULL, NULL));
