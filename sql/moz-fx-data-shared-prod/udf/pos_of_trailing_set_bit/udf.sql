/*

Identical to bits28_days_since_seen.

Returns a 0-based index of the rightmost set bit in the passed bit pattern
or null if no bits are set (bits = 0).

To determine this position, we take a bitwise AND of the bit pattern and
its complement, then we determine the position of the bit via base-2 logarithm;
see https://stackoverflow.com/a/42747608/1260237

*/
CREATE OR REPLACE FUNCTION udf.pos_of_trailing_set_bit(bits INT64) AS (
  CAST(SAFE.LOG(bits & -bits, 2) AS INT64)
);

-- Tests
SELECT
  mozfun.assert.null(udf.pos_of_trailing_set_bit(0)),
  mozfun.assert.equals(0, udf.pos_of_trailing_set_bit(1)),
  mozfun.assert.equals(3, udf.pos_of_trailing_set_bit(8)),
  mozfun.assert.equals(0, udf.pos_of_trailing_set_bit(8 + 1))
