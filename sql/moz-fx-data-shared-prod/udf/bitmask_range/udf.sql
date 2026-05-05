/*

Returns a bitmask that can be used to return a subset of an integer representing
a bit array. The start_ordinal argument is an integer specifying the starting
position of the slice, with start_ordinal = 1 indicating the first bit.
The length argument is the number of bits to include in the mask.

The arguments were chosen to match the semantics of the SUBSTR function; see
https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#substr

*/
CREATE OR REPLACE FUNCTION udf.bitmask_range(start_ordinal INT64, _length INT64) AS (
  (
    SELECT
      SUM(1 << (_n - 1))
    FROM
      UNNEST(GENERATE_ARRAY(start_ordinal, start_ordinal + _length - 1)) AS _n
  )
);

-- Tests
SELECT
  mozfun.assert.equals(1, udf.bitmask_range(1, 1)),
  mozfun.assert.equals(30, udf.bitmask_range(2, 4)),
  -- Taking just the second and third bits (from the right) of binary 11011 should give us 00010 (decimal 2)
  mozfun.assert.equals(2, ((1 << 4) + (1 << 3) + (1 << 1) + (1 << 0)) & udf.bitmask_range(2, 2))
