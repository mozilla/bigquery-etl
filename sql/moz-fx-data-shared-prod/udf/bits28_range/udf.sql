-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_range(bits INT64, start_offset INT64, n_bits INT64)
RETURNS INT64 AS (
  mozfun.bits28.range(bits, start_offset, n_bits)
);

-- Tests
SELECT
  mozfun.assert.equals(1 << 3, udf.bits28_range(1 << 10, -13, 7)),
  mozfun.assert.equals(0, udf.bits28_range(1 << 10, -6, 7)),
  mozfun.assert.equals(1, udf.bits28_range(1, 0, 1)),
  mozfun.assert.equals(0, udf.bits28_range(0, 0, 1));
