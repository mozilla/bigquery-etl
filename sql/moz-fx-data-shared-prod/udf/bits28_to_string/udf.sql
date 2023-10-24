-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.bits28_to_string(bits INT64) AS (
  mozfun.bits28.to_string(bits)
);

-- Tests
SELECT
  mozfun.assert.equals('0100000000000000000000000010', udf.bits28_to_string((1 << 1) | (1 << 26)))
