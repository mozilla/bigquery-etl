/*
An array of 11 zeroes, followed by a supplied value
*/
CREATE OR REPLACE FUNCTION udf.array_11_zeroes_then(val INT64) AS (
  ARRAY[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, val]
);

-- Test
SELECT
  mozfun.assert.array_equals([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114], udf.array_11_zeroes_then(114))
