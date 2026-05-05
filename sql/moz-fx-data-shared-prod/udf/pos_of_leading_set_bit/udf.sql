/*
Returns the 0-based index of the first set bit.

No set bits returns NULL.
*/
CREATE OR REPLACE FUNCTION udf.pos_of_leading_set_bit(i INT64) AS (
  NULLIF(CAST(CEIL(SAFE.LOG(i + 1, 2)) AS INT64) - 1, -1)
);

-- Tests
SELECT
  mozfun.assert.equals(udf.pos_of_leading_set_bit(0), NULL),
  mozfun.assert.equals(udf.pos_of_leading_set_bit(1), 0),
  mozfun.assert.equals(udf.pos_of_leading_set_bit(2), 1);
