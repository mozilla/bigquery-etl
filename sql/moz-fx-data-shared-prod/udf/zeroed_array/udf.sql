/*
Generates an array if all zeroes, of arbitrary length
*/
CREATE OR REPLACE FUNCTION udf.zeroed_array(len INT64) AS (
  ARRAY(SELECT 0 FROM UNNEST(generate_array(1, len)))
);

-- Tests
SELECT
  assert.array_equals([0, 0], udf.zeroed_array(2)),
  assert.array_equals([0], udf.zeroed_array(1)),
  assert.array_equals([], udf.zeroed_array(0)),
  assert.array_equals([], udf.zeroed_array(-1));
