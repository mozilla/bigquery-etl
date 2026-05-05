-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.get_key(map ANY TYPE, k ANY TYPE) AS (
  mozfun.map.get_key(map, k)
);

-- Tests
SELECT
  mozfun.assert.equals(12, udf.get_key([STRUCT('foo' AS key, 42 AS value), ('bar', 12)], 'bar'));
