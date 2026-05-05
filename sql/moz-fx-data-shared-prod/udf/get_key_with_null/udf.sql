-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.get_key_with_null(map ANY TYPE, k ANY TYPE) AS (
  mozfun.map.get_key_with_null(map, k)
);

-- Tests
SELECT
  mozfun.assert.equals(
    12,
    udf.get_key_with_null([STRUCT('foo' AS key, 42 AS value), ('bar', 12)], 'bar')
  ),
  mozfun.assert.equals(
    12,
    udf.get_key_with_null(
      [STRUCT('foo' AS key, 42 AS value), (CAST(NULL AS STRING), 12)],
      CAST(NULL AS STRING)
    )
  );
