CREATE OR REPLACE FUNCTION map.extract_keyed_scalar_sum(
  keyed_scalar ARRAY<STRUCT<key STRING, value INT64>>
)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(keyed_scalar))
);

SELECT
  assert.equals(
    3,
    map.extract_keyed_scalar_sum([STRUCT("a" AS key, 1 AS value), STRUCT("b" AS key, 2 AS value)])
  ),
  -- Some NULL values and some non-NULL values -> ignore NULLs
  assert.equals(
    1,
    map.extract_keyed_scalar_sum(
      [STRUCT("a" AS key, 1 AS value), STRUCT("b" AS key, NULL AS value)]
    )
  ),
  -- All values NULL -> return NULL
  assert.null(map.extract_keyed_scalar_sum([STRUCT("a" AS key, NULL AS value)])),
  -- Empty array -> return NULL
  assert.null(map.extract_keyed_scalar_sum([])),
  -- NULL input -> return NULL
  assert.null(map.extract_keyed_scalar_sum(NULL))
