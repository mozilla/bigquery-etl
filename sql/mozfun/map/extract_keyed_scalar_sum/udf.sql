CREATE OR REPLACE FUNCTION extract_keyed_scalar_sum(
  keyed_scalar ARRAY<STRUCT<key STRING, value INT64>>
)
RETURNS INT64 AS (
  (SELECT SUM(value) FROM UNNEST(keyed_scalar))
);

SELECT
  assert.true(
     map.extract_keyed_scalar_sum([
        STRUCT(
          "a" AS key,
          1 AS value
        ),
        STRUCT(
          "b" AS key,
          2 AS value
        )
    ])
    = 3
   )
