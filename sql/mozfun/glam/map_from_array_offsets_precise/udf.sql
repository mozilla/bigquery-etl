-- udf_get_values
CREATE OR REPLACE FUNCTION glam.map_from_array_offsets_precise(
  required ARRAY<FLOAT64>,
  `values` ARRAY<FLOAT64>
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  (
    SELECT
      ARRAY_AGG(
        STRUCT<key STRING, value FLOAT64>(
          CAST(ROUND((key), 1) AS STRING),
          `values`[OFFSET(SAFE_CAST(key * 10 AS INT64))]
        )
        ORDER BY
          key
      )
    FROM
      UNNEST(required) AS key
  )
);

SELECT
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 2.0), ("0.2", 4.0)],
    glam.map_from_array_offsets_precise([0.0, 0.2], [2.0, 3.0, 4.0])
  ),
  -- required ordered
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 2.0), ("0.2", 4.0)],
    glam.map_from_array_offsets_precise([0.2, 0.0], [2.0, 3.0, 4.0])
  ),
  -- intended use-case, for approx_quantiles
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("25", 1.0), ("50", 2.0), ("75", 3.0), ("99.9", 4.0)],
    glam.map_from_array_offsets_precise(
      [25.0, 50.0, 75.0, 99.9],
      (
        SELECT
          array_agg(CAST(y * 1.0 AS FLOAT64) ORDER BY i)
        FROM
          UNNEST((SELECT approx_quantiles(x, 1000) FROM UNNEST([1, 2, 3, 4]) AS x)) AS y
          WITH OFFSET i
      )
    )
  )
